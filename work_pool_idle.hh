// WorkPool's worker parking: a futex Parker per worker, Tokio's idle bookkeeping, a helper's backoff.
/*
 * Copyright (c) 2026 Germán Méndez Bravo
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "work_pool_sync.hh"

// The idle protocol follows Tokio's runtime/scheduler/multi_thread/idle.rs (MIT).
namespace workpool_detail {

// One worker's sleep: park() returns once an unpark() comes after the epoch it observed.
struct alignas(64) Parker {
	std::atomic<std::uint32_t> seq{0};

	void park(std::uint32_t observed) noexcept {
		// acquire: pairs with unpark()'s release, so the waker's writes are visible on wake.
		while (seq.load(std::memory_order_acquire) == observed) {
			futex_wait(&seq, observed);
		}
	}

	void unpark() noexcept {
		seq.fetch_add(1, std::memory_order_release);
		futex_wake_one(&seq);
	}
};

// Tokio's idle bookkeeping: one word counts searching workers (low half) and unparked
// workers (high half); sleepers wait in a LIFO list under a mutex, so the warmest wakes
// first. Only parking and waking take the mutex; a post that wakes nobody reads one word.
class Idle {
	static constexpr std::uint64_t one_searching = 1;
	static constexpr std::uint64_t one_unparked = std::uint64_t(1) << 32;

	alignas(64) std::atomic<std::uint64_t> _state;
	std::mutex _mu;
	std::vector<unsigned> _sleepers;  // under _mu
	std::unique_ptr<Parker[]> _parkers;
	unsigned _n;

	static unsigned searching(std::uint64_t s) noexcept { return static_cast<unsigned>(s & 0xffffffffu); }
	static unsigned unparked(std::uint64_t s) noexcept { return static_cast<unsigned>(s >> 32); }

	// Under _mu: removes idx from the sleepers; false when it is not there.
	bool remove(unsigned idx) noexcept {
		for (std::size_t i = 0; i < _sleepers.size(); ++i) {
			if (_sleepers[i] == idx) {
				_sleepers[i] = _sleepers.back();
				_sleepers.pop_back();
				return true;
			}
		}
		return false;
	}

public:
	explicit Idle(unsigned n) :
		_state(static_cast<std::uint64_t>(n) << 32),
		_parkers(new Parker[n]),
		_n(n) {
		_sleepers.reserve(n);
	}

	// After a post: wakes one sleeper when nobody searches. The post must be a seq_cst RMW
	// the parker's final check reads, so this load and to_parked()'s RMW are in one total
	// order: either the post sees the parker or the parker sees the post.
	bool notify_one() noexcept {
		auto s = _state.load(std::memory_order_seq_cst);
		if (searching(s) != 0 || unparked(s) >= _n) {
			return false;
		}
		unsigned idx;
		{
			std::lock_guard<std::mutex> lk(_mu);
			s = _state.load(std::memory_order_seq_cst);
			if (searching(s) != 0 || _sleepers.empty()) {
				return false;
			}
			idx = _sleepers.back();
			_sleepers.pop_back();
			// The woken worker starts searching; counting it now keeps racing posts from waking more.
			_state.fetch_add(one_searching | one_unparked, std::memory_order_seq_cst);
		}
		_parkers[idx].unpark();
		return true;
	}

	// Wakes every sleeper (shutdown).
	void notify_all() noexcept {
		std::lock_guard<std::mutex> lk(_mu);
		for (auto idx : _sleepers) {
			_state.fetch_add(one_searching | one_unparked, std::memory_order_seq_cst);
			_parkers[idx].unpark();
		}
		_sleepers.clear();
	}

	// False when half the workers already search; the relaxed read makes the cap approximate.
	bool to_searching() noexcept {
		if (2 * searching(_state.load(std::memory_order_relaxed)) >= _n) {
			return false;
		}
		// seq_cst: posts that read this count skip their wake, so it must be in the same order.
		_state.fetch_add(one_searching, std::memory_order_seq_cst);
		return true;
	}

	// True when the caller was the last searcher: it must wake another (Tokio's ramp-up).
	bool from_searching() noexcept {
		return searching(_state.fetch_sub(one_searching, std::memory_order_seq_cst)) == 1;
	}

	// Registers idx as a sleeper and returns the epoch to park on. *final_check: the caller
	// was the last searcher and must look for work once more before parking.
	std::uint32_t to_parked(unsigned idx, bool was_searching, bool* final_check) noexcept {
		std::lock_guard<std::mutex> lk(_mu);
		auto observed = _parkers[idx].seq.load(std::memory_order_relaxed);
		_sleepers.push_back(idx);
		auto prev = _state.fetch_sub(one_unparked + (was_searching ? one_searching : 0), std::memory_order_seq_cst);
		*final_check = was_searching && searching(prev) == 1;
		return observed;
	}

	// Takes idx back out of the sleepers (it found work after registering); false when a
	// waker already took it, so its park() returns at once.
	bool unpark_self(unsigned idx) noexcept {
		std::lock_guard<std::mutex> lk(_mu);
		if (!remove(idx)) {
			return false;
		}
		_state.fetch_add(one_unparked, std::memory_order_seq_cst);
		return true;
	}

	void park(unsigned idx, std::uint32_t observed) noexcept { _parkers[idx].park(observed); }
};

// Bounded backoff for a helper with nothing to run: spin, then yield, then short sleeps.
inline void backoff(unsigned& round) noexcept {
	if (round < 16) {
		cpu_relax();
	} else if (round < 32) {
		std::this_thread::yield();
	} else {
		unsigned us = round - 32 < 10 ? 1u << (round - 32) : 1000u;
		std::this_thread::sleep_for(std::chrono::microseconds(us < 1000 ? us : 1000));
	}
	++round;
}

}  // namespace workpool_detail
