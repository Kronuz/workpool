// WorkPool's ready-list: a growable chain of Vyukov bounded MPMC rings of group pointers.
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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "work_pool_sync.hh"

// The ring is Dmitry Vyukov's bounded MPMC queue (1024cores.net, BSD-2-Clause).
namespace workpool_detail {

struct GroupState;

// The ready-list: Vyukov's bounded MPMC ring of group pointers, chained so it grows as
// groups open and a post never blocks: a larger ring becomes the tail, the old one drains
// first, and retired rings live until the pool ends. Each entry is one listed ticket, so
// reserve() keeps room for the live groups.
class ReadyRing {
	static constexpr std::size_t closed = std::size_t(1) << (sizeof(std::size_t) * 8 - 1);

	struct Cell {
		std::atomic<std::size_t> seq;
		GroupState* data;
		std::atomic<std::uint64_t> stamp;  // when the entry was pushed, in ticks; 0: not recorded
	};

	struct Ring {
		std::size_t mask;
		std::unique_ptr<Cell[]> cells;
		alignas(64) std::atomic<std::size_t> enq{0};  // the top bit closes the ring to pushes
		alignas(64) std::atomic<std::size_t> deq{0};
		alignas(64) std::atomic<Ring*> next{nullptr};

		explicit Ring(std::size_t cap) :
			mask(cap - 1),
			cells(new Cell[cap]) {
			for (std::size_t i = 0; i < cap; ++i) {
				cells[i].seq.store(i, std::memory_order_relaxed);
				cells[i].data = nullptr;
				cells[i].stamp.store(0, std::memory_order_relaxed);
			}
		}

		// False when closed (or, against the sizing, full): retry on the tail.
		bool push(GroupState* x, std::uint64_t stamp) noexcept {
			auto pos = enq.load(std::memory_order_relaxed);
			for (;;) {
				if (pos & closed) {
					return false;
				}
				Cell& c = cells[pos & mask];
				// acquire: the slot's last consumer is done with it.
				auto seq = c.seq.load(std::memory_order_acquire);
				auto diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos);
				if (diff == 0) {
					// seq_cst: the post's half of the Dekker pair with a parker's final check
					// (Idle::notify_one); on failure pos reloads, closed bit included.
					if (enq.compare_exchange_weak(pos, pos + 1, std::memory_order_seq_cst, std::memory_order_relaxed)) {
						c.data = x;
						c.stamp.store(stamp, std::memory_order_relaxed);
						// release: publishes data and stamp to the consumer that acquires seq.
						c.seq.store(pos + 1, std::memory_order_release);
						return true;
					}
				} else if (diff < 0) {
					return false;
				} else {
					pos = enq.load(std::memory_order_relaxed);
				}
			}
		}

		// The stamp of the entry next in line, or 0 when there is none (or it is not yet
		// published). Another consumer may take it meanwhile; the answer is then a stamp
		// of a recent head, which at worst delays or repeats an aging decision.
		std::uint64_t head_stamp() const noexcept {
			auto pos = deq.load(std::memory_order_relaxed);
			const Cell& c = cells[pos & mask];
			if (c.seq.load(std::memory_order_acquire) != pos + 1) {
				return 0;
			}
			return c.stamp.load(std::memory_order_relaxed);
		}

		GroupState* pop() noexcept {
			auto pos = deq.load(std::memory_order_relaxed);
			for (;;) {
				Cell& c = cells[pos & mask];
				// acquire: pairs with push()'s release of seq, so data is visible.
				auto seq = c.seq.load(std::memory_order_acquire);
				auto diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos + 1);
				if (diff == 0) {
					// relaxed: the cell's seq carries the ordering, not deq.
					if (deq.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
						GroupState* x = c.data;
						// release: hands the slot back to the producer a lap later.
						c.seq.store(pos + mask + 1, std::memory_order_release);
						return x;
					}
				} else if (diff < 0) {
					return nullptr;  // empty, or the next entry is still being published
				} else {
					pos = deq.load(std::memory_order_relaxed);
				}
			}
		}
	};

	alignas(64) std::atomic<Ring*> _head;   // consumers start here
	alignas(64) std::atomic<Ring*> _tail;   // producers push here
	std::mutex _grow;
	std::vector<std::unique_ptr<Ring>> _rings;  // under _grow

	static std::size_t pow2(std::size_t n) noexcept {
		std::size_t cap = 64;
		while (cap < n) {
			cap <<= 1;
		}
		return cap;
	}

public:
	ReadyRing() {
		_rings.push_back(std::make_unique<Ring>(pow2(0)));
		_head.store(_rings.back().get(), std::memory_order_relaxed);
		_tail.store(_rings.back().get(), std::memory_order_relaxed);
	}

	// stamp: the push time in ticks for head_stamp(), or 0.
	void push(GroupState* x, std::uint64_t stamp = 0) noexcept {
		for (;;) {
			// acquire: pairs with reserve()'s publication of a new tail.
			if (_tail.load(std::memory_order_acquire)->push(x, stamp)) {
				return;
			}
			cpu_relax();  // closed: reserve() already published its successor as the tail
		}
	}

	GroupState* pop() noexcept {
		Ring* r = _head.load(std::memory_order_acquire);
		while (r) {
			if (GroupState* x = r->pop()) {
				return x;
			}
			Ring* next = r->next.load(std::memory_order_acquire);
			if (!next) {
				return nullptr;
			}
			// A closed ring whose every claimed entry was taken is done: move the head past it.
			auto e = r->enq.load(std::memory_order_acquire);
			if ((e & closed) && r->deq.load(std::memory_order_acquire) >= (e & ~closed)) {
				_head.compare_exchange_strong(r, next, std::memory_order_acq_rel, std::memory_order_acquire);
			}
			r = next;  // the new ring takes posts while the old one drains
		}
		return nullptr;
	}

	// The stamp of the oldest entry still queued, or 0 when there is none.
	std::uint64_t head_stamp() const noexcept {
		for (Ring* r = _head.load(std::memory_order_acquire); r; r = r->next.load(std::memory_order_acquire)) {
			if (auto st = r->head_stamp()) {
				return st;
			}
		}
		return 0;
	}

	// The parker's final check. seq_cst: the parker's half of the Dekker pair with a post's
	// enq CAS (see Ring::push).
	bool empty() const noexcept {
		for (Ring* r = _head.load(std::memory_order_seq_cst); r; r = r->next.load(std::memory_order_seq_cst)) {
			if ((r->enq.load(std::memory_order_seq_cst) & ~closed) > r->deq.load(std::memory_order_seq_cst)) {
				return false;
			}
		}
		return true;
	}

	// Entries now; approximate.
	std::size_t size() const noexcept {
		std::size_t n = 0;
		for (Ring* r = _head.load(std::memory_order_acquire); r; r = r->next.load(std::memory_order_acquire)) {
			auto e = r->enq.load(std::memory_order_relaxed) & ~closed;
			auto d = r->deq.load(std::memory_order_relaxed);
			n += e > d ? e - d : 0;
		}
		return n;
	}

	// Grows the ring to hold at least `entries`. Called when a group opens, before it posts.
	void reserve(std::size_t entries) {
		std::lock_guard<std::mutex> lk(_grow);
		Ring* t = _tail.load(std::memory_order_relaxed);
		if (t->mask + 1 >= entries) {
			return;
		}
		_rings.push_back(std::make_unique<Ring>(pow2(2 * entries)));
		Ring* n = _rings.back().get();
		// seq_cst: a parker's empty() that sees the closed ring must also see its successor.
		t->next.store(n, std::memory_order_seq_cst);
		_tail.store(n, std::memory_order_seq_cst);
		t->enq.fetch_or(closed, std::memory_order_seq_cst);
	}
};

}  // namespace workpool_detail
