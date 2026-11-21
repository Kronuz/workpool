// WorkPool's platform layer: the futex, cpu_relax, the cycle counter and SpinLock.
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
#include <cstdint>
#include <thread>

#if defined(__linux__)
#  include <climits>             // for INT_MAX
#  include <ctime>               // for timespec
#  include <linux/futex.h>       // for FUTEX_WAIT_PRIVATE, FUTEX_WAKE_PRIVATE
#  include <sys/syscall.h>       // for SYS_futex
#  include <unistd.h>            // for syscall
#elif defined(__APPLE__)
#  include <Availability.h>      // for __MAC_OS_X_VERSION_MIN_REQUIRED
#  if defined(__MAC_OS_X_VERSION_MIN_REQUIRED) && __MAC_OS_X_VERSION_MIN_REQUIRED >= 140400
#    include <os/clock.h>        // for OS_CLOCK_MACH_ABSOLUTE_TIME
#    include <os/os_sync_wait_on_address.h>
#    define WORKPOOL_OS_SYNC 1
#  else
extern "C" int __ulock_wait(std::uint32_t operation, void* addr, std::uint64_t value, std::uint32_t timeout_us);
extern "C" int __ulock_wake(std::uint32_t operation, void* addr, std::uint64_t wake_value);
#  endif
#elif defined(__FreeBSD__)
#  include <climits>             // for INT_MAX
#  include <ctime>               // for CLOCK_MONOTONIC
#  include <sys/types.h>
#  include <sys/umtx.h>          // for _umtx_op, struct _umtx_time
#endif


namespace workpool_detail {

// The platform futex on a 32-bit word: Linux futex(2), macOS os_sync_wait_on_address (or
// __ulock before 14.4), FreeBSD _umtx_op, else C++20 atomic wait. Spurious returns allowed.
inline void futex_wait(std::atomic<std::uint32_t>* addr, std::uint32_t expected) noexcept {
#if defined(__linux__)
	syscall(SYS_futex, addr, FUTEX_WAIT_PRIVATE, expected, nullptr, nullptr, 0);
#elif defined(__APPLE__) && defined(WORKPOOL_OS_SYNC)
	os_sync_wait_on_address(addr, expected, sizeof(std::uint32_t), OS_SYNC_WAIT_ON_ADDRESS_NONE);
#elif defined(__APPLE__)
	__ulock_wait(1 /* UL_COMPARE_AND_WAIT */ | 0x01000000 /* ULF_NO_ERRNO */, addr, expected, 0);
#elif defined(__FreeBSD__)
	_umtx_op(addr, UMTX_OP_WAIT_UINT_PRIVATE, expected, nullptr, nullptr);
#else
	addr->wait(expected, std::memory_order_relaxed);
#endif
}

// As futex_wait, for at most ns nanoseconds.
inline void futex_wait_for(std::atomic<std::uint32_t>* addr, std::uint32_t expected, std::uint64_t ns) noexcept {
#if defined(__linux__)
	timespec ts{static_cast<time_t>(ns / 1'000'000'000u), static_cast<long>(ns % 1'000'000'000u)};
	syscall(SYS_futex, addr, FUTEX_WAIT_PRIVATE, expected, &ts, nullptr, 0);
#elif defined(__APPLE__) && defined(WORKPOOL_OS_SYNC)
	os_sync_wait_on_address_with_timeout(addr, expected, sizeof(std::uint32_t), OS_SYNC_WAIT_ON_ADDRESS_NONE, OS_CLOCK_MACH_ABSOLUTE_TIME, ns);
#elif defined(__APPLE__)
	auto us = ns / 1000 + 1;
	__ulock_wait(1 | 0x01000000, addr, expected, us < 0xffffffffu ? static_cast<std::uint32_t>(us) : 0xffffffffu);
#elif defined(__FreeBSD__)
	struct _umtx_time tm{};
	tm._timeout.tv_sec = static_cast<time_t>(ns / 1'000'000'000u);
	tm._timeout.tv_nsec = static_cast<long>(ns % 1'000'000'000u);
	tm._flags = 0;  // relative
	tm._clockid = CLOCK_MONOTONIC;
	_umtx_op(addr, UMTX_OP_WAIT_UINT_PRIVATE, expected, reinterpret_cast<void*>(sizeof tm), &tm);
#else
	if (addr->load(std::memory_order_relaxed) == expected) {
		std::this_thread::sleep_for(std::chrono::nanoseconds(ns < 1'000'000 ? ns : 1'000'000));
	}
#endif
}

inline void futex_wake_one(std::atomic<std::uint32_t>* addr) noexcept {
#if defined(__linux__)
	syscall(SYS_futex, addr, FUTEX_WAKE_PRIVATE, 1, nullptr, nullptr, 0);
#elif defined(__APPLE__) && defined(WORKPOOL_OS_SYNC)
	os_sync_wake_by_address_any(addr, sizeof(std::uint32_t), OS_SYNC_WAKE_BY_ADDRESS_NONE);
#elif defined(__APPLE__)
	__ulock_wake(1 | 0x01000000, addr, 0);
#elif defined(__FreeBSD__)
	_umtx_op(addr, UMTX_OP_WAKE_PRIVATE, 1, nullptr, nullptr);
#else
	addr->notify_one();
#endif
}

inline void futex_wake_all(std::atomic<std::uint32_t>* addr) noexcept {
#if defined(__linux__)
	syscall(SYS_futex, addr, FUTEX_WAKE_PRIVATE, INT_MAX, nullptr, nullptr, 0);
#elif defined(__APPLE__) && defined(WORKPOOL_OS_SYNC)
	os_sync_wake_by_address_all(addr, sizeof(std::uint32_t), OS_SYNC_WAKE_BY_ADDRESS_NONE);
#elif defined(__APPLE__)
	__ulock_wake(1 | 0x01000000 | 0x00000100 /* ULF_WAKE_ALL */, addr, 0);
#elif defined(__FreeBSD__)
	_umtx_op(addr, UMTX_OP_WAKE_PRIVATE, INT_MAX, nullptr, nullptr);
#else
	addr->notify_all();
#endif
}

inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
	__asm__ __volatile__("pause" ::: "memory");
#elif defined(__aarch64__)
	__asm__ __volatile__("isb" ::: "memory");
#else
	std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
}

// The cycle counter: a few nanoseconds per read where steady_clock can cost microseconds
// (31 µs under a VM's hpet clocksource). Only differences on one thread are used.
inline std::uint64_t ticks() noexcept {
#if defined(__x86_64__) || defined(__i386__)
	return __builtin_ia32_rdtsc();
#elif defined(__aarch64__)
	std::uint64_t v;
	__asm__ __volatile__("mrs %0, cntvct_el0" : "=r"(v));
	return v;
#else
	return static_cast<std::uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
#endif
}

// Ticks per nanosecond, measured once per process.
inline double ticks_per_ns() noexcept {
	static const double rate = [] {
#if defined(__aarch64__)
		std::uint64_t freq;
		__asm__ __volatile__("mrs %0, cntfrq_el0" : "=r"(freq));
		return double(freq) / 1e9;
#elif defined(__x86_64__) || defined(__i386__)
		// Both clocks run while the thread sleeps, so a preempted calibration stays right.
		auto t0 = std::chrono::steady_clock::now();
		auto c0 = ticks();
		std::this_thread::sleep_for(std::chrono::milliseconds(2));
		auto c1 = ticks();
		auto t1 = std::chrono::steady_clock::now();
		auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
		return ns > 0 && c1 > c0 ? double(c1 - c0) / double(ns) : 1.0;
#else
		using period = std::chrono::steady_clock::period;
		return double(period::den) / double(period::num) / 1e9;
#endif
	}();
	return rate;
}

// Test-and-test-and-set; yields after a while so a preempted holder can finish.
class SpinLock {
	std::atomic<bool> _locked{false};

public:
	void lock() noexcept {
		for (unsigned spins = 0;; ++spins) {
			// acquire: the critical section reads what the last holder wrote before unlock().
			if (!_locked.exchange(true, std::memory_order_acquire)) {
				return;
			}
			while (_locked.load(std::memory_order_relaxed)) {
				if (++spins < 64) {
					cpu_relax();
				} else {
					std::this_thread::yield();
				}
			}
		}
	}

	void unlock() noexcept {
		// release: publishes the critical section to the next lock().
		_locked.store(false, std::memory_order_release);
	}
};

}  // namespace workpool_detail
