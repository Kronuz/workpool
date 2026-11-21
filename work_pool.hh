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

// WorkPool: one pool for many owners (WorkGroups), served by deficit round robin, queries
// first, with caller-owned items and futex-parked workers. See ARCHITECTURE.md, "WorkPool".
// The idle protocol follows Tokio's runtime/scheduler/multi_thread/idle.rs (MIT); the ring
// is Dmitry Vyukov's bounded MPMC queue (1024cores.net, BSD-2-Clause).
#pragma once

#include <algorithm>             // for std::max
#include <atomic>                // for std::atomic
#include <chrono>                // for std::chrono
#include <cstddef>               // for std::size_t
#include <cstdio>                // for std::fprintf
#include <cstdlib>               // for std::abort
#include <cstdint>               // for std::uint32_t, std::uint64_t, std::int64_t
#include <format>                // for std::vformat, std::make_format_args
#include <memory>                // for std::unique_ptr
#include <mutex>                 // for std::mutex, std::lock_guard
#include <new>                   // for std::nothrow
#include <string>                // for std::string
#include <thread>                // for std::thread, std::this_thread
#include <type_traits>           // for std::decay_t, std::is_invocable_v
#include <utility>               // for std::forward, std::move
#include <vector>                // for std::vector

#include "thread.hh"             // for Thread, ThreadPolicyType

#ifdef WORKPOOL_TRACE_HEADER
#  include WORKPOOL_TRACE_HEADER
#else
#  include "workpool_trace.h"
#endif

#include "work_pool_idle.hh"
#include "work_pool_ready_ring.hh"
#include "work_pool_sync.hh"


class WorkPool;
class WorkGroup;


// What an item is for. Queries run first; flushes and merges never take the reserve.
enum class WorkClass : std::uint8_t { query = 0, flush = 1, merge = 2 };


// A unit of work, owned by the caller: the pool links it while queued and calls run once.
// It must stay alive until run starts; run may destroy or repost it.
struct WorkItem {
	void (*run)(WorkItem*) noexcept = nullptr;
	WorkGroup* group = nullptr;  // nullptr: the pool's own group
	WorkItem* next = nullptr;    // the pool's while queued; cancel()'s list after
	std::uint32_t epoch = 0;     // the pool's: the group's epoch when the item started
	WorkClass cls = WorkClass::flush;
	std::uint8_t flags = 0;      // the pool's

	// Whether the item's group was cancelled after the item started; call it from run.
	bool stop_requested() const noexcept;
};


// A one-shot idle notification, owned by the caller: WorkGroup::on_idle() links it, and fn
// runs once, on the thread that makes the group idle (nothing queued, nothing running), or on
// the calling thread when the group already is. fn runs inside the scheduler, so it only
// posts (resumes a coroutine, wakes a loop) and never blocks; it may free the node.
struct WorkIdle {
	void (*fn)(WorkIdle*) noexcept = nullptr;
	WorkIdle* next = nullptr;    // the pool's while linked
};


namespace workpool_detail {

inline constexpr std::uint8_t item_boxed = 1;

// A boxed callable (WorkGroup::enqueue): destroy() frees it without running it.
struct BoxedItem : WorkItem {
	void (*destroy)(BoxedItem*) noexcept = nullptr;
};

inline constexpr unsigned classes = 3;

inline unsigned class_of(const WorkItem& item) noexcept {
	auto c = static_cast<unsigned>(item.cls);
	return c < classes ? c : static_cast<unsigned>(WorkClass::flush);
}

// One class of one group: its queue and its place in line, all under the group's lock.
struct Lane {
	WorkItem* head = nullptr;
	WorkItem* tail = nullptr;
	std::uint32_t pending = 0;   // queued items
	std::uint32_t tickets = 0;   // turns on the ready ring or being served from it
	std::uint32_t listed = 0;    // of those, on the ring now
	std::uint32_t helpers = 0;   // waiting threads serving it outside the ring
	std::int64_t deficit = 0;    // service owed back, in ticks (DRR's counter; <= 0)

	void push(WorkItem* item) noexcept {
		item->next = nullptr;
		if (tail) {
			tail->next = item;
		} else {
			head = item;
		}
		tail = item;
		++pending;
	}

	WorkItem* pop() noexcept {
		WorkItem* item = head;
		if (item) {
			head = item->next;
			if (!head) {
				tail = nullptr;
			}
			--pending;
		}
		return item;
	}
};

// A group's state. WorkGroup owns it until its destructor; then the last ticket off the
// ring frees it, so a group closes without waiting for its turn to come around.
struct alignas(64) GroupState {
	SpinLock mu;
	Lane lanes[classes];         // under mu, by WorkClass
	std::uint32_t running = 0;   // items executing, under mu
	std::uint32_t waiters = 0;   // threads parked in WorkGroup::wait(), under mu
	WorkIdle* idle = nullptr;    // on_idle() notifications not yet fired, under mu
	unsigned width = 0;          // under mu; per class, 0: no cap
	unsigned weight = 1;         // quanta per turn, under mu
	bool owned = true;           // under mu; false once the WorkGroup is gone
	std::atomic<std::uint32_t> epoch{0};  // cancel() bumps it
	std::atomic<std::uint32_t> sleep{0};  // sleep_until()'s futex word; cancel() bumps it

	std::uint32_t pending() const noexcept {
		return lanes[0].pending + lanes[1].pending + lanes[2].pending;
	}

	bool unreferenced() const noexcept {
		for (const auto& l : lanes) {
			if (l.tickets || l.helpers) {
				return false;
			}
		}
		return true;
	}
};

// What runs on this thread. One thread_local, so a worker resolves it once per item
// (macOS reaches each thread_local through a call).
// One item running on this thread's stack; run_item() links it, innermost first.
struct Frame {
	GroupState* state;
	WorkItem* item;
	const Frame* parent;
};

struct Here {
	const WorkPool* pool = nullptr;   // the pool whose worker this is
	const Frame* top = nullptr;       // the innermost item running here

	// The innermost item of group s on this stack, or null.
	const Frame* find(const GroupState* s) const noexcept {
		for (const Frame* f = top; f; f = f->parent) {
			if (f->state == s) {
				return f;
			}
		}
		return nullptr;
	}
};

inline thread_local Here tls_here;

class WorkPoolWorker : public Thread<WorkPoolWorker, ThreadPolicyType::regular> {
	WorkPool* _pool;
	unsigned _idx;
	std::string _name;

public:
	WorkPoolWorker(WorkPool* pool, unsigned idx, std::string name) :
		_pool(pool),
		_idx(idx),
		_name(std::move(name)) {}

	void operator()();

	const std::string& name() const noexcept { return _name; }
};

}  // namespace workpool_detail


// A fixed set of named workers shared by many WorkGroups. Items run in class order,
// queries first; `reserve` workers stay free of flush and merge work so a query never
// waits for a long item to end, and merges hold at most `merge_cap` workers. Idle workers
// park on a futex each and use no CPU; post() never blocks or allocates.
class WorkPool {
	friend workpool_detail::WorkPoolWorker;
	friend WorkGroup;
	using GroupState = workpool_detail::GroupState;
	using Lane = workpool_detail::Lane;
	static constexpr unsigned query = static_cast<unsigned>(WorkClass::query);
	static constexpr unsigned flush = static_cast<unsigned>(WorkClass::flush);
	static constexpr unsigned merge = static_cast<unsigned>(WorkClass::merge);

public:
	struct Options {
		unsigned threads = 0;              // 0: the hardware's concurrency
		unsigned reserve = 1;              // workers kept free of flush and merge work
		unsigned merge_cap = 0;            // workers merges may hold; 0: threads - reserve
		std::chrono::nanoseconds quantum = std::chrono::microseconds(100);  // service per turn
		const char* name_format = "W{:02}";  // a std::format string over the worker index
		// A merge ticket that has waited this long is picked ahead of flush tickets (0: never).
		std::chrono::nanoseconds merge_age = std::chrono::milliseconds(100);
		// Called with a site name when a thread that is not a worker enters wait(), before
		// it blocks: a host aborts or logs there when the thread is one that must never
		// wait (an event loop; it joins a group with on_idle()). Null: no check.
		void (*block_guard)(const char* site) noexcept = nullptr;
	};

private:
	unsigned _n;
	unsigned _bg_cap;                  // workers flush and merge turns may hold
	unsigned _merge_cap;               // workers merge turns may hold
	std::int64_t _quantum;             // in ticks
	std::int64_t _merge_age;           // in ticks; 0: merges do not age
	void (*_block_guard)(const char*) noexcept;
	workpool_detail::Idle _idle;
	workpool_detail::ReadyRing _ready[workpool_detail::classes];
	std::atomic<bool> _stop{false};
	alignas(64) std::atomic<unsigned> _busy_bg{0};     // workers in flush or merge turns
	alignas(64) std::atomic<unsigned> _busy_merge{0};  // workers in merge turns
	alignas(64) std::atomic<std::uint64_t> _aged{0};   // merge turns picked ahead of flush for their age
	alignas(64) std::atomic<std::size_t> _states{1};   // group states alive, the pool's own included
	alignas(64) std::atomic<std::uint32_t> _group_wake{0};  // WorkGroup::wait()'s futex word
	GroupState _own;                   // items posted without a group

	std::vector<std::unique_ptr<workpool_detail::WorkPoolWorker>> _workers;

	static unsigned resolve(unsigned threads) noexcept {
		if (threads) {
			return threads;
		}
		auto hw = std::thread::hardware_concurrency();
		return hw ? hw : 1;
	}

	GroupState& state_of(const WorkItem& item) noexcept;

	void reserve_rings(std::size_t states) {
		for (auto& ring : _ready) {
			ring.reserve(states + 1);
		}
	}

	unsigned cap(const GroupState& s) const noexcept {
		return s.width && s.width < _n ? s.width : _n;
	}

	// Under s.mu: lists one more ticket of lane c when it has queued items, none listed, and
	// room under its width. At most one ticket of a lane is listed, so a ring holds at most
	// one entry per lane; the worker that pops it lists the next (turn()) while items remain,
	// so a lane widens to its width as workers come free, with no count of idle workers.
	bool grant(GroupState& s, unsigned c) noexcept {
		Lane& l = s.lanes[c];
		if (l.pending && l.listed == 0 && l.tickets + l.helpers < cap(s)) {
			++l.tickets;
			++l.listed;
			return true;
		}
		return false;
	}

	// Under s.mu: gives back one ticket of lane c.
	static void drop(GroupState& s, unsigned c) noexcept {
		--s.lanes[c].tickets;
	}

	// Under s.mu: whether the group has nothing queued and nothing running.
	static bool idle(const GroupState& s) noexcept {
		return s.pending() == 0 && s.running == 0;
	}

	// Under s.mu, after a turn ended or a cancel: whether a parked wait() of s has something
	// to do, because the group is idle or a helper may serve a lane now. Bumps the word the
	// waiters sleep on; the caller wakes them after unlocking.
	bool waiters_due(GroupState& s) noexcept {
		if (s.waiters && (idle(s) || helpable(s) >= 0)) {
			// release: under s.mu anyway; the futex compares the word.
			_group_wake.fetch_add(1, std::memory_order_release);
			return true;
		}
		return false;
	}

	// Under s.mu: detaches the idle notifications of a group that is idle, to run unlocked.
	static WorkIdle* take_idle(GroupState& s) noexcept {
		WorkIdle* list = nullptr;
		if (s.idle && idle(s)) {
			list = s.idle;
			s.idle = nullptr;
		}
		return list;
	}

	static void fire_idle(WorkIdle* list) noexcept {
		while (list) {
			WorkIdle* next = list->next;  // fn may free the node
			list->fn(list);
			list = next;
		}
	}

	// After unlocking s: what its critical section decided. The common case is inline.
	void release(GroupState* s, unsigned c, bool push, bool wake, bool free) noexcept {
		if (push | wake | free) {
			release_slow(s, c, push, wake, free);
		}
	}

	[[gnu::noinline]] void release_slow(GroupState* s, unsigned c, bool push, bool wake, bool free) noexcept {
		// A merge entry is stamped for aging; the clock is read only for merges.
		auto stamp = c == merge ? std::max<std::uint64_t>(workpool_detail::ticks(), 1) : 0;
		if (push) {
			_ready[c].push(s, stamp);
		}
		if (push) {
			_idle.notify_one();
		}
		if (wake) {
			workpool_detail::futex_wake_all(&_group_wake);
		}
		if (free) {
			delete s;
			_states.fetch_sub(1, std::memory_order_relaxed);
		}
	}

	// `here` is this thread's tls_here, resolved once by the caller (per turn, not per item).
	static void run_item(workpool_detail::Here& here, GroupState& s, WorkItem* item) noexcept {
		workpool_detail::Frame frame{&s, item, here.top};
		here.top = &frame;
		item->run(item);
		here.top = frame.parent;
	}

	// Under s.mu with a ticket of lane c held (a ring ticket, or a helper's): one turn, a
	// quantum of service plus the lane's debt, then relist, keep, or drop the ticket.
	// Returns with s.mu released.
	void turn(GroupState& s, unsigned c, bool helper) noexcept {
		Lane& l = s.lanes[c];
		std::int64_t budget = l.deficit + _quantum * s.weight;
		l.deficit = 0;
		auto& here = workpool_detail::tls_here;
		auto t = workpool_detail::ticks();
		bool first = true;
		while (budget > 0) {
			WorkItem* item = l.pop();
			if (!item) {
				break;
			}
			++s.running;
			// relaxed: stop_requested() is advisory, read relaxed like TaskGroupStopToken.
			item->epoch = s.epoch.load(std::memory_order_relaxed);
			// Items remain and no ticket is listed: list one before running, so a worker that
			// idles now serves the lane beside this one, however long this item runs.
			bool relay = first && grant(s, c);
			first = false;
			s.mu.unlock();
			if (relay) {
				release(&s, c, true, false, false);
			}
			run_item(here, s, item);
			auto now = workpool_detail::ticks();
			budget -= static_cast<std::int64_t>(now - t);
			t = now;
			s.mu.lock();
			--s.running;
		}
		bool push = false;
		if (helper) {
			--l.helpers;
		}
		if (l.pending == 0) {
			l.deficit = 0;  // DRR: an idle lane keeps neither credit nor debt
			if (!helper) {
				drop(s, c);
			}
		} else {
			if (budget < 0) {
				l.deficit += budget;
			}
			if (helper) {
				push = grant(s, c);  // a helper's ticket never joins the ring
			} else if (l.listed == 0) {
				++l.listed;
				push = true;
			} else {
				drop(s, c);  // the lane is listed already
			}
		}
		bool wake = waiters_due(s);
		WorkIdle* notify = take_idle(s);
		bool free = !s.owned && s.unreferenced();
		s.mu.unlock();
		release(&s, c, push, wake, free);
		if (notify) [[unlikely]] {
			fire_idle(notify);
		}
	}

	// A ring entry of class c: one listed ticket of s.
	void serve(GroupState& s, unsigned c) noexcept {
		s.mu.lock();
		Lane& l = s.lanes[c];
		--l.listed;
		// A waiting helper was admitted while this ticket sat on the ring; the width holds
		// by giving the ticket up (the helper requeues one when its turn ends).
		if (l.helpers && s.width && l.tickets - l.listed + l.helpers > s.width) {
			drop(s, c);
			bool free = !s.owned && s.unreferenced();
			s.mu.unlock();
			release(&s, c, false, false, free);
			return;
		}
		turn(s, c, false);
	}

	// Under s.mu: the lane a waiting worker may serve as a helper, or -1: one with queued
	// items while fewer than `width` turns of it are being served (tickets still on the ring
	// do not count).
	static int helpable(const GroupState& s) noexcept {
		for (unsigned c = 0; c < workpool_detail::classes; ++c) {
			const auto& l = s.lanes[c];
			if (l.pending && (!s.width || l.tickets - l.listed + l.helpers < s.width)) {
				return static_cast<int>(c);
			}
		}
		return -1;
	}

	// The background slots a worker holds, kept across consecutive flush and merge turns
	// so a stream of them costs no RMW on the shared counts.
	struct Slots {
		bool bg = false;
		bool merge = false;
	};

	void release_merge(Slots& held) noexcept {
		if (held.merge) {
			held.merge = false;
			_busy_merge.fetch_sub(1, std::memory_order_relaxed);
		}
	}

	void release_slots(Slots& held) noexcept {
		release_merge(held);
		if (held.bg) {
			held.bg = false;
			_busy_bg.fetch_sub(1, std::memory_order_relaxed);
		}
	}

	// Whether the oldest merge entry has waited longer than merge_age.
	bool merge_overdue() const noexcept {
		if (!_merge_age) {
			return false;
		}
		auto stamp = _ready[merge].head_stamp();
		return stamp && static_cast<std::int64_t>(workpool_detail::ticks() - stamp) > _merge_age;
	}

	// The next merge entry if a merge slot (at most _merge_cap) is free, else null.
	GroupState* pick_merge(unsigned& c, Slots& held) noexcept {
		if (_ready[merge].size() == 0) {
			return nullptr;
		}
		if (!held.merge) {
			if (_busy_merge.fetch_add(1, std::memory_order_relaxed) < _merge_cap) {
				held.merge = true;
			} else {
				_busy_merge.fetch_sub(1, std::memory_order_relaxed);
			}
		}
		if (held.merge) {
			if (GroupState* s = _ready[merge].pop()) {
				c = merge;
				return s;
			}
		}
		return nullptr;
	}

	// The next ring entry in class order, with one exception: a merge entry older than
	// merge_age goes before flush, so a flood of flushes cannot starve merges. Flush and
	// merge turns need a background slot (at most _bg_cap held, so the reserve stays free
	// for queries) and merges a merge slot (at most _merge_cap). Slots are taken by RMW
	// before the pop, so the caps are exact; a query or an empty pick gives them back.
	GroupState* pick(unsigned& c, Slots& held) noexcept {
		if (GroupState* s = _ready[query].pop()) {
			release_slots(held);
			c = query;
			return s;
		}
		if (!held.bg) {
			if (_ready[flush].size() == 0 && _ready[merge].size() == 0) {
				return nullptr;
			}
			// relaxed: a count for the cap; the turn is ordered by the ring and the group lock.
			if (_busy_bg.fetch_add(1, std::memory_order_relaxed) >= _bg_cap) {
				_busy_bg.fetch_sub(1, std::memory_order_relaxed);
				return nullptr;
			}
			held.bg = true;
		}
		if (merge_overdue()) {
			if (GroupState* s = pick_merge(c, held)) {
				_aged.fetch_add(1, std::memory_order_relaxed);
				return s;
			}
		}
		if (GroupState* s = _ready[flush].pop()) {
			release_merge(held);
			c = flush;
			return s;
		}
		if (GroupState* s = pick_merge(c, held)) {
			return s;
		}
		release_slots(held);
		return nullptr;
	}

	// The parker's final check: work this worker could take now. A slot held by another
	// worker's pick() at this moment is released by a worker that then looks again.
	bool ready_for_me() const noexcept {
		if (!_ready[query].empty()) {
			return true;
		}
		if (_busy_bg.load(std::memory_order_seq_cst) >= _bg_cap) {
			return false;
		}
		if (!_ready[flush].empty()) {
			return true;
		}
		return _busy_merge.load(std::memory_order_seq_cst) < _merge_cap && !_ready[merge].empty();
	}

	// Parks worker idx after it found nothing; false once the pool is stopping.
	bool park(unsigned idx, bool& searching) noexcept {
		if (_stop.load(std::memory_order_acquire)) {
			return false;
		}
		bool final_check = false;
		auto observed = _idle.to_parked(idx, searching, &final_check);
		searching = false;
		// acquire: to_parked() took the mutex after the destructor's notify_all() did, or
		// before it and is woken by it; either way a stop is not slept through.
		bool stopping = _stop.load(std::memory_order_acquire);
		if (stopping || (final_check && ready_for_me())) {
			if (_idle.unpark_self(idx)) {
				if (stopping) {
					return false;
				}
				searching = _idle.to_searching();
				return true;
			}
		}
		_idle.park(idx, observed);
		searching = true;  // a woken worker is counted as searching
		return true;
	}

	void work(unsigned idx) noexcept {
		workpool_detail::tls_here.pool = this;
		bool searching = false;
		Slots held;
		for (;;) {
			unsigned c = flush;
			GroupState* s = pick(c, held);
			if (!s && !searching) {
				searching = _idle.to_searching();
				if (searching) {
					s = pick(c, held);
				}
			}
			if (s) {
				if (searching) {
					searching = false;
					if (_idle.from_searching()) {
						_idle.notify_one();
					}
				}
				serve(*s, c);
				continue;
			}
			if (!park(idx, searching)) {
				break;
			}
		}
		workpool_detail::tls_here.pool = nullptr;
	}

	void stop_and_join() noexcept {
		_stop.store(true, std::memory_order_release);
		_idle.notify_all();
		for (auto& worker : _workers) {
			try {
				worker->join(std::chrono::steady_clock::time_point::max());
			} catch (...) {
				L_EXC("Unhandled exception in a WorkPool worker");
			}
		}
	}

public:
	WorkPool() :
		WorkPool(Options{}) {}

	explicit WorkPool(Options options) :
		_n(resolve(options.threads)),
		_bg_cap(_n - (options.reserve < _n ? options.reserve : _n - 1)),
		_merge_cap(options.merge_cap && options.merge_cap < _bg_cap ? options.merge_cap : _bg_cap),
		_quantum(static_cast<std::int64_t>(double(options.quantum.count()) * workpool_detail::ticks_per_ns())),
		_merge_age(options.merge_age.count() > 0 ? std::max<std::int64_t>(1, static_cast<std::int64_t>(double(options.merge_age.count()) * workpool_detail::ticks_per_ns())) : 0),
		_block_guard(options.block_guard),
		_idle(_n) {
		if (_quantum < 1) {
			_quantum = 1;
		}
		reserve_rings(1);
		_workers.reserve(_n);
		try {
			for (unsigned idx = 0; idx < _n; ++idx) {
				_workers.push_back(std::make_unique<workpool_detail::WorkPoolWorker>(this, idx, std::vformat(options.name_format, std::make_format_args(idx))));
				_workers.back()->run();
			}
		} catch (...) {
			stop_and_join();
			throw;
		}
	}

	WorkPool(const WorkPool&) = delete;
	WorkPool& operator=(const WorkPool&) = delete;

	// Runs what is queued, then stops and joins the workers. Every WorkGroup must be gone,
	// and only the pool's own items may post once the destructor starts.
	~WorkPool() noexcept {
		stop_and_join();
	}

	unsigned threads() const noexcept { return _n; }

	bool on_worker() const noexcept { return workpool_detail::tls_here.pool == this; }

	// Merge entries waiting for a worker now (approximate), for a backlog report.
	std::size_t merge_waiting() const noexcept { return _ready[merge].size(); }

	// How long the oldest waiting merge entry has waited; zero when none does.
	std::chrono::nanoseconds merge_oldest() const noexcept {
		auto stamp = _ready[merge].head_stamp();
		if (!stamp) {
			return {};
		}
		auto waited = static_cast<std::int64_t>(workpool_detail::ticks() - stamp);
		return std::chrono::nanoseconds(waited > 0 ? static_cast<std::int64_t>(double(waited) / workpool_detail::ticks_per_ns()) : 0);
	}

	// Merge turns taken ahead of flush because they were older than merge_age.
	std::uint64_t merges_aged() const noexcept { return _aged.load(std::memory_order_relaxed); }

	// Queues item on item.group (the pool's own group when null) in item.cls, from any
	// thread; never blocks, never allocates.
	void post(WorkItem& item) noexcept {
		GroupState& s = state_of(item);
		unsigned c = workpool_detail::class_of(item);
		Lane& l = s.lanes[c];
		s.mu.lock();
		l.push(&item);
		bool push = grant(s, c);
		bool wake = s.waiters != 0;
		if (wake) {
			_group_wake.fetch_add(1, std::memory_order_release);  // a waiting owner may help
		}
		s.mu.unlock();
		release(&s, c, push, wake, false);
	}

	// Sleeps until deadline, returning true, or until group is cancelled, returning false
	// (cancelled after the calling item started, when called from one of its items). One
	// timed futex wait for the whole interval; the cancel wakes it.
	bool sleep_until(std::chrono::steady_clock::time_point deadline, WorkGroup& group) noexcept;
};


// One owner's FIFOs on a shared WorkPool, one per WorkClass: one group per index. Groups
// take turns of `quantum * weight` service per class; `width` caps how many of its items
// of one class run at once (0: no cap). The pool must outlive the group.
class WorkGroup {
	friend WorkPool;
	friend WorkItem;
	using GroupState = workpool_detail::GroupState;

	WorkPool* _pool;
	GroupState* _s;

	template <typename F>
	struct Box final : workpool_detail::BoxedItem {
		F fn;

		explicit Box(F&& f) :
			fn(std::move(f)) {}
		explicit Box(const F& f) :
			fn(f) {}

		static void invoke(WorkItem* w) noexcept {
			auto* box = static_cast<Box*>(w);
			try {
				if constexpr (std::is_invocable_v<F&, const WorkItem&>) {
					box->fn(static_cast<const WorkItem&>(*box));
				} else {
					box->fn();
				}
			} catch (...) {
				L_EXC("ERROR: Task died with an unhandled exception");
			}
			delete box;
		}

		static void drop(BoxedItem* b) noexcept { delete static_cast<Box*>(b); }
	};

	// A wait on a group with an item on this thread's stack can only wait for itself.
	void check_stack(const char* site) const noexcept {
		if (workpool_detail::tls_here.find(_s)) {
			std::fprintf(stderr, "workpool: %s on a group that has an item on this thread's stack (a wait inside its own group, or a cycle of waits); post the work to a child group and wait on that\n", site);
			std::abort();
		}
	}

	// The one place cancelled items are dealt with (cancel(), cancel_and_wait() and the
	// destructor): unlinks the queued items, frees the boxed ones unrun and returns the
	// caller-owned ones through `next`. `dropped` counts both.
	WorkItem* cancel_items(std::size_t& dropped) noexcept {
		WorkItem* list = nullptr;
		WorkItem** tail = &list;
		_s->mu.lock();
		for (auto& l : _s->lanes) {
			if (l.head) {
				*tail = l.head;
				tail = &l.tail->next;
			}
			l.head = nullptr;
			l.tail = nullptr;
			l.pending = 0;
		}
		// relaxed: under mu; readers check it advisorily, and sleep's release orders it.
		_s->epoch.fetch_add(1, std::memory_order_relaxed);
		// release: a sleeper that sees the new word sees the new epoch.
		_s->sleep.fetch_add(1, std::memory_order_release);
		bool wake = _pool->waiters_due(*_s);
		WorkIdle* notify = WorkPool::take_idle(*_s);
		_s->mu.unlock();
		workpool_detail::futex_wake_all(&_s->sleep);
		// Boxed items are freed before the group's waiters and idle nodes hear it is idle.
		WorkItem* owned = nullptr;
		WorkItem** keep = &owned;
		dropped = 0;
		for (WorkItem* item = list; item;) {
			WorkItem* next = item->next;
			++dropped;
			if (item->flags & workpool_detail::item_boxed) {
				auto* box = static_cast<workpool_detail::BoxedItem*>(item);
				box->destroy(box);
			} else {
				*keep = item;
				keep = &item->next;
			}
			item = next;
		}
		*keep = nullptr;
		if (wake) {
			workpool_detail::futex_wake_all(&_pool->_group_wake);
		}
		WorkPool::fire_idle(notify);
		return owned;
	}

public:
	explicit WorkGroup(WorkPool& pool, unsigned width = 0, unsigned weight = 1) :
		_pool(&pool),
		_s(new GroupState) {
		_s->width = width;
		_s->weight = weight ? weight : 1;
		auto states = pool._states.fetch_add(1, std::memory_order_relaxed) + 1;
		try {
			pool.reserve_rings(states);
		} catch (...) {
			pool._states.fetch_sub(1, std::memory_order_relaxed);
			delete _s;
			throw;
		}
	}

	WorkGroup(const WorkGroup&) = delete;
	WorkGroup& operator=(const WorkGroup&) = delete;

	// Cancels (freeing queued boxed items; the owner keeps its own), then waits for the
	// running ones, as wait() does: aborts when called from an item of this group. Must not
	// race other calls.
	~WorkGroup() noexcept {
		check_stack("WorkGroup::~WorkGroup");
		std::size_t dropped;
		cancel_items(dropped);
		wait();
		_s->mu.lock();
		_s->owned = false;
		bool free = _s->unreferenced();
		_s->mu.unlock();
		if (free) {
			delete _s;
			_pool->_states.fetch_sub(1, std::memory_order_relaxed);
		}
	}

	WorkPool& pool() const noexcept { return *_pool; }

	void post(WorkItem& item) noexcept {
		item.group = this;
		_pool->post(item);
	}

	// Boxes func (callable as func() or func(const WorkItem&), whose stop_requested() is
	// the group's stop) and posts it in class cls: one allocation. False when the box
	// cannot be allocated.
	template <typename Func>
	bool enqueue(Func&& func, WorkClass cls = WorkClass::flush) {
		using F = std::decay_t<Func>;
		static_assert(std::is_invocable_v<F&> || std::is_invocable_v<F&, const WorkItem&>, "a WorkGroup task takes no arguments or a const WorkItem&");
		auto* box = new (std::nothrow) Box<F>(std::forward<Func>(func));
		if (!box) {
			return false;
		}
		box->run = &Box<F>::invoke;
		box->destroy = &Box<F>::drop;
		box->cls = cls;
		box->flags = workpool_detail::item_boxed;
		post(*box);
		return true;
	}

	// Drops the queued items that have not started: boxed ones are freed unrun, caller-owned
	// ones come back as a list through `next`, queries then flushes then merges, each oldest
	// first, for the owner to free or recycle. Running items see stop_requested() turn true.
	// Items posted afterwards run normally.
	WorkItem* cancel() noexcept {
		std::size_t dropped;
		return cancel_items(dropped);
	}

	// Blocks until the group has nothing queued and nothing running. On a worker the caller
	// serves the group meanwhile, inline, as a helper within the group's width, and nothing
	// else (no other group's work, no query); anywhere else it parks. It creates no thread.
	// A wait on a group that has an item on the calling thread's stack, this group included,
	// is a programming error and aborts: to join work from inside a group, post it to a child
	// group and wait on that. Items of the group may run on the waiter's thread.
	void wait() noexcept {
		check_stack("WorkGroup::wait");
		auto& pool = *_pool;
		GroupState* s = _s;
		bool worker = pool.on_worker();
		if (!worker && pool._block_guard) {
			pool._block_guard("WorkGroup::wait");
		}
		for (;;) {
			// acquire: pairs with the bump made under s->mu by whatever changed the group.
			auto word = pool._group_wake.load(std::memory_order_acquire);
			s->mu.lock();
			if (WorkPool::idle(*s)) {
				s->mu.unlock();
				return;
			}
			int c = worker ? WorkPool::helpable(*s) : -1;
			if (c >= 0) {
				++s->lanes[c].helpers;
				pool.turn(*s, static_cast<unsigned>(c), true);
				continue;
			}
			++s->waiters;
			s->mu.unlock();
			workpool_detail::futex_wait(&pool._group_wake, word);
			s->mu.lock();
			--s->waiters;
			s->mu.unlock();
		}
	}

	// Runs node->fn once when the group is idle (nothing queued, nothing running): on the
	// thread that makes it idle, or here and now when it already is. The way for an event
	// loop or a coroutine to join a group: it never blocks, so it needs no block_guard.
	void on_idle(WorkIdle& node) noexcept {
		_s->mu.lock();
		bool now = WorkPool::idle(*_s);
		if (!now) {
			node.next = _s->idle;
			_s->idle = &node;
		}
		_s->mu.unlock();
		if (now) {
			node.fn(&node);
		}
	}

	// Unlinks a notification that has not fired; false when it already fired or is firing.
	bool cancel_idle(WorkIdle& node) noexcept {
		_s->mu.lock();
		bool found = false;
		for (WorkIdle** link = &_s->idle; *link; link = &(*link)->next) {
			if (*link == &node) {
				*link = node.next;
				found = true;
				break;
			}
		}
		_s->mu.unlock();
		return found;
	}

	// Drops the queued items as cancel() does (the caller-owned ones are not returned), then
	// waits as wait() does; returns how many queued items it dropped.
	std::size_t cancel_and_wait() noexcept {
		check_stack("WorkGroup::cancel_and_wait");
		std::size_t dropped;
		cancel_items(dropped);
		wait();
		return dropped;
	}

	// Items queued and not started, and items executing, now: the group's backlog and load.
	// Each is exact at the instant it is read under the group's lock; the two are not read
	// together.
	std::size_t pending() const noexcept {
		_s->mu.lock();
		std::size_t n = _s->pending();
		_s->mu.unlock();
		return n;
	}

	std::size_t running() const noexcept {
		_s->mu.lock();
		std::size_t n = _s->running;
		_s->mu.unlock();
		return n;
	}

	// Whether the item running on this thread belongs to this group and saw a cancel.
	bool stop_requested() const noexcept {
		auto* frame = workpool_detail::tls_here.find(_s);
		return frame && frame->item->stop_requested();
	}

	void set_width(unsigned width) noexcept {
		_s->mu.lock();
		_s->width = width;
		_s->mu.unlock();
	}

	void set_weight(unsigned weight) noexcept {
		_s->mu.lock();
		_s->weight = weight ? weight : 1;
		_s->mu.unlock();
	}
};


inline bool
WorkItem::stop_requested() const noexcept
{
	return group && group->_s->epoch.load(std::memory_order_relaxed) != epoch;
}


inline WorkPool::GroupState&
WorkPool::state_of(const WorkItem& item) noexcept
{
	return item.group ? *item.group->_s : _own;
}


inline bool
WorkPool::sleep_until(std::chrono::steady_clock::time_point deadline, WorkGroup& group) noexcept
{
	GroupState* s = group._s;
	auto* frame = workpool_detail::tls_here.find(s);
	auto started = frame ? frame->item->epoch : s->epoch.load(std::memory_order_relaxed);
	for (;;) {
		// acquire: pairs with cancel()'s release of the word, so a new word means a new epoch.
		auto word = s->sleep.load(std::memory_order_acquire);
		if (s->epoch.load(std::memory_order_relaxed) != started) {
			return false;
		}
		auto now = std::chrono::steady_clock::now();
		if (now >= deadline) {
			return true;
		}
		auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(deadline - now).count();
		workpool_detail::futex_wait_for(&s->sleep, word, static_cast<std::uint64_t>(ns));
	}
}


inline void
workpool_detail::WorkPoolWorker::operator()()
{
	_pool->work(_idx);
}
