# Source layout

How workpool's sources are cut, following cpp-libraries' `docs/SPLITTING.md`. Users include
`work_pool.hh` as before; it includes its pieces, which sit beside it with its name as prefix.

## `work_pool.hh` and its pieces

| File | Holds |
|---|---|
| `work_pool.hh` | The scheduler: `WorkClass`, `WorkItem`, `WorkIdle`, the group's state (`BoxedItem`, `Lane`, `GroupState`), the per-thread stack of running items (`Frame`, `Here`), `WorkPoolWorker`, `WorkPool` and `WorkGroup`. Includes the three pieces below. |
| `work_pool_sync.hh` | The platform layer: the futex on a 32-bit word per OS (Linux futex(2), macOS `os_sync_wait_on_address` or `__ulock`, FreeBSD `_umtx_op`, else C++20 atomic wait), `cpu_relax`, the cycle counter (`ticks`, `ticks_per_ns`) and `SpinLock`. |
| `work_pool_idle.hh` | Worker parking: `Parker` (one worker's futex sleep), `Idle` (Tokio's idle bookkeeping: the searching and unparked counts and the LIFO sleeper list) and `backoff`. |
| `work_pool_ready_ring.hh` | `ReadyRing`: the ready-list, Vyukov's bounded MPMC ring of group pointers, chained so it grows as groups open, with the stamps merge aging reads. |

Each piece is a unit a maintainer opens alone: porting to a new OS touches only
`work_pool_sync.hh`; the idle protocol and the ring each carry their own memory-order argument,
and each states its half of the Dekker pair between a post and a parker's final check
(`Idle::notify_one` and `to_parked`, `ReadyRing::Ring::push` and `ReadyRing::empty`).

## Kept whole

- `work_pool.hh` (1,039 lines): `WorkPool` and `WorkGroup` are one scheduler. They are friends
  both ways, and `WorkGroup::wait`, `cancel_items` and the destructor run the pool's own
  `turn`, `helpable`, `waiters_due`, `take_idle` and `fire_idle` on the `GroupState` both share
  under its lock; `WorkPool::state_of` and `sleep_until` read `WorkGroup`'s state. Apart, neither
  class could be read or changed without the other open. `WorkPool`'s class body (about 500
  lines) can't be cut, and the group state types are the data both classes act on.
