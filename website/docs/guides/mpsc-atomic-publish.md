---
sidebar_label: "MPSC Determinism Under the Lookahead Scheduler (Consumer-Tick-Driven Arbitration)"
---

# MPSC Determinism Under the Lookahead Scheduler (Consumer-Tick-Driven Arbitration)

**Status**: Implemented and in production. This document is retrospective: it
describes the only MPSC arbitration path used by the lookahead scheduler. A
previous `BulkBarrier` backend was removed once this path became the default
(see commit `65a8fed` and the removal change).

## Background

The lookahead scheduler (`executeEpochProgressBased` in `TickSimulation.hpp`)
issues a single `stdexec::bulk` for an entire epoch. Worker threads advance
independently, spinning on predecessor-cluster `completed_cycle` atomics
(`thread_progress_array_`) instead of rejoining the main thread at every cycle.
There is no per-cycle sync point where an auxiliary main-thread arbiter can run.

MPSC admission uses a two-stage path (`Connection::transfer`,
`InPort::arbitrateMPSC`):

1. Producer writes into `Connection::staging_` — one SPSC ring per connection,
   owned by the producer thread.
2. Arbiter drains every registered connection's staging into the shared
   `MultiProducerQueueAdapter` in conn_id-ascending order. The arbiter is the
   sole writer to the shared per-thread rings, making ring-level admission
   single-writer and deterministic.
3. Consumer `tryReceive` pops from the shared MPSC adapter via k-way merge on
   `(arrive_cycle, conn_id)`.

Without a global sync point, arbitration rides on the consumer's own execution.

## Consumer-tick-driven arbitration

Arbitration shifts from "main thread at global sync" to "consumer thread at the
start of every tick that owns the InPort". The lookahead scheduler already
gates the consumer's tick on predecessor progress atomics, so the arbiter can
safely drain everything up to the slowest predecessor's published cycle.

### Sequencing

Consumer thread `C` is about to tick unit `U` at `localCycle` `K`. `U` owns
MPSC InPort `P`. Sequence:

```wavedrom
{ "signal": [
  { "name": "clk",                       "wave": "p........" },
  {},
  ["Producer threads",
    { "name": "prod_0.send()",            "wave": "x=.=.=...", "data": ["a","b","c"] },
    { "name": "prod_1.send()",            "wave": "x.=..=...", "data": ["d","e"] },
    { "name": "progress[0]",             "wave": "x=.=.=...", "data": ["K-2","K-1","K"] },
    { "name": "progress[1]",             "wave": "x=..=.=..", "data": ["K-2","K-1","K"] }
  ],
  {},
  ["Consumer thread (unit U, cycle K)",
    { "name": "S = min(progress)",        "wave": "x...=....", "data": ["K-1"] },
    { "name": "drain staging (c≤S)",      "wave": "x...==...", "data": ["conn0","conn1"] },
    { "name": "user tick()",              "wave": "x.....=..", "data": ["run"] },
    { "name": "tryReceive (k-merge)",     "wave": "x.....==.", "data": ["a","d"] }
  ]
],
  "head": { "text": "Consumer-tick-driven MPSC arbitration sequence" },
  "config": { "hscale": 2 }
}
```

Pseudocode equivalent:

```
At start of U's tick for cycle K (before user tick() runs):
  S := min over p in producer_clusters(P) of
          thread_progress_array_[p].completed_cycle.load(acquire)

  if S > last_arbitrated_cycle_:
      for c in (last_arbitrated_cycle_ + 1 .. S):
          for conn in P.mpsc_connections_ sorted by conn_id:
              drain staging entries with enqueue_cycle == c
      last_arbitrated_cycle_ = S

  user tick() runs → tryReceive pops via k-way merge on (arrive_cycle, conn_id)
```

Hook: `TickableUnit::executeTick` calls `arbitrateOwnedMPSCPorts_()` before the
user's `tick()`. The helper walks the owner's ports and calls
`arbitrateMPSCConsumerDriven` on each.

The `min` over producer clusters is the critical invariant. Even if one producer
is 20 cycles ahead and another is 3 cycles behind, the arbiter only consumes
up to the slower producer's progress, so both producers' cycle-`c` entries are
arbitrated in a topologically deterministic `(send_cycle, conn_id)` order.

Under Sequential or Barrier execution the per-InPort producer-progress pointer
set is empty, so `arbitrateMPSCConsumerDriven` degrades to the legacy unbounded
drain. Those modes still use the main-thread `arbitrateAllMPSCPorts_()` at
their own sync points.

## Determinism proof sketch

**Claim**: for a fixed topology, every run produces the same shared-queue
commit order — the same `(send_cycle, conn_id)` sequence — regardless of
worker-thread interleaving.

Let `E(c, i)` denote the set of staging entries produced by Connection `i`
during producer cycle `c`. Given a fixed topology and fixed initial state:

1. **Producer output determinism (by induction on cycle).** `tick()` is
   deterministic and its inputs at cycle `c` are a deterministic function of
   arbitrated entries from cycles `< c` (see step 3).

2. **Arbitration drains every entry exactly once.** `last_arbitrated_cycle_`
   advances monotonically (single writer, per-port). Staging is SPSC and
   popped only by the arbiter.

3. **Arbitration order is `(send_cycle, conn_id)`.** Outer loop iterates `c`
   strictly ascending. Inner loop iterates `P.mpsc_connections_` in
   `conn_id`-sorted order (enforced at `registerMPSCConnection`). `conn_id`
   is assigned at topology construction time.

4. **Shared MPSC k-way merge keyed on `(arrive_cycle, conn_id)`.** Given
   arbitration writes entries in `(send_cycle, conn_id)` order and
   `arrive_cycle = send_cycle + delay` is monotonic in `send_cycle` for a
   fixed delay per connection, pop order equals arbitration order.

5. **`S` is monotone and eventually sufficient.** Each
   `thread_progress_array_[p].completed_cycle` is monotonically increasing.
   At epoch end, every cluster reaches `end_cycle`, so every producer
   eventually publishes every cycle's completion and every entry gets drained
   before the next call into user code that would observe it.

The proof does NOT rely on wall-clock fairness between producer threads.

## Epoch-end drain (R8)

At epoch end every thread reaches `end_cycle` but publishes `end_cycle - 1` as
completed just before exiting the per-thread loop. Staging entries with
`enqueue_cycle == end_cycle - 1` are still pending. After the epoch
`sync_wait`, `executeEpochProgressBased` calls `arbitrateAllMPSCPorts_()` once
to drain the tail. Inexpensive; runs once per epoch.

## Termination

A unit calling `requestTermination` mid-epoch sets `stop_token`. Spin-waits
exit. Staging may contain entries never arbitrated; the epoch-end flush covers
them. If a consumer's `tryReceive` runs before termination is observed, it
sees whatever arrived via the last consumer-tick arbitration.

## Performance

Every consumer tick loads up to `|producer_clusters(P)|` atomics plus the drain
inner loop — one extra load pair per MPSC InPort per tick. Measured
~2.9× wall-clock speedup over the prior `BulkBarrier` backend on Dhrystone at
`num_workers=8`, driven largely by eliminating per-iteration `sync_wait`
round-trips.

## Audit of concurrent shared state

For producer on thread A pushing `conn_7` vs. consumer on thread B arbitrating
`conn_3`:

- `Connection::staging_` — per-connection SPSC ring. `conn_3` and `conn_7`
  have distinct rings. No share.
- `Connection::cancel_epoch_` — per-connection atomic. No share.
- `Connection::pushes_this_cycle_` — producer-only, per-connection. No share.
- `MultiProducerQueueAdapter` (shared across connections feeding the same
  InPort) — only the port's consumer thread ever arbitrates this port, so the
  adapter has a single writer per port.
- `thread_queues_` vector — populated at `initialize()`, read-only during run.
- `thread_progress_array_[p].completed_cycle` — producer cluster writes its own slot
  (release), arbiter reads predecessor slots (acquire). Atomic, safe.
- `InPort::last_arbitrated_cycle_` — single writer (port's consumer thread).

No cross-connection shared mutable state between producer-A-pushing-`conn_7`
and arbiter-on-B-draining-`conn_3`. The only consumer/producer shared state is
within a single Connection (its staging ring), handled by SPSC ordering.
