---
sidebar_label: "Port Transport Architecture"
---

# Port Transport Architecture

**Status:** implemented. This page describes the direct-lane transport that
replaced Chronon's former two-stage MPSC staging and scheduler arbitration.

## Requirements

Port transport is part of the simulation's timing contract, not just a generic
container. The implementation must preserve all of these properties:

- `arrive_cycle = send_cycle + connection.delay`;
- identical results for sequential, barrier, and lookahead execution;
- deterministic fan-in order independent of host-thread interleaving;
- non-blocking `send()` with model-visible back pressure;
- no allocation, lock, or reclamation on the steady-state data path;
- receiver-owned filtering and cancellation;
- bounded cache-line sharing between producer and consumer.

The topology supplies a stronger invariant than a general MPMC queue can use:
each `Connection` has exactly one producer, while each `InPort` has exactly one
consumer. Chronon therefore implements MPSC as a deterministic merge of SPSC
lanes rather than as one contended multi-producer queue.

## Data flow

```text
 producer A ── Connection A ── SPSC lane A ─┐
 producer B ── Connection B ── SPSC lane B ─┼─ receiver frontier ─ filter ─ InPort<T>
 producer C ── Connection C ── SPSC lane C ─┘
                         producer-owned          consumer-owned
```

There is one payload publication and one payload consumption. A producer no
longer writes a `Connection` staging ring for a scheduler arbiter to copy into a
second InPort queue. The scheduler no longer scans ports at unit ticks,
barriers, or epoch boundaries.

## SPSC lane

Each lane is a preallocated power-of-two ring. `head` and `tail` are monotonic
absolute tickets; masking is used only to select a slot. Absolute tickets avoid
modulo-index ambiguity and allow every physical slot to hold an entry—there is
no reserved sentinel slot.

The producer performs:

1. a relaxed load of its private `tail`;
2. an acquire load of `head` only when its cached value says the ring is full;
3. payload and metadata stores into `buffer[tail & mask]`;
4. a release store of `tail + 1`.

The consumer is symmetric: it refreshes `tail` with acquire only when its
private cache says empty, reads the published slot, and releases `head + 1`
after consuming it. The producer-written tail and consumer-written head occupy
different 64-byte-aligned regions with their owner-private caches. This avoids
the classic head/tail false-sharing pair.

Bounded lanes allocate the smallest sufficient power-of-two ring at
initialization. An unbounded model-visible edge still uses bounded physical
storage (4096 entries by default), and lookahead headroom validation prevents a
producer from outrunning that storage.

## Deterministic MPSC frontier

The receiver selects the smallest lane head by:

```text
(arrive_cycle, stable_connection_id, lane_id)
```

`stable_connection_id` is assigned from topology construction order, so the
result is independent of worker count and partitioning.

For fewer than 32 lanes, a direct linear scan is faster than maintaining an
index, and producers skip notification atomics entirely. For 32 or more lanes,
producers publish notifications into cache-line-aligned shards covering 64
lanes each. The single consumer exchanges those shards and maintains a private
min-heap with at most one head per active lane. Sharding bounds producer
contention while reducing an empty readiness check to one atomic exchange per
64 lanes.

The bits are notifications, not persistent ownership flags. This distinction
prevents the producer-set/consumer-clear lost-wakeup race:

- repeated pushes may coalesce into one notification;
- after every pop, the consumer reinserts the lane while it remains nonempty;
- a push racing with an empty transition either appears in that recheck or
  leaves a new notification for the next refresh.

`tryPop`, `hasReady`, `minArrivalCycle`, and `empty` reuse this same frontier.
Readiness checks therefore do not silently reintroduce an O(number-of-lanes)
scan in activity scheduling.

| Operation | Small fan-in | Large sparse fan-in |
|---|---:|---:|
| push | O(1) | O(1), one sharded `fetch_or` |
| select/pop | O(P) | O(W + log A) |
| ready/min-arrival | O(P) | O(W) refresh, then O(1) |
| payload allocation | none | none |

`P` is the lane count, `A` the number of active lanes, and `W` the number of
64-lane notification shards. The heap and lane registry are reserved during
initialization and touched only by the consumer.

## Back pressure is simulation state

Physical ring availability and architectural queue capacity are deliberately
separate:

- **physical storage** prevents memory overwrite on the host;
- **model admission** determines whether hardware represented by the model is
  ready in a particular simulated cycle;
- **edge rate** limits sends per producer cycle.

Using a live host-time queue size as model back pressure is incorrect: whether
the consumer thread happens to publish a pop first would change `canSend()` and
therefore simulation results. `Connection` takes a cycle-local admission
snapshot and tracks its own successful pushes. The delay-one,
capacity-one/rate-one DFF case also recognizes the entry consumed by the current
simulated cycle while retaining enough physical space for the next value.

Finite-capacity lanes enable a preallocated SPSC pop-credit history so the
producer can distinguish prior-cycle credits from same-cycle host pops. Truly
unbounded model admission disables this ledger entirely. Credit history has the
same power-of-two size as its payload ring: there cannot be more unobserved pop
credits than published payloads. It therefore remains compact and never
allocates during execution.

The epoch-free scheduler separately proves that every direct lane has a
producer-progress dependency and enough physical headroom. A physical overflow
is counted as a correctness failure; it is not silently converted into model
back pressure.

## Receiver filtering and cancellation

`tryReceiveFiltered` performs arbitrary receiver-owned cancellation without
putting shared filter state on producer cache lines:

```cpp
auto valid_epoch = [epoch](const Message& message) noexcept {
    return message.epoch == epoch;
};

if (auto message = in.tryReceiveFiltered(localCycle(), valid_epoch)) {
    consume(*message);
}
```

Ready messages rejected by the predicate are permanently consumed. The
predicate is a constrained `noexcept` template parameter: there is no
`std::function`, indirect predicate call, heap allocation, or producer-side
read. Chronon's watermark APIs (`cancelOlderThan`, `cancelYoungerThan`, and
`cancelOutsideInclusive`) remain available for persistent, in-flight-scoped
filters; `PortPolicy::StageSelective` keeps their mutable state on the receiver.

Sender-side `OutPort::cancelInFlight()` remains an epoch-stamped lazy cancel:
the sender advances an atomic epoch and the receiver discards older envelopes.
Neither side mutates slots owned by the other.

## Why not one general lock-free MPMC queue?

A Michael–Scott or bounded sequence-number MPMC queue solves a harder problem
than Chronon has. It introduces a contended global enqueue position (or CAS
retry loop), and an unbounded form also needs safe memory reclamation. More
importantly, successful CAS order reflects host interleaving, while Chronon
requires topology-stable simulated order. A deterministic merge would still be
needed after the queue.

Per-producer subqueues are a well-established scalability technique (for
example, moodycamel's `ConcurrentQueue`), but Chronon can specialize further:
the producer identity and single consumer are known at initialization, message
timestamps are monotonic within a fixed-delay connection, and global
linearizability is neither required nor desirable.

The same argument rules out a mutex, ticket spinlock, or queue lock on the
payload path. More scalable locks can reduce waiter-to-waiter cache traffic,
but they still serialize every producer and expose host lock-acquisition order
to the simulation. Per-Connection SPSC ownership removes the mutual-exclusion
problem instead of tuning it. Locks remain appropriate for initialization and
rare control paths, not cycle-level publication.

## Validation and benchmark

The regression suite covers ring wraparound, real two-thread SPSC stress,
same-cycle ties, mixed delays, 30-run worker-count determinism, dynamic
rebalancing, finite-capacity back pressure, sparse 256-lane fan-in, hot-lane
reinsertion, and receiver filtering. Release validation passes all 45 tests.

`chronon_mpsc_lane_frontier_benchmark` records sparse and four-active-lane
traffic without allocating on the timed path. On the reference development
host (Release, GCC 12), representative results were:

| Lanes | Selection | Sparse round trip | Four-way merge/message | Empty min-arrival |
|---:|---|---:|---:|---:|
| 16 | scan | 15.62 ns | 10.58 ns | 8.10 ns |
| 64 | frontier | 19.31 ns | 14.21 ns | 4.22 ns |
| 256 | frontier | 26.44 ns | 25.14 ns | 12.11 ns |

Treat nanosecond values as comparative microbenchmarks, not portable promises.
The former 16-way empty arbitration benchmark measured 34.74 ns/call; the
direct-lane empty receive path measures about 6.5 ns/call on the same host. The
registered-edge SPSC round trip measured 2.87 ns/message versus 14.36 ns for the
legacy admission adapter (about 5.0x); both measurements include the admission
query used by `Connection`.

## Relationship to PR #91 and #92

- [PR #91](https://github.com/chronon-sim/chronon/pull/91) identified sparse
  high-fan-in scans and proposed active-lane tracking. The direct-lane frontier
  retains that insight while using notification semantics to avoid a lost-clear
  race and sharing it with readiness queries.
- [PR #92](https://github.com/chronon-sim/chronon/pull/92) demonstrated the
  benefit of monotonic power-of-two rings for fixed-delay local transport. The
  cross-thread ring now uses the same absolute-ticket principle. A specialized
  same-thread fixed-delay queue remains useful follow-up work; the generic
  `SingleThreadMessageQueue` still supports non-monotonic manual enqueues.

## Research basis

- [Linux circular-buffer memory-ordering documentation](https://docs.kernel.org/next/core-api/circular-buffers.html)
- [Michael and Scott, Simple, Fast, and Practical Non-Blocking and Blocking Concurrent Queue Algorithms](https://www.cs.rochester.edu/u/scott/papers/1996_PODC_queues.pdf)
- [LMAX Disruptor technical paper](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf)
- [Boost.Lockfree documentation](https://www.boost.org/doc/libs/latest/doc/html/lockfree.html)
- [moodycamel ConcurrentQueue design notes](https://github.com/cameron314/concurrentqueue)
- [WG21 P0154: hardware interference size](https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2015/p0154r0.html)
- [Nikolaev, A Scalable, Portable, and Memory-Efficient Lock-Free FIFO Queue](https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.DISC.2019.28)

These designs informed the memory-ordering and cache-layout choices. The final
structure follows Chronon's topology and determinism requirements rather than
copying a general-purpose queue wholesale.
