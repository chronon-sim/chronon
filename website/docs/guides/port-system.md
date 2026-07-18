---
sidebar_label: "Port System"
---

# Port System

## Overview

Ports provide type-safe communication between units with timestamped message queues for deterministic delivery. The connection delay determines latency; the queue type is automatically selected based on thread topology during simulation initialization.

For the queue layout, memory-ordering contract, deterministic fan-in proof, and
performance rationale, see [Port Transport Architecture](mpsc-atomic-publish.md).

**Internal optimization:** The framework uses different queue implementations as an internal optimization:
- **delay=0**: Same-cycle delivery (0-cycle latency), may use direct delivery
- **delay>0**: Queued delivery with N-cycle latency, queue type selected based on thread topology

This is an internal detail — users specify the delay when connecting ports, and the framework selects the appropriate queue type.

## OutPort

```cpp
template<typename T>
class OutPort {
public:
    // Construction (auto-registers with owner)
    // per_cycle_capacity: max source sends per cycle
    // (default = 1 registered-edge entry; use UNLIMITED_CAPACITY to opt out).
    OutPort(Unit* owner, std::string name,
            size_t per_cycle_capacity = 1);

    // Connect to input port
    Connection<T>* connect(InPort<T>* to, uint32_t delay);

    // Send - returns success/failure
    // Respects per-cycle capacity: returns false if exhausted
    bool send(const T& data);
    bool send(T&& data);

    // Back pressure support (checks both per-cycle cap AND destination capacity)
    bool canSend() const;
    bool trySend(const T& data);

    // Cancel all in-flight messages previously sent on this OutPort
    void cancelInFlight();

    // Per-cycle capacity control
    void setPerCycleCapacity(size_t cap);    // Runtime adjustment (UNLIMITED_CAPACITY = unlimited)
    size_t perCycleCapacity() const;
    size_t sentThisCycle() const;             // 0 if unlimited
    size_t remainingThisCycle() const;        // UNLIMITED_CAPACITY if unlimited

    // Sender-based send (for composition)
    PortSendSender<T> send(T data);

    // Query
    const std::string& name() const;
    size_t connectionCount() const;
    bool isConnected() const;
    bool isAnyDestinationFull() const;
};
```

## Atomic multi-port transactions

Use `reserve()` when one logical model transition must publish through several
independent `OutPort`s. A successful handle has already claimed every port's
per-cycle rate and every connection's queue-depth admission; no payload is
visible until `commit()` validates the complete claim set.

```cpp
OutPort<InstData>* selected_iq = selectIssueQueue(inst);

auto tx = reserve(*selected_iq, out_rob_write);
if (!tx) {
    return;  // retain model state and retry next tick
}

// Positional commit: values follow the reserve() port order.
if (tx.commit(inst, inst)) {
    advanceDispatchState();
}
```

The same transaction can stage values by port identity. `send()` on a
transaction is only a staging operation; it does not call `OutPort::send()`:

```cpp
auto tx = reserve(*selected_iq, out_rob_write);
if (!tx) return;

if (!tx.send(*selected_iq, inst) || !tx.send(out_rob_write, inst) || !tx.commit()) return;
advanceDispatchState();
```

The contract is deliberately narrow:

- every participating port must be owned by the same `Unit`, and a port may
  appear only once;
- a reservation is valid only in the producer's current simulated cycle;
- changing an `OutPort` or destination capacity, changing port topology, or
  calling an enabled `cancelInFlight()` invalidates an outstanding transaction;
- an invalid commit releases every claim and delivers nothing; a second commit
  also returns `false`, so retry cannot duplicate a previously delivered subset;
- dropping the handle or calling `tx.cancel()` releases every claim without
  publishing;
- connected payload types need non-throwing move construction and assignment;
  a non-broadcast fanout additionally needs a non-throwing copy constructor.

The transaction handle is stack-backed. Producer-owned claim bookkeeping
reuses the existing cycle-local admission counters and object padding; a cold
destination epoch invalidates claims after configuration or cancellation
changes. Ordinary `send()` and `canSend()` gain no transaction check, lock,
atomic operation, allocation, or virtual dispatch. MPSC claims target the
connection's private SPSC lane; receiver activity can only free a claimed slot
before commit.

## InPort

```cpp
template<typename T>
class InPort {
public:
    static constexpr size_t UNLIMITED_CAPACITY = /* ... */;

    // Construction (auto-registers with owner)
    InPort(Unit* owner, std::string name,
           size_t capacity = UNLIMITED_CAPACITY);

    // Queue type selection (call during initialization before simulation starts)
    void useSingleThreadQueue();      // Same-thread connections (no mutex)
    void useLockFreeQueue();          // Cross-thread SPSC (lock-free atomics)
    void useMultiProducerQueue();     // Cross-thread MPSC ingress + shared FIFO
    void configureForSourceThreadCount(size_t source_thread_count);

    // Multi-producer queue API (only valid in MPSC mode)
    size_t registerProducerThread(size_t thread_id);
    size_t getQueueIdForThread(size_t thread_id) const;
    bool isMultiProducerMode() const;
    bool pushToThreadQueue(size_t queue_id, T data, uint64_t arrive_cycle);
    bool pushToThreadQueueCancelable(size_t queue_id, T data, uint64_t arrive_cycle,
                                     const std::atomic<uint64_t>* cancel_epoch,
                                     uint64_t epoch_snapshot);
    bool canAcceptOnThreadQueue(size_t queue_id, uint32_t pending = 0) const;

    // Receive data at current cycle
    std::optional<T> tryReceive(uint64_t current_cycle);

    // Permanently consume ready messages rejected by a noexcept receiver predicate
    template<class Filter>
    std::optional<T> tryReceiveFiltered(uint64_t current_cycle, Filter&& filter);

    // Receive all available messages
    std::vector<T> receiveAll(uint64_t current_cycle);
    std::vector<T> receiveAll();  // Uses owner->localCycle()

    // Sender-based receive (for composition)
    PortReceiveSender<T> receive();

    // Drop all queued messages (including future arrivals)
    void flush();

    // Receiver-owned selective flush (messages from earlier enqueue cycles)
    template<auto KeyFn>
    void flush(FlushRange keep_range);

    // Source-compatible wrappers over FlushRange
    template<auto KeyFn, typename K>
    void cancelOlderThan(K watermark);
    template<auto KeyFn, typename K>
    void cancelYoungerThan(K watermark);
    template<auto KeyFn, typename MinK, typename MaxK>
    void cancelOutsideInclusive(MinK min_keep, MaxK max_keep);
    void resetSelectiveCancellation();

    // Query
    bool hasData(uint64_t current_cycle) const;
    bool hasMessages() const;  // Uses owner->localCycle()
    // Destination FIFO occupancy (never exceeds capacity on bounded MPSC)
    size_t queuedMessageCount() const;
    // Per-Connection ingress entries, outside the destination FIFO (diagnostic)
    size_t transportPendingMessageCount() const;
    size_t sharedFifoHighWatermark() const;
    bool canAccept(uint32_t pending = 0) const;
    size_t capacity() const;
    size_t available() const;
    void setCapacity(size_t capacity);
    std::optional<uint64_t> minArrivalCycle() const;
};
```

## Connection

Connections are established via `TickSimulation::connect()`:

```cpp
// delay=0: Same-cycle delivery (0-cycle latency)
sim.connect(producer->out, consumer->in, 0);

// delay>0: Queued delivery with N-cycle latency
// Queue type selected automatically based on thread topology:
// - SingleThreadMessageQueue: Both on same thread
// - LockFreeQueueAdapter: Cross-thread SPSC (single producer)
// - MultiProducerQueueAdapter: direct SPSC lane per Connection + receiver merge
sim.connect(producer->out, consumer->in, 5);

// Cancellation / flush (CPU squash / pipeline flush)
// - cancelInFlight() invalidates messages already sent but not yet received
// - flush() drops queued messages immediately
producer->out.cancelInFlight();
consumer->in.flush();
```

## Selective Flush

Selective flush is available on **InPort**. It keeps an explicit key range for
messages that were already in flight while leaving messages produced in the
flush cycle or later untouched.

For one-shot arbitrary filtering, `tryReceiveFiltered(cycle, predicate)` scans
ready messages in deterministic delivery order and permanently consumes every
message for which the `noexcept` predicate returns `false`. The predicate is
inlined—no `std::function` or allocation is introduced:

```cpp
auto current_epoch = [epoch](const Message& message) noexcept {
    return message.epoch == epoch;
};
auto message = in.tryReceiveFiltered(localCycle(), current_epoch);
```

```cpp
// Cancel the flush point and every younger instruction. UID zero and UINT64_MAX
// require no model-side +/- 1 conversion.
in_instr.flush<&Instruction::id>(FlushRange::atAndYounger(flush_point_id));

// Keep only an inclusive architectural window.
in_instr.flush<&Instruction::id>(FlushRange::outsideInclusive(oldest, youngest));

// All-or-nothing cancel on OutPort
out_icache_miss.cancelInFlight();
```

**API:**
```cpp
template<auto KeyFn>
void InPort<T>::flush(FlushRange keep_range);

FlushRange::youngerThan(uid);       // cancel key > uid
FlushRange::atAndYounger(uid);      // cancel key >= uid
FlushRange::olderThan(uid);         // cancel key < uid
FlushRange::atAndOlder(uid);        // cancel key <= uid
FlushRange::outsideInclusive(a, b); // cancel key < a or key > b

// Compatibility spellings, implemented by the same FlushRange engine:
cancelOlderThan<KeyFn>(uid);
cancelYoungerThan<KeyFn>(uid);
cancelOutsideInclusive<KeyFn>(a, b);
```

- `KeyFn` — pointer-to-member or callable that extracts a key from `T` (e.g., `&MyMsg::id`), projected to `uint64_t`
- `FlushRange` retains open/closed bounds explicitly; it never performs
  overflow-prone boundary arithmetic

**Semantics:**
- **One contract for every backend and policy**: `LegacyFastPath` and
  `StageSelective`, same-thread queues, direct SPSC, direct-lane MPSC, and
  shared broadcast all use the same receiver-owned predicates
- **Cycle boundary**: a flush installed in receiver cycle `F` applies only to
  messages stamped with `enqueue_cycle < F`; cycle `F` and later are post-flush
- **Monotonic overlap**: predicates remain independent across cycles. Calls for
  the same extractor in one cycle intersect their keep ranges, and calls for
  different extractors compose. `resetSelectiveCancellation()` is a
  compatibility no-op and cannot resurrect a zombie
- **Arbitrary positive delays**: retirement follows the stable queue-arrival
  frontier using the maximum incoming edge delay. Epoch-free MPSC additionally
  gates retirement on its existing per-producer scheduler progress. It does not
  assume enqueue cycles are monotonic, which is essential for
  heterogeneous-delay MPSC fan-in
- **Receiver ownership**: producers stamp only their local enqueue cycle and
  never read mutable receiver state. Predicate state uses no lock or atomic;
  only retirement may acquire-load existing scheduler progress while a flush
  is live
- **Low overhead when unused**: receive performs one predictable empty-state
  check; sender and envelope paths carry no selective generation state
- **Composable with epoch cancel**: both checks are evaluated — a message is dropped if *either* condition is met

| Cancel Type | Scope | Granularity | Use Case |
|---|---|---|---|
| `OutPort::cancelInFlight()` | All in-flight | All-or-nothing | Full pipeline flush |
| `InPort::flush<KeyFn>(FlushRange)` | Earlier enqueue cycles | Explicit open/closed key range | Precise architectural squash |
| `InPort::flush()` | Receiver queue | Immediate drop | Clear local queue |

**Delay-0 caveat:** A range flush intentionally treats every message enqueued in
the flush cycle as post-flush. For an exact mid-cycle all-or-nothing cutoff, use
the sender-owned `OutPort::cancelInFlight()` epoch instead.

## Build-Time Non-Cancelable OutPort Path

Models that never call `OutPort::cancelInFlight()` can configure Chronon with:

```bash
cmake -S . -B build -DCHRONON_ENABLE_OUTPORT_CANCELLATION=OFF
```

The default is `ON` and preserves full cancellation semantics. `OFF` is an
explicit whole-build contract: `cancelInFlight()` becomes a no-op, the sender
path omits its cancellation-epoch acquire, direct MPSC lane envelopes omit the
epoch stamp, and `PortEnvelope` omits the cancellation pointer and snapshot.
Receiver-side `InPort` selective flush remains available.

Do not disable the option merely because cancellation is rare. It is safe only
after auditing the complete model and every linked component for
`cancelInFlight()` calls. Chronon's bundled CPU pipeline examples, their flush
validation test, and their counter-metadata test are therefore not built when
this option is `OFF`; those models rely on sender-side cancellation for correct
flush semantics.

## Automatic Shared Delay-One Broadcast

Normal `OutPort`/`InPort` graphs automatically use a shared data plane when an
entire connected Port component is safe for the optimization: every source has
at least four destinations, every edge has delay one, payloads are copyable,
destinations are unbounded, and no registered edge capacity or rate changes the
admission contract. Selection is all-or-nothing for the component.

`SharedBroadcastTransport` stores each source payload once and gives every
destination connection an independent cache-line-aligned cursor. On the receive
side, `SharedBroadcastQueueAdapter` exposes those cursors through the same
`IMessageQueue` contract as SPSC and MPSC transports. Consequently `hasData`,
`minArrivalCycle`, `tryReceiveFiltered`, receiver cancellation, bulk receive,
queue statistics, and `flush` all use the normal `InPort` control plane. A slow
or flushed consumer changes only its own cursor and cannot destroy another
consumer's replay.

This optimization is transparent to models. Set
`CHRONON_EXPERIMENTAL_TRANSPARENT_BROADCAST=0` only for A/B diagnosis.

## Explicit Shared Delay-One Broadcast Fabric

`DelayOneBroadcastFabric<T, P, C>` is an explicit specialization for a complete
`P`-producer by `C`-consumer, delay-one broadcast bus. It stores each producer
payload once per source cycle and lets all consumers replay it in stable
producer-id and send order. The declared ports remain dependency edges, while
their physical queues are disabled after topology validation.

```cpp
using WakeupBus = DelayOneBroadcastFabric<Wakeup, 10, 11, 512, 8>;

WakeupBus bus;
bus.bindProducer(0, producer0.out_wakeup);
bus.bindConsumer(0, consumer0.in_wakeup);
// Bind every stable producer and consumer id after graph construction.
bus.sealPortTopology();

// Producer tick at cycle S:
if (producer0.out_wakeup.sendImmediate(wakeup)) {
    bus.publish(0, producer0.localCycle(), wakeup);
}

// Consumer tick at cycle S+1 (must be called once per local cycle):
bus.consume(0, consumer0.localCycle(), process_wakeup);
```

The fabric deliberately does not emulate bounded destination queues,
receiver-side selective cancellation, or `OutPort::cancelInFlight()`. Use it
only when the model has proved those operations are absent and every bound edge
has delay one. `publish()` preserves the normal delay-one consumer wakeup, so
activity-scheduled units may jump over empty cycles; they must call `consume()`
on every tick in which they run. `sealPortTopology()` validates the complete
delay-one fanout before changing any connection to dependency-only transport
and exposes the finite ring depth as scheduler headroom. When necessary, the
lookahead scheduler adds reverse dependencies that prevent a producer from
wrapping an unread bucket.

For new models, prefer normal Ports and their automatic shared transport. The
explicit fabric remains available for existing code that intentionally owns a
fixed compile-time ring and calls `publish()`/`consume()` itself.

## Usage Pattern

```cpp
class Producer : public TickableUnit {
    OutPort<int> out{this, "out"};
    int count_ = 0;

public:
    void tick() override {
        if (out.canSend()) {
            out.send(count_++);
        }
    }
};

class Consumer : public TickableUnit {
    InPort<int> in{this, "in"};
    int sum_ = 0;

public:
    void tick() override {
        if (auto value = in.tryReceive(localCycle())) {
            sum_ += *value;
        }
    }
};
```

## Port Registration

Ports auto-register when constructed with owner and name:

```cpp
class MyUnit : public TickableUnit {
    // Ports auto-register - no manual registration needed
    OutPort<Data> out{this, "out"};
    InPort<Data> in{this, "in"};

public:
    MyUnit() : TickableUnit("my_unit") {
        setTreeNode(node);  // Triggers pending port registration
    }
};
```

## Queue Types

The framework auto-selects queue type based on thread topology during simulation initialization:

| Queue Type | Selection Criteria | Implementation | Overhead |
|------------|-------------------|----------------|----------|
| `SingleThreadMessageQueue` | Producer and consumer on same thread | Direct queue access | Zero |
| `LockFreeQueueAdapter` | Cross-thread, single producer (SPSC) | Lock-free ring buffer with atomics | Near-zero |
| `MultiProducerQueueAdapter` | Multiple Connections into one InPort | Direct per-Connection SPSC lanes + deterministic frontier | Near-zero |

**Manual queue type selection:**

You can override automatic selection by calling queue switching methods on `InPort` during initialization (before simulation starts):

```cpp
// Same-thread optimization (no mutex overhead)
consumer->in.useSingleThreadQueue();

// Cross-thread SPSC/MPSC selected from source-thread count
consumer->in.configureForSourceThreadCount(source_thread_count);

// Cross-thread SPSC (lock-free)
consumer->in.useLockFreeQueue();

// Cross-thread MPSC (private ingress lanes; bounded ports add a shared FIFO)
consumer->in.useMultiProducerQueue();
```

**MPSC mode:**

For MPSC destinations, initialization assigns one dedicated lane to every
`Connection` (the legacy API calls the stable connection key a `thread_id`).
These low-level methods are intended for framework initialization and tests;
normal models use `OutPort::send()` and `InPort::tryReceive()`:

```cpp
// During initialization, each producer thread registers
size_t queue_id = consumer->in.registerProducerThread(thread_id);

// Later, during sending
if (consumer->in.canAcceptOnThreadQueue(queue_id)) {
    consumer->in.pushToThreadQueue(queue_id, data, arrive_cycle);
}
```

The producer publishes the envelope directly into its private SPSC ingress
lane. There is no contended global tail, producer-side arbitration, or scheduler
arbitration pass. The consumer merges lane heads by
`(arrive_cycle, stable_connection_id, lane_id)`. Small fan-in uses a direct
scan; large fan-in uses sharded activity notifications and a consumer-owned
min-heap.

An unbounded InPort consumes the selected lane slot in place, preserving the
direct-lane fast path. A bounded InPort has a preallocated, receiver-only shared
ring. Immediately before the destination Unit ticks, it admits ready lane heads
into that ring in deterministic merge order. Both aggregate occupancy and new
admissions in one receiver cycle are capped by `InPort::capacity()`.
Connection/YAML registered capacity is applied to an otherwise-unbounded
destination before MPSC transport selection, so it enforces this same aggregate
depth rather than acting only as per-lane transport headroom.

Private lanes are transport storage, not extra architectural FIFO entries.
`queuedMessageCount()` reports the shared destination FIFO;
`transportPendingMessageCount()` exposes ingress backlog for diagnostics. A
successful `send()` means the Connection accepted the message into its ingress
lane. If the shared FIFO remains full, the receiver stops draining ingress;
those bounded lanes then fill and propagate non-blocking backpressure to their
individual producers.

Bounded ingress lanes and the aggregate destination FIFO allocate power-of-two
storage once during configuration; receive performs no allocation. Unlimited
model-visible ports retain bounded physical storage. Physical storage,
simulated-cycle admission credit, aggregate FIFO admission, and per-edge rate
are separate checks; a same-cycle host pop never nondeterministically reopens
model capacity.
For bounded edges whose endpoints share one epoch-free worker, a non-atomic
consumer-owned pop summary preserves that same cycle-start contract when the
lookahead floor temporarily changes cluster execution order. Sequential and
unbounded same-thread queues bypass this ledger, and no same-thread path takes a
lock.

## Per-Cycle Capacity (OutPort Bandwidth Limiting)

OutPort supports per-cycle send limits to model hardware bandwidth constraints. This separates two distinct hardware concepts:

- **InPort capacity** = buffer depth (how many items can be queued)
- **OutPort per-cycle capacity** = wire/bus bandwidth (how many items can be sent per cycle)

### Design Philosophy

In real hardware, a 4-wide decode stage can produce at most 4 micro-ops per cycle — regardless of how large the downstream queue is. Previously, this was modeled by setting `InPort.capacity` to the pipeline width, conflating buffer depth with bandwidth. This led to unrealistic buffer sizes (e.g., 128-entry wakeup port when only ~4 wakeups occur per cycle).

Per-cycle capacity on OutPort is:
- **Thread-safe without atomics**: Only the owning unit's thread calls `canSend()`/`send()`
- **Explicitly unbounded when needed**: Passing 0 or `UNLIMITED_CAPACITY` opts out of the rate cap
- **Low overhead**: No atomics; only the producing unit tracks its per-cycle sends

### Usage

**Compile-time constant** (for fixed-width ports):
```cpp
// 1 flush event per cycle max
OutPort<FlushSignal> out_flush{this, "out_flush", 1};

// 1 dispatch per IQ per cycle
OutPort<InstData> out_iq0{this, "out_iq0", 1};
```

**Runtime parameterized** (for configurable pipeline widths):
```cpp
// In constructor:
OutPort<InstData> out_uop_queue{this, "out_uop_queue", 1};

// In initialize():
out_uop_queue.setPerCycleCapacity(num_to_decode_);  // set from YAML param
```

### Behavior

- `canSend()` returns `false` when per-cycle limit is reached (fast-path check before destination checks)
- `send()` returns `false` without attempting delivery when limit is reached
- Failed sends (InPort full) do **not** consume a bandwidth slot
- Counter resets automatically on cycle boundary (lazy reset on next access)
- `cancelInFlight()` does **not** reset the per-cycle counter

### Query Methods

```cpp
out.perCycleCapacity();    // UNLIMITED_CAPACITY = unlimited
out.sentThisCycle();       // sends completed this cycle (0 if unlimited)
out.remainingThisCycle();  // remaining capacity (UNLIMITED_CAPACITY if unlimited)
```

## Back Pressure

Handle back pressure when destination queue is full:

```cpp
void tick() override {
    if (out.canSend()) {
        out.send(data);
    } else {
        // Queue full - stall or buffer locally
        pending_data_ = data;
    }
}
```

**Notes:**
- `send()` remains non-blocking. It returns `false` when the destination SPSC
  queue or the Connection's MPSC ingress lane is full.
- Lock-free SPSC/MPSC queues use monotonic absolute tickets and power-of-two
  masking. Every physical slot is usable; no sentinel slot is reserved. Bounded
  rings are rounded up at initialization. Unlimited model-visible ports still
  have bounded physical storage protected by scheduler headroom.

**MPSC back pressure behavior:**

In multi-producer mode, back pressure is checked per-producer queue:

```cpp
// Check specific producer's queue capacity
if (in_port.canAcceptOnThreadQueue(queue_id)) {
    in_port.pushToThreadQueue(queue_id, data, arrive_cycle);
} else {
    // This producer's queue is full - stall
    stalled_ = true;
}
```

Each producer connection owns one direct SPSC ingress lane. One producer filling
its lane does not contend on a global enqueue tail or directly block unrelated
lanes. For bounded destinations, a full shared FIFO stops receiver admission;
backpressure then reaches each producer as its own ingress fills. The consumer
processes heads in arrival-cycle order with a stable connection-id tiebreak.
Receiver-side filters and selective flushes run after deterministic admission,
preserve enqueue-cycle scope, and permanently consume rejected messages.

## Multiple Connections

OutPort supports multiple destinations (fanout):

```cpp
sim.connect(source->out, consumer1->in, 1);
sim.connect(source->out, consumer2->in, 1);
// Both consumers receive the same data
```

## Port Design Decisions

**Delay at connection time, not per-message:**
- Delay represents physical wire/bus latency
- Simplifies message handling
- Enables static dependency analysis

**Messages are pure data:**
- Timestamps are framework-internal
- Users don't manage timing manually
- Framework handles scheduling

```wavedrom
{ "signal": [
  { "name": "clk",          "wave": "p........" },
  {},
  { "name": "send()",       "wave": "x=x......", "data": ["val"] },
  { "name": "send_cycle",   "wave": "x=x......", "data": ["T"] },
  {},
  { "name": "(delay = 3)",  "wave": "x........" },
  {},
  { "name": "arrive_cycle", "wave": "x...=x...", "data": ["T+3"] },
  { "name": "tryReceive()", "wave": "x...=x...", "data": ["val"] }
],
  "head": { "text": "Port timing: arrive_cycle = send_cycle + delay" },
  "config": { "hscale": 2 }
}
```
