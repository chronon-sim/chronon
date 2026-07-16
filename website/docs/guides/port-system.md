---
sidebar_label: "Port System"
---

# Port System

## Overview

Ports provide type-safe communication between units with timestamped message queues for deterministic delivery. The connection delay determines latency; the queue type is automatically selected based on thread topology during simulation initialization.

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
    void useMultiProducerQueue();     // Cross-thread MPSC (per-thread queues)
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

    // Receive all available messages
    std::vector<T> receiveAll(uint64_t current_cycle);
    std::vector<T> receiveAll();  // Uses owner->localCycle()

    // Sender-based receive (for composition)
    PortReceiveSender<T> receive();

    // Drop all queued messages (including future arrivals)
    void flush();

    // Receiver-side selective cancellation (in-flight scoped by default)
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
    size_t queuedMessageCount() const;
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
// - MultiProducerQueueAdapter: Cross-thread MPSC (multiple producers)
sim.connect(producer->out, consumer->in, 5);

// Cancellation / flush (CPU squash / pipeline flush)
// - cancelInFlight() invalidates messages already sent but not yet received
// - flush() drops queued messages immediately
producer->out.cancelInFlight();
consumer->in.flush();
```

## Selective Cancellation

Selective cancellation is available on **InPort** (receiver-side). It allows fine-grained key-based filtering of in-flight messages without dropping everything.

`cancelOlderThan<KeyFn>(watermark)` selectively drops in-flight messages where `KeyFn(msg) < watermark`, without affecting newer messages.

`cancelYoungerThan<KeyFn>(watermark)` selectively drops in-flight messages where `KeyFn(msg) > watermark`, without affecting older messages.

`resetSelectiveCancellation()` clears LegacyFastPath receiver-side bounds. It is intentionally a no-op for `PortPolicy::StageSelective`, where flush predicates retire automatically on the receiver pop path.

```cpp
// Receiver-side selective filtering on the destination port
in_instr.cancelOlderThan<&Instruction::id>(flush_point_id + 1);
in_instr.cancelYoungerThan<&Instruction::id>(flush_point_id);
in_instr.resetSelectiveCancellation();

// All-or-nothing cancel on OutPort
out_icache_miss.cancelInFlight();
```

**API:**
```cpp
template<auto KeyFn, typename K>
void InPort<T>::cancelOlderThan(K watermark);

template<auto KeyFn, typename K>
void InPort<T>::cancelYoungerThan(K watermark);

template<auto KeyFn, typename MinK, typename MaxK>
void InPort<T>::cancelOutsideInclusive(MinK min_keep, MaxK max_keep);
```

- `KeyFn` — pointer-to-member or callable that extracts a key from `T` (e.g., `&MyMsg::id`), projected to `uint64_t`
- `watermark` — lower/upper threshold used by the selected API

**Semantics:**
- **Legacy monotonic bounds**: `LegacyFastPath` raises only the lower bound and lowers only the upper bound for its current in-flight generation
- **Timestamp-scoped range**: `StageSelective` installs a receiver-only `{flush_cycle, min_keep, max_keep}` predicate. Same-cycle calls intersect their ranges; messages enqueued at or after the flush cycle are unaffected
- **Fixed-delay ordering**: `StageSelective` predicate retirement relies on monotonic enqueue cycles and is intended for fixed-delay stage-register ports
- **InPort state**: receiver-side bounds are tracked per input port
- **Single extractor per InPort**: first `KeyFn` set on an input port is retained; mismatched extractors are ignored
- **Low overhead when unused**: receive path does one extractor-pointer load and exits immediately when selective cancellation is not configured
- **Composable with epoch cancel**: both checks are evaluated — a message is dropped if *either* condition is met
- **In-flight default for InPort**: first receiver-side selective call auto-scopes to currently in-flight messages (including future-arrival queued messages). New enqueues are unaffected unless re-scoped/reset.

| Cancel Type | Scope | Granularity | Use Case |
|---|---|---|---|
| `OutPort::cancelInFlight()` | All in-flight | All-or-nothing | Full pipeline flush |
| `InPort::cancelOlderThan<KeyFn>(wm)` | Key-based | Selective | Squash older than misprediction point |
| `InPort::cancelYoungerThan<KeyFn>(wm)` | Key-based | Selective | Squash younger speculative messages |
| `InPort::cancelOutsideInclusive<KeyFn>(min,max)` | Key-based | Selective range | Keep only bounded window |
| `InPort::flush()` | Receiver queue | Immediate drop | Clear local queue |

**Delay-0 caveat:** For zero-delay (INLINE) connections, messages are delivered in the same cycle they are sent. Selective cancel still works but the window between send and receive is very small — cancel must occur before `tryReceive()` on the same cycle.

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
| `MultiProducerQueueAdapter` | Cross-thread, multiple producers (MPSC) | Per-producer SPSC rings + merge | Near-zero |

**Manual queue type selection:**

You can override automatic selection by calling queue switching methods on `InPort` during initialization (before simulation starts):

```cpp
// Same-thread optimization (no mutex overhead)
consumer->in.useSingleThreadQueue();

// Cross-thread SPSC/MPSC selected from source-thread count
consumer->in.configureForSourceThreadCount(source_thread_count);

// Cross-thread SPSC (lock-free)
consumer->in.useLockFreeQueue();

// Cross-thread MPSC (per-thread queues)
consumer->in.useMultiProducerQueue();
```

**MPSC mode:**

For MPSC destinations, producer threads register dedicated per-thread queues during
simulation initialization. Each producer thread gets its own queue ID:

```cpp
// During initialization, each producer thread registers
size_t queue_id = consumer->in.registerProducerThread(thread_id);

// Later, during sending
if (consumer->in.canAcceptOnThreadQueue(queue_id)) {
    consumer->in.pushToThreadQueue(queue_id, data, arrive_cycle);
}
```

The consumer polls all per-producer queues and merges messages in arrival-cycle order.
For bounded `InPort` capacities, Chronon sizes both the per-connection MPSC
staging ring and the downstream per-producer lock-free ring to cover the declared
capacity. Unlimited-capacity ports keep a bounded default physical ring, so a
model that needs large cross-thread burst tolerance should prefer an explicit
bounded capacity. Back pressure is checked per queue:
`canAcceptOnThreadQueue(queue_id, pending)` checks the per-producer cycle budget
plus the specific producer queue's physical capacity. `isFull()`,
`canAcceptFromProducer()`, and the old `canAcceptThreadQueue*()` names remain as
deprecated compatibility aliases.

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
- `send()` remains non-blocking. It returns `false` when the destination
  queue (or per-thread MPSC queue) is full.
- Lock-free SPSC/MPSC queues use ring buffers with one reserved slot; effective
  storable capacity is `N-1`. For bounded ports, the physical ring is rounded up
  at initialization so the declared `InPort` capacity is storable. For unlimited
  ports, the physical lock-free ring remains bounded by the default ring size.

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

Each producer connection has its own staging ring, and the MPSC adapter has a
per-producer lock-free queue sized from the destination port capacity. One
producer filling its queue does not block other producers until the consumer
polls and drains queues. The consumer processes all per-producer queues in
arrival-cycle order with a stable connection-id tiebreak.

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
