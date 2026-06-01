---
sidebar_label: "Versioned Register"
---

# Versioned Register

Lock-free temporal register for lookahead-safe shared state.

## Problem

In parallel lookahead execution, units advance at different simulation cycles. When they share mutable state via direct function calls (not ports), a writer at cycle N+K **contaminates** a reader at cycle N:

```
CLINT at cycle 1000  writes MIP[MTIP] = 1  (timer fires)
ROB   at cycle 100   reads  MIP[MTIP] = 1  (sees future interrupt!)
```

Ports solve this with timestamped message queues (`arrive_cycle`), but shared registers (CSRs, physical register files) have no such mechanism.

## Solution

`VersionedRegister<T>` maintains a ring buffer of `(value, cycle)` pairs. The depth (number of retained versions) is set at construction — use `DependencyGraph::requiredVersionedRegisterDepth()` after `initialize()` to compute it from the topology. Writes are timestamped; reads are temporally filtered.

```cpp
#include "chronon/Chronon.hpp"
using namespace chronon;

VersionedRegister<uint64_t> mip{0, /*depth=*/100};

// Writer (CLINT at cycle 1000):
mip.write(0x80, 1000);       // MTIP set at cycle 1000

// Reader (ROB at cycle 100 -- does NOT see the future write):
uint64_t val = mip.read(100);  // returns 0 (pre-write value)

// Reader (ROB at cycle 1000 -- sees the write):
uint64_t val = mip.read(1000); // returns 0x80
```

## API

```cpp
template <typename T>
class VersionedRegister {
    // Default-construct (value = T{}, depth = 16).
    VersionedRegister();
    // Construct with initial value and depth (from requiredVersionedRegisterDepth).
    VersionedRegister(T initial_value, uint32_t depth = 16);

    // Write a new value at the given simulation cycle.
    void write(T value, uint64_t write_cycle);

    // Read the most recent value with write_cycle <= reader_cycle.
    T read(uint64_t reader_cycle) const;

    // Read the latest value unconditionally (sequential mode fast-path).
    T readLatest() const;

    // Reset all versions to the given value at cycle 0.
    void reset(T value = T{});

    // Number of retained versions.
    uint32_t depth() const;
};
```

### Thread Safety

A single atomic `write_head_` with acquire/release ordering allows one writer and multiple readers on different threads without locks. For multiple concurrent writers, ensure monotonically increasing `write_cycle` values (naturally satisfied when each writer uses its own `localCycle()`).

### Depth

The constructor `depth` parameter controls how many versions are retained. If a reader's cycle is older than all retained versions (skew exceeds depth), the oldest version is returned as a best-effort fallback.

Use `DependencyGraph::requiredVersionedRegisterDepth(writer, readers, max_lookahead)` to compute the correct depth after `initialize()`. The result is `max_skew + 1` — the extra slot retains the baseline version that the slowest reader may still need. Do not use `max_lookahead_cycles` directly as the depth; that is off by one.

## Usage Pattern

The typical pattern for shared state accessed by multiple Chronon units:

```cpp
// In a shared data structure (e.g., CSRFile):
VersionedRegister<uint64_t> cycle_counter_;

// Construct with depth from requiredVersionedRegisterDepth():
CSRFile(uint32_t depth) : cycle_counter_(uint64_t{0}, depth) {}

void incrementCycle(uint64_t at_cycle) {
    uint64_t cur = cycle_counter_.readLatest();
    cycle_counter_.write(cur + 1, at_cycle);
}

uint64_t getCycle(uint64_t reader_cycle) const {
    return cycle_counter_.read(reader_cycle);
}

// Callers pass their localCycle():
// ROB:   csr_file->incrementCycle(localCycle());
// CLINT: uint64_t mtime = csr_file->getTime(localCycle());
```

For backward compatibility, methods can default to `readLatest()`:

```cpp
static constexpr uint64_t LATEST = UINT64_MAX;

uint64_t getCycle(uint64_t reader_cycle = LATEST) const {
    return (reader_cycle == LATEST) ? cycle_counter_.readLatest()
                                    : cycle_counter_.read(reader_cycle);
}
```

## When to Use

Use `VersionedRegister` when:
- Two or more Chronon units share mutable state via function calls (not ports)
- The units can be at different simulation cycles (parallel/lookahead execution)
- A reader must not see writes from future cycles

Do NOT use when:
- Communication already goes through ports (ports have built-in timestamped delivery)
- State is only written at one specific pipeline point (e.g., ROB retirement) and all readers are on the same thread at the same cycle
- State is immutable or append-only
