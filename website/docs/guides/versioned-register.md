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

`VersionedRegister<T, HistoryDepth>` maintains a ring buffer of `(value, cycle)` pairs. Writes are timestamped; reads are temporally filtered.

```cpp
#include "chronon/Chronon.hpp"
using namespace chronon;

VersionedRegister<uint64_t, 16> mip{0};

// Writer (CLINT at cycle 1000):
mip.write(0x80, 1000);       // MTIP set at cycle 1000

// Reader (ROB at cycle 100 -- does NOT see the future write):
uint64_t val = mip.read(100);  // returns 0 (pre-write value)

// Reader (ROB at cycle 1000 -- sees the write):
uint64_t val = mip.read(1000); // returns 0x80
```

## API

```cpp
template <typename T, size_t HistoryDepth = 16>
class VersionedRegister {
    // Write a new value at the given simulation cycle.
    void write(T value, uint64_t write_cycle);

    // Read the most recent value with write_cycle <= reader_cycle.
    T read(uint64_t reader_cycle) const;

    // Read the latest value unconditionally (sequential mode fast-path).
    T readLatest() const;

    // Reset all versions to the given value at cycle 0.
    void reset(T value = T{});
};
```

### Thread Safety

A single atomic `write_head_` with acquire/release ordering allows one writer and multiple readers on different threads without locks. For multiple concurrent writers, ensure monotonically increasing `write_cycle` values (naturally satisfied when each writer uses its own `localCycle()`).

### History Depth

The `HistoryDepth` template parameter controls how many versions are retained. If a reader's cycle is older than all retained versions (skew exceeds buffer depth), the oldest version is returned as a best-effort fallback.

Choose `HistoryDepth` based on the maximum expected lookahead skew between writers and readers:

| Use Case | Typical Depth | Rationale |
|----------|---------------|-----------|
| Physical register file | 16 | Execution units are close in cycle to retirement |
| Cycle/instret counters | 32 | Island nodes (MMIO devices) can race ahead by `epoch_size` |
| Interrupt pending (MIP) | 16 | Written by CLINT/PLIC, read by ROB |

## Usage Pattern

The typical pattern for shared state accessed by multiple Chronon units:

```cpp
// In a shared data structure (e.g., CSRFile):
VersionedRegister<uint64_t, 32> cycle_counter_{0};

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
