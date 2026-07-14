---
sidebar_label: "Counter System"
---

# Counter System

## Overview

Chronon provides per-unit counters via the `Counter` class. Each unit instance declares its own counters as members, and they are automatically registered with the observation system when the unit's context is attached.

## Counter (Per-Unit)

Declare counters as unit members for automatic per-instance tracking:

```cpp
class ALUUnit : public TickableUnit, public ObservableUnit {
    // Per-instance counters - each ALU has its own
    Counter ops_{this, "ops", "Operations executed", "ops"};
    Counter stalls_{this, "stalls", "Stall cycles", "cycles"};

public:
    void tick() override {
        ++ops_;          // ~2-3ns
        stalls_ += 5;    // Add 5
    }
};
```

### Output (CSV)

**Pivoted format** (default, `csv_format: pivoted`):
Rows are dump cycles, columns are counters — compact and written incrementally:

```csv
cycle,alu0.ops,alu1.ops,alu2.ops
10000,2500,2400,2550
20000,5100,4900,5050
30000,7650,7300,7600
```

Each periodic dump appends one row, so the file can be monitored during simulation (like log files). Column names are discovered from the first dump and written as the CSV header.

All schedulers use one owner-based push implementation. Sequential and barrier
execution publish at the exact boundary. Lookahead workers snapshot and reset
the counters owned by their scheduler clusters when local progress crosses the
boundary, then publish records through their lock-free SPSC observation queue.
Rows use the nominal periodic cycle; lookahead contributors may be sampled
within one configured run-ahead window. Periodic output never introduces a run
split, intermediate MPSC port flush, or worker relaunch.

Chronon supports up to 64 observation producer threads in one process. If that
pool is exhausted, periodic counter execution fails explicitly instead of
silently omitting owner rows.

**Long format** (`csv_format: long`):
One row per (cycle, unit, counter, value) — traditional streaming format:

```csv
cycle,unit_name,counter_name,value
10000,alu0,ops,2500
10000,alu1,ops,2400
10000,alu2,ops,2550
```

### Delta Semantics

Counter values are **per-interval deltas** — each row shows only the count for that dump interval. Counters reset to zero at the source after each periodic dump snapshot.

```csv
cycle,alu0.ops,alu1.ops
10000,2500,2400
20000,2600,2500
30000,2550,2400
```

This makes interval-local rate metrics (hit rate, IPC, stall rate) directly computable from a single row without post-processing.

### Counter API

```cpp
class Counter {
public:
    Counter(ObservableUnit* owner,
            std::string_view name,
            std::string_view description = "",
            std::string_view unit = "");

    Counter& operator++() noexcept;
    Counter& operator+=(uint64_t delta) noexcept;

    uint64_t get() const noexcept;
    void reset() noexcept;
};
```

## Hierarchical Naming

Counter paths include the unit's full tree hierarchy:

```
# With unit.setTreeNode(node)
cpu0.alu0.ops
cpu0.alu1.ops
cpu1.alu0.ops
```

**Note:** Hierarchical naming depends on the unit being attached to a `TreeNode` hierarchy via `setTreeNode()`. Without this, counter names use the unit's local name only.

## Storage Architecture

### SimpleCounter

Optimized for Chronon's single-threaded-per-unit model:

```cpp
struct SimpleCounter {
    uint64_t value = 0;
    uint64_t epoch_base = 0;  // For rollback

    void increment(uint64_t delta = 1) noexcept { value += delta; }
    uint64_t get() const noexcept { return value; }

    void commitEpoch() noexcept;    // epoch_base = value
    void rollbackEpoch() noexcept;  // value = epoch_base
};
```

### FixedCounterStorage

Dynamic array that grows as counters are added:

```cpp
class FixedCounterStorage {
public:
    // Constructor starts empty; grows via addCounter()
    explicit FixedCounterStorage(std::string name);

    CounterId addCounter(const std::string& name,
                         const std::string& description = "",
                         const std::string& unit = "");

    SimpleCounter& getUnchecked(CounterId id) noexcept;  // ~1-2ns

    void commitAllEpochs();
    void rollbackAllEpochs();
    void resetAll();
};
```

**Sizing Behavior:**
- Starts empty, grows as `Counter` members call `addCounter()` during context attachment
- Enables O(1) `getUnchecked()` access without bounds checking

### Memory Comparison

Memory usage scales with the number of counters per unit:

| Architecture | Per-Unit (17 counters) | 9 Units (17 counters each) |
|--------------|------------------------|----------------------------|
| Old (Dense) | 64 KB | 576 KB |
| **New (Sparse)** | **272 bytes** | **2.4 KB** |
| **Savings** | **99.6%** | **99.6%** |

**Formula:** Per-unit memory = `N counters × 16 bytes`

## Registration and Snapshot Paths

Counters register with ObservationManager at initialization:

```
1. Unit Construction
   └─► Counter members created (pending)

2. Context Attachment
   └─► Counter::onContextAttached(ctx)
       └─► ctx.counters().addCounter(name, desc, unit)

3. Periodic Counter Snapshots
   └─► scheduler owner snapshots its cluster counters
       └─► worker SPSC queue ─► observation backend

4. Final Counter Snapshot
   └─► manager.dumpFinalCounterSnapshot(final_cycle)
       └─► Read remaining values from registered addresses
```

## Epoch Operations

For lookahead/speculative execution:

```cpp
SimpleCounter counter;

counter.increment(100);
counter.commitEpoch();  // epoch_base = 100

// Speculative execution
counter.increment(50);  // value = 150

// Rollback on misprediction
counter.rollbackEpoch();  // value = 100

// Or commit on success
counter.commitEpoch();  // epoch_base = 150
```

## Derived Counters

Derived counters compute values from raw counters at CSV dump time. They add zero overhead to the simulation hot path — all computation happens in the ObservationBackend thread.

### Declaration

```cpp
class Fetch : public TickableUnit, public ObservableUnit {
    Counter hits_{this, "hits", "Cache hits"};
    Counter misses_{this, "misses", "Cache misses"};
    Counter retired_{this, "retired", "Instructions retired"};
    Counter cycles_{this, "cycles", "Active cycles"};

    // Convenience formula: a / (a + b)
    DerivedCounter hit_rate_{this, "hit_rate", "Cache hit rate",
        {hits_, misses_}, DerivedFormula::Ratio};

    // Custom lambda — any computation
    DerivedCounter ipc_{this, "ipc", "Instructions per cycle",
        {retired_, cycles_},
        [](std::span<const uint64_t> v) {
            return v[1] > 0 ? double(v[0]) / v[1] : 0.0;
        }};

    // Multi-source with arbitrary formula
    DerivedCounter branch_mpki_{this, "branch_mpki", "Branch MPKI",
        {mispred_, retired_}, DerivedFormula::PerKilo};
};
```

### Available Convenience Formulas

| Formula | Computation | Use Case |
|---------|------------|----------|
| `DerivedFormula::Ratio` | `a / (a + b)` | Hit rates, miss rates |
| `DerivedFormula::Divide` | `a / b` | IPC, throughput |
| `DerivedFormula::PerKilo` | `a * 1000 / b` | MPKI metrics |

Custom lambdas receive a `std::span<const uint64_t>` with the per-interval delta values of each source counter (in declaration order) and return a `double`.

### CSV Output

**Pivoted format:**
```csv
cycle,fetch.hits,fetch.misses,fetch.hit_rate,fetch.ipc
10000,850,150,0.850000,1.234567
20000,900,100,0.900000,1.345678
```

**Long format:**
```csv
cycle,unit,counter_name,value
10000,fetch,hits,850
10000,fetch,misses,150
10000,fetch,hit_rate,0.850000
```

### Edge Cases

| Case | Behavior |
|------|----------|
| Division by zero (all sources = 0) | Depends on lambda; convenience formulas return `0.0` |
| Source counter not in first dump batch | Derived counter skipped with stderr warning |
| Any number of source counters | Supported (not limited to 2) |

### Backend Semantics

Derived values are computed from **per-interval deltas** (same as raw counters). In pivoted mode, the derived columns appear after all raw counter columns. The computation function is called once per CSV row flush.

## Complete Example

```cpp
#include "chronon/Chronon.hpp"
using namespace chronon;

class ALUUnit : public TickableUnit, public ObservableUnit {
    Counter ops_{this, "ops", "Operations", "ops"};
    uint32_t id_;

public:
    ALUUnit(uint32_t id)
        : TickableUnit("alu" + std::to_string(id)), id_(id) {}

    void tick() override {
        ++ops_;  // Per-instance
    }

    uint64_t getOps() const { return ops_.get(); }
};

int main() {
    TickSimulation sim;
    auto* alu0 = sim.createUnit<ALUUnit>(0);
    auto* alu1 = sim.createUnit<ALUUnit>(1);

    // ... setup observation contexts ...

    sim.run(1000000);

    std::cout << "ALU0 ops: " << alu0->getOps() << "\n";
    std::cout << "ALU1 ops: " << alu1->getOps() << "\n";
}
```
