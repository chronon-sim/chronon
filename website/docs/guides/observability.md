---
sidebar_label: "Observability System"
---

# Observability System

## Overview

Chronon provides a unified observability system with three integrated capabilities:

| Feature | Purpose | API | Hot Path |
|---------|---------|-----|----------|
| Counters | Statistics collection | `++counter_` / `counter_ += n` | ~1-2ns |
| Traces | Structured event capture | `trace<"fmt">(CAT, ...)` | ~2ns disabled |
| Logs | Debug output | `debug<"fmt">(...)` | ~2ns disabled |

## Design Principles

- **Zero overhead when disabled**: No runtime cost
- **Minimal overhead when enabled**: Pre-registered format strings
- **Lock-free hot path**: No mutex contention
- **Lookahead-compatible**: Thread-local counters, buffered events

## Quick Start

### Define Categories and Counters

```cpp
#include "chronon/Chronon.hpp"
using namespace chronon;

// Categories - bit positions auto-assigned at program startup
// Category<> objects auto-register when constructed as global/static variables
inline const auto CACHE_HIT = Category<"cache_hit", "Cache hit events">{};
inline const auto CACHE_MISS = Category<"cache_miss", "Cache miss events">{};

```

### Use in Units

```cpp
class FetchUnit : public TickableUnit, public ObservableUnit {
    Counter cache_hits_{this, "cache_hits", "Cache hit count"};

public:
    void tick() override {
        // Increment counter
        ++cache_hits_;

        // Emit trace with compile-time format string
        trace<"Cache HIT: pc=0x{:x}">(CACHE_HIT, pc);

        // Logging
        debug<"Fetch cycle {} pc=0x{:x}">(localCycle(), pc);
        info<"Started fetching">();
    }
};
```

### Initialize and Run

For most users, `SimulationApp` handles observation setup automatically from YAML:

```cpp
int main(int argc, char* argv[]) {
    return chronon::SimulationApp("My Simulator")
        .setDefaultConfig("config.yaml")
        .run(argc, argv);
}
```

For manual setup without `SimulationApp`:

```cpp
int main() {
    TickSimulation sim;
    auto* fetch = sim.createUnit<FetchUnit>();

    auto& obs = ObservationManager::instance();
    obs.initialize(yaml_config);

    auto* ctx = obs.createContextForUnit(
        "fetch", [&]() { return sim.currentCycle(); });
    fetch->setObservationContext(ctx);

    // Enable trace categories
    ctx->filter().enableCategory(CACHE_HIT);
    ctx->filter().setMinLogLevel(LogLevel::Debug);

    obs.startBackend();

    sim.initialize();
    sim.run(1000);

    obs.stopBackend();
    obs.shutdown();
}
```

## Log Levels

```cpp
enum class LogLevel : uint8_t {
    Debug = 0,  // Verbose debugging
    Info  = 1,  // General information
    Warn  = 2,  // Warnings
    Error = 3   // Errors
};

ctx.filter().setMinLogLevel(LogLevel::Info);  // Debug filtered
```

## Category Filtering

O(1) bitmask-based filtering:

```cpp
ctx.filter().enableCategory(CACHE_HIT);
ctx.filter().disableCategory(CACHE_MISS);

if (ctx.shouldTrace(CACHE_HIT)) { /* ... */ }
```

## Lookahead Support

For speculative execution:

```cpp
ctx.setLookaheadMode(true);

// Speculative work
counter_ += 10;
trace<"Speculative event">(CAT, value);

if (mispredicted) {
    ctx.rollbackEpoch();  // Counters restored, events discarded
} else {
    ctx.commitEpoch();    // Epoch advances, events flushed
}

ctx.setLookaheadMode(false);
```

## Output Files

```
out/
└── 20260124_143052/
    ├── counters.csv         # Counter snapshots
    ├── timeline.pftrace     # Perfetto protobuf timeline (traces, counters, scheduler)
    └── events.log           # Text logs (debug/info/warn/error, optional trace mirror)
```

- **events.log** holds text output for the debug/info/warn/error log channels. Trace events are mirrored into it only when the trace channel sets `text: true`.
- **timeline.pftrace** is the unified Perfetto timeline (see below); created when `observation.timeline.enabled` is true (the default).
- **counters.csv** is created when counters are enabled with `csv_output: true`.

## Perfetto Timeline

Trace events, counter snapshots, and (optionally) the scheduler execution
timeline are written to a single `timeline.pftrace` file in Perfetto protobuf
format. Open it directly in [ui.perfetto.dev](https://ui.perfetto.dev) (drag
and drop the file), or query it offline with Perfetto's `trace_processor`.

The timeline contains:

- **Simulation trace events** — instant events on one track per unit, grouped
  under a "Simulation" process. The timestamp is the simulation cycle (1 cycle
  rendered as 1 ns) and the event name is the formatted trace message.
  Hierarchical unit paths ("cpu0.lsu.mshr") become nested track groups, so the
  UI sidebar mirrors the design hierarchy; counter tracks nest under their
  owning unit instead of forming one flat list.
- **Timeline lanes** — occupancy spans, lane instants, and push-model counter
  samples emitted through the declarative `TimelineLane` / `TimelineCounter`
  members (see below), nested under their owning unit's track.
- **Counter tracks** — one Perfetto counter track per `unit.counter`, sampled
  at counter dump cycles.
- **Scheduler execution timeline** — when enabled, wall-clock
  unit/wait/epoch/arbitration slices under a "Chronon Scheduler" process
  group, with one lane per worker stream plus a `scheduler` lane. Slices
  carry `cycle` and `detail` debug annotations. See
  [Scheduler Timeline Trace](scheduler-timeline.md).

The file is produced by `src/observe/PerfettoTraceWriter`, a thin wrapper over
the Perfetto SDK's protozero message writers (no tracing session or category
registration — packets are written straight to the file).

### Wire format

The writer uses the size-oriented parts of the Perfetto data model; all of this
is transparent to ui.perfetto.dev and `trace_processor`:

- **Custom cycle clock** — simulation events are stamped on a sequence-scoped
  incremental clock (clock id 64, declared via `ClockSnapshot` and paired 1:1
  with the boot clock), so packets carry small varint cycle *deltas* instead of
  absolute timestamps. Out-of-order events (e.g. a reorder-buffer force flush)
  fall back to an absolute timestamp on that packet rather than corrupting the
  incremental state. The scheduler execution timeline stays on the default
  wall-clock sequence.
- **Interning** — event names, categories, and debug-annotation names are
  emitted once per sequence as `InternedData` and referenced by id afterwards.
  Incremental state is checkpointed periodically (and whenever an intern table
  hits its cap) so traces stay seekable and crash-truncation-safe.
- **Compression** — flushed packet batches are wrapped in zlib-deflated
  `TracePacket.compressed_packets` (on by default; `timeline.compress: false`
  disables it). Microarchitecture traces are highly repetitive, so this is
  where most of the size win comes from: on the bundled CPU pipeline example
  the timeline shrinks from ~115 MB to ~17 MB (~7×) with no measurable change
  in simulation wall time (encoding runs on the backend thread).

## Timeline Lanes and Counters

`trace<>()` emits point events. For microarchitecture state that *occupies*
something over many cycles — MSHR entries, ROB/LSQ slots, DRAM requests in
flight, busy functional units — declare timeline lanes as unit members (same
pattern as `Counter`, no macros or registration calls):

```cpp
class LSU : public Unit, public ObservableUnit {
    // One sub-lane per slot; renders as a track group "mshr" with
    // children mshr[0..7] under this unit's track.
    TimelineLane mshr_{this, "mshr", /*lanes=*/8};
    TimelineLane ld_port_{this, "ld_port", 2};
    // Push-model counter samples (independent of counters.csv).
    TimelineCounter occ_{this, "lsq_occupancy", "entries"};

    inline static const auto MISS = Category<"dcache_miss", "D$ miss lifetime">{};

    void tick() override {
        // Span addressed by (lane, slot): begin and end are separate calls
        // and may land in different tick() invocations — no RAII scopes.
        mshr_.begin(slot, MISS, "miss"_ev, flow(instr.uid),
                    arg<"addr">(paddr), arg<"set">(set));
        ...
        mshr_.end(slot);              // possibly many cycles later

        occ_.sample(lsq_.size());
    }
};
```

The vocabulary is deliberately SQL-shaped:

- **`"miss"_ev`** — event names are low-cardinality compile-time literals
  (interned once in the trace), so `SELECT dur FROM slice WHERE name='miss'`
  style analysis works in `trace_processor`.
- **`arg<"addr">(value)`** — per-event details go into *typed* debug
  annotations (uint/int/double/bool/pointer), not formatted into the name.
- **`flow(uid)`** — pass the instruction/transaction uid the model already
  carries; Perfetto links the uid's slices across lanes and stages into one
  flow (click an instruction → see its whole journey), and offline analysis
  can join stages through the flow id to compute per-stage latency
  distributions. The bundled CPU pipeline example is instrumented this way:
  every stage stamps `flow(instr_id)` (fetch/dispatch/commit instants, EX
  occupancy spans per ALU, L2 miss spans), so `examples/cpu_pipeline.yaml`
  produces a timeline where each instruction's fetch→dispatch→ex→commit
  journey is one connected flow.

Semantics under the existing machinery:

- Producers write fixed-size records to their SPSC queue (no allocation, cost
  at or below the `trace<>()` instant path); all Perfetto encoding happens on
  the backend thread. With observation disabled, calls are a null-check.
- Category and temporal filters apply to `begin`/`instant`/`sample`.
  `end()` skips temporal filters so a span begun inside an observation window
  still closes outside it; an `end` whose `begin` was suppressed is dropped by
  the backend's open-span table.
- A `begin` on an occupied slot implicitly closes the previous span (hardware
  slot reuse); spans still open at shutdown are closed at the last seen cycle.
- Lookahead rollback discards speculative lane events; commit publishes them.

### Offline analysis

`scripts/trace_sql/` ships canned `trace_processor` queries shaped for this
data model: per-stage latency through flow edges, span-duration histograms per
event name, lane occupancy, stall attribution (cycles beyond each event kind's
best case), and counter statistics — all keyed by the hierarchical track paths,
so they work unchanged on any model using the timeline API. See
`scripts/trace_sql/README.md` for usage.

## Pre-Registered Format Strings

Traditional approach (~30ns):
```cpp
trace("Request id={}", id);  // Runtime formatting
```

Chronon approach (~4ns):
```cpp
trace<"Request id={}">(CAT, id);
// Format registered at program start
// Runtime only: FormatId + args
```

## ObservationManager

`ObservationManager` is the central coordinator for the observation system:

```cpp
auto& obs = ObservationManager::instance();

// Initialize from YAML config
obs.initialize(yaml_config);

// Create context for each unit
auto* ctx = obs.createContextForUnit(
    "fetch",
    [&]() { return sim.currentCycle(); }
);
unit->setObservationContext(ctx);

// Start backend thread
obs.startBackend();

// ... run simulation ...

// Stop backend and shutdown
obs.stopBackend();
obs.shutdown();
```

### Key Responsibilities

- **YAML configuration initialization**: Parses config and sets up queues/backend
- **Context creation**: Creates `ObservationContext` per unit with filtering rules
- **Backend lifecycle**: Manages start/stop of background worker thread
- **Counter registration**: Central registry for sparse counter pull model

## ObservationQueue

The queue is the transport layer between simulation threads and the backend:

```cpp
// Constructor: capacity in bytes (rounded up to power-of-2)
ObservationQueue queue(256 * 1024);  // 256 KB default

// Larger queues reduce dropped events at the cost of memory
ObservationQueue queue(1024 * 1024);  // 1 MB for high-volume sims
```

### Size Guidelines

| Simulation Type | Recommended Size | Rationale |
|----------------|------------------|-----------|
| Low-volume (< 1M events/sec) | 256 KB (default) | Minimal overhead |
| Medium-volume (1-10M events/sec) | 512 KB - 1 MB | Balance memory/drops |
| High-volume (> 10M events/sec) | 2-4 MB | Prevent drops during bursts |

The queue uses 2x the requested capacity internally for lock-free mirroring.

## Per-Thread Queues

Trace and log events use per-thread SPSC queues to eliminate lock contention:

```cpp
// Each worker thread gets its own lock-free queue
// Backend drains all queues in round-robin fashion
// Queue capacity is configured via YAML: simulation.observation.queue_capacity
```

Benefits:
- **No mutex contention** on hot path
- **Better cache locality** (thread-local writes)
- **Scales linearly** with thread count

Trade-offs:
- **More memory** (one queue per thread)
- **Out-of-order events** (backend sees events from different threads)

## ReorderBuffer

The backend can reorder events by cycle for deterministic output:

```cpp
ObservationBackend::Config config;
config.enable_reordering = true;
config.reorder_watermark_cycles = 1000;  // Emit events older than current_cycle - 1000
config.reorder_max_events = 100000;      // Max buffered events before forced flush
```

### How It Works

```
Producer threads → Per-thread queues → Backend → ReorderBuffer → Output files
                                                      ↓
                                           Sort by (cycle, source_id)
                                           Emit when cycle < watermark
```

### Configuration

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `enable_reordering` | `true` | Enable cycle-based sorting |
| `reorder_watermark_cycles` | `1000` | Cycles to buffer before emitting |
| `reorder_max_events` | `100000` | Max events before forced flush |

**Trade-offs:**
- **Larger watermark** → more deterministic, more memory
- **Smaller watermark** → less memory, less out-of-order tolerance

## ObservationBackend::Config

Full configuration structure:

```cpp
struct Config {
    std::string output_dir = "out";
    std::chrono::microseconds poll_interval{100};
    bool enable_counter_csv = true;
    CounterCsvFormat counter_csv_format = CounterCsvFormat::Pivoted;

    // Mirror structured trace events to the text log
    // (the Perfetto timeline is the primary trace sink)
    bool trace_text = false;

    // Unified Perfetto timeline (timeline.pftrace)
    bool timeline_enabled = true;
    std::string timeline_file = "timeline.pftrace";
    bool timeline_trace_events = true;
    bool timeline_counters = true;

    // Reorder buffer
    bool enable_reordering = true;
    uint64_t reorder_watermark_cycles = 1000;
    size_t reorder_max_events = 100000;

    // Simulation metadata
    std::string simulation_name;
};
```

### Output Routing

Log channels (debug/info/warn/error) are text-only and write to `events.log`.
Trace events go to the Perfetto timeline; set `trace_text = true` (YAML:
`trace: { text: true }`) to additionally mirror them into `events.log`. The
old per-channel output-format selection and per-category format overrides
have been removed.

## Debug Build Backpressure (No-Drop Guarantee)

In **debug builds** (`NDEBUG` not defined), the no-drop guarantee applies when:

- The backend is running (wake callback registered by `ObservationBackend::start()`)

Under those conditions, when a per-thread SPSC queue is full, the producer thread:

1. Wakes the backend immediately (bypassing the 100us poll sleep)
2. Spin-waits with architecture-specific pause instructions until space is available
3. Yields to the OS scheduler every 64 iterations to avoid starving the backend

This ensures complete trace/log output during debugging sessions, at the cost of brief producer stalls under extreme event pressure.

If the backend is not running (or the record is larger than queue capacity), debug builds fall back to dropping the event rather than spinning forever.

In **release builds**, the original fire-and-forget behavior is preserved — events are silently dropped when queues are full, with zero additional overhead.

The backend also uses **eager read commits** in all builds, making freed queue space visible to producers immediately rather than waiting for the 4KB batch threshold. This reduces the stale-free-space window that causes unnecessary drops.

```
Debug build event flow:
  Producer → prepareWrite() → nullptr?
    → wakeBackend() → spin-yield → prepareWrite() → success → write

Release build event flow:
  Producer → prepareWrite() → nullptr?
    → incrementDropped() → return (fire-and-forget)
```

## Emergency Flush on Crash

When a simulation throws a C++ exception, buffered observer data would normally be lost. `CrashHandler::emergencyFlush()` provides best-effort recovery:

```cpp
// Called automatically by SimulationApp exception handlers
chronon::sender::CrashHandler::emergencyFlush();
```

This performs two steps:
1. `ThreadContextManager::flushAll()` - commits per-thread SPSC queue write pointers so the backend can see all buffered events
2. `ObservationManager::stopBackend()` - drains queues and flushes output files

For fatal signals (SIGSEGV/SIGABRT/etc), the installed signal handler is strict async-signal-safe and exits immediately after printing crash info.

When using `SimulationApp`, exception-path flushing is handled automatically. For manual setups, install signal handlers and call `emergencyFlush()` in your own catch blocks:

```cpp
chronon::sender::CrashHandler::install();  // Signal handlers
try {
    sim.runUntilTermination(max_cycles);
} catch (...) {
    chronon::sender::CrashHandler::emergencyFlush();
    throw;
}
```

## Related Documentation

- [Counter System](counters.md) - Detailed counter documentation
- [Configuration](configuration.md) - YAML observation configuration
- [Scheduler Timeline Trace](scheduler-timeline.md) - Scheduler execution slices in the Perfetto timeline
