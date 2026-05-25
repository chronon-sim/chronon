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
    ├── counters.csv      # Counter snapshots
    ├── events.ctrace     # Binary output (FlatBuffers + zstd)
    └── events.log        # Text output (logs and/or traces)
```

Output files are created based on channel format configuration:
- **events.log** is created if any channel uses `format: text` or `format: both`
- **events.ctrace** is created if any channel uses `format: binary` or `format: both`
- **counters.csv** is always created when counters are enabled

## Binary Trace Format

Traces are stored in a compact binary format (`.ctrace`) using FlatBuffers with optional zstd compression:

```
┌─────────────────────────────────────┐
│ File Header (64 bytes)              │  Magic "CTRC", version, offsets
├─────────────────────────────────────┤
│ Schema Section (FlatBuffers)        │  Format strings, units, arg types
├─────────────────────────────────────┤
│ Event Block 0 (zstd compressed)     │  Events packed sequentially
├─────────────────────────────────────┤
│ Event Block 1...N                   │
├─────────────────────────────────────┤
│ Footer (block index)                │  Cycle-based seeking support
└─────────────────────────────────────┘
```

Benefits:
- **~8x smaller** than text output
- **~6x faster** write throughput
- **Self-describing**: Embedded schema for offline analysis
- **Seekable**: Block index enables cycle-based random access

### trace_reader Tool

Use `trace_reader` to inspect binary traces:

```bash
# Show file info
trace_reader info events.ctrace

# Dump all events as text
trace_reader dump events.ctrace

# Filter by cycle range
trace_reader filter events.ctrace --cycles 1000-2000

# Filter by unit
trace_reader filter events.ctrace --unit rob

# Convert to text file
trace_reader convert events.ctrace -o trace.log
```

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

    // Per-channel formats (default: trace=Binary, logs=Text)
    OutputFormat trace_format = OutputFormat::Binary;
    OutputFormat debug_format = OutputFormat::Text;
    OutputFormat info_format = OutputFormat::Text;
    OutputFormat warn_format = OutputFormat::Text;
    OutputFormat error_format = OutputFormat::Text;

    // Per-category format overrides
    std::unordered_map<CategoryMask, CategoryFormatOverride> category_format_overrides;

    // Binary trace settings
    TraceChannelConfig trace_config;  // Compression, block size, etc.

    // Reorder buffer
    bool enable_reordering = true;
    uint64_t reorder_watermark_cycles = 1000;
    size_t reorder_max_events = 100000;

    // Simulation metadata
    std::string simulation_name;
};
```

### Per-Channel Formats

Each event type can independently choose output format:

```cpp
enum class OutputFormat {
    Text,    // events.log only
    Binary,  // events.ctrace only
    Both,    // Both files
    None     // Disabled
};
```

Example: Traces to binary, logs to text:
```cpp
config.trace_format = OutputFormat::Binary;
config.debug_format = OutputFormat::Text;
config.info_format = OutputFormat::Text;
```

### Category-Specific Overrides

Override format for specific categories:

```cpp
config.category_format_overrides[CACHE_HIT] = {
    .trace_format = OutputFormat::Both  // Write CACHE_HIT to both text and binary
};
```

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
- [Binary Trace Format](binary-trace-format) - Trace file format specification
