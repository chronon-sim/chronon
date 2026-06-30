---
sidebar_label: "Configuration Guide"
---

# Configuration Guide

## Parameter System

### ParameterSet

Define parameters with automatic registration and YAML support:

```cpp
struct CPUParams : public ParameterSet {
    Param<uint32_t> fetch_width{this, "fetch_width", 4, "Fetch width per cycle"};
    Param<uint32_t> rob_size{this, "rob_size", 128, "Reorder buffer size"};
    Param<bool> enable_smt{this, "enable_smt", false, "Enable SMT"};
    Param<double> frequency{this, "frequency", 3.0, "Frequency in GHz"};
};
```

### Param Template

```cpp
template<typename T>
class Param : public ParamBase {
public:
    Param(ParameterSet* owner, const std::string& name,
          T default_value, const std::string& description = "");

    operator const T&() const;    // Implicit read
    const T& value() const;       // Explicit read
    Param& operator=(const T& v); // Assignment
};
```

### Usage

```cpp
void MyUnit::tick() {
    uint32_t width = params_->fetch_width;  // Implicit conversion

    if (params_->enable_smt.value()) {
        // SMT logic
    }
}
```

## Factory System

### AutoRegisteredUnit

Automatic factory registration via CRTP:

```cpp
class MyUnit : public AutoRegisteredUnit<MyUnit> {
public:
    using ParameterSet = MyUnitParams;
    static constexpr const char* unit_type_name = "MyUnit";
    static constexpr const char* unit_description = "My unit";

    // CHRONON_UNIT_CONSTRUCTOR generates factory constructor
    CHRONON_UNIT_CONSTRUCTOR(MyUnit, ParameterSet,
        params->width, params->depth)
    (uint32_t width = 4, uint32_t depth = 8)
        : AutoRegisteredUnit("my_unit")
        , width_(width), depth_(depth) {}
};
```

### Usage

```cpp
// Direct C++ instantiation
auto* unit = sim.createUnit<MyUnit>(8, 16);
auto* unit2 = sim.createUnit<MyUnit>();  // Defaults

// YAML factory instantiation (automatic)
// type: MyUnit, params: {width: 8, depth: 16}
```

## SimulationApp

Unified entry point with CLI support:

```cpp
#include "chronon/Chronon.hpp"

int main(int argc, char* argv[]) {
    return chronon::SimulationApp("CPU Simulator")
        .setDefaultConfig("config.yaml")
        .setConfigSearchPaths({".", "../configs"})
        .setVersion("1.0.0")
        .onPostBuild([](auto& result) {
            // Custom setup after build
        })
        .run(argc, argv);
}
```

### CLI Options

| Option | Description |
|--------|-------------|
| `[config.yaml]` | Configuration file |
| `-c, --config <path>` | Configuration file |
| `-p, --param KEY=VALUE` | Override YAML value |
| `-o, --output-dir <path>` | Override output directory |
| `-n, --run-cycles <N>` | Override run cycles |
| `-t, --threads <N>` | Override thread count |
| `--epoch-size <N>` | Override simulation epoch size in cycles |
| `--no-observe` | Disable observation |
| `-v, --verbose` | Verbose output |

### Examples

```bash
./my_sim config.yaml
./my_sim --threads=4 --run-cycles=1000000
./my_sim -p simulation.num_workers=4
./my_sim -p simulation.observation.logging.debug.enabled=false
./my_sim -p simulation.observation.timeline.scheduler.enabled=true \
         -p simulation.observation.timeline.scheduler.end_cycle=2000
```

## YAML Configuration

### Basic Structure

```yaml
simulation:
  name: my_simulation
  num_workers: 4
  enable_parallel: true    # Enable parallel execution (requires num_workers > 1)
  enable_lookahead: true   # Enable lookahead scheduling
  enable_epoch_free_lookahead: true   # Drop the per-epoch barrier when safe
  epoch_size: 64           # Deprecated: only used by the fallback scheduler
  run_cycles: 1000000

  unit:
    fetch:
      type: FetchUnit
      params:
        fetch_width: 4
      port:
        out_inst:
          to: decode.in_inst
          delay: 1

    decode:
      type: DecodeUnit
      params:
        decode_width: 4

    uart:
      type: UART
      tick_interval: 1000  # Optional: run tick() only on every 1000th global cycle
      params:
        poll_interval: 1
```

`tick_interval` is a unit-level scheduler hint, not a constructor parameter. It
defaults to `1`. Off-edge cycles skip the unit's `tick()` body but still advance
the unit's local cycle and scheduler progress. Port arrivals and explicit
`wakeAt()` calls can make a sleeping unit active again; the tick body still runs
only on an allowed interval edge. Applying `tick_interval` from YAML preserves
any constructor-established `sleepUntil()` or `sleepForever()` target; it only
seeds cycle 0 activity when the unit has not already deferred itself.

### Observation Configuration

Chronon uses a unified logging model where each event type (debug, info, warn, error, trace) is an independent "channel" with its own enable settings. Categories and temporal filters are shared across all channels. Log channels are text-only (`events.log`); trace events go to the unified Perfetto timeline (`timeline.pftrace`).

```yaml
simulation:
  observation:
    enabled: true
    output_dir: "out"
    queue_capacity: 262144

    counters:
      enabled: true
      csv_output: true
      periodic_dump_cycles: 10000
      dump_on_shutdown: true

    timeline:                  # Unified Perfetto timeline (timeline.pftrace)
      enabled: true            # Default true
      file: timeline.pftrace
      trace_events: true       # Simulation trace channel -> instant events
      counters: true           # Counter snapshots -> counter tracks
      compress: true           # Deflate packet batches (compressed_packets)
      scheduler:               # Scheduler execution timeline (see below)
        enabled: false

    logging:
      enabled: true

      # Per-channel configuration
      debug:
        enabled: true
        file: debug.log        # Optional; empty = default events.log
      info:
        enabled: true
      warn:
        enabled: true
      error:
        enabled: true
      trace:
        enabled: true
        text: false            # true mirrors trace events into events.log

      # Shared temporal filters (apply to all channels)
      temporal:
        - range: [0, 100000]

      # Shared category patterns (apply to all channels)
      categories:
        - pattern: "icache_*"
          temporal:
            - range: [0, 50000]
        - pattern: "branch_pred"
          temporal:
            - periodic:
                window: 1000
                period: 10000
        - pattern: "flush"
```

### Output Files

| File | Contents |
|------|----------|
| `events.log` | Text logs (debug/info/warn/error); trace events mirrored when `trace.text: true` |
| `timeline.pftrace` | Perfetto protobuf timeline: trace instant events, counter tracks, scheduler slices |
| `counters.csv` | Counter snapshots |

Per-channel `format:` keys (and the removed binary trace output they used to
select) are deprecated; they are parsed but ignored with a warning. Logs are
always text, and trace events always go to the Perfetto timeline.

### Category Format Overrides (Removed)

Category patterns no longer accept per-category format overrides (`format`,
`trace_format`, `debug_format`, ...). These keys are deprecated and ignored
with a warning. Categories still control enable/disable and temporal filters.

### Scheduler Timeline Trace

The scheduler timeline records Chronon scheduler streams, unit tick slices,
lazy idle fast-path slices, cross-thread spin waits, epoch spans, and MPSC
arbitration slices. It is part
of the unified Perfetto timeline: when the observation backend runs with a
timeline sink, the scheduler slices merge into `timeline.pftrace` under a
"Chronon Scheduler" process group, with one lane per worker stream (`stream 0`,
`stream 1`, ...) plus a `scheduler` lane for epoch, rebalance, and MPSC
arbitration spans. Slices carry `cycle` and `detail` debug annotations.

```yaml
simulation:
  observation:
    timeline:
      scheduler:
        enabled: false
        max_events: 1000000
        start_cycle: 0
        end_cycle: 2000
        trace_units: true
        trace_waits: true
        trace_epochs: true
        trace_arbitration: true
        min_duration_ns: 0
```

| Field | Description |
|-------|-------------|
| `enabled` | Enable timeline collection. Disabled by default. |
| `max_events` | Event cap; additional events are reported as dropped. |
| `start_cycle`, `end_cycle` | Inclusive start and exclusive end cycle capture window. |
| `trace_units` | Record each unit tick as a duration slice. |
| `trace_waits` | Record spin waits on predecessor cluster progress atomics. |
| `trace_epochs` | Record progress-epoch spans on the scheduler lane. |
| `trace_arbitration` | Record MPSC arbitration spans on the scheduler lane. |
| `min_duration_ns` | Drop events below this duration. |

The old top-level `simulation.timeline_trace:` key is deprecated; it is still
parsed but prints a warning. When observation is disabled (e.g. `--no-observe`),
the scheduler timeline falls back to a standalone Perfetto protobuf file at the
configured path (default `chronon_timeline.pftrace`).

See [Scheduler Timeline Trace](scheduler-timeline.md) for event semantics and
Perfetto usage.

### Temporal Filters

| Type | Format | Description |
|------|--------|-------------|
| Range | `range: [start, end]` | Enable within cycle range |
| Periodic | `periodic: {window, period, offset}` | Enable first `window` cycles every `period` |

### Category Patterns

| Pattern | Matches |
|---------|---------|
| `"*"` | All categories |
| `"icache_*"` | `icache_hit`, `icache_miss`, etc. |
| `"*_miss"` | `icache_miss`, `dcache_miss`, etc. |
| `"branch"` | Exact match |

### Per-Unit Overrides

```yaml
simulation:
  observation:
    unit_overrides:
      fetch:
        logging:
          debug:
            enabled: true     # Enable debug for fetch only
          trace:
            enabled: true
          categories:
            - pattern: "*"    # Trace all categories for this unit
```

### Runtime Overrides

```bash
./my_sim config.yaml \
    -p simulation.num_workers=4 \
    -p simulation.observation.logging.debug.enabled=false
```

Type conversion:
- `true`, `false` → boolean
- All digits → integer
- Digits with `.` → double
- Otherwise → string

## Lifecycle Phases

Tree nodes progress through phases:

```
BUILDING → CONFIGURING → BINDING → FINALIZED
```

| Phase | Allowed Operations |
|-------|-------------------|
| BUILDING | Add children, modify params |
| CONFIGURING | Modify params, create resources |
| BINDING | Bind ports |
| FINALIZED | Read-only, run simulation |

## TreeNode Hierarchy

Path-based addressing with dot notation:

```
root
├── cpu                    → "cpu"
│   ├── core0              → "cpu.core0"
│   │   ├── fetch          → "cpu.core0.fetch"
│   │   └── decode         → "cpu.core0.decode"
│   └── core1              → "cpu.core1"
└── memory                 → "memory"
```

```cpp
node->getChild("cpu.core0.fetch")  // Traverses hierarchy
```
