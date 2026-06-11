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
./my_sim -p simulation.timeline_trace.enabled=true \
         -p simulation.timeline_trace.file=out/chronon_timeline.json
```

## YAML Configuration

### Basic Structure

```yaml
simulation:
  name: my_simulation
  num_workers: 4
  enable_parallel: true    # Enable parallel execution (requires num_workers > 1)
  enable_lookahead: true   # Enable lookahead scheduling for parallel epochs
  enable_epoch_free_lookahead: false  # Opt-in: drop the per-epoch barrier (see Scheduling guide)
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
```

### Observation Configuration

Chronon uses a unified logging model where each event type (debug, info, warn, error, trace) is an independent "channel" with its own enable/format settings. Categories and temporal filters are shared across all channels.

```yaml
simulation:
  observation:
    enabled: true
    output_dir: "out"
    queue_capacity: 262144

    counters:
      enabled: true
      csv_output: true
      dump_on_shutdown: true

    logging:
      enabled: true

      # Per-channel configuration
      debug:
        enabled: true
        format: text           # text | binary | both
      info:
        enabled: true
        format: text
      warn:
        enabled: true
        format: text
      error:
        enabled: true
        format: text
      trace:
        enabled: true
        format: binary         # Binary output for traces
        compression:
          enabled: true
          algorithm: "zstd"
          level: 3
          block_size: 65536
        embed_schema: true
        generate_index: true

      # Shared temporal filters (apply to all channels)
      temporal:
        - range: [0, 100000]

      # Shared category patterns (apply to all channels)
      categories:
        - pattern: "icache_*"
          temporal:
            - range: [0, 50000]
        - pattern: "branch_pred"
          format: text         # Override: route this category to text for all channels
          temporal:
            - periodic:
                window: 1000
                period: 10000
        - pattern: "flush"
```

### Output Format

Each channel (debug, info, warn, error, trace) can independently route its events to text, binary, or both outputs:

| Format | Output File | Description |
|--------|-------------|-------------|
| `text` | `events.log` | Human-readable text output |
| `binary` | `events.ctrace` | Compact binary format (FlatBuffers + zstd) |
| `both` | Both files | Write to both text and binary outputs |

Files are only created if at least one channel uses that format.

### Per-Category Format Overrides

Categories can override the output format for specific channels:

```yaml
categories:
  - pattern: "verif"
    format: text              # Shorthand: override all channels to text
    trace_format: binary      # Override: trace channel stays binary
  - pattern: "branch_pred"
    debug_format: both        # Override: debug events for this category go to both
```

Priority: channel-specific (`trace_format`) > shorthand (`format`) > channel default.

### Scheduler Timeline Trace

The scheduler timeline is separate from observation logging and counters. It
records Chronon scheduler streams, unit tick slices, cross-thread spin waits,
epoch spans, and MPSC arbitration slices into Chrome Trace / Perfetto JSON.
Worker lane names are zero-based (`stream 0`, `stream 1`, ...), while the JSON
`tid` values are 1-based to avoid Perfetto's special handling of `tid=0`. Each
duration event also carries `args.stream`, the original zero-based logical
stream id. The `scheduler` lane is a separate non-worker row for epoch,
rebalance, and MPSC arbitration spans.

```yaml
simulation:
  timeline_trace:
    enabled: false
    file: chronon_timeline.json
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
| `file` | Output JSON path. |
| `max_events` | Event cap; additional events are reported as dropped. |
| `start_cycle`, `end_cycle` | Inclusive start and exclusive end cycle capture window. |
| `trace_units` | Record each unit tick as a duration slice. |
| `trace_waits` | Record spin waits on predecessor cluster progress atomics. |
| `trace_epochs` | Record progress-epoch spans on the scheduler lane. |
| `trace_arbitration` | Record MPSC arbitration spans on the scheduler lane. |
| `min_duration_ns` | Drop events below this duration. |

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
