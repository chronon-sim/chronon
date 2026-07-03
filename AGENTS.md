# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Standard build
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)

# Debug build
cmake .. -DCMAKE_BUILD_TYPE=Debug
make -j$(nproc)

# Configure without tests/examples
cmake .. -DCMAKE_BUILD_TYPE=Release -DCHRONON_BUILD_TESTS=OFF
make -j$(nproc)
```

## Testing

```bash
# Run all tests
cd build && ctest --output-on-failure

# Run tests in parallel
ctest --output-on-failure --parallel $(nproc)

# Run a specific test by name
ctest -R test_tree_node --output-on-failure

# Run test executable directly
./build/test_tree_node
./build/test/sender/test_sender
```

## Architecture Overview

Chronon is a high-performance tick-based simulation framework for CPU microarchitecture modeling written in C++20, using stdexec for parallel execution.

### Key Components

| Component | Location | Purpose |
|-----------|----------|---------|
| Sender Framework | `src/sender/` | Core simulation framework (Unit, TickSimulation, Port) |
| - Core | `src/sender/core/` | Unit, TickableUnit, TickSimulation, CrashHandler |
| - Port | `src/sender/port/` | OutPort, InPort, Connection, MessageQueue with auto-registration |
| - Schedule | `src/sender/schedule/` | DependencyGraph, CycleAnalyzer, WeightedPartitioner, SimulatedAnnealingPartitioner, TickCostProfiler, SchedulerTimelineTrace |
| - Utilities | `src/sender/util/` | Graph, StageReg, SingleStageReg, PipelinePhase, PriorityArbiter, VersionedRegister |
| Observability | `src/observe/` | Unified counters, traces, logs with macro-free API |
| Tree | `src/tree/` | TreeNode hierarchy for unit organization |
| Parameters | `src/params/` | Self-registering parameter system |
| Factory | `src/sender/factory/` | Unit factory for YAML-driven instantiation |
| Configuration | `src/sender/config/` | SenderConfigLoader, SenderSimulationBuilder for YAML parsing |
| App | `src/sender/app/` | SimulationApp unified entry point with CLI support |

**Execution modes** (selected automatically; all cycle-accurate, differ only in speed):
- **Sequential** — single thread, or when parallelism isn't beneficial
- **Barrier** — parallel, per-cycle `sync_wait`; used when tight (`delay=0`) edges cross thread clusters
- **Lookahead** — parallel, per-cluster progress atoms, one `sync_wait` per epoch (default fast path)

There is no separate scheduler class; all three live in `TickSimulation` (`TickSimulation.cpp`, `TickSimulationParallel.cpp`).

### Detailed Documentation

For comprehensive design documentation, see the Docusaurus site under `website/docs/guides/`:

- **[Architecture Overview](website/docs/guides/architecture.md)**: High-level component relationships
- **[Scheduling and Parallelization](website/docs/guides/scheduling.md)**: Dependency graph, lookahead, weighted partitioning
- **[Port System](website/docs/guides/port-system.md)**: OutPort/InPort, connection delays, queue types, backpressure
- **[Observability](website/docs/guides/observability.md)**: Counters, traces, logs with macro-free API
- **[Configuration](website/docs/guides/configuration.md)**: YAML-driven unit instantiation, SimulationApp

### Quick Reference

**Port delays and queue modes:**
- `delay=0` means same-cycle delivery eligibility.
- `delay>0` means delivery is delayed by N cycles.
- Queue implementation is selected during simulation initialization from thread topology and producer count: same-thread, SPSC, or MPSC.

**Port registration (automatic):**
```cpp
// Ports auto-register on construction. Signature: (owner, name, capacity?).
// Delay is a property of the Connection, not the port.
OutPort<Data> out{this, "out", 256};  // per-cycle capacity 256
InPort<Data> in{this, "in"};          // unlimited capacity by default
```

**Modern observability API:**
```cpp
// Define categories (auto-assigned bit positions)
inline const auto MY_CATEGORY = Category<"my_category", "Description">{};

// Declare counters as unit members
Counter ops_{this, "ops", "Operations executed"};

// Use in ObservableUnit (no macros needed)
++ops_;                                      // Increment counter
trace<"Event: {}">(MY_CATEGORY, value);     // Emit trace
debug<"Debug info: {}">(value);              // Debug log
info<"Info message">();                      // Info log
```

**Simplified headers:**
```cpp
// Single include for all Chronon functionality
#include "chronon/Chronon.hpp"
using namespace chronon;

// All types available directly in chronon:: namespace:
// - TickableUnit, TickSimulation, TickSimulationConfig
// - OutPort<T>, InPort<T>, Connection<T>
// - Counter<>, Category<>, ObservableUnit
// - Param<T>, ParameterSet, AutoRegisteredUnit
// - StageReg<T, N>, SingleStageReg<T>
// - TerminationReason, TerminationRequest, TerminationController
```

**Unit constructor pattern (with YAML support):**
```cpp
// Define ParameterSet for your unit
struct MyUnitParams : public ParameterSet {
    Param<uint32_t> width{this, "width", 4, "Processing width"};
    Param<uint32_t> depth{this, "depth", 8, "Queue depth"};
};

// Use CHRONON_UNIT_CONSTRUCTOR macro to generate dual constructors
class MyUnit : public AutoRegisteredUnit<MyUnit> {
public:
    using ParameterSet = MyUnitParams;
    static constexpr const char* unit_type_name = "MyUnit";
    static constexpr const char* unit_description = "Example unit";

    OutPort<Data> out{this, "out"};
    InPort<Data> in{this, "in"};

    // Macro generates factory constructor, you write the direct constructor once
    CHRONON_UNIT_CONSTRUCTOR(MyUnit, ParameterSet,
        params->width, params->depth)
    (uint32_t width = 4, uint32_t depth = 8)
        : AutoRegisteredUnit("my_unit")
        , width_(width), depth_(depth) {
        // Initialization logic written once
    }

    void tick() override { /* implementation */ }

private:
    uint32_t width_;
    uint32_t depth_;
};

// Direct C++ instantiation
auto* unit = sim.createUnit<MyUnit>(8, 16);  // Custom params
auto* unit2 = sim.createUnit<MyUnit>();      // Default params

// YAML factory instantiation (automatic via AutoRegisteredUnit)
// In YAML: type: MyUnit, params: {width: 8, depth: 16}
```

**Tick-based simulation (primary execution model):**
```cpp
// TickableUnit - state machine based, high performance
class MyUnit : public TickableUnit {
    OutPort<Data> out{this, "out"};
    InPort<Data> in{this, "in"};
    int state_ = 0;  // State as member variable
public:
    MyUnit() : TickableUnit("my_unit") {}

    void tick() override {
        // Check inputs
        if (auto data = in.tryReceive(localCycle())) {
            process(*data);
        }
        // Produce outputs
        if (out.canSend()) {
            out.send(state_++);
        }
    }

    bool isCompleted() const override { return state_ >= 1000; }
};

// TickSimulation - uses stdexec::static_thread_pool
TickSimulationConfig config;
config.num_threads = 8;
config.enable_parallel = true;
TickSimulation sim(config);
auto* unit = sim.createUnit<MyUnit>();
sim.initialize();
sim.run(1000000);  // ~90+ Mcycles/sec
```

**Unit-initiated termination:**
```cpp
// Units can request simulation termination with context
class ROB : public TickableUnit {
    uint64_t retired_ = 0;
    uint64_t target_ = 1'000'000;

    void tick() override {
        retired_ += retire_this_cycle;
        if (retired_ >= target_) {
            // Primary API: requestTermination(reason, exit_code, message)
            requestTermination(TerminationReason::Completed, 0,
                "Retired " + std::to_string(retired_) + " instructions");
        }
    }
};

// Convenience methods
requestExitSyscall(exit_code);  // For exit syscall
requestError("message");         // For error conditions

// Run until any unit requests termination
uint64_t cycles = sim.runUntilTermination(max_cycles);
if (sim.wasTerminationRequested()) {
    auto& req = sim.terminationRequest();
    // req.reason, req.exit_code, req.cycle, req.unit_name, req.message
}

// Termination reasons: Completed, ExitSyscall, Error, UserInterrupted, MaxCyclesReached, CheckpointRequested
```

**SimulationApp (unified YAML-driven entry point):**
```cpp
// Minimal entry point - handles CLI, YAML loading, observation lifecycle
#include "chronon/Chronon.hpp"

int main(int argc, char* argv[]) {
    return chronon::SimulationApp("My Simulator")
        .setDefaultConfig("config.yaml")
        .run(argc, argv);
}

// With customization hooks
int main(int argc, char* argv[]) {
    return chronon::SimulationApp("My Simulator")
        .setDefaultConfig("config.yaml")
        .setConfigSearchPaths({".", "../configs"})
        .setVersion("0.3.1")
        .onPostBuild([](auto& result) {
            // Custom setup after units are created
        })
        .run(argc, argv);
}
```

**SimulationApp CLI options:**
```
-c, --config <path>       YAML configuration file
-p, --param KEY=VALUE     Override YAML value (repeatable)
-o, --output-dir <path>   Override observation output directory
-n, --run-cycles <N>      Override simulation run cycles
-t, --threads <N>         Override number of worker threads
--no-observe              Disable observation system
-v, --verbose             Verbose output
-h, --help                Show help
--version                 Show version
```

## Code Conventions

- All code in `chronon::` namespace - use single include `#include "chronon/Chronon.hpp"`
- Internal subnamespaces: `chronon::sender` (incl. `::config`, `::app`), `chronon::observe`, `chronon::tree`, `chronon::params`, `chronon::factory`
- All commonly-used types re-exported to `chronon::` namespace (no need for subnamespace prefixes)
- Headers use `.hpp` extension
- C++20 features required (GCC 12+ required)
- Tests use simple assertion-based pattern with `std::cout` for pass/fail output
- **Observability**: Use member `Counter` objects (`++counter_`, `counter_ += n`) and `trace<>()`/`debug<>()`; legacy macros (`OCOUNT`, `OTRACE`) no longer exist
- **Port registration**: Automatic via constructor - never call registration methods manually
- **Tick-based execution**: Use `TickableUnit` + `TickSimulation` for all simulations
