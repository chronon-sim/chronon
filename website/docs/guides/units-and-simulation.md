---
sidebar_label: "Units and Simulation"
---

# Units and Simulation

## TickableUnit

The base class for all simulation components using tick-based execution.

```cpp
class TickableUnit : public Unit {
public:
    explicit TickableUnit(std::string name);

    virtual void tick() = 0;                        // Main execution method
    virtual bool isCompleted() const { return false; }  // Optional completion signal

    uint64_t localCycle() const;                    // Current local cycle
};
```

### Creating a Unit

```cpp
class FetchUnit : public TickableUnit {
    OutPort<Instruction> out{this, "out"};
    uint64_t pc_ = 0;

public:
    FetchUnit() : TickableUnit("fetch") {}

    void tick() override {
        if (out.send(Instruction{.pc = pc_})) ++pc_;
    }

    bool isCompleted() const override { return pc_ >= 1000000; }
};
```

### Unit with Parameters (YAML Support)

```cpp
struct MyUnitParams : public ParameterSet {
    Param<uint32_t> width{this, "width", 4, "Processing width"};
    Param<uint32_t> depth{this, "depth", 8, "Queue depth"};
};

class MyUnit : public AutoRegisteredUnit<MyUnit> {
public:
    using ParameterSet = MyUnitParams;
    static constexpr const char* unit_type_name = "MyUnit";
    static constexpr const char* unit_description = "Example unit";

    OutPort<Data> out{this, "out"};
    InPort<Data> in{this, "in"};

    // CHRONON_UNIT_CONSTRUCTOR generates factory constructor
    CHRONON_UNIT_CONSTRUCTOR(MyUnit, ParameterSet,
        params->width, params->depth)
    (uint32_t width = 4, uint32_t depth = 8)
        : AutoRegisteredUnit("my_unit")
        , width_(width), depth_(depth) {}

    void tick() override { /* ... */ }

private:
    uint32_t width_, depth_;
};
```

## TickSimulation

The simulation driver using stdexec for parallel execution.

Use the standard umbrella header and the canonical execution settings:

```cpp
#include "chronon/Chronon.hpp"
using namespace chronon;

TickSimulationConfig config;
config.num_threads = 8;
config.setExecutionPolicy(ExecutionPolicy::Auto);
config.setPollingIntervalCycles(64);
```

`Auto` requests parallel execution where topology and safety allow it.
`Sequential` forces the reference path. The polling interval controls host
predicates and sequential termination checks; it does not create execution epochs.
Set the worker count explicitly when comparing C++ and YAML models.

| Operation | Purpose |
|---|---|
| `createUnit<T>(...)` and `connect(...)` | Construct the model before initialization |
| `initialize()` | Validate topology and select the execution path |
| `run(cycles)` | Advance a bounded number of additional cycles |
| `runUntilComplete(limit)` | Advance until every unit reports completion |
| `runUntilTermination(limit)` | Advance until a unit requests termination |
| `finalize()` | End the session and report finalizer errors |

See the [compiled quickstart](../intro) for a complete program and the
[API reference](/docs/api/) for signatures. Registration, placement inspection
and queue integration methods are advanced adapter APIs, outside the basic model
workflow. Compatibility spellings are listed in [API contracts](api-contracts).

## Unit-Initiated Termination

Units can request simulation termination with rich context.

Inside `tick()`, use `requestTermination(reason, exit_code, message)`,
`requestExitSyscall(exit_code)` or `requestError(message)`. The simulation records
the requesting unit and cycle; callers inspect the result after the run returns.

### Termination Reasons

| Reason | Use Case |
|--------|----------|
| `Completed` | Normal completion (e.g., retired N instructions) |
| `ExitSyscall` | Exit syscall encountered |
| `Error` | Error condition detected |
| `UserInterrupted` | External stop (Ctrl+C, API call) |
| `MaxCyclesReached` | Hit cycle limit |
| `CheckpointRequested` | Checkpoint reached |

### Example: ROB Termination

```cpp
class ReorderBuffer : public TickableUnit {
    uint64_t retired_ = 0;
    uint64_t target_ = 1'000'000;

    void tick() override {
        retired_ += retire_this_cycle;
        if (retired_ >= target_) {
            requestTermination(TerminationReason::Completed, 0,
                "Retired " + std::to_string(retired_) + " instructions");
        }
    }
};
```

### Running Until Termination

```cpp
uint64_t cycles = sim.runUntilTermination(max_cycles);
if (sim.wasTerminationRequested()) {
    auto& req = sim.terminationRequest();
    std::cout << "Terminated: " << req.reasonString()
              << " at cycle " << req.cycle
              << " by " << req.unit_name << "\n";
}
```

### Multi-Run Scenarios

To continue the same simulation after a termination request, reset its termination
controller and stop source together. This does not reset model state or reopen a
finalized simulation:

```cpp
TickSimulation sim(config);
// ... build units, initialize ...

// First run
uint64_t cycles1 = sim.runUntilTermination(1000000);

// Reset termination state for next run
sim.resetTermination();

// Second run
uint64_t cycles2 = sim.runUntilTermination(1000000);
```

Termination is observed at scheduler boundaries; parallel execution also
propagates the stop token into dependency waits. Polling and multiclock boundaries
are described in [API contracts](api-contracts).

## Crash Handling

When a unit's `tick()` throws an exception or causes a fatal signal (SIGSEGV, SIGBUS, etc.), Chronon provides two layers of crash protection that preserve observer data and identify which unit crashed.

### Signal Handling

`CrashHandler::install()` registers handlers for SIGSEGV, SIGBUS, SIGABRT, SIGFPE, and SIGILL. On crash:

1. Prints crash context (unit name + cycle) to stderr using async-signal-safe `write()`
2. Calls `emergencyFlush()` to best-effort drain observer queues to files
3. Exits with code `128 + signal_number`

Output format:
```
=== CHRONON CRASH ===
Signal: SIGSEGV (11)
Unit:   fetch
Cycle:  42857
Flushing observers...
Done.
```

Signal handlers are installed automatically by `SimulationApp::run()`. For manual simulation setup, call:
```cpp
chronon::sender::CrashHandler::install();
```

### C++ Exception Handling

All execution paths in `TickSimulation` wrap tick calls with try-catch:

- **Sequential paths**: try-catch around the outer loop (zero overhead on non-exception path)
- **Parallel paths**: try-catch inside each bulk lambda body; first exception is captured atomically and rethrown after `sync_wait()`

Exceptions are wrapped as `TickException` with unit context:

```cpp
try {
    sim.runUntilTermination(max_cycles);
} catch (const chronon::sender::TickException& e) {
    std::cerr << "Unit: " << e.unitName() << "\n";
    std::cerr << "Cycle: " << e.cycle() << "\n";
    std::cerr << "Cause: " << e.cause() << "\n";
    // Observer data is automatically flushed by SimulationApp
}
```

### Emergency Flush

`CrashHandler::emergencyFlush()` can be called from any context to flush observer data:

```cpp
chronon::sender::CrashHandler::emergencyFlush();
```

This calls `ThreadContextManager::flushAll()` to commit per-thread queues, then `ObservationManager::stopBackend()` to drain and flush output files. `SimulationApp` calls this automatically in all exception handlers.

## Lifecycle Hooks

```cpp
class MyUnit : public TickableUnit {
public:
    void initialize() override {
        // Called after construction, before simulation starts
    }

    void finalize() override {
        // Called once when the simulation session is finalized
    }
};
```

`run()` advances a resumable simulation; it does not call `finalize()` after
each segment. Call `sim.finalize()` once all segments are complete to observe
errors from unit finalizers. Repeated finalization is harmless. A finalized
simulation cannot be initialized or run again. `SimulationApp` finalizes before
the final counter snapshot and observation shutdown. The simulation destructor
also performs best-effort finalization, but cannot report finalizer exceptions.

Only units whose `initialize()` completed successfully are finalized, in creation
order. Every eligible finalizer is attempted even when an earlier one throws;
explicit finalization then rethrows the first error. Failed initialization cannot
be retried on the same simulation. `state()` reports `Created`, `Initialized` or
`Finalized` accordingly.

## Pipeline Registers

Chronon provides stage-level pipeline register abstractions. See [Stage Registers](stage-registers.md) for full documentation.

### StageReg (Multi-Pipe)

For superscalar pipeline stages with N parallel pipes:

```cpp
class LSU : public PhasedTickableUnit<LSU> {
    static constexpr size_t NUM_PIPES = 4;
    StageReg<AddrData, NUM_PIPES> a0_, a1_, a2_;

    template<ValidPhase P>
    void tickPhase() {
        a0_.beginCycle<P>();
        a1_.beginCycle<P>();
        a2_.beginCycle<P>();

        // Forward a1 -> a2, then a0 -> a1
        simpleForward<P>(a1_, a2_);
        simpleForward<P>(a0_, a1_);

        // Feed new entries
        for (size_t i = 0; i < NUM_PIPES; ++i) {
            if (hasInput(i)) a0_.set<P>(i, fetchInput(i));
        }
    }
};
```

### SingleStageReg (Single Entry)

For stages with a single data element:

```cpp
class FillUnit : public PhasedTickableUnit<FillUnit> {
    SingleStageReg<FillData> fill_;

    template<ValidPhase P>
    void tickPhase() {
        fill_.beginCycle<P>();
        if (fill_.valid<P>()) {
            process(fill_.get<P>());
        }
        if (hasNewFill()) {
            fill_.set<P>(newFill());
        }
    }
};
```

## Time Model

Each unit maintains local time; there is no global cycle counter.

```cpp
void tick() override {
    uint64_t cycle = localCycle();  // Unit's current cycle
    // Units may be at different cycles due to lookahead
}
```
