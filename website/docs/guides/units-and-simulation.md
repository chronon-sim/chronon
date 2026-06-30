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
    void executeTick() { tick(); advanceLocalCycle(); }  // Called by simulation
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
        if (out.canSend()) {
            Instruction inst{.pc = pc_++};
            out.send(inst);
        }
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

```cpp
struct TickSimulationConfig {
    size_t num_threads = std::thread::hardware_concurrency();
    bool enable_parallel = true;
    bool enable_lookahead = true;
    // Lookahead uses per-cluster progress atomics (consumer-tick-driven MPSC
    // arbitration). Placement is always cluster-aware: when there are no
    // tight (delay=0) edges every unit becomes its own single-member cluster.
    uint32_t max_lookahead_cycles = 100;     // Upper bound for per-unit lookahead window
    uint64_t epoch_size = 64;                // Deprecated: fallback-only
    bool enable_epoch_free_lookahead = true; // Keep enabled; fallback is deprecated
    bool trace_execution = false;            // Print execution policy details

    // Cluster-aware partitioning
    bool enable_weighted_partitioning = true;
    PartitionSolverType partition_solver = PartitionSolverType::SA;
    double initial_partition_sync_cost_ns = 8.0;  // Locality weight for placement

    // Dynamic rebalancing
    bool enable_dynamic_rebalance = false;
    double rebalance_imbalance_threshold = 1.3;
    uint64_t rebalance_check_interval_cycles = 8192;
    double rebalance_min_gain = 0.05;
    uint64_t rebalance_cooldown_cycles = 0;
};

class TickSimulation {
public:
    explicit TickSimulation(const TickSimulationConfig& config = {});

    template<typename UnitT, typename... Args>
    UnitT* createUnit(Args&&... args);

    template<typename T>
    void connect(OutPort<T>& from, InPort<T>& to, uint32_t delay = 1);

    // Register externally-created connections (e.g. from YAML-driven builders)
    void registerConnection(ConnectionBase* conn);

    void initialize();
    uint64_t run(uint64_t cycles);
    uint64_t runUntilComplete(uint64_t max_cycles = UINT64_MAX);
    uint64_t runUntilTermination(uint64_t max_cycles = UINT64_MAX);
};
```

### Basic Usage

```cpp
TickSimulationConfig config;
config.num_threads = 8;
config.enable_parallel = true;

TickSimulation sim(config);

auto* fetch = sim.createUnit<FetchUnit>();
auto* decode = sim.createUnit<DecodeUnit>();

sim.connect(fetch->out, decode->in, 1);

sim.initialize();
sim.run(1000000);  // ~90+ Mcycles/sec
```

## Unit-Initiated Termination

Units can request simulation termination with rich context.

### Termination API

```cpp
class TickableUnit {
protected:
    // Primary API
    void requestTermination(TerminationReason reason,
                           int32_t exit_code = 0,
                           std::string_view message = "");

    // Convenience methods
    void requestExitSyscall(int32_t exit_code);
    void requestError(std::string_view message);
};
```

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

For running multiple simulations in sequence, reset termination state between runs:

```cpp
TickSimulation sim(config);
// ... build units, initialize ...

// First run
uint64_t cycles1 = sim.runUntilTermination(1000000);

// Reset termination state for next run
sim.terminationController().reset();

// Second run
uint64_t cycles2 = sim.runUntilTermination(1000000);
```

### Performance

| Path | Overhead |
|------|----------|
| Hot path (per epoch) | ~1-2ns (single atomic load) |
| Per cycle (amortized) | ~0.02ns (checked every ~64 cycles) |
| Termination request | ~80ns (mutex-protected, happens once) |

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

### Performance Impact

| Component | Overhead |
|-----------|----------|
| Thread-local context store (per tick) | ~1ns (one `fs:`-relative mov on x86-64) |
| try-catch (sequential, non-exception path) | Zero (Itanium zero-cost exception ABI) |
| try-catch (parallel, per lambda) | Zero per iteration (adds exception table entries only) |
| Signal handler installation | One-time at startup |

## Lifecycle Hooks

```cpp
class MyUnit : public TickableUnit {
public:
    void initialize() override {
        // Called after construction, before simulation starts
    }

    void finalize() override {
        // Called at simulation end
    }
};
```

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
