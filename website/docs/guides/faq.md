---
sidebar_label: "FAQ and Troubleshooting"
---

# FAQ and Troubleshooting

## Common Issues

### Port Not Found

```
Error: Port not found: cpu.Fetch.out_inst
```

**Causes:**
- Port constructed without name
- Port constructed without owner
- `setTreeNode()` not called
- Typo in YAML port path

**Solution:**
```cpp
// Ensure port has owner and name:
OutPort<T> out{this, "port_name"};

// Ensure setTreeNode is called:
setTreeNode(node);
```

### Type Mismatch

```
Error: Type mismatch: cpu.Fetch.out_inst (Instruction) -> cpu.Decode.in_inst (uint64_t)
```

**Cause:** Source and destination ports have different data types.

**Solution:** Ensure both ports use the same template parameter `T`.

### Factory Not Found

```
Error: No factory registered for type: MyUnit
```

**Causes:**
- Using `AutoRegisteredUnit` but unit not linked
- Typo in YAML `type:` field

**Solution:**
```cpp
// Ensure unit inherits from AutoRegisteredUnit
class MyUnit : public AutoRegisteredUnit<MyUnit> { ... };
```

### Phase Violation

```
Error: Cannot add child in CONFIGURING phase
```

**Cause:** Attempting structural changes after BUILDING phase.

**Solution:** Complete tree structure before transitioning phases.

## FAQ

### Do I need to register ports manually?

No. Ports auto-register when constructed with owner and name:

```cpp
OutPort<Data> out{this, "out"};  // Auto-registers
```

### Can I have multiple connections from one OutPort?

Yes. OutPort supports fanout:

```cpp
sim.connect(source->out, consumer1->in, 1);
sim.connect(source->out, consumer2->in, 1);
// Both receive the same data
```

### How do I access another unit?

Navigate via tree:

```cpp
auto* other = treeNode()->parent()
    ->getChild("Other")
    ->getResource<OtherUnit>();
```

### Can I create units without YAML?

Yes. Use `createUnit()` directly:

```cpp
auto* unit = sim.createUnit<MyUnit>(arg1, arg2);
```

### What's the difference between `delay=0` and `delay>0`?

| Delay | Mode | Latency | Execution |
|-------|------|---------|-----------|
| 0 | INLINE | 0 cycles | Sequential (same thread) |
| >0 | SPSC | N cycles | Parallel possible |

### How do I use counters?

Declare `Counter` members in your unit class:

```cpp
Counter ops_{this, "ops", "Operations executed", "ops"};
```

Each unit instance gets its own counter with hierarchical naming (e.g., `alu0.ops`, `alu1.ops`).

### How do I disable observability for benchmarks?

```bash
./my_sim --no-observe
```

Or in YAML:
```yaml
observation:
  enabled: false
```

### How do I override YAML values from command line?

```bash
./my_sim config.yaml -p simulation.num_workers=4
```

### Why isn't my simulation using multiple threads?

Check:
1. `enable_parallel = true` in config
2. Connection delays are >0 (delay=0 forces sequential)
3. Units have sufficient independence (no tight cycles)

### How do I profile my simulation?

```bash
# Use perf
perf record ./my_sim config.yaml
perf report

# Or with observation
./my_sim config.yaml -o /tmp/profile_output
```

### How do I write a termination condition?

```cpp
void tick() override {
    if (retired_count_ >= target_) {
        requestTermination(TerminationReason::Completed, 0,
            "Reached target: " + std::to_string(target_));
    }
}
```

Then use:
```cpp
sim.runUntilTermination(max_cycles);
```

### What log levels are available?

```cpp
debug<"...">(args...);  // LogLevel::Debug
info<"...">(args...);   // LogLevel::Info
warn<"...">(args...);   // LogLevel::Warn
error<"...">(args...);  // LogLevel::Error
```

Set minimum level:
```cpp
ctx.filter().setMinLogLevel(LogLevel::Info);  // Debug filtered out
```

### My simulation crashes (segfault) and I lose all observer data. How do I debug?

Chronon automatically installs signal handlers when using `SimulationApp`. On fatal signals, Chronon now uses a strict async-signal-safe handler: it prints minimal crash context and exits immediately.

```
=== CHRONON CRASH ===
Signal: SIGSEGV (11)
TickCtx: active
UnitPtr: 0x0000000000001234
Cycle:   12345
Exiting now.
```

For C++ exceptions (for example, unit `tick()` throwing), `SimulationApp` catches `TickException` and performs `CrashHandler::emergencyFlush()`, so observer files are still flushed.

For manual simulation setups (not using `SimulationApp`), install signal handlers explicitly:
```cpp
chronon::sender::CrashHandler::install();
```

### A unit's tick() throws an exception. How do I find which unit?

`TickSimulation` wraps all execution paths with try-catch and rethrows as `TickException` containing the unit name and cycle:

```cpp
try {
    sim.runUntilTermination(max_cycles);
} catch (const chronon::sender::TickException& e) {
    std::cerr << "Unit: " << e.unitName()
              << " Cycle: " << e.cycle()
              << " Error: " << e.cause() << "\n";
}
```

`SimulationApp` handles this automatically and calls `CrashHandler::emergencyFlush()` to save observer data.

### Can I manually flush observer data in an error handler?

Yes:
```cpp
chronon::sender::CrashHandler::emergencyFlush();
```

This commits per-thread observer queues and flushes all output files.

## Migration from Older Versions

### From Manual Port Registration

Old:
```cpp
registerOutPort("out", &out_);  // No longer exists
```

New:
```cpp
OutPort<T> out_{this, "out"};  // Auto-registers
```

### From Legacy Counter Macros

Old:
```cpp
OCOUNT(ctx, COUNTER_ID);
```

New:
```cpp
Counter my_counter_{this, "my_counter", "desc"};  // Per-unit member
++my_counter_;                                      // Increment
my_counter_ += 5;                                   // Add
```

### From Manual Counter IDs

Old:
```cpp
OBSERVE_COUNTER_ID(MY_COUNTER, 5);
```

New:
```cpp
Counter my_counter_{this, "my_counter", "desc"};  // Per-unit member
```

### From AutoPipelineReg / PipelineReg

All old pipeline register types (`AutoPipelineReg`, `PipelineReg`, `PipelineRegMulti`, `PhasedPipelineReg`, `PhasedPipelineRegArray`) have been removed. Use:

```cpp
// Single-entry stage
SingleStageReg<Data> stage_;

// Multi-pipe stage (4-wide)
StageReg<Data, 4> stage_;
```

See [Stage Registers](stage-registers.md) for full API documentation.

### From Parameter Macros

Old:
```cpp
BEGIN_PARAMETER_SET(MyParams, ParameterSet)
    PARAM(uint32_t, width, 4, "Width")
END_PARAMETER_SET()
```

New:
```cpp
struct MyParams : public ParameterSet {
    Param<uint32_t> width{this, "width", 4, "Width"};
};
```

For all breaking changes and migration snippets, see `docs/changelog.md`.
