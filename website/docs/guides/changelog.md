---
sidebar_label: "Changelog"
---

# Changelog

## 2026-06-11 - Epoch-Free Lookahead

### New Feature

`TickSimulationConfig::enable_epoch_free_lookahead` (default off) collapses the
per-epoch `std::barrier` of the persistent-worker lookahead path into a single
run-spanning window. Run-ahead is then bounded only by `lookahead_floor_ +
max_lookahead_cycles` and per-connection MPSC arbitration, with one MPSC flush at
the end of the run instead of one per epoch. Results stay bit-identical across all
scheduling modes; only wall-clock changes.

It is an opt-in A/B knob: it wins on imbalanced, high-worker-count topologies
where the per-epoch barrier's straggler wait dominates, and is neutral or slightly
negative on balanced or dependency-bound chains.

### Safety

The dispatch gate keeps the per-epoch barrier path (no result change) unless every
MPSC connection can absorb the configured run-ahead without overflowing its staging
ring or back-pressuring where the barrier flush would not. The supported run-ahead
per connection is `min(InPort capacity, ring slots) / per_cycle_send_rate -
edge_delay`; an uncapped source rate forces a fallback. See the
[Scheduling guide](scheduling.md) for details.

## 2026-05-07 - Scheduler Timeline Trace

### New Feature

Chronon can emit a Chrome Trace / Perfetto JSON timeline for scheduler-level
parallelism diagnostics. The timeline shows logical Chronon streams, unit tick
duration slices, cluster dependency spin waits, scheduler epoch spans, and MPSC
arbitration spans.

The Perfetto thread ids in the JSON are 1-based so worker stream 0 does not use
`tid=0`, which some Chrome Trace viewers display with main-thread-like
semantics. The lane names remain zero-based (`stream 0`, `stream 1`, ...), and
each duration slice includes `args.stream` with the original Chronon logical
stream id. The separate `scheduler` lane records scheduler-side spans and is
not an additional simulation worker.

### Motivation

Low wall-clock throughput can come from poor stream overlap, low-delay
dependencies forcing near-lockstep execution, overloaded critical streams, or
visible arbitration cost. Existing counters and pipeline traces describe model
state, but they do not directly show scheduler stream overlap or spin-wait wall
time. The scheduler timeline fills that gap without requiring unit-level
instrumentation.

### Configuration

```yaml
simulation:
  timeline_trace:
    enabled: true
    file: out/chronon_timeline.json
    end_cycle: 2000
```

See [Scheduler Timeline Trace](scheduler-timeline.md) for all fields and event
semantics.

### New Files

| File | Description |
|------|-------------|
| `src/sender/schedule/SchedulerTimelineTrace.hpp` | Timeline configuration, per-stream event buffers, and Chrome Trace JSON writer |
| `docs/scheduler-timeline.md` | User guide for capture, event interpretation, and overhead |

### Modified Files

| File | Change |
|------|--------|
| `src/sender/core/TickSimulation.hpp` | Records unit, wait, epoch, and arbitration slices in scheduler paths |
| `src/sender/config/SenderConfigLoader.hpp` | Parses `simulation.timeline_trace` |
| `src/sender/config/SenderSimulationBuilder.hpp` | Passes timeline config into `TickSimulationConfig` |
| `src/sender/config/SenderUnitConfig.hpp` | Adds `SchedulerTimelineTraceConfig` to YAML config |
| `src/sender/app/SimulationApp.cpp` | Writes the timeline after simulation run completion |
| `docs/configuration.md` | Documents YAML fields |
| `docs/scheduling.md` | Documents scheduler diagnosis workflow |
| `docs/performance.md` | Adds performance-investigation usage note |

### Performance Impact

The feature is disabled by default. Disabled runs do not take per-cycle timeline
timestamps. Enabled timeline captures intentionally perturb wall-clock
throughput and should be used for diagnosis rather than final benchmark numbers.

---

## 2026-03-05 - Remove Legacy Pipeline Register Types

### Breaking API Changes

- Removed `PipelineReg<T>` and `PipelineRegMulti<T, N>` (manual tick-based registers).
- Removed `TrackedPipelineReg<T>` and `TrackedPipelineRegArray<T, N>` (write-tracking wrappers).
- Removed `PipelineControl.hpp` (`PipelineStallController`, `PipelineFlushController`).
- Removed `pipeline_tick()`, `pipeline_reset()`, `pipeline_tick_with_stall()` helper functions.
- Removed `PhasedPipelineReg<T>` and `PhasedPipelineRegArray<T, N>` — their ping-pong slot logic has been inlined into `SingleStageReg` and `StageReg` respectively.

### Migration

All pipeline register usage should migrate to `StageReg<T, N>` (multi-pipe) or `SingleStageReg<T>` (single-entry).
See [Stage Registers](stage-registers.md) for the full API.

### Removed Files

| File | Replacement |
|------|-------------|
| `src/sender/util/PipelineReg.hpp` | `SingleStageReg<T>` or `StageReg<T, 1>` |
| `src/sender/util/PipelineRegMulti.hpp` | `StageReg<T, N>` |
| `src/sender/util/WriteTracker.hpp` | `StageReg` / `SingleStageReg` (built-in write tracking) |
| `src/sender/util/PipelineControl.hpp` | Use `StagePipeline<S...>::flushIf()` / `reset()` |
| `test/sender/test_pipeline_reg_multi.cpp` | — |
| `test/sender/bench_phased_pipeline_reg.cpp` | — |

---

## 2026-02-14 - Cost-Aware Weighted Partitioning for Thread Assignment

### Problem

The previous thread assignment used topology-only heuristics (cluster size, unit count) that ignored actual per-unit execution cost. This led to poor load balancing when units had vastly different tick costs (e.g., Fetch at 5000ns vs Decode at 200ns). The parallel-beneficial heuristic (`max < 1.5 * avg`) also rejected parallelization that would provide significant speedup at moderate imbalance.

### Changes

- **Unified cluster-aware + cost-aware partitioning**: New default path (`enable_weighted_partitioning = true`) that profiles per-unit tick costs, detects tight clusters, builds a cluster-level partition graph, and runs `WeightedPartitioner` to minimize max thread time.
- **WeightedPartitioner** (`src/sender/schedule/WeightedPartitioner.hpp`): Four-phase algorithm — LPT initial placement (4/3-OPT makespan), FM refinement (up to 5 passes, move units between heaviest/lightest threads), all-pairs pairwise swap (escapes balanced-but-suboptimal local minima), and multi-unit relocate (removes pairs from bottleneck thread).
- **Delay-aware sync cost model**: Sync cost scales as `platform_sync_ns * connections * delay_factor`. Delay=0 edges get 100x penalty (forcing co-location for inline connections), delay=1 gets 1x, delay=N gets 1/N. Adjacency uses directed edges (one entry per Connection) to avoid double-counting bus connections.
- **Improved parallel-beneficial heuristic**: `max_cost * 1.10 < total_cost` instead of `max < 1.5 * avg`. Correctly accepts parallelization at moderate imbalance (e.g., 1.75x speedup) while rejecting extreme cases.

### New Files

| File | Description |
|------|-------------|
| `src/sender/schedule/WeightedPartitioner.hpp` | Cost-aware min-max graph partitioner |
| `test/sender/test_weighted_partitioner.cpp` | 12 test cases covering partitioner correctness |

### Modified Files

| File | Change |
|------|--------|
| `src/sender/core/TickSimulation.hpp` | Unified partitioning pipeline, `profileAndAssignThreadsClustered_()`, extracted `buildPartitionAdjacency_()`, new `parallelBeneficialWeighted_()` heuristic |
| `test/sender/test_thread_queue_hardening.cpp` | Disabled weighted partitioning in topology-only fallback test |
| `test/sender/CMakeLists.txt` | Added weighted partitioner test target |

### Configuration

New `TickSimulationConfig` fields:

| Field | Default | Description |
|-------|---------|-------------|
| `enable_weighted_partitioning` | `true` | Enable cost-aware weighted partitioning |
| `profiling_warmup_cycles` | `512` | Warmup ticks before measuring |
| `profiling_measurement_cycles` | `1024` | Measurement window for tick costs |
| `enable_dynamic_rebalance` | `true` | Runtime rebalancing based on tick samples |
| `rebalance_imbalance_threshold` | `1.3` | Imbalance ratio to trigger rebalance |
| `rebalance_check_interval_cycles` | `8192` | Cycles between rebalance checks |
| `rebalance_min_gain` | `0.05` | Skip rebalance if predicted gain is below this fraction |
| `rebalance_cooldown_cycles` | `0` | Minimum cycles between applied rebalances |

## 2026-02-11 - Debug Build Backpressure for Observation Events

### Problem

The observation system silently drops events when per-thread SPSC queues fill up. This makes debug output unreliable under high event volume — exactly when you need it most.

### Changes

- **Debug spin-wait**: In debug builds (`NDEBUG` not defined), `emitStructured()` now spins until queue space is available instead of dropping events. Uses architecture-specific pause instructions (`__builtin_ia32_pause` on x86-64, `yield` on AArch64) with periodic `std::this_thread::yield()` to avoid starving the backend.
- **Eager read commits**: Backend drain loops now call `eagerCommitRead()` instead of batched `commitRead()`, making freed queue space visible to producers immediately rather than waiting for the 4KB batch threshold.
- **Backend wakeup callback**: `ThreadContextManager` exposes `setBackendWakeup()`/`wakeBackend()` so producers can wake the backend from its 100us sleep when spinning on a full queue.

### Performance Impact

- **Release builds**: Zero impact. The spin-wait is compiled out via `#ifndef NDEBUG`, and `eagerCommitRead()` adds at most one atomic store per queue per drain cycle.
- **Debug builds**: Events are never dropped. Producers may briefly spin-yield when queues are full, which can reduce peak throughput under extreme event pressure but guarantees complete debug output.

### Modified Files

| File | Change |
|------|--------|
| `src/observe/SPSCQueue.hpp` | Added `eagerCommitRead()` |
| `src/observe/ThreadContextManager.hpp` | Added `setBackendWakeup()` and `wakeBackend()` |
| `src/observe/ObservationBackend.cpp` | Registered wakeup callback; switched to eager commits |
| `src/observe/ObservationContext.hpp` | `#ifndef NDEBUG` spin-wait in both `emitStructured()` overloads |
| `test/observe/test_observation_hardening.cpp` | Added no-drops-under-pressure test |

## 2026-02-10 - Crash Handler for Simulation Tick Exceptions

### New Features

- **Signal handling**: `CrashHandler::install()` registers handlers for SIGSEGV, SIGBUS, SIGABRT, SIGFPE, SIGILL. On fatal signal, prints minimal async-signal-safe crash context and exits immediately.
- **Exception handling**: All `TickSimulation` execution paths (sequential, parallel, progress-based) wrap tick calls with try-catch. Exceptions are wrapped as `TickException` with unit name, cycle, and cause.
- **Emergency flush**: `CrashHandler::emergencyFlush()` commits per-thread observer queues and drains output files. Called automatically by `SimulationApp` exception handlers.
- **Thread-local tick context**: `executeTick()` and `executeTickPhased()` track the currently-executing unit in thread-local storage for crash diagnostics.

### New Files

- `src/sender/core/CrashHandler.hpp` - `CrashHandler`, `TickException`, thread-local `TickContext`
- `src/sender/core/CrashHandler.cpp` - Signal handler and emergency flush implementation

### Modified Files

- `src/sender/core/TickableUnit.hpp` - `executeTick()` / `executeTickPhased()` set/clear thread-local context
- `src/sender/core/TickSimulation.hpp` - try-catch on all execution paths, `captureTickException()` / `checkAndRethrowTickException()` for parallel exception propagation
- `src/sender/app/SimulationApp.cpp` - Installs crash handler, catches `TickException`, calls `emergencyFlush()`
- `src/CMakeLists.txt` - Added `CrashHandler.cpp` to library sources

### Performance Impact

Zero overhead on non-exception paths. Thread-local context store costs ~1ns per tick (one `fs:`-relative mov on x86-64).

## 2026-02-07 - Dead Code Cleanup and API Simplification

This release removes deprecated/unused code identified by `docs/DEAD_CODE_AUDIT.md`.

### Breaking API Changes

- Removed `AutoPipelineReg<T>` and `makeAutoPipelineRegArray()`.
- Removed `TickableUnit::registerPipelineRegCallback(...)` and automatic pipeline advancement hooks.
- Removed legacy sender factory macro `REGISTER_SENDER_UNIT`.
- Removed legacy observation macro header `ObserveMacros.hpp` and transitive include from the observation umbrella headers.
- Removed legacy parameter macros from `ParameterSet.hpp`:
  - `BEGIN_PARAMETER_SET`
  - `PARAM`
  - `END_PARAMETER_SET`
  - `WITH_YAML_SERIALIZATION`

### Build and Test System Changes

- Removed `CHRONON_BUILD_BENCHMARK` CMake option.
- Removed obsolete benchmark tree under `test/benchmark/`.
- Removed empty `test/exec/` CMake target.
- Removed stale install reference to `src/exec/` in `src/CMakeLists.txt`.

### Other Removals

- Removed dead `src/factory/ResourceFactory.hpp`.
- Removed empty `src/sender/port/PortDirectory.cpp`.

## Migration Guide

### 1) All Pipeline Registers -> StageReg / SingleStageReg

Before (any old style):

```cpp
class U : public TickableUnit {
    PipelineReg<Data> s1_;      // or AutoPipelineReg, PhasedPipelineReg, etc.
    PipelineReg<Data> s2_;
};
```

After:

```cpp
class U : public PhasedTickableUnit<U> {
    SingleStageReg<Data> s1_;
    SingleStageReg<Data> s2_;

    template<ValidPhase P>
    void tickPhase() {
        s1_.beginCycle<P>();
        s2_.beginCycle<P>();
        simpleForward<P>(s1_, s2_);
        if (hasInput()) s1_.set<P>(fetchInput());
    }
};
```

See [Stage Registers](stage-registers.md) for full API.

### 2) REGISTER_SENDER_UNIT -> AutoRegisteredUnit

Before:

```cpp
class MyUnit : public TickableUnit {
    // ...
};
REGISTER_SENDER_UNIT(MyUnit);
```

After:

```cpp
class MyUnit : public AutoRegisteredUnit<MyUnit> {
public:
    static constexpr const char* unit_type_name = "MyUnit";
    static constexpr const char* unit_description = "...";
    // ...
};
```

### 3) Observation Macros -> Template API

Before:

```cpp
OCOUNT(ctx, COUNTER_ID);
OTRACE(ctx, CAT_ID, "pc={}", pc);
OLOG_INFO(ctx, "done");
```

After:

```cpp
count(MY_COUNTER);
trace<"pc={}">(MY_CATEGORY, pc);
info<"done">();
```

### 4) Legacy Parameter Macros -> Param<T>

Before:

```cpp
BEGIN_PARAMETER_SET(MyParams, ParameterSet)
    PARAM(uint32_t, width, 4, "Width")
END_PARAMETER_SET()
```

After:

```cpp
struct MyParams : public ParameterSet {
    Param<uint32_t> width{this, "width", 4, "Width"};
};
```

## Upgrade Checklist

- Replace all pipeline register usage with `StageReg<T, N>` or `SingleStageReg<T>`.
- Remove `REGISTER_SENDER_UNIT(...)` from translation units.
- Replace observation macros with `count()/trace<>/debug<>/info<>/warn<>/error<>`.
- Rewrite any macro-based `ParameterSet` declarations to `Param<T>` fields.
