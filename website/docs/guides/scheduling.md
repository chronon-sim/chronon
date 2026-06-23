---
sidebar_label: "Scheduling and Parallelization"
---

# Scheduling and Parallelization

## Overview

Chronon automatically parallelizes simulation based on dependency analysis and lookahead scheduling.

For wall-clock scheduler diagnosis, Chronon can also emit a Perfetto/Chrome
Trace timeline of logical execution streams, unit tick slices, cross-thread
dependency spin waits, epoch spans, and MPSC arbitration. See
[Scheduler Timeline Trace](scheduler-timeline.md).

## Dependency Graph

The dependency graph captures unit interconnections using Floyd-Warshall for all-pairs shortest path analysis:

```cpp
class DependencyGraph {
public:
    // Build from units and connections
    void build(const std::vector<Unit*>& units,
               const std::vector<ConnectionBase*>& connections);

    // Lookahead queries
    uint32_t lookahead(Unit* source, Unit* dest) const;  // Min path delay
    bool hasPath(Unit* source, Unit* dest) const;

    // Dependency queries
    std::vector<Unit*> getDependencies(Unit* unit) const;  // All predecessors
    std::vector<Unit*> getDependents(Unit* unit) const;     // All successors

    // Direct neighbor queries (returns pairs of Unit* and delay)
    std::vector<std::pair<Unit*, uint32_t>> predecessors(Unit* unit) const;
    std::vector<std::pair<Unit*, uint32_t>> successors(Unit* unit) const;

    // Graph access
    size_t unitIndex(Unit* unit) const;
    Unit* unitAt(size_t index) const;
    const std::vector<std::vector<uint32_t>>& distances() const;

    // Modification
    void addConnection(Unit* source, Unit* dest, uint32_t delay);
    void recomputeLookahead();
};
```

## Cycle Classification

Cycles are classified based on total delay:

| Type | Total Delay | Handling | Example |
|------|-------------|----------|---------|
| Tight | = 0 | Invalid topology (initialization error) | A <--(0)--> B |
| Loose | > 0 | Lookahead (parallel) | A <--(3,2)--> B |

### Tight Cycles

When total delay = 0, the topology has combinational feedback with no registered
state boundary. Chronon rejects this during `TickSimulation::initialize()` because
the `tick()` API permits side effects and does not provide a pure combinational
function contract for convergence iteration:

```wavedrom
{ "signal": [
  { "name": "clk",             "wave": "p.........." },
  {},
  ["Cycle N",
    { "name": "A.tick()",       "wave": "x=x........", "data": ["run"] },
    { "name": "A→B (delay=0)", "wave": "x.=..=..=..", "data": ["v0","v1","v1"] },
    { "name": "B.tick()",       "wave": "x..=x......", "data": ["run"] },
    { "name": "B→A (delay=0)", "wave": "x...=..=...", "data": ["w0","w0"] },
    {},
    { "name": "init error",     "wave": "0.1........" }
  ]
],
  "head": { "text": "Tight cycle: zero-delay feedback is rejected" },
  "config": { "hscale": 1.5 }
}
```

### Loose Cycles (Lookahead)

When total delay > 0, units can run ahead within the lookahead window:

```wavedrom
{ "signal": [
  { "name": "clk",            "wave": "p.........." },
  {},
  ["A ──(3)──▶ B ──(2)──▶ A",
    { "name": "A.localCycle", "wave": "=.=.=.=.=.=", "data": ["0","1","2","3","4","5"] },
    { "name": "A.send()",    "wave": "x=.=.=.=.=.", "data": ["d0","d1","d2","d3","d4"] },
    {},
    { "name": "B.localCycle", "wave": "=.=.=.=.=.=", "data": ["0","1","2","3","4","5"] },
    { "name": "B.receive()",  "wave": "x...=.=.=.=", "data": ["d0","d1","d2","d3"] }
  ]
],
  "head": { "text": "Loose cycle: A and B run in parallel (lookahead = delay)" },
  "foot": { "text": "B is at most 3 cycles behind A (delay A→B = 3). A is at most 2 behind B (delay B→A = 2)." },
  "config": { "hscale": 1.5 }
}
```

## Cycle Analyzer

Uses Tarjan's SCC and Johnson's algorithm to detect and classify cycles:

```cpp
struct CycleInfo {
    std::vector<Unit*> units;     // Units in cycle order
    std::vector<uint32_t> delays; // Delays between consecutive units
    uint32_t total_delay;          // Sum of all delays

    bool isTight() const { return total_delay == 0; }
    uint32_t minEdgeDelay() const;  // Minimum delay on any edge
    bool contains(Unit* unit) const;
};

struct AnalysisResult {
    // All detected cycles
    std::vector<CycleInfo> all_cycles;

    // Cycles classified by type
    std::vector<CycleInfo> tight_cycles;   // delay = 0, invalid for TickSimulation
    std::vector<CycleInfo> loose_cycles;   // delay > 0, can use lookahead

    // Units involved in tight cycles
    std::set<Unit*> tight_cycle_units;

    // Independent groups (no dependencies between groups)
    std::vector<std::vector<Unit*>> independent_groups;

    // Strongly connected components
    std::vector<std::vector<Unit*>> sccs;

    // Lookahead map: {source, dest} -> minimum path delay
    std::map<std::pair<Unit*, Unit*>, uint32_t> lookahead;

    // Query methods
    bool inTightCycle(Unit* unit) const;
    uint32_t safeLookahead(Unit* source, Unit* dest) const;
    bool canParallelize(Unit* a, Unit* b) const;
};

// CycleAnalyzer - static analysis methods
class CycleAnalyzer {
public:
    static AnalysisResult analyze(const DependencyGraph& dep_graph,
                                   size_t max_cycles = 1000);
    static bool hasSelfLoop(const DependencyGraph& dep_graph, Unit* unit);
    static uint32_t minCycleLength(const DependencyGraph& dep_graph, Unit* unit);

    // TickSimulation rejects tight_cycles during initialization.
};
```

## Zero-Delay Handling

Acyclic `delay=0` paths are valid and execute in topological order, so a producer
can make a message visible to its consumer in the same cycle. Zero-delay feedback
loops are invalid: insert `delay>0` on at least one feedback edge, or collapse the
combinational logic into a single unit with explicit internal ordering.

## TickSimulation Execution Model

TickSimulation uses stdexec::static_thread_pool for parallel execution. All scheduling logic (sequential, barrier, and lookahead modes) lives inside `TickSimulation` — there is no separate scheduler class.

```cpp
// TickSimulation uses stdexec
::exec::static_thread_pool pool_{num_threads};

// Barrier mode: per-cycle sync
auto work = stdexec::bulk(
    stdexec::just(),
    stdexec::par,
    units_.size(),
    [this](std::size_t idx) {
        unit_ptrs_[idx]->executeTick();
    }
);
auto scheduled_work = stdexec::starts_on(sched, std::move(work));
stdexec::sync_wait(std::move(scheduled_work));

// Lookahead mode: iterate until convergence
while (made_progress) {
    compute_targets_for_all_units();
    stdexec::bulk(...);  // Execute units to targets
    stdexec::sync_wait(...);
    update_progress();
}
```

### Execution Modes

TickSimulation selects execution mode based on topology:

| Mode | Condition | Description |
|------|-----------|-------------|
| Sequential | Single-threaded or not beneficial | Units execute in order per cycle |
| Barrier | Tight connections present | Per-cycle sync with stdexec::bulk |
| Lookahead | No tight connections | Units run ahead within safe boundaries |
| Cluster-aware | Tight intra-cluster only | Groups on same thread, lookahead between |

### Lazy Wakeup And Multi-Rate Ticks

Chronon can skip a unit's user `tick()` body on cycles where the unit is known to
be inactive while still advancing its local cycle and scheduler progress. This
keeps the runtime tick-driven: there is no global event queue and no callback is
executed on the producer thread.

Units can use activity controls from their own `tick()` body:

```cpp
class TimerDevice : public TickableUnit {
public:
    TimerDevice() : TickableUnit("timer") {
        setTickInterval(1000);  // only run tick() on global cycles divisible by 1000
    }

    void tick() override {
        if (!has_work) {
            sleepUntil(localCycle() + 500);
            return;
        }
        process();
        sleepForever();  // wait for a port arrival or explicit wakeAt()
    }
};
```

The scheduler evaluates a unit as active when both conditions are true:

```text
global_cycle >= unit.nextActiveCycle()
global_cycle % unit.tickInterval() == 0
```

When the unit is inactive, Chronon runs a cheap idle path that only advances the
unit's local cycle. The cluster's completed-cycle progress still advances, so
downstream lookahead dependencies do not stall behind idle units.

In the progress-based lookahead scheduler, if every unit in a cluster is
inactive, Chronon advances the whole cluster in one batch to the next active
unit cycle, dependency boundary, or epoch end. The scheduler timeline still
records this fast path because dependency progress is advancing, but it uses the
`unit idle` category instead of `unit`. The slice detail includes `cycles=N` for
batched idle advances.

Port delivery is an input-driven wakeup source. When a connection successfully
enqueues a message with arrival cycle `A`, Chronon wakes the destination unit at
`A`; the destination still receives the message through its `InPort` during its
own later `tick()` context. This gives event-like behavior without executing
target-unit code from the producer thread.

`wakeAt()` is intentionally only a scheduler hint. If a model communicates
through shared memory or another side channel outside Chronon ports, the model
must still expose the causal relationship to the scheduler, for example by
converting the side-channel write into a port message or an explicit wake source
with a conservative dependency. Otherwise an isolated sleeping unit may have
already advanced past the event's nominal cycle under lookahead and will process
the wake at its next scheduled opportunity.

### Epoch-Free Lookahead

By default the persistent-worker lookahead path synchronizes all worker threads
at a `std::barrier` every `epoch_size` cycles. Within an epoch, clusters advance
out of order on their own predecessor dependencies, but the per-epoch barrier
still makes every thread wait for the slowest one at each boundary — a straggler
tax that hurts most when load is imbalanced.

`enable_epoch_free_lookahead` (default **off**) removes that barrier: the whole
run becomes a single window in which run-ahead is bounded solely by
`lookahead_floor_ + max_lookahead_cycles` (refreshed lazily as the global-minimum
cluster advances) and per-connection MPSC arbitration, with one MPSC flush at the
end of the run instead of one per epoch. Results stay bit-identical to every other
mode; only wall-clock changes.

It is an opt-in A/B knob, not a universal default — it wins on imbalanced,
high-worker-count topologies (where the straggler tax is real) and is neutral or
slightly negative on balanced or dependency-bound chains (where the lazy floor
refresh costs more than the barrier it removes). Measure on your own model.

**When it engages.** The dispatch gate keeps the per-epoch barrier path unless
*all* of the following hold; otherwise it transparently falls back (no result
change):

- `enable_epoch_free_lookahead` is set and `max_lookahead_cycles > 0`;
- the run uses the persistent path — reached via `TickSimulation::run()`;
  `runUntilTermination()` (periodic dumps / termination polling) deliberately
  keeps the per-epoch path for its boundaries, so epoch-free is a no-op there;
- every MPSC input port has fully-resolved per-connection producer progress;
- **cross-thread buffer headroom suffices for every connection** (see below).

**Cross-thread buffer headroom.** Without the per-epoch drain, a producer can run
ahead of a consumer and leave entries buffered in the connection's cross-thread
ring — the per-connection MPSC staging ring for a multi-producer port, or the
SPSC lock-free ring for a single-producer cross-thread edge. For bounded
`InPort`s, lock-free rings are sized at initialization so the declared capacity
fits. For unlimited-capacity `InPort`s, the physical lock-free rings remain
bounded by the default ring size, and the port never model-side back-pressures,
so a producer could silently overflow the physical ring. The gate therefore
vetoes epoch-free unless each connection can absorb the configured run-ahead.
The headroom (in cycles) a connection supports is roughly:

```
headroom = min(InPort capacity, ring slots) / per_cycle_send_rate - edge_delay
```

where `per_cycle_send_rate` is the source `OutPort`'s per-cycle send cap (an
uncapped source forces a veto), `ring slots` is the usable physical ring
capacity, and `edge_delay` accounts for not-yet-due entries the consumer cannot
drain. Same-thread connections drain synchronously and impose no bound. If any
cross-thread connection's headroom is `<= max_lookahead_cycles`, the run falls
back to the barrier path. To use epoch-free with unlimited-capacity cross-thread
edges, give the producing `OutPort` a per-cycle send cap and keep
`max_lookahead_cycles + edge_delay` within the default physical ring, or use an
explicit bounded `InPort` capacity large enough for the desired run-ahead.

## Scheduler Timeline Diagnostics

In progress-based lookahead mode, each tight-coupling cluster publishes its
completed cycle in a cache-line-aligned progress atomic. A worker stream scans
the clusters assigned to it and executes any cluster whose direct predecessor
clusters have reached the required cycle. If no local cluster is ready, the
stream spins until one becomes ready. The scheduler timeline records that time
as `cluster dependency` events and includes the blocking predecessor cluster in
the event detail.

This keeps delay=0 groups atomic while allowing independent clusters assigned
to the same stream to advance out of order. Dynamic rebalance, when enabled,
migrates whole clusters at epoch boundaries; it does not split delay=0 clusters
or migrate units in the middle of an epoch.

Typical investigation workflow:

1. Capture a short window with `simulation.timeline_trace.enabled=true`.
2. Open the JSON in `ui.perfetto.dev` or `chrome://tracing`.
3. Inspect whether `stream N` lanes overlap tightly.
4. Identify streams with long `cluster dependency` slices.
5. Correlate the waiting streams with unit placement and connection delays.

Timeline `stream N` lane names are zero-based Chronon logical stream ids. The
Chrome Trace `tid` values are intentionally 1-based for Perfetto compatibility;
use each slice's `args.stream` field when correlating viewer output back to
Chronon stream ids. The `scheduler` lane is separate from worker streams and
only records scheduler-side spans.

Example:

```bash
./my_sim config.yaml --no-observe \
  -p simulation.timeline_trace.enabled=true \
  -p simulation.timeline_trace.file=out/chronon_timeline.json \
  -p simulation.timeline_trace.end_cycle=2000
```

Long wait slices usually indicate that one predecessor cluster is on the
critical path, that low-delay edges are forcing near-lockstep execution, or that
the current partition assigned too much work to a dependency anchor stream.

## Configuration

```cpp
struct TickSimulationConfig {
    // Thread pool configuration
    size_t num_threads = std::thread::hardware_concurrency();

    // Scheduler selection (placement is always cluster-aware: cluster.size()==1
    // is the no-tight-coupling fallback).
    //   enable_parallel=false              -> Sequential
    //   enable_parallel && !enable_lookahead -> Barrier (per-cycle sync across clusters)
    //   enable_parallel &&  enable_lookahead -> Lookahead (per-cluster progress atomics)
    bool enable_parallel = true;
    bool enable_lookahead = true;

    // Lookahead configuration
    uint32_t max_lookahead_cycles = 100;    // Max cycles a unit can run ahead
    uint64_t epoch_size = 64;               // Cycles per epoch before sync
    bool enable_epoch_free_lookahead = false; // Drop the per-epoch barrier (see below)

    // Debug options
    bool trace_execution = false;           // Log execution mode selection

    // Cost-aware weighted partitioning (default: enabled)
    bool enable_weighted_partitioning = true;
    uint64_t profiling_warmup_cycles = 512;       // Warmup before measuring
    uint64_t profiling_measurement_cycles = 1024; // Measurement window

    // Dynamic rebalancing
    bool enable_dynamic_rebalance = true;
    double rebalance_imbalance_threshold = 1.3;
    uint64_t rebalance_check_interval_cycles = 8192;
    double rebalance_min_gain = 0.05;
    uint64_t rebalance_cooldown_cycles = 0;
};
```

These settings can be configured via YAML (`enable_parallel`, `enable_lookahead`) or set directly in code. All scheduling modes produce identical cycle-accurate results — they differ only in wall-clock performance.

Dynamic rebalance is enabled by default for throughput-oriented runs. It samples
unit tick cost periodically, computes per-stream total sampled work, and
migrates whole tight clusters at epoch boundaries when the heaviest stream is
more than `rebalance_imbalance_threshold` above the active-stream average. Set
`enable_dynamic_rebalance: false` for deterministic partitioning studies or
A/B runs that must preserve a fixed initial assignment.
`rebalance_min_gain` can suppress migrations with too little predicted speedup,
and `rebalance_cooldown_cycles` can enforce a minimum cycle gap between applied
rebalances.

### Exception Handling in Execution Paths

All execution modes wrap tick calls with try-catch to capture exceptions with crash context:

| Mode | Strategy | Overhead |
|------|----------|----------|
| Sequential | try-catch outside outer loop | Zero (Itanium zero-cost ABI) |
| Parallel (bulk) | try-catch inside each lambda body; stdexec propagates exceptions natively via `set_error` / `sync_wait` | Zero per non-exception iteration |
| Progress-based (stdexec::bulk) | try-catch around inner loop; on exception, calls `stop_source_->request_stop()` to break spin-waits, then rethrows for stdexec native propagation | Zero per non-exception iteration |

A unified `stdexec::inplace_stop_source` handles both exception-driven abort and unit-initiated termination. Worker spin-waits check `token.stop_requested()` to exit promptly on either condition. Exceptions are wrapped as `TickException` with unit name and cycle, then rethrown on the main thread by `stdexec::sync_wait`.

## Cost-Aware Weighted Partitioning

When `enable_weighted_partitioning = true` (default) and at least 4 units exist, TickSimulation uses a unified cluster-aware + cost-aware partitioning pipeline:

### Algorithm Pipeline

1. **Platform benchmarking**: Measures atomic round-trip sync cost (~36ns typical)
2. **Tick cost profiling**: Runs each unit for `profiling_warmup_cycles` + `profiling_measurement_cycles` to measure per-unit tick latency (mean/median in nanoseconds)
3. **Tight cluster detection**: Groups units with delay=0 connections into clusters (units within a cluster must share a thread)
4. **Cluster-level graph partitioning**: Treats each cluster as a super-node with aggregated cost and runs `WeightedPartitioner` to minimize max thread time
5. **Thread assignment**: Maps cluster assignments back to per-unit thread assignments
6. **Queue optimization**: Selects optimal queue type per connection based on thread placement

### WeightedPartitioner

Four-phase algorithm in `src/sender/schedule/WeightedPartitioner.hpp`:

- **Phase 1 (LPT)**: Longest Processing Time first — sorts units by decreasing cost and assigns each to the thread with the minimum current load. Pure makespan minimization (no coupling considered). Provides a 4/3-OPT approximation for the multiprocessor scheduling problem.
- **Phase 2 (FM Refinement)**: Iteratively moves units from heaviest to lightest thread when the move reduces max thread time (up to 5 passes). Accounts for sync cost changes from the move.
- **Phase 3 (Pairwise Swap)**: Tries swapping units between all pairs of threads to escape local minima (handles balanced-but-suboptimal assignments where tightly coupled units were arbitrarily separated by LPT).
- **Phase 4 (Multi-Unit Relocate)**: Tries removing pairs of units from the heaviest thread and distributing them to the two lightest threads. Handles cases where no single move or swap improves the makespan.

### Delay-Aware Sync Cost Model

The partition adjacency graph uses **directed edges** — each `Connection` object creates one adjacency entry (source → destination). For bidirectional communication (e.g., wakeup buses), separate `Connection` objects in each direction naturally produce edges in both directions. This avoids double-counting bus connections, which expand to N×M individual connections at config load time.

Cross-thread synchronization cost scales inversely with connection delay:

```
sync_cost(edge) = platform_sync_ns * num_connections * delay_factor(min_delay)
```

| Delay | Factor | Rationale |
|-------|--------|-----------|
| 0 | 100.0 | Inline/same-cycle: prohibitively expensive to split |
| 1 | 1.0 | Tight spin-waiting every cycle |
| N > 1 | 1/N | Higher delay = less frequent synchronization |

This ensures delay=0 connections force co-location, while high-delay connections can tolerate cross-thread placement.

### Parallel Execution Decision

With weighted partitioning, parallel execution is beneficial when:

```
max_thread_cost * 1.10 < total_sequential_cost
```

The 10% overhead factor accounts for synchronization costs. This heuristic correctly accepts parallelization at moderate imbalance (e.g., 1.75x speedup) while rejecting extreme cases where one thread dominates.

### Fallback Paths

When weighted partitioning is disabled or fewer than 4 units exist:

| Condition | Path |
|-----------|------|
| `tight_connections` present | Topology-only cluster assignment |
| Otherwise | Greedy thread assignment with unit-count heuristic |

The unit-count heuristic requires: no thread has >50% of units AND >= 3 units per active thread.

## Queue Optimization

Based on thread assignment, connections are optimized:

| Connection Type | Queue Implementation | Overhead |
|-----------------|---------------------|----------|
| Same thread (intra-cluster) | SingleThreadMessageQueue | Zero (no synchronization) |
| Cross-thread SPSC | LockFreeMessageQueue | Atomics only |
| Cross-thread MPSC | MultiProducerQueueAdapter | Per-thread queues + merge |

**Impact**: Eliminates ~18% mutex overhead from message queues when units are properly clustered.

When parallelism is not beneficial, simulation falls back to optimized sequential execution with single-thread queues and non-atomic cycle counters.

See `port-system.md` for detailed queue implementation and performance characteristics.

## Performance

| Threads | Throughput |
|---------|------------|
| 1 | ~9.35 Mcycles/sec |
| 2 | ~10.00 Mcycles/sec (7% faster with lock-free) |
| 4+ | Workload dependent |

Multi-thread performance depends on dependency structure. Tight coupling (delay=0) must execute on same thread.
