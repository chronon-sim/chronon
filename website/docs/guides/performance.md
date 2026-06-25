---
sidebar_label: "Performance Characteristics"
---

# Performance Characteristics

## Simulation Throughput

| Configuration | Throughput |
|--------------|------------|
| Tick-based execution | ~90+ Mcycles/sec |
| 1 thread (`cpu_pipeline_yaml_example --no-observe`) | ~11.2 Mcycles/sec |
| 2 threads (`cpu_pipeline_yaml_example --no-observe`) | ~11.5 Mcycles/sec |

## Operation Latency

### Core Operations

| Operation | Latency |
|-----------|---------|
| `tick()` call | ~2-5 ns |
| `send()` | ~5-10 ns |
| `tryReceive()` | ~5-10 ns |
| Epoch sync | ~100 ns |

### Counter Operations

| Operation | Latency |
|-----------|---------|
| Counter increment (enabled) | ~1-2 ns |
| Counter increment (disabled) | ~0 ns |
| `get()` (single counter) | ~0.1 ns |
| `commitEpoch()` | ~0.3 ns |
| `rollbackEpoch()` | ~0.3 ns |

### Observability Operations

| Operation | Latency |
|-----------|---------|
| Trace (category disabled) | ~2 ns |
| Trace (category enabled) | ~5-10 ns |
| `shouldTrace()` check | ~0.2 ns |
| `anyEnabled()` | ~0.2 ns |

### Termination Checking

| Path | Overhead |
|------|----------|
| Hot path (per epoch) | ~1-2 ns |
| Per cycle (amortized) | ~0.02 ns |
| Termination request | ~80 ns |

### Pipeline Register Operations (StageReg / SingleStageReg)

StageReg and SingleStageReg use phase-based compile-time slot selection internally, achieving ~0.5-1.2 ns per pipeline stage per cycle. Port operations (~70 ns/cycle) typically dominate total cycle cost in full simulation.

### Debug vs Release Observation Behavior

| Build Mode | Queue Full Behavior | Drop Guarantee | Overhead |
|------------|-------------------|----------------|----------|
| Release (`NDEBUG`) | Fire-and-forget drop | Events may be dropped | Zero additional |
| Debug | Spin-wait until space | No drops | Producer may stall briefly |

In debug builds, producers spin-yield when the per-thread SPSC queue is full, waking the backend to drain events. This guarantees complete debug output but may reduce peak throughput under extreme event pressure.

## Queue Performance

| Queue Type | Push | Pop |
|------------|------|-----|
| SingleThreadMessageQueue | ~2 ns | ~2 ns |
| LockFreeQueueAdapter | ~5-10 ns | ~5-10 ns |
| MultiProducerQueueAdapter | ~5-10 ns | ~10-20 ns |
| MessageQueue (mutex) | ~50-100 ns | ~50-100 ns |

## Memory Overhead

### Core Components

| Component | Size |
|-----------|------|
| Unit base | ~64 bytes |
| OutPort | ~48 bytes + connections |
| InPort | ~64 bytes + queue |
| MessageQueue | ~128 bytes + entries |

### Counter Storage

| Architecture | Per-Unit | 9 Units |
|--------------|----------|---------|
| v2.0-v2.1 (Dense) | 64 KB | 576 KB |
| v2.2 (Sparse) | 272 bytes | 2.4 KB |
| **Savings** | **99.6%** | **99.6%** |

### Other Components

| Component | Size |
|-----------|------|
| ObservationQueue | 256 KB (default) |
| LookaheadBuffer | 4 KB per unit |
| SimpleCounter | 16 bytes |
| FormatRegistry | ~16 KB |

## Thread Scaling

| Scenario | Scaling |
|----------|---------|
| Independent units | Near-linear |
| Tight coupling (delay=0) | Same thread (no parallelism) |
| Loose coupling (delay>0) | Parallel within lookahead window |

### Thread Assignment Algorithm

TickSimulation selects the best thread assignment strategy based on configuration:

**Primary path: Cost-aware weighted partitioning** (`enable_weighted_partitioning = true`, default)

When at least 4 units exist, the unified cluster-aware + cost-aware pipeline:

1. **Measure platform sync cost**: Benchmarks atomic round-trip latency (~36-50ns typical)
2. **Profile tick costs**: Measures per-unit tick latency in nanoseconds (warmup + measurement phases)
3. **Detect tight clusters**: Groups delay=0 units into clusters that must share a thread
4. **Partition clusters**: `WeightedPartitioner` assigns cluster super-nodes to threads via a 4-phase algorithm (LPT placement → FM refinement → pairwise swap → multi-unit relocate), minimizing max thread time with delay-aware sync cost model
5. **Optimize queues**: Same-thread connections use `SingleThreadMessageQueue` (zero overhead), cross-thread uses lock-free queues

The sync cost model uses directed adjacency (one edge per Connection object) to avoid double-counting bus connections. Cost scales inversely with connection delay: delay=0 edges get 100x the sync penalty (forcing co-location), while high-delay edges tolerate cross-thread placement.

**Parallel beneficial heuristic** (weighted):
```
max_thread_cost * 1.10 < total_sequential_cost
```
Accepts parallelization whenever the speedup exceeds the ~10% sync overhead margin.

**Fallback path: Topology-only assignment** (when weighted partitioning is disabled or < 4 units)

- Cluster affinity groups delay=0 units on the same thread
- Unit-count heuristic: `max_thread_units * 2 <= total_units` AND `total_units >= active_threads * 3`

When parallelism is not beneficial, simulation falls back to single-threaded execution with single-thread queues and non-atomic cycle counters.

### Lock-Free Optimization Impact

Before (mutex-protected):
- `pthread_mutex_lock`: 19.95% CPU time
- 2-thread mode 77% slower than 1-thread

After (lock-free):
- `pthread_mutex_lock`: <1% CPU time
- 2-thread mode 7% faster than 1-thread

## Optimization Tips

### Reduce Port Overhead

```cpp
// Batch sends when possible
if (out.canSend()) {
    for (auto& item : batch) {
        out.send(item);
    }
}
```

### Counter Efficiency

```cpp
// Per-unit counter (~2-3ns increment)
Counter ops_{this, "ops", "Operations"};
```

### Disable Observability for Benchmarks

```bash
./my_sim --no-observe
```

### Inspect Scheduler Parallelism

Chronon can emit a Perfetto scheduler timeline without adding unit-level
instrumentation. The trace is disabled by default because detailed unit slices
intentionally measure the hot path.

```yaml
simulation:
  observation:
    timeline:
      scheduler:
        enabled: true
        start_cycle: 0
        end_cycle: 2000
        max_events: 1000000
        trace_units: true
        trace_waits: true
        trace_epochs: true
        trace_arbitration: true
```

CLI override example:

```bash
./build_release/nucleus test.elf -n 200000 --no-observe \
  -p simulation.observation.timeline.scheduler.enabled=true \
  -p simulation.observation.timeline.scheduler.end_cycle=2000
```

With the observation backend running, the scheduler slices appear in the run's
`timeline.pftrace` under a "Chronon Scheduler" process group; with
`--no-observe`, a standalone Perfetto file (default `chronon_timeline.pftrace`)
is written instead. Open the `.pftrace` file in `ui.perfetto.dev`. Lanes named
`stream N (logical worker)` are Chronon logical execution streams. Unit events
on a lane show which unit executed there at that time; after dynamic rebalance,
the same unit may appear on a different stream. The separate `scheduler` lane
records scheduler work, not a simulation worker. Long `cluster dependency`
slices indicate spin-wait time on predecessor cluster progress atomics; sparse
or non-overlapping `unit` slices indicate poor stream packing or insufficient
lookahead.

### Tune Epoch Size

Larger epochs reduce sync overhead but increase memory:

```cpp
TickSimulationConfig config;
config.epoch_size = 1024;  // Default: 64
```

YAML and CLI equivalents:

```yaml
simulation:
  epoch_size: 1024
  max_lookahead_cycles: 256              # uint32_t
  enable_epoch_free_lookahead: true      # Drop the per-epoch barrier when safe
  enable_weighted_partitioning: true     # Cost-aware thread assignment (default)
  profiling_warmup_cycles: 512           # Warmup ticks before measuring
  profiling_measurement_cycles: 1024     # Measurement window for tick costs
  enable_dynamic_rebalance: false        # Runtime cluster migration (opt-in)
  rebalance_check_interval_cycles: 8192  # Epoch-boundary imbalance checks
  rebalance_min_gain: 0.05               # Skip if predicted gain is too small
  rebalance_cooldown_cycles: 0           # Minimum cycles between rebalances
```

Dynamic rebalance is an opt-in epoch-boundary migration path. It is most useful
when timeline traces show one stream doing
most unit work while others spend time in dependency waits. The rebalancer
samples unit tick cost, sums sampled work per stream, and migrates whole
tight clusters at epoch boundaries; it never splits a delay=0 cluster. Use
`rebalance_min_gain` and `rebalance_cooldown_cycles` to avoid low-value or too
frequent migrations.

```bash
./examples/cpu_pipeline_yaml_example config.yaml --epoch-size=1024
```

When the staging-capacity gate is satisfied, `enable_epoch_free_lookahead`
(default on) removes the per-epoch barrier entirely instead of just widening it:
run-ahead is then bounded only by dependency progress and
`max_lookahead_cycles`. If the gate rejects the topology, Chronon falls back to
the per-epoch path, where dynamic rebalance can still operate at epoch
boundaries; see [Epoch-Free Lookahead](scheduling.md) for the conditions and the
MPSC headroom requirement.

Notes:
- In single-thread mode, Chronon uses a per-cycle fast path to avoid lookahead bookkeeping overhead.
- In multi-thread mode, cost-aware weighted partitioning profiles per-unit tick costs and assigns threads to minimize max thread time. Set `enable_weighted_partitioning: false` for topology-only assignment.

### Use delay>0 for Parallelism

```cpp
// delay=0: sequential execution (same thread)
sim.connect(a->out, b->in, 0);

// delay>0: parallel execution possible
sim.connect(a->out, b->in, 1);
```
