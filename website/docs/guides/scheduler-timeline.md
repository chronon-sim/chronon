---
sidebar_label: "Scheduler Timeline Trace"
---

# Scheduler Timeline Trace

Chronon can emit a scheduler timeline in Chrome Trace / Perfetto JSON format.
This is intended for diagnosing whether Chronon logical streams are tightly
overlapped, whether a partition leaves streams under-filled, and where workers
spend wall time spinning on predecessor cluster progress atomics.

The trace is disabled by default. When disabled, `TickSimulation` does not take
per-cycle timestamps for the timeline path.

## What Changed

The scheduler timeline support is implemented entirely in the Chronon scheduler
layer:

- `SchedulerTimelineTraceConfig` stores YAML/C++ configuration.
- `SchedulerTimelineTrace` buffers per-stream events and writes Chrome Trace
  JSON.
- `TickSimulation` records events in the existing progress-based and barrier
  scheduler paths.
- `SimulationApp` flushes the timeline after `runUntilTermination()` completes.

No simulation unit code needs to be modified. The feature observes Chronon
execution streams (`thread_units_[N]`), not model-specific pipeline state.

## Configuration

YAML:

```yaml
simulation:
  timeline_trace:
    enabled: true
    file: out/chronon_timeline.json
    max_events: 1000000
    start_cycle: 0
    end_cycle: 2000
    trace_units: true
    trace_waits: true
    trace_epochs: true
    trace_arbitration: true
    min_duration_ns: 0
```

Equivalent CLI overrides with `SimulationApp`:

```bash
./my_sim config.yaml --no-observe \
  -p simulation.timeline_trace.enabled=true \
  -p simulation.timeline_trace.file=out/chronon_timeline.json \
  -p simulation.timeline_trace.end_cycle=2000
```

## Fields

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `false` | Enables scheduler timeline collection. |
| `file` | `chronon_timeline.json` | Output path. Parent directories are created if possible. |
| `max_events` | `1000000` | Global event cap. Further events are counted as dropped. |
| `start_cycle` | `0` | First simulation cycle to record. |
| `end_cycle` | `UINT64_MAX` | Stop recording at this cycle, exclusive. |
| `trace_units` | `true` | Record each `TickableUnit::executeTick()` slice. |
| `trace_waits` | `true` | Record predecessor cluster dependency spin waits. |
| `trace_epochs` | `true` | Record scheduler epoch duration on the scheduler lane. |
| `trace_arbitration` | `true` | Record MPSC arbitration duration on the scheduler lane. |
| `min_duration_ns` | `0` | Drop events shorter than this wall-time duration. |

## Output Lanes

The trace uses one process named `Chronon Scheduler`.

| Lane | Meaning |
|------|---------|
| `stream N (logical worker)` | Chronon logical execution stream. Unit duration events on the lane show which unit executed there at that time. |
| `scheduler` | Main scheduler lane for epoch and arbitration events. |

These lanes map to logical scheduler streams, not necessarily stable OS thread
IDs. The current `stdexec::static_thread_pool` may execute bulk work on worker
threads chosen by the runtime, but the logical lane identity remains the
Chronon stream assignment.

Chrome Trace `tid` values are intentionally 1-based to avoid Perfetto treating
`tid=0` as a special main-thread-like lane. The user-visible lane name remains
zero-based: `stream 0 (logical worker)` is written with `tid=1`, `stream 1` with
`tid=2`, and so on. The final `scheduler` lane uses the next `tid` after the
worker streams and is expected to appear as its own row. It is not an extra
simulation worker; it contains scheduler-side spans such as epoch and MPSC
arbitration work.

## Event Types

| Category | Event name | Meaning |
|----------|------------|---------|
| `unit` | Unit name | Wall time spent executing one unit tick on that stream. |
| `wait` | `cluster dependency` | Time spent spinning because no local cluster is ready. Detail names the blocking predecessor cluster. |
| `scheduler` | `progress epoch` | Wall time for one progress-based lookahead epoch. |
| `scheduler` | `mpsc arbitration` | Per-cycle MPSC arbitration in barrier mode. |
| `scheduler` | `epoch-end mpsc arbitration` | End-of-epoch MPSC flush in progress-based mode. |
| `summary` | `dropped events` | Instant event emitted when `max_events` is exceeded. |

Every duration event includes a `cycle` argument and a `stream` argument. The
`stream` value is the original zero-based Chronon logical stream id, which is
useful when a viewer displays the 1-based Chrome Trace `tid` instead of the lane
name. Wait events include details such as `cluster`, `pred_cluster`, `needed`,
`observed`, and `delay`.

## Reading The Trace

Open the JSON in `ui.perfetto.dev` or `chrome://tracing`.

Useful patterns:

- Long `cluster dependency` slices mean a stream has no ready local cluster and
  is waiting for a predecessor cluster progress atomic. This often points to a
  critical cluster, low connection delay, or poor partitioning.
- Streams with sparse `unit` slices or large gaps are underutilized.
- A stream with dense long `unit` slices and many dependent wait slices on other
  streams is likely on the critical path.
- Large scheduler-lane arbitration slices indicate MPSC queue arbitration is a
  visible cost for the selected partition.
- Frequent dropped-event summaries mean the capture window or `max_events`
  should be reduced/increased.

For performance investigations, keep the window small first:

```bash
-p simulation.timeline_trace.start_cycle=0 \
-p simulation.timeline_trace.end_cycle=2000 \
-p simulation.timeline_trace.max_events=1000000
```

If the trace is too large or unit-level timing perturbs the run too much, start
with waits only:

```bash
-p simulation.timeline_trace.trace_units=false \
-p simulation.timeline_trace.trace_waits=true \
-p simulation.timeline_trace.trace_epochs=true
```

## Overhead

The feature is designed for diagnosis, not production benchmarking. With
`enabled=false`, the hot path only evaluates disabled checks and does not take
timeline timestamps. With the trace enabled, wall-clock timestamping and event
buffering intentionally perturb throughput; compare scheduler shapes rather
than using traced runs as final benchmark numbers.

## Scheduler Semantics

The progress-based scheduler tracks readiness at tight-cluster granularity.
Within a cluster, units execute in their fixed order because delay=0
dependencies must remain atomic. Across clusters assigned to the same stream,
execution is dependency-aware rather than strictly list-ordered: if one cluster
is blocked, another ready cluster on the same stream may advance.

Stream lanes are logical worker lanes, not fixed unit ownership labels. Dynamic
rebalance is enabled by default and migrates whole clusters at epoch boundaries
when sampled per-stream work is imbalanced. A later unit event can therefore
appear on a different stream than it used at initialization.

The scheduler lane records a `dynamic rebalance` event whenever a new assignment
is applied. Its `detail` field lists the migrated clusters and the old/new
stream ids.

It does not move individual units independently and does not migrate work in the
middle of an epoch.
