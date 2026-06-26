---
sidebar_label: "Scheduler Timeline Trace"
---

# Scheduler Timeline Trace

Chronon can emit a scheduler timeline in Perfetto protobuf format. This is
intended for diagnosing whether Chronon logical streams are tightly
overlapped, whether a partition leaves streams under-filled, and where workers
spend wall time spinning on predecessor cluster progress atomics.

The trace is disabled by default. When disabled, `TickSimulation` does not take
per-cycle timestamps for the timeline path.

## What Changed

The scheduler timeline recording is implemented entirely in the Chronon
scheduler layer; output goes through the observation backend's unified
Perfetto timeline:

- `SchedulerTimelineTraceConfig` stores YAML/C++ configuration.
- `SchedulerTimelineTrace` buffers per-stream events.
- `TickSimulation` records events in the existing progress-based and barrier
  scheduler paths.
- When the observation backend is running with a timeline sink, the recorded
  scheduler slices merge into the unified `timeline.pftrace` (written during
  backend shutdown), alongside simulation trace events and counter tracks.
  When observation is disabled (e.g. `--no-observe`), `SimulationApp` writes a
  standalone Perfetto `.pftrace` file after `runUntilTermination()` completes.

No simulation unit code needs to be modified. The feature observes Chronon
execution streams (`thread_units_[N]`), not model-specific pipeline state.

## Configuration

YAML:

```yaml
simulation:
  observation:
    timeline:
      scheduler:
        enabled: true
        max_events: 1000000
        start_cycle: 0
        end_cycle: 2000
        trace_units: true
        trace_waits: true
        trace_epochs: true
        trace_arbitration: true
        min_duration_ns: 0
```

The old top-level `simulation.timeline_trace:` key is deprecated; it is still
parsed but prints a warning. Use `simulation.observation.timeline.scheduler:`
instead.

Equivalent CLI overrides with `SimulationApp`:

```bash
./my_sim config.yaml --no-observe \
  -p simulation.observation.timeline.scheduler.enabled=true \
  -p simulation.observation.timeline.scheduler.end_cycle=2000
```

## Fields

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `false` | Enables scheduler timeline collection. |
| `file` | `chronon_timeline.pftrace` | Standalone output path, used only when observation is disabled. Parent directories are created if possible. With the observation backend running, slices merge into `timeline.pftrace` instead. |
| `max_events` | `1000000` | Global event cap. Further events are counted as dropped. |
| `start_cycle` | `0` | First simulation cycle to record. |
| `end_cycle` | `UINT64_MAX` | Stop recording at this cycle, exclusive. |
| `trace_units` | `true` | Record each `TickableUnit::executeTick()` slice. |
| `trace_waits` | `true` | Record predecessor cluster dependency spin waits. |
| `trace_epochs` | `true` | Record scheduler epoch duration on the scheduler lane. |
| `trace_arbitration` | `true` | Record MPSC arbitration duration on the scheduler lane. |
| `min_duration_ns` | `0` | Drop events shorter than this wall-time duration. |

## Output Lanes

The scheduler timeline appears as a process group named `Chronon Scheduler` in
the Perfetto UI (separate from the `Simulation` process group that holds trace
events and counter tracks).

| Lane | Meaning |
|------|---------|
| `stream N (logical worker)` | Chronon logical execution stream. Unit duration events on the lane show which unit executed there at that time. |
| `scheduler` | Main scheduler lane for epoch and arbitration events. |

These lanes map to logical scheduler streams, not necessarily stable OS thread
IDs. The current `stdexec::static_thread_pool` may execute bulk work on worker
threads chosen by the runtime, but the logical lane identity remains the
Chronon stream assignment.

The `scheduler` lane appears as its own row after the worker streams. It is
not an extra simulation worker; it contains scheduler-side spans such as epoch
and MPSC arbitration work. Chronon uses a stable dark-red color seed for
scheduler stall slices so waits are visually distinct from normal unit work
without using bright warning colors.

## Event Types

| Category | Event name | Meaning |
|----------|------------|---------|
| `unit` | Unit name | Wall time spent executing one unit tick on that stream. |
| `unit idle` | Unit name | Lazy wakeup fast path that advances local cycle/progress without running the unit's `tick()` body. Detail includes `cycles=N` when multiple inactive cycles were batched. |
| color-stable wait category | `stall: cluster-dep` | Time spent spinning on a predecessor cluster. Detail names the blocked and blocking clusters. |
| color-stable wait category | `stall: lookahead-floor` | Time spent throttled by the epoch-free lookahead floor. |
| color-stable wait category | `stall: no-ready-cluster` | Time spent with no locally assigned cluster ready to run. |
| `scheduler` | `progress epoch` | Wall time for one progress-based lookahead epoch. |
| `scheduler` | `mpsc arbitration` | Per-cycle MPSC arbitration in barrier mode. |
| `scheduler` | `epoch-end mpsc arbitration` | End-of-epoch MPSC flush in progress-based mode. |
| `summary` | `dropped events` | Instant event emitted when `max_events` is exceeded. |

Every duration slice carries `cycle` and `detail` debug annotations (visible in
the Perfetto slice details panel). For wait events, the exported name identifies
the stall reason and `detail` names the blocking predecessor cluster with fields
such as `cluster`, `pred_cluster`, `needed`, `observed`, and `delay`.

## Reading The Trace

Open `timeline.pftrace` (or the standalone `.pftrace` file when observation is
disabled) in `ui.perfetto.dev`, or query it with Perfetto's `trace_processor`.

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
-p simulation.observation.timeline.scheduler.start_cycle=0 \
-p simulation.observation.timeline.scheduler.end_cycle=2000 \
-p simulation.observation.timeline.scheduler.max_events=1000000
```

If the trace is too large or unit-level timing perturbs the run too much, start
with waits only:

```bash
-p simulation.observation.timeline.scheduler.trace_units=false \
-p simulation.observation.timeline.scheduler.trace_waits=true \
-p simulation.observation.timeline.scheduler.trace_epochs=true
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
rebalance is opt-in and migrates whole clusters at scheduler fence points when
sampled per-stream work and dependency pressure predict a useful improvement. A
later unit event can therefore appear on a different stream than it used at
initialization.

The scheduler lane records a `dynamic rebalance` event whenever a new assignment
is applied. Its `detail` field lists the migrated clusters and the old/new
stream ids.

It does not move individual units independently and does not migrate work in the
middle of a scheduler window.
