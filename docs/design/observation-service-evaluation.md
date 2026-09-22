# Scheduler-owned observation service evaluation

Date: 2026-09-20. Branch: `feat/scheduler-observation-backend`.
This implements and evaluates the direction in
[issue #14](https://github.com/chronon-sim/chronon/issues/14#issuecomment-5750356155).
This report records the initial opt-in implementation. The subsequent
[SAGE diagnosis and default-path update](observation-service-sage-diagnosis.md)
supersedes its rollout recommendation. A 50% performance improvement is an
investigation target, not a correctness gate or a claim about all workloads.

## Implementation

`TickSimulation::hostServices()` owns registrations for host work. Services have
no model cycle, dependency edge, clock domain or lookahead frontier. Static and
dynamic epoch-free workers, native-clock workers and serial fallback paths poll
between sweeps; dependency waits also offer service opportunities. Registrations
rotate, serialize consumers with `try_lock`, and receive readiness notifications
from queue publication, clock progress and I/O completion. No persistent drain
loop is submitted to an already occupied `stdexec` pool.

Both the ordinary `ObservationBackend` and native `ClockTraceRecorder` support
this path. Worker polls copy at most 256 records; the ordinary backend also caps
each copy batch at 256 KiB. A producer filling its queue inside one model tick
can assist the same serialized consumer, avoiding a dependency on reaching the
next scheduler boundary. Slow I/O still causes backpressure under lossless mode.
Sorting, arena compaction, encoding, compression and file output stay on an
isolated, sleeping I/O thread. Shutdown may finish the retained ordinary-backend
tail on the caller after model workers have stopped.

The ordinary reorder arena has a configurable admission byte bound, separately
from its existing event-count bound. Forced flushing remains best-effort ordering,
as with the existing count limit. Native clocks preserve exact frontier ordering:
capture the frontier before all producer heads, retain that snapshot across
bounded batches, and acknowledge only after its final batch reaches the sink.
The added native handoff/head storage counts against its ingress/staging budget.
These are backlog bounds, not whole-process memory limits.

Detach waits for a current consumer claim and makes cached assistance handles
inert. Restart, early failure, full queues, final snapshots and late TLS retirement
use the existing producer-quiescence contract. Stats expose calls, records, total
host elapsed nanoseconds and maximum poll duration. Dynamic tick/wait cost samples
subtract actual service-poll wall time. These measurements include preemption;
they are neither CPU time nor a hard real-time latency guarantee.

See [the usage and lifecycle documentation](../../website/docs/guides/observability.md#scheduler-service).

## Downstream integration

Nucleus requires no source changes. Its YAML setting
`simulation.observation.scheduler_service` selects the backend. The benchmark
uses the same rebuilt binary for both modes.

SAGE integration is on `feature/chronon-observation-service` in the isolated
worktree `/tmp/chronon-service-sage-src`. `--perfetto-service` accompanies
`--perfetto` and enables service mode for both native and ordinary tracing.
The integration also uses the current simulation-owned port directory API.
SAGE's shared simulation YAML previously disabled epoch-free lookahead: merely
requesting multiple threads silently fell back to serial execution. Both A/B
modes now enable it, and `execution_policy` reports the actual scheduler selected
by Chronon. This is a runtime scheduling change, not a hardware timing-model change.
The original Nucleus and SAGE worktrees are untouched.

## Measurement protocol

- Chronon base: `2acdd6f73f82a3ec28f1ea2a3f7b53969f320951`.
- Nucleus base: `273f804a14559c9b8d78c76dbb8d023403444504`.
- SAGE base: `0e9cd5f0cafc5acf87b0aa873d3459faea7cb521`.
- Host: Intel Core i9-14900K, Linux; both modes restricted to the same six
  physical P cores, logical CPUs `0,2,4,6,8,10`. Five and six simulation workers
  are measured separately. This is affinity, not exclusive CPU reservation.
- Downstream builds: Clang 20, Release, same local Chronon sources. Compilation
  and correctness tests finish before timed runs. Each case gets one warmup per
  mode and five measured repetitions, with deterministically randomized A/B order.
- Process wall time includes initialization and final output. Nucleus also records
  its own simulation wall time. Report medians and full ranges; do not infer a
  statistically established population effect from five samples.
- Nucleus uses the existing system configuration, periodic counters and full
  Dhrystone/CoreMark runs. CoreMark requires CRC `0x6751` and successful SYSCON
  poweroff. SAGE uses lossless native-clock Perfetto plus domain text logs on
  GP102: vector add (112 CTAs, 28,672 elements) and memory copy (512 CTAs,
  131,072 elements). Both must actually select a parallel scheduler.

Commands, binaries' SHA-256, CPU details, scenario YAML, stdout and every output
are retained under `out/observation-service-20260920/`. The first exploratory
series is retained separately from final measurements. Its FlashAttention SM120
candidate failed on the same unsupported instruction in both modes; that workload
is excluded, not attributed to the scheduler service. The final series uses memory
copy instead. The final timing runner uses a watchdog with blocking `wait()` to
avoid Python timeout polling quantizing subsecond wall times.

```bash
python3 scripts/benchmark_observation_service.py \
  out/observation-service-20260920/manifest-final.json \
  --output out/observation-service-20260920/measurements-r2 \
  --warmups 1 --repetitions 5
python3 scripts/validate_observation_service_runs.py \
  out/observation-service-20260920/measurements-r2
```

## Results

| Case | Workers | Dedicated median [min–max], s | Service median [min–max], s | Speedup | Wall-time reduction |
|---|---:|---:|---:|---:|---:|
| nucleus-dhrystone | 5 | 1.446 [1.411–1.550] | 1.441 [1.421–1.483] | 1.004× | +0.4% |
| nucleus-coremark | 5 | 2.459 [2.336–2.652] | 2.425 [2.314–2.509] | 1.014× | +1.4% |
| sage-vector-native | 5 | 0.773 [0.769–0.774] | 0.781 [0.779–0.782] | 0.990× | -1.0% |
| sage-copy-native | 5 | 1.861 [1.860–1.873] | 1.899 [1.897–1.916] | 0.980× | -2.0% |
| nucleus-dhrystone | 6 | 3.203 [2.747–3.386] | 1.394 [1.381–1.407] | 2.298× | +56.5% |
| nucleus-coremark | 6 | 7.952 [6.330–8.691] | 2.308 [2.291–2.341] | 3.446× | +71.0% |
| sage-vector-native | 6 | 0.906 [0.900–0.914] | 0.916 [0.906–0.931] | 0.990× | -1.0% |
| sage-copy-native | 6 | 2.014 [2.004–2.016] | 2.093 [2.058–2.110] | 0.962× | -3.9% |

Positive reduction means faster. At six workers, Nucleus achieves 2.30× and
3.45× throughput (56.5% and 71.0% lower process wall time). At five workers its
median improvement is only 0.4–1.4%. This supports the contention explanation:
removing the ordinary backend's spinning drain thread matters when workers already
occupy every available physical core. It is not an intrinsic 2–3× scheduler speedup.

SAGE does **not** meet the 50% aspiration. Native vector/copy tracing is 1.0–3.9%
slower in these measurements. Unlike the ordinary backend, its original native
recorder already combines draining and output on one thread. Service mode still
needs an I/O thread, moves copying to workers, and adds a handoff; it does not remove
the expensive encoding/output work. This explains a plausible source of overhead,
but this experiment does not isolate the cost of each stage.

Keep the feature opt-in, especially for native-clock workloads. Moving more CPU
encoding into bounded service stages with a separate bounded byte-output queue
would be a distinct follow-up; it must not put blocking file I/O in model callbacks.
Do not enable it automatically based only on requested thread count.

The final series contains 96 completed runs, including 16 warmups. A one-second
host monitor collected 196 samples with no overlapping `peek`/`baseline` main-thread
affinity on the six selected P cores. The other Codex task confirmed that its
independent rerun pinned workers to E cores 16–23 (main thread 23); the main-thread
sample alone is insufficient to describe all workers. This is a shared host, not
an exclusive machine. An earlier visibly overlapping
series was stopped and retained under `final-measurements/`, and is excluded above.
The other Codex task was contacted to coordinate resource use; subsequent analysis
is pinned to CPUs 28 and 30, and the P cores have been released.

Raw results: `out/observation-service-20260920/measurements-r2/{metadata,runs,summary}.json`;
resource sampling: `out/observation-service-20260920/host-interference-r2.jsonl`.


## Validation

The focused tests cover burst and sparse publication, slow sinks, 4 KiB full
queues with `SpinWait`, large variable-size records against a 64 KiB arena bound,
one/four workers and static/dynamic scheduling, multiple services, repeated runs,
backend restart and sink failures. Native tests cover clock phases/rates, quiet
streams, large clock skew, incremental frontiers, compressed/uncompressed output,
and shutdown. Determinism fixtures compare periodic/final counters across migration.

Downstream validation compares all Nucleus CSV cells, including the final row;
all SAGE model metrics except the scenario pathname; output buffers; and complete
native text records after sorting away cross-stream arrival order. Each stream's
cycle and ordinal order is checked independently. Binary traces are additionally
imported by the real Perfetto Trace Processor and compared with their text records
and expected transaction-flow graph. Sanitizers and the complete CTest suite are
reported with their actual scope below.

| Check | Result |
|---|---|
| Release CTest | 162/162 passed; final unstarted-recorder lifetime fix additionally rechecked with both recorder modes, 2/2 |
| ASan + UBSan + LeakSanitizer | 17/17 focused host/backend tests, plus 4/4 large-record cases; final recorder lifetime case rechecked, 1/1 |
| ThreadSanitizer | 17/17 focused host/backend tests, plus 4/4 large-record cases; final recorder lifetime case rechecked, 1/1 |
| SAGE native-clock integration | 13/13 internal cases in `sage_clock_domains`, including four-worker dedicated/service modes versus serial reference |
| Native fixture Trace Processor imports | Recorder and multi-clock service fixtures passed independent reference, timestamp and flow checks |
| SAGE workload outputs | 48/48 runs: all model metrics, binary buffers, domain manifests and complete text records match within each case; zero drops |
| SAGE functional output spot checks | Six-worker service vector output equals input A + B; memory-copy output equals input byte for byte |
| Nucleus periodic CSV | All 48 runs match every periodic cell within each case; 736 columns, Dhrystone 41 rows / CoreMark 104 rows including final |
| Nucleus full CSV including final | Strict comparison reports 37/48 mismatched runs; details below, not counted as a pass |

All final-series Nucleus differences occur in the last row. Most are
`simulation.obs_info_emitted`, reflecting host diagnostic output. One dedicated
Dhrystone six-worker repetition additionally has `decode_starved` and
`decode_stall_fetch_empty` each lower by one than the other runs. No intermediate
snapshot differs; termination cycle, successful exit and CoreMark CRC are stable.
The exploratory series also found small final-row differences in repeated
dedicated CoreMark runs. Thus full final-counter determinism is not established
by this evaluation, and the evidence does not attribute that existing variance
to service mode. The comparison script intentionally exits nonzero and preserves
every differing cell in `measurements-r2/correctness.json`.

The six-worker binary native traces from both modes were separately imported
with Perfetto Trace Processor v57.2: vector has 262,638 events / 206,084 flows;
copy has 795,600 events / 624,388 flows. Each imported record's identity, cycle,
timestamp and transaction flow matches the domain logs, with no import errors.
Text/binary agreement validates capture integrity; the separate reference fixtures
and serial-versus-parallel SAGE tests supply simulation correctness evidence.

Logs, the retained SAGE integration patch and the exact native-workload validation
script are under `out/observation-service-20260920/`. The implementation remains
uncommitted on the two named branches for review; no remote changes were made.
