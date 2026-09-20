# SAGE diagnosis and default observation service

This follows the [initial implementation and evaluation](observation-service-evaluation.md).
It changes the default on `feat/scheduler-observation-backend` and addresses the
native recorder's repeated polls while its handoff is owned by I/O.

The measurements below predate the subsequent
[scheduler-owned I/O unification](observation-service-io.md). The old consumer
switches used in this historical comparison are no longer available in current
source. Saved binaries, commands and hashes remain the reproduction baseline.

## Why SAGE differs from Nucleus

SAGE's ordinary application log volume is not the workload measured previously.
Without `--perfetto`, SAGE does not create these observation backends at all:
`Gpu::simulatePreparedLaunches` only enables capture when `perfetto_config_` is
present. Changing the backend cannot accelerate that path. The JSON model metrics
written after a run are also separate from this trace consumer.

With `--perfetto`, the GP102 runs use Chronon's native multi-clock recorder. The
vector workload emitted 262,638 records and the copy workload 795,600 records.
Every record in these captures is an automatic CDC FIFO event, not an application
log: `fifo.write`, `fifo.visible`, `fifo.read`, `fifo.output`, `fifo.consume`,
`fifo.empty` or `fifo.full`. Text output alone is about 9.8 MB / 30.2 MB, plus the
compressed Perfetto stream. The exact counts are in `event-inventory.json`.

The native recorder already combines draining, staging, bucket sorting, encoding,
compression and output in **one** background thread. The ordinary Nucleus backend
had a drain thread plus a separate output thread. Moving the Nucleus drain work
onto six workers frees a competing CPU consumer on six available physical cores.
Moving native ingress copying onto those workers keeps its output thread and adds
a batch handoff. It does not remove the same source of CPU contention.

CPU sampling of the saved pre-fix binary supports this distinction. `perf record`
sampled `cycles:u` at 997 Hz with DWARF stacks, on the same six P cores. Self sample
percentages below describe all sampled threads, including workers and their waits;
they are not percentages of elapsed wall time. Thread identities were classified
from recorder call stacks. Compression is a subset of the backend thread's work.

| Native workload / mode | Backend thread samples | zlib self samples | Worker service-poll self samples |
|---|---:|---:|---:|
| Vector, dedicated | ~14.05% | ~6.52% | 0% |
| Vector, initial service | ~12.28% | ~6.00% | ~2.77% |
| Copy, dedicated | ~15.80% | ~7.78% | 0% |
| Copy, initial service | ~15.15% | ~7.71% | ~1.53% |

Approximately half of the native backend thread's sampled CPU work is compression,
which remains on the output thread in both modes. The original service path adds
measurable worker polling work without removing that cost. Turning compression off
removes zlib samples but preserves the same model and event-record contents; its
unprofiled timing control is reported below. File-byte sizes intentionally differ.

## Fix and defaults

The original service accepted notifications while its only ingress buffer was
owned by I/O. Every claim took the registration mutex and two timing-clock reads,
only for `poll()` to notice `service_in_flight` and return. The previous six-worker
medians were 403,827 calls for 262,638 vector events and 1,042,112 calls for 795,600
copy events, despite a batch capacity of 256 records.

The registration now has a runnable gate. A dispatched batch disables claims;
publication still latches readiness, and completion re-enables claims. Both normal
scheduler visits and urgent producer assistance check the gate before the mutex
and timing reads. The existing serialized consumer, bounded copy sizes, native
frontier protocol and isolated file I/O are preserved. Tests explicitly cover
publication and urgent assistance while disabled, and resumption afterward.

Simulation-managed ordinary observation and native-clock capture now default to
the scheduler service. Set `simulation.observation.scheduler_service: false` or
`ClockTraceRecorder::Config::scheduler_service = false` to select the dedicated
consumer. A standalone backend without an attached scheduler still uses its own
thread; it cannot depend on a scheduler that does not exist.

The SAGE companion worktree `/tmp/chronon-service-sage-src`, branch
`feature/chronon-observation-service`, makes `--perfetto` use the service by default.
`--perfetto-thread` provides the explicit control mode; `--perfetto-service` remains
an explicit selection of the default. Runs without `--perfetto` remain uncaptured.

## Follow-up measurement

Artifacts are under `out/observation-service-followup-20260920/`. `sage-before`
retains the original binary; `matrix/metadata.json` records both binary hashes.
The other Codex profiling task confirmed it had finished and released its cores.
Builds and tests finish before this matrix starts. Both modes use six workers on
logical CPUs `0,2,4,6,8,10`, six separate physical P cores, with one warmup and five
measured runs per mode and deterministic randomized order. No `perf` sampler runs
during the wall-time measurements. `run-matrix.py` records commands, outputs,
wall time, user/system CPU time, cycles and recorder statistics.

| Workload | Trace off | Dedicated | Initial service | Default service after fix | Default, no compression |
|---|---:|---:|---:|---:|---:|
| vector | 0.5308 s | 0.9098 s | 0.9186 s | 0.9167 s | 0.7411 s |
| copy | 1.1238 s | 2.0080 s | 2.0658 s | 2.0584 s | 1.6800 s |

These are medians of five unprofiled process wall times. Complete min/max ranges
and user/system CPU times are in `matrix/summary.json`. The default service remains
0.75% slower than dedicated for vector and 2.51% slower for copy. The small
0.21% / 0.36% median improvement over initial service is within run variation and
is not evidence of a material throughput improvement. Switching the default is a
policy choice made with those measurements visible, not a claim of 50% SAGE gain.

Disabling compression reduces default-service wall time by 19.15% / 18.38% while
preserving every model result and trace event; the output files are larger.
This ablation and the CPU samples identify compression/encoding/output as the
substantial cost that ingress scheduling does not remove. Disabling all trace
lowers wall time to 0.531 s / 1.124 s, but intentionally removes the requested
observability and is only an attribution control, not a proposed replacement.

| Workload | Initial service calls | Calls after fix | Reduction | Initial poll wall sum | Poll wall sum after fix |
|---|---:|---:|---:|---:|---:|
| Vector | 401,577 | 36,454 | 90.9% | 113.4 ms | 100.9 ms |
| Copy | 1,060,868 | 47,673 | 95.5% | 160.0 ms | 126.2 ms |

The large call-count reduction has a much smaller effect on time: most removed
polls immediately returned. Remaining useful polls copy records and scan producer
heads to preserve the native frontier. Total process CPU time does not show a
reliable reduction in this small sample. Admission-retry counts decrease when the
sink keeps up (the uncompressed runs have zero), but retries are spin-loop counts,
not lost events or simulated cycles. No event was dropped in any trace mode.

## Validation and scope

- Chronon full Release CTest: **162/162 passed** with the new defaults.
- ASan/UBSan/LeakSanitizer: **21/21 passed** for service publication, burst/sparse/
  large records, slow sinks, output failures and native multi-clock scheduling.
- ThreadSanitizer: the same **21/21 passed**, with no reported races.
- SAGE clock integration: **13/13 internal cases passed**, including default
  configuration, dedicated override and serial-reference equivalence.
- All **60 runs** across trace-off, dedicated, initial service, default service
  and uncompressed default have identical model metrics and output buffers within
  each workload (only the scenario pathname is normalized).
- All **48 trace-enabled runs** have identical complete native text records and
  manifests within each workload, ordered correctly within each stream, zero drops.
- Representative default compressed/uncompressed traces are imported with the
  real Perfetto Trace Processor v57.2 and checked against their complete text
  records and transaction-flow graph. Vector: 262,638 events / 206,084 flows;
  copy: 795,600 events / 624,388 flows.
- Nucleus's unchanged YAML, without a scheduler-service override, successfully
  selects the new default and completes Dhrystone (401,268 cycles) and CoreMark
  (1,037,093 cycles, CRC `0x6751`). These are default-path smoke checks, not a new
  performance estimate; the earlier report's final-counter variance remains noted.

The implementation and SAGE adaptation remain on their existing branches, with
uncommitted changes for review. No default compression or lossless policy changed.
A further SAGE speedup would need work on the native encoding/output stages or the
simulation scheduler itself; moving ingress work alone does not cover those costs.

