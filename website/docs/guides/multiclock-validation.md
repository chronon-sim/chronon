---
title: Multi-clock validation and performance
---

# Multi-clock validation and performance

This is a measured first-version result, not a GPU calibration claim. The
implementation and timing contract are in [Multi-clock domains and CDC](./multiclock-cdc.md).
No SAGE source, frequency, FIFO sizing, or other product default was changed.

## Tested scope

The original single-clock regression executables passed in Release, including
epoch-free scheduler equivalence, same-cycle connections, stage registers,
termination, queue hardening, and the existing observation tests. The new
assertion-based test translation units keep assertions enabled in Release.

| Layer | Evidence |
| --- | --- |
| Exact time | Rational periods/phases, strict and non-strict edge lookup, invalid inputs, overflow, a direct edge at index `10^12`, and one million calendar batches checked against independent arithmetic |
| FIFO circuit | Independent unwrapped-pointer/deque reference, hand-derived latency and full-release timelines, invalid depth/stages/phase operations, owned move-only packets, and 1.2 million differential transitions |
| Integrated simulation | 972 combinations of ratios, phases, depth, synchronizer length, and continuous/burst/random traffic, each with 3,000 event batches; an additional million-step stress run |
| Ordering and lifecycle | Requested thread counts 1/2/4, reversed unit/domain creation, same-domain zero-delay ordering, local StageReg phases, termination, continuation, drain, and rejected topology changes |
| Native recording | Four actual concurrent producer threads, two streams per domain, 80,000 events per variant, different batching/order/compression, incremental checkpoints, and timestamps that move backwards between batches |
| Imported semantics | Real Trace Processor v57.2, physical timestamps, local identities, FIFO flows, same-time edges, non-integer clock ratios, large cross-stream disorder, loss accounting, and recoverable file prefixes |
| Memory safety | All five new clock/FIFO/recorder/example targets passed ASan, UBSan, LeakSanitizer, and `-Werror` in an instrumented build |

Ratios include equal clocks, fast/slow producer and consumer, 3:2, 5:3,
1000/1001 MHz, and 914/1326 MHz. Phase scans include 0, 1, 137, and 500 ps;
depths include 2, 4, and 16 in the combined matrix, and 64 in the circuit test;
synchronizer chains include 2, 3, and 5 stages. Seeds are fixed in the tests.
An integrated mismatch prints its configuration and the last 32 state snapshots.

**Requested parallel multi-clock configurations currently exercise the explicit
serial fallback.** These tests do not establish an epoch-free parallel CDC
implementation. Recorder concurrency is exercised by real concurrent threads.
TSan has not been run.

### Trace Processor results

The checked importer is `Perfetto v57.2-da1d152cf`; the writer SDK and reviewed
clock protocol/translation implementation are v56.1. Tests use `--full-sort`,
not an assumption about every importer's default out-of-order window. Imported
timestamps are compared to independently computed physical time rounded down
to nanoseconds. The expected error is in `[0, 1 ns)` and does not accumulate.

| Fixture | Retained events | Flow edges | Reference identity |
| --- | ---: | ---: | --- |
| Serial 914/1326 MHz | 15,758 | 11,620 | `5190719cbd1bc767...` |
| Requested-parallel fallback, reversed creation | 15,758 | 11,620 | Same full reference as serial |
| Equal clocks, coincident edges | 7,200 | 4,800 | `efd57048204e0906...` |
| Near frequencies, 1000/1001 MHz | 18,850 | 11,424 | `4695b3275fcd9ee8...` |
| Four concurrent recorder variants | 80,000 each | 0 | `fa253bbd2df35ddb...` each |

In the final standalone lossy run, 2,001 events were retained and 13,757 dropped;
their sum was 15,758. The retained subset, reported loss, and complete hardware
reference agreed. Drop counts are host-scheduling-dependent and are not golden
byte-level output. The validator prints full SHA-256 reference identities.

The tests check sequence-local clocks 64/65 and reference clock 11, imported
timestamps across hundreds of simulated seconds of file disorder, checkpoint
re-declarations, interning, and compressed batches. A separate sequence with
local cycles `1000, 1, 2000, 0, 3000` checks normalization of regressing input.
Trace Processor reported no nonzero error-severity import statistics.

Truncation validation recovers complete top-level packet prefixes in raw and
compressed traces. A torn compressed wrapper is discarded as a whole. The
supported recovery is a prefix, not recovery after arbitrary middle-file
corruption or loss of the initial clock/dictionary declarations.

The offline text merge reconstructed all 15,758 serial events, including exact
rational physical times, and agreed with the independent reference. Its
same-time presentation order is not a hardware causal relation.

### Same-nanosecond native flow regression

The original implementation allowed cross-stream drain order to reverse native
flows after ns quantization. A deterministic 4 GHz writer fixture now covers
`write@0 ns -> visible@0.75 ns` in both append orders, as well as exact-time
Evaluate/Commit ties across units whose names sort in the opposite order.
Four compression/order variants exercise strict bucket boundaries, checkpoints,
explicit flushes, owned string metadata and real imports of files copied before
writer close. Separate capacity tests reject late records and exhausted buckets
without accepting or duplicating them, then flush the unannounced tail.
Integrated 4 GHz FIFO fixtures cover forward/reverse drain, batch sizes 1 and 64,
lossy recording, and 3/4 GHz clocks with a 125 ps phase offset. Expected FIFO
events still come from the independent circuit reference. The importer checks
the complete retained causal graph, not just nondecreasing rounded timestamps.

Concurrent streams now publish acknowledged progress every 16 records; hundreds
of simulated seconds of skew remain bounded rather than being spooled to disk.
A permanently empty stream explicitly finishes. A sleeping stream test publishes
progress without any event, unblocking its peer; late and post-finish records are
rejected. The integrated scheduler publishes after 64 completed edge batches and
the existing reverse-drain, lossy, phase and prefix-recovery matrix remains active.

## Measured performance

The measurements below are the original pre-finalization baseline. They predate
the same-nanosecond flow fix and must not be used as performance claims for the
current bounded streaming writer. Re-run the supplied benchmark (whose wall
timer includes `close()`) when measuring the current path.

Measured on 2026-09-09, Intel Core i9-14900K, Linux 6.1, GCC 12.2, Release.
Processes were pinned to logical CPUs `0,1`. The tables below summarize a local
measurement run; its generated raw files are not distributed with the repository.
The reproduction commands below generate a fresh result set, including raw CSV,
summaries, and invocation metadata, in the output directory selected by the reader.

Five repetitions used fresh processes with a fixed-seed shuffled/interleaved
mode order. Compilation and regression tests had finished before this run.
CPU frequency and unrelated system activity were not controlled. Wall time
includes final recorder drain and file close, but not `fsync`; output was to
`/tmp`, so this is not a durable-storage throughput measurement.

### FIFO simulation and observation modes

Each run executes 500,000 physical-time event batches with 914/1326 MHz clocks,
a 137 ps phase offset, a depth-16 FIFO, and a two-stage pointer synchronizer.
For this phase arrangement, there is one unit edge per batch. Traffic and
backpressure are identical in all four modes.

| Mode | Median wall time | Min / max | Events/s | Total ns/event | Additional ns/event over off |
| --- | ---: | ---: | ---: | ---: | ---: |
| Off | 44.765 ms | 44.748 / 45.069 ms | Not recorded | Not applicable | Not applicable |
| Per-domain text | 238.943 ms | 238.788 / 241.027 ms | 5,038,896 | 198.46 | 161.28 |
| Perfetto | 1,386.510 ms | 1,385.730 / 1,391.240 ms | 868,374 | 1,151.58 | 1,114.40 |
| Both | 1,673.990 ms | 1,672.890 / 1,676.160 ms | 719,245 | 1,390.35 | 1,353.17 |

All enabled modes recorded 1,204,009 events with zero drops. Every repetition
and every mode produced `sent=204018`, `received=204015`, and
`checksum=20811162120`. The fixed event-budget benchmark intentionally ends
with a few in-flight packets; the lifecycle tests and example separately drain.

`Total ns/event = median total wall time / retained events`.
`Additional ns/event = (median mode wall time - median off wall time) / events`.
Neither number is an isolated producer-side instruction cost. Lossless queue
saturation can stall the host, and encoding/compression dominates this
deliberately event-dense workload. The implementation must **not** be described
as negligible-overhead tracing based on these results.

| Mode | Allocated ingress bytes | Peak ingress bytes | Output bytes | Process peak RSS |
| --- | ---: | ---: | ---: | ---: |
| Off | 0 | 0 | 0 | 17,120 KiB |
| Text | 327,680 | 196,600 | 46,793,686 | 17,120 KiB |
| Perfetto | 327,680 | 199,440 | 10,228,842 | 17,120 KiB |
| Both | 327,680 | 199,440 | 57,021,720 | 17,120 KiB |

Ingress allocation is two 4,096-entry rings of 40-byte records. Reported peak
ingress is the sum of individual stream high-water marks, an upper bound on
their simultaneous occupancy, not total process memory. Encoder buffers,
text buffers, metadata, and bounded dictionaries are additional. RSS is the
process high-water reading, including startup/runtime overhead; its identical
value in this experiment cannot resolve those smaller allocation differences.
Output bytes include the manifest/statistics files and use decimal bytes.

### Scheduler overhead and original single-clock baseline

The scalar-body microbenchmarks separate scheduling from FIFO work:

| Mode | Work | Median wall time | ns per unit tick |
| --- | --- | ---: | ---: |
| Legacy single clock | 500,000 cycles, two units | 1.935 ms | 1.935 |
| Two-clock event calendar, no FIFO | 500,000 batches, one due unit each | 39.035 ms | 78.070 |
| Two-clock calendar plus FIFO, tracing off | Same event budget | 44.765 ms | 89.530 |

These modes have different edge counts per batch, so per-unit-tick cost is
reported explicitly. Calendar work is much more expensive than the optimized
single-clock loop for a trivial unit body. The design skips nonexistent edges,
but it does not make rational time comparisons or calendar operations free.

For a before/after comparison, the exact same `single_clock_regression.cpp`
was compiled against the independently rebuilt original
`origin/main` commit `efcb9a50fe3e63ab48467c875d4799d2b27fc660` and against this
implementation, using original-compatible APIs. Each of five interleaved
repetitions executes 100 million cycles after warmup.

| Revision | Median wall time | Digest, every run |
| --- | ---: | --- |
| Original main | 210.475 ms | `6454394033915408529` |
| Candidate | 210.117 ms | `6454394033915408529` |

Candidate/baseline is `0.99830`, approximately -0.17%, within observed timing
variation. This is evidence of no material regression in this microbenchmark,
not a universal bound for all Chronon or SAGE workloads.

## Historical disk-backed finalization measurement

The now-removed disk-backed flow-ordering fix was measured on 2026-09-09 on
the same machine and Release configuration, pinned to CPUs `0,1`. These are
medians of three interleaved runs of 100,000 scheduler steps, including recorder
`close()` and its external sort. Each enabled observation mode retained 240,793
events. This smaller workload is not a like-for-like comparison with the
500,000-step historical table above.

| Mode | Total wall time | Events/s | Total ns/event | Incremental ns/event | Final file bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Off | 0.008833 s | n/a | n/a | n/a | 0 |
| Text | 0.045079 s | 5,341,602 | 187.21 | 150.53 | 8,900,141 |
| Perfetto | 1.098050 s | 219,291 | 4,560.14 | 4,523.46 | 2,105,324 |
| Both | 1.150590 s | 209,278 | 4,778.34 | 4,741.65 | 11,004,658 |

The enabled modes allocated 327,680 bytes of ingress rings. Maximum sampled
ingress occupancy across repetitions was 194,480 bytes for text and 199,440
bytes for Perfetto/both. The harness reported maximum process RSS of 17,120 KiB
for every mode; RSS is not an isolated measurement of the sorting buffers.
File bytes excluded the temporary sorting workspace. That removed implementation
used 8192-record/4-MiB sort runs and a 16-way merge, with temporary disk space
proportional to the recorded events.

Total wall-time ranges were 0.008810-0.008847 s (off), 0.045044-0.045136 s
(text), 1.094380-1.104270 s (Perfetto), and 1.149960-1.153920 s (both).
The scalar-body controls measured 1.91 ns/unit tick for the legacy single clock
and 75.04 ns/unit tick for the exact multi-clock calendar.

The historical finalization cost was substantial: that fix prioritized correct native flow
direction at colliding nanosecond timestamps over streaming Perfetto output.
It required successful `close()` before the final Perfetto file was ready.
These numbers are a comparison baseline, not a performance claim for the new
streaming implementation. There is no retained disk-backed runtime fallback.

Run the same workload on the current implementation with:

```bash
python3 scripts/run_multiclock_benchmark.py \
  --binary build-release/benchmark/chronon_multiclock_benchmark \
  --steps 100000 --repetitions 3 \
  --output-dir out/flow-ordering-benchmark
```

## Bounded streaming measurement

Measured on the same host, GCC 12.2 Release, CPUs `0,1`, after builds and tests
finished. The baseline is `e52983c`, measured again with its separately retained
benchmark executable; no old implementation remains in the source/runtime path.
Each 100,000-step variant used three fresh-process repetitions with fixed-seed
interleaved observation modes. Baseline and current short-run sets were separate;
the long Perfetto baseline/current runs were interleaved across three repetitions.
All hardware counts/checksums matched, with zero dropped lossless events.

| Mode, 100,000 steps | Baseline total | Streaming total | Streaming run | Streaming close | Events/s | Total ns/event | Final bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Off | 8.850 ms | 8.774 ms | 8.774 ms | <0.001 ms | n/a | n/a | 0 |
| Text | 45.003 ms | 46.747 ms | 46.074 ms | 0.669 ms | 5,150,994 | 194.14 | 8,900,258 |
| Perfetto | 1,106.290 ms | 428.315 ms | 426.218 ms | 2.113 ms | 562,187 | 1,778.77 | 2,105,453 |
| Both | 1,161.080 ms | 486.133 ms | 484.039 ms | 2.120 ms | 495,323 | 2,018.88 | 11,004,787 |

Enabled modes retained 240,793 events. Incremental cost relative to tracing off
was 157.70 ns/event (text), 1,742.33 ns/event (Perfetto), and 1,982.45 ns/event
(both). The respective total-time ranges were 46.652-46.804 ms, 428.236-430.872 ms,
and 484.098-489.602 ms. Text was about 3.9% slower in this short-run comparison;
this is not a claim that every mode improved. Perfetto total time improved 2.58x.

| Perfetto, 1,000,000 steps / 2,408,026 events | Baseline | Streaming |
| --- | ---: | ---: |
| Median total wall time | 12.76370 s | 4.32751 s |
| Median simulation-stage wall time | 2.06654 s | 4.32521 s |
| Median close time | 10.69890 s | 0.00230 s |
| First event-containing file batch | Only during finalization; not separately timed | 7.731 ms after writer open |
| Final file bytes | 20,793,132 | 20,793,261 |
| Maximum reported process RSS | 16,660 KiB | 16,660 KiB |

Long-run total wall-time ranges were 12.72990-12.84060 s and 4.31546-4.33364 s:
**2.95x total speedup**, not merely deferred work. Streaming throughput was about
556,446 events/s (1,797.12 ns/event). The simulation-stage time is higher because
encoding and acknowledged observation backpressure now happen during execution;
this changes host pacing, never simulated clock edges or FIFO results.

The native open-bucket peak was **163 records** in both 100,000- and
1,000,000-step workloads. Its conservative fixed-storage/metadata bound was
5,599,674 bytes. Ingress allocation remained 327,680 bytes; sampled ingress peaks
were at most 6,240 bytes for streaming Perfetto/both and 199,400 bytes for text.
The short-run harness reported 17,140 KiB maximum RSS for all current modes;
RSS is a process-level value, not an isolated allocation measurement.
First output was 7.710 ms for short Perfetto and 8.743 ms for both.

Streaming temporary-file reads/writes are **zero**: there are no scratch files or
spill path. This does not mean final-file IO is zero. Historical temporary IO was
not separately instrumented, so no device-IO speedup is inferred. Measurements
include close/drain but not fsync durability. The scripts retain raw timing,
buffer, first-output and checksum fields for independent comparisons. Use the
same reproduction command with `--steps 1000000` for the longer workload.

The four skewed-worker fixtures retained 80,000 events each with **65** pending
native records at peak and a 209,342-byte conservative native buffer bound,
despite hundreds of simulated seconds of progress skew. There were no drops;
these are bounded-buffer checks, not a same-workload baseline timing comparison.

An additional three-repetition, interleaved 100-million-cycle empty-unit control
measured 385.788 ms for the baseline and 407.127 ms for the current benchmark
executable (200 million unit ticks in both): an observed **5.5% slowdown**.
This is a residual performance regression, not a claim of single-clock timing
equivalence. Its cause has not been isolated. The single-clock functional
regressions still pass; do not generalize the multi-clock Perfetto speedup to
all single-clock or text-only workloads.

## Reproduce

```bash
TP=/absolute/path/to/trace_processor
cmake -S . -B build-clock -DCMAKE_BUILD_TYPE=Release \
  -DCHRONON_BUILD_TESTS=ON -DCHRONON_BUILD_BENCHMARKS=ON
cmake --build build-clock -j4

# Core tests and standalone real-import acceptance are independently runnable.
ctest --test-dir build-clock --output-on-failure --parallel 8
python3 scripts/validate_clock_traces.py --trace-processor "$TP" \
  --multiclock-binary build-clock/test/sender/sender_test_multiclock \
  --recorder-binary build-clock/test/observe/test_clock_trace --check-prefix

build-clock/examples/multiclock_cdc_example /tmp/chronon-three-clocks
python3 scripts/merge_clock_logs.py /tmp/chronon-three-clocks \
  --output /tmp/chronon-three-clocks.tsv
```

ASan/UBSan reproduction, with an instrumented Perfetto SDK as configured by
the repository's sanitizer option:

```bash
cmake -S . -B build-clock-asan -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_CXX_FLAGS_RELWITHDEBINFO='-O1 -g1 -DNDEBUG' \
  -DCHRONON_ENABLE_ASAN=ON -DCHRONON_ENABLE_WERROR=ON
cmake --build build-clock-asan --target sender_test_clock_time \
  sender_test_async_fifo_circuit sender_test_multiclock test_clock_trace \
  multiclock_cdc_example -j4
ASAN_OPTIONS=detect_leaks=1 ctest --test-dir build-clock-asan \
  --output-on-failure -R 'clock_time|async_fifo_circuit|multiclock|clock_trace'
```

LeakSanitizer cannot operate inside this environment's ptrace-based sandbox;
the successful leak-check run used the same binaries outside that sandbox.
It was not suppressed or reported as passing with leak detection disabled.

To reproduce the original-revision benchmark without changing the working tree:

```bash
BASE=efcb9a50fe3e63ab48467c875d4799d2b27fc660
mkdir -p /tmp/chronon-baseline-src
git archive "$BASE" | tar -x -C /tmp/chronon-baseline-src
cmake -S /tmp/chronon-baseline-src -B /tmp/chronon-baseline-build \
  -DCMAKE_BUILD_TYPE=Release -DCHRONON_BUILD_TESTS=OFF
cmake --build /tmp/chronon-baseline-build --target chronon -j4
c++ -std=c++20 -O3 -DNDEBUG \
  -I/tmp/chronon-baseline-src/src \
  -I/tmp/chronon-baseline-build/_deps/stdexec-src/include \
  benchmark/single_clock_regression.cpp \
  -L/tmp/chronon-baseline-build/src -lchronon \
  -L/tmp/chronon-baseline-build/src/observe -lchronon_observe \
  -L/tmp/chronon-baseline-build -lperfetto_sdk \
  -lfmt -lyaml-cpp -lz -pthread -o /tmp/chronon-baseline-benchmark
python3 scripts/run_multiclock_benchmark.py \
  --binary build-clock/benchmark/chronon_multiclock_benchmark \
  --baseline-single /tmp/chronon-baseline-benchmark \
  --candidate-single build-clock/benchmark/chronon_single_clock_regression_benchmark \
  --baseline-revision "$BASE" --output-dir /tmp/chronon-clock-performance
```

Use fresh output directories. Configure cached dependency source overrides if
running offline; use `CCACHE_DISABLE=1` if the environment's ccache directory is
not writable. Do not compare a previously installed library with a different
compiler or optimization setting and call it an exact main-branch baseline.

## Remaining limits

Clock frequencies and phases are fixed for a run.
The FIFO is a deterministic digital CDC model, not an analog metastability or
MTBF model. Only power-of-two FIFO depths and registered synchronous reads are
implemented. Explicit multi-clock execution is serial with reported fallback.
Legacy observation entry points and YAML clock-domain configuration are not
automatically migrated; use the C++ clock/CDC and native recorder APIs in the
guide. Legacy multi-clock observation is rejected rather than mis-timestamped.
Perfetto display is quantized to nanoseconds while simulation remains exact
within its checked finite rational representation.
