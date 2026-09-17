---
title: Multi-clock scaling measurements
---

# Multi-clock scheduler scaling (issue #142)

The changes improve sparse-domain serial execution and shared-endpoint CDC
scheduling. They do **not** make cheap ticks uniformly faster in parallel.
The fixed-work comparisons below include regressions, initialization, recorder
backpressure and the limitations of this host. No coordinator redistribution,
ready-work notification, whole-domain barrier or speculative profiling ticks
were added.

[Download raw measurements and trace fixtures](/benchmarks/multiclock-142/measurements.tar.gz).
The archive contains **3,616 fresh-process measurements**, per-run commands,
raw CSV/JSONL, summaries, binary SHA-256 hashes, CMake caches, topology, and
patches for two rejected prototypes. Representative native traces and their
text/reference records are included. The larger throughput traces can be
regenerated from the recorded commands; they are not included in the archive.

## Build and host

- Baseline: `000ebb3`, the `7c0766d` scheduler with the common extended benchmark
  and diagnostics. Candidate: `9a9bd5b`. Subsequent changes add tests and this
  report. Both binaries use the same workload code. This measures the runtime
  changes against the common harness; it does not measure the harness's own
  overhead against an unmodified `7c0766d` executable.
- GCC 12.2.0, C++20, Release, static libraries, `max_lookahead_cycles=32`.
  Diagnostics are **off** in throughput comparisons and run separately.
- Intel i9-14900K: 8 performance cores with SMT and 16 efficiency cores,
  32 logical CPUs, one NUMA node. The original matrix uses CPUs `0,2,4,6`;
  the expanded matrix uses `0,2,4,6,8,10,12,14`. These select distinct
  performance cores and do not select their SMT siblings.
- The 16-worker experiment adds CPUs `16..23`: eight efficiency cores.
  It tests a heterogeneous 16-core mask, not 16 identical performance cores.
  Workers and the recorder inherit a process affinity mask; there is no
  per-worker pinning or NUMA allocation policy. Eight workers plus the recorder
  share eight selected cores in the tracing experiment.
- Frequency/turbo and OS scheduling were not locked. This is one workstation,
  not an isolated host or a cross-NUMA study. Min/max are empirical ranges,
  not confidence intervals or a characterization of rare tails.

The runner shuffles scenarios and execution variants with seed `9141326`.
Hardware traffic and work are deterministic, without warmup ticks. Every scenario
includes an independent serial run and checks ticks, sends, receives, checksums,
per-lane/FIFO state digests, overflow, and actual parallel selection. Lossless
trace modes additionally require identical event counts and zero drops.

## Implementation and ordering

1. **Serial CDC index (`37e6918`).** Native FIFOs opt in to an endpoint-only
   callback contract. Serial batches visit the union of participating domains'
   FIFO lists, deduplicated in stable FIFO-ID order. Empty FIFOs still receive
   every endpoint edge, including synchronizer/flag transitions. Custom bridges
   retain every-batch callbacks unless they explicitly opt in.
2. **Calendar/retirement (`71eb725`).** Pending batches use a reusable bounded
   ring. Dense calendar indices
   resolve domain references for pending edges. Each coordinator sweep caches a
   conservative acquired completion frontier per visited domain. The canonical
   retirement calendar remains distinct from admission: unused grants are still
   discarded at stop, all begun edges settle, and resume starts at the committed
   boundary. Admission has not become retirement.
3. **CDC groups (`d3f2712`).** Only identical *ordered endpoint cluster pairs and
   clock pairs* share a scheduling actor. All lanes begin before either endpoint
   is released, and all lanes commit before completion or migration. FIFO state,
   capacity, synchronizers, payload order, IDs, logical trace producers, and
   actual record credits remain per lane. Placement traffic and work priors sum
   lane costs; live timing samples measure the entire group's begin/commit work.
4. **Stable ownership (`bbda6d5`).** The existing stable-sweep protocol avoids
   redundant per-actor ownership loads. Only the source worker transfers ownership,
   after its entire sweep; targets acquire a refreshed actor list on a later
   sweep. Migration fences remain checked, and every executed tick publishes
   progress. Ring indexing also avoids integer division.

After the measured revision, review fixes cap scheduler workers using the grouped
actor count and grow pending storage only as batches are admitted. A large
`max_lookahead_cycles` no longer preallocates the entire window at initialization;
edge storage grows with participating domains and is retained across runs. The
archived measurements below describe the original revisions, before these fixes.

A cached-readiness prototype did not improve the matrix consistently and was
removed. A CDC endpoint consumes one prepared edge per tick, leaving little
opportunity to reuse a ready-through bound. The prototype's bridge-free cache
included allowance and resolved ordinary/headroom dependencies, and reset at
stop/segmented boundaries; correctness passed but some timings regressed.
A separate cache-line isolation experiment also produced mixed results and was
not retained. Their raw measurements and patches are preserved in the archive.

## Original small/light/heavy/skewed matrix

20,000 physical batches, five repetitions, four distinct performance cores.
Values are median **baseline → candidate run milliseconds**, excluding
initialization and recorder close. All 1/2/4-worker measurements and ranges are
in `final-original` in the archive; this table highlights serial and four workers.

| Pairs / domains / work / skew | Serial ms | 4 static ms | 4 dynamic ms |
| --- | ---: | ---: | ---: |
| 4 / 2 / 0 / 1 | 2.271 → 2.439 | 9.318 → 25.677 | 11.603 → 10.839 |
| 32 / 2 / 0 / 1 | 11.316 → 9.892 | 22.641 → 27.955 | 65.154 → 29.612 |
| 32 / 8 / 0 / 1 | 5.621 → 3.887 | 11.693 → 12.677 | 13.811 → 13.744 |
| 32 / 32 / 0 / 1 | 5.369 → 2.868 | 7.258 → 6.926 | 8.385 → 6.773 |
| 4 / 2 / 4000 / 1 | 225.853 → 225.883 | 71.165 → 118.733 | 75.940 → 82.195 |
| 16 / 8 / 4000 / 1 | 227.294 → 274.668 | 84.971 → 70.661 | 75.606 → 70.007 |
| 16 / 8 / 4000 / 16 | 311.658 → 310.999 | 178.545 → 147.871 | 141.531 → 141.549 |

Regressions are material. In this sample, four-worker static `p4-d2-w0-s1`
regressed from 9.32 to 25.68 ms, and heavy `p4-d2-w4000-s1` from 71.17 to
118.73 ms. Candidate ranges were 9.62–49.01 ms and 76.94–135.88 ms respectively.
The same binaries did not reproduce those large ratios consistently on later
runs; they must not be explained away as a proven scheduler or host cause.

A separate five-repetition, **100,000-batch** follow-up on the same four-core
mask (`final-long`) gives:

| Scenario | 4 static, baseline → candidate ms | Candidate min–max ms |
| --- | ---: | ---: |
| p4-d2-w0-s1 | 41.526 → 48.765 | 45.225–51.203 |
| p4-d2-w4000-s1 | 380.576 → 359.467 | 358.453–470.278 |
| p32-d2-w0-s1 | 118.717 → 122.551 | 105.191–184.825 |

The longer cheap four-worker case still regresses. These results support
workload-specific improvements, not a uniform speedup or a resolved latency tail.

## Shared endpoints, sparse domains and larger graphs

The expanded matrix uses five repetitions and an eight-performance-core mask.
Each shared scenario has four endpoint pairs with eight independent FIFO lanes
per pair. `sparse-64` has 64 pairs and 64 domains with traffic attempted every
16 local edges. `segmented-shared` calls `runClockEvents` in chunks of 137.
`fanin-coprime` and `feedback` add fan-in, non-power-of-two clock ratios and a
CDC ring; their full results are included without filtering.

| Scenario | Workers / mode | Candidate ms | Baseline / candidate |
| --- | --- | ---: | ---: |
| shared-8 | 4 / static | 11.766 | 1.29× |
| shared-8 | 8 / static | 17.405 | 1.25× |
| shared-coincident | 1 / serial | 12.135 | 0.77× |
| shared-coincident | 4 / static | 14.025 | 1.47× |
| shared-coincident | 8 / static | 22.229 | 1.25× |
| segmented-shared | 8 / static | 24.521 | 2.94× |
| sparse-64 | 1 / serial | 2.942 | 3.27× |
| sparse-64 | 4 / static | 6.278 | 1.15× |
| sparse-64 | 8 / static | 8.208 | 0.98× |

Sparse serial execution improves strongly; shared coincident **serial** execution
regresses because building a union adds work on dense batches. Eight workers do
not consistently improve sparse parallel execution. Grouping's benefit depends
on lanes sharing the same endpoints, and cannot be inferred from independent-pair
benchmarks alone.

The separate heterogeneous-core experiment has three repetitions. Comparing
8 versus 16 workers **within its same CPU mask**:

| Scenario | Static 8 → 16, candidate ms | Dynamic 8 → 16, candidate ms | 16 static baseline / candidate |
| --- | ---: | ---: | ---: |
| p32-d2-w0-s1 | 24.310 → 42.530 | 28.226 → 49.034 | 0.55× |
| p32-d32-w0-s1 | 7.808 → 8.577 | 7.867 → 8.994 | 1.48× |
| sparse-64 | 8.044 → 12.979 | 7.531 → 12.212 | 0.71× |
| p16-d8-w4000-s16 | 114.707 → 143.075 | 110.315 → 145.142 | 1.80× |

More workers are often slower here. In particular the dense two-domain static
case regresses against baseline at 16 workers. This does not establish scaling
on homogeneous 16-core hosts, with SMT siblings, or across NUMA nodes.

## Attribution, initialization and resources

`profile_clock_scheduler` uses the existing steady-clock/sparse-sampling approach,
without speculative ticks. It samples one sweep in 64, staggered by worker, and
records coordinator retirement/admission, actor traversal/readiness, useful
cluster execution, bridge begin/commit, and idle wait scopes. Cluster counts distinguish
allowance waits from ordinary/bridge dependencies; completion loads and useful
work are also counted. Samples are host wall time and can include descheduling; periodic sampling
can alias periodic schedules. They are not an exhaustive CPU profile or a
critical-path proof. Actor time includes tick and bridge scopes, so those must
be subtracted before attributing polling overhead.

The separate paired diagnostic runs (`final-profile`) show these baseline
four-worker static shares of sampled measured scopes:

| Scenario | Coordinator admission + retirement | Actor traversal/readiness excluding ticks/commits | Useful cluster polls |
| --- | ---: | ---: | ---: |
| p32-d2-w0-s1 | 9.9% | 61.4% | 22.0% |
| p32-d32-w0-s1 | 26.4% | 59.9% | 3.6% |
| sparse-64 | 15.4% | 73.4% | 2.2% |
| shared-8 | 10.0% | 66.5% | 19.2% |

Actor polling consumes substantial aggregate worker time in these regimes,
while calendar/coordinator work is also significant in sparse graphs. Allowance
waits alone do not distinguish a slow coordinator from actors waiting behind a
slow peer. This evidence justifies reducing repeated scans, allocations and
actor count, but is insufficient to select coordinator redistribution or a
notification-based dispatch redesign. Heavy ticks instead dominate useful work.

For 20,000-batch static four-worker runs, counted C++ allocations during `run`
fall from about **21,684 to 17**. The residual allocations include run setup;
pending edge storage is reused. Dynamic migration and recording can still
allocate, and this counter excludes aligned `new`, `malloc`, and allocations
inside shared libraries. It is not total allocation traffic.

Resource examples below use the eight-core-mask expanded matrix. CPU time is
process user+system CPU for the run, including any recorder thread; RSS is the
whole-process high-water mark. `total_s` spans construction through recorder
close, while `process_wall_s` includes subprocess startup/shutdown. Neither
includes an `fsync` guarantee.

| Scenario / static workers | Init baseline → candidate ms | Candidate run ms | Candidate total ms | Run CPU ms | Peak RSS KiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| shared-8 / 4 | 3.020 → 0.200 | 11.766 | 12.147 | 46.667 | 27120 |
| shared-8 / 8 | 6.849 → 0.648 | 17.405 | 18.207 | 139.063 | 27120 |
| sparse-64 / 4 | 37.580 → 39.305 | 6.278 | 45.669 | 24.618 | 27120 |
| sparse-64 / 8 | 74.747 → 86.723 | 8.208 | 95.579 | 64.037 | 27120 |
| p4-d2-w4000-s1 / 4 | 0.091 → 0.102 | 71.962 | 72.178 | 287.411 | 27844 |

Initialization remains expensive for larger independent graphs. Shared placement
scoring and per-run scratch policy remain outside this change (#143).

## Recorder throughput and bounded backpressure

The lossless matrix measures off/text/native Perfetto/both for cheap independent
pairs, shared lanes and heavy ticks, with 1/2/4/8 workers, static/dynamic variants,
2,000 fixed batches and three repetitions. Default ingress capacity is 4,096.
All model-state fields and lossless event counts match across variants; drops
are zero. Below is the candidate shared-lane static case. Throughput divides
retained event count by run plus close time; CPU excludes final close.

| Workers | Mode | Run ms | Retained Mevents/s | Run CPU ms | Producer stall ms (sum) | Admission retries |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | off | 0.795 | 0.000 | 0.855 | 0.000 | 0 |
| 1 | text | 16.498 | 7.860 | 30.446 | 14.783 | 0 |
| 1 | perfetto | 217.997 | 0.677 | 435.970 | 0.000 | 0 |
| 1 | both | 243.849 | 0.605 | 487.585 | 0.000 | 0 |
| 4 | off | 1.206 | 0.000 | 4.425 | 0.000 | 0 |
| 4 | text | 30.862 | 4.727 | 149.538 | 0.000 | 447003 |
| 4 | perfetto | 195.782 | 0.739 | 976.859 | 0.000 | 3026890 |
| 4 | both | 256.124 | 0.565 | 1279.680 | 0.000 | 4810713 |
| 8 | off | 1.910 | 0.000 | 14.474 | 0.000 | 0 |
| 8 | text | 38.085 | 3.848 | 295.598 | 0.000 | 390710 |
| 8 | perfetto | 272.247 | 0.536 | 2177.054 | 0.000 | 3692338 |
| 8 | both | 425.412 | 0.345 | 3402.146 | 0.000 | 5954770 |

Native recording dominates these light-tick runs. Millions of failed admission
attempts with zero ingress producer stalls mean the **bounded recorder staging
window** is the constraint; they do not establish an actor-readiness problem.
Serial native recording instead reports safe-progress stalls. Eight workers plus
the recorder oversubscribe the eight selected cores, increasing CPU consumption.
Recorder optimizations are not implied by the scheduler's tracing-off gains.

A separate stress matrix (`final-lossy`) uses two-record ingress rings and
1,000 batches. It reports retained throughput and actual drops, not equivalent
observation coverage. Lossy streams can still stall to publish bounded gap
metadata; model traffic and FIFO state remain identical. Example candidate
four-worker static measurements:

| Mode | Retained events | Dropped events | Run ms | Producer stalls | Producer stall ms (sum) |
| --- | ---: | ---: | ---: | ---: | ---: |
| text | 30269 | 44131 | 9.003 | 8489 | 28.960 |
| perfetto | 32090 | 42310 | 48.098 | 7981 | 184.517 |
| both | 30117 | 44283 | 51.532 | 9346 | 197.146 |

Raw measurements also retain ingress/staging allocation, ingress high-water
bounds, native encoder peak bytes, output bytes, process RSS, recorder close
latency and safe-progress stalls. These are different memory/time scopes and
must not be added as if they were disjoint measurements.

## Correctness and validation

The new differential fixture tests 2/4/8 workers against serial execution, eight
independent lanes with different depths and synchronizer lengths, backpressure,
coincident and phased edges, non-dense domain IDs, unrelated endpoint pairs,
reverse edges, self-loops, and multiple unit owners in the same tight cluster.
It also checks conservative custom callbacks, bounded storage reuse, segmented
runs, stop/settle/resume, and forced ownership handoffs while a group is active.

Native shared-lane fixtures each import **4,771 events and 2,644 flow edges** with
zero drops and no Trace Processor errors. Native events match text records that
are checked against serial execution, including stable per-lane identities.
The existing lazy-wakeup, feedback, finite-headroom, stop/failure, bounded
recording and differential suites remain in the test run.

Validation at runtime revision `9a9bd5b` plus regression test `14a79b8`:

- Full Release build and **126/126 CTest tests**, including installed-consumer
  validation and native Trace Processor import checks.
- **8/8 ASan/UBSan/LeakSanitizer tests** with `-Werror`, including clock time,
  FIFO circuit, serial/parallel clocks, grouped lanes, migration, recorder
  budgets and recording. LeakSanitizer ran outside the ptrace sandbox because
  it cannot perform leak checks under ptrace.
- **8/8 TSan tests** covering the same clock/FIFO/recorder subset.
- Docusaurus production build passes with Node 22.5.1. The API index reports
  two broken-link warnings outside this change.

Validation logs and fixture import digests are included in the archive.

## Reproduce

Use a fresh output directory on each run and choose masks from your host's
physical-core topology. Do not copy this host's CPU numbers without checking.

The original benchmark commits were rewritten during rebase. The checked-in
[baseline harness patch](/benchmarks/multiclock-142/baseline-harness.patch) and
[candidate patch](/benchmarks/multiclock-142/candidate-runtime.patch) reconstruct
their exact source trees from the public ancestor `7c0766d14ee9ed10323e75b4d82d1aa6a71e650a`.
The following builds both archived versions; it does not substitute the current
PR head for the historical candidate. See the artifact README for full source
and tree hashes. Run these commands from a checkout containing this report.

```bash
artifacts="$PWD/website/static/benchmarks/multiclock-142"
(cd "$artifacts" && sha256sum -c SHA256SUMS)
base_revision=7c0766d14ee9ed10323e75b4d82d1aa6a71e650a
git fetch https://github.com/chronon-sim/chronon.git "$base_revision"
git worktree add --detach /tmp/chronon-142-baseline "$base_revision"
git -C /tmp/chronon-142-baseline apply "$artifacts/baseline-harness.patch"
git worktree add --detach /tmp/chronon-142-candidate "$base_revision"
git -C /tmp/chronon-142-candidate apply "$artifacts/baseline-harness.patch"
git -C /tmp/chronon-142-candidate apply "$artifacts/candidate-runtime.patch"

cmake -S /tmp/chronon-142-baseline -B /tmp/chronon-142-baseline/build \
  -DCMAKE_BUILD_TYPE=Release -DCHRONON_BUILD_BENCHMARKS=ON
cmake --build /tmp/chronon-142-baseline/build --target chronon_multiclock_benchmark -j8
cmake -S /tmp/chronon-142-candidate -B /tmp/chronon-142-candidate/build \
  -DCMAKE_BUILD_TYPE=Release -DCHRONON_BUILD_BENCHMARKS=ON
cmake --build /tmp/chronon-142-candidate/build --target chronon_multiclock_benchmark -j8

python3 /tmp/chronon-142-candidate/scripts/run_multiclock_benchmark.py \
  --binary /tmp/chronon-142-candidate/build/benchmark/chronon_multiclock_benchmark \
  --baseline-binary /tmp/chronon-142-baseline/build/benchmark/chronon_multiclock_benchmark \
  --baseline-revision 000ebb383ed9a75f6a495a339797b6c6df8a6a8c \
  --scaling --extended --steps 20000 \
  --repetitions 5 --threads 1,2,4,8 --cpus 0,2,4,6,8,10,12,14 \
  --output-dir out/multiclock-142-repro
```

For the original matrix omit `--extended` and use `--threads 1,2,4 --cpus 0,2,4,6`.
For component attribution add `--profile` to a separate invocation.
For lossless throughput use `--steps 2000 --repetitions 3 --trace-modes off,text,perfetto,both`
and `--scenarios p4-d2-w0-s1,shared-8,p4-d2-w4000-s1`.
For lossy stress use `--steps 1000 --scenarios shared-8 --lossy --trace-capacity 2`.
The archive's per-run commands specify every scenario option, affinity and output.

The `final-16` experiment uses `--threads 1,2,4,8,16`,
`--cpus 0,2,4,6,8,10,12,14,16,17,18,19,20,21,22,23`, three repetitions and
`--scenarios p32-d2-w0-s1,p32-d32-w0-s1,p16-d8-w4000-s16,sparse-64`.
The runner's automatic default chooses up to eight distinct cores and discloses
their topology; it never treats the requested worker count as a core count.
