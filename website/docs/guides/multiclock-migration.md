---
title: Multi-clock migration policy
---

Dynamic multi-clock execution reuses the existing sparse active/inactive sampler,
placement objective and actor-local ownership handoff.
The default policy adds a conservative admission check; model code and existing
configuration fields are unchanged. A bridge still moves only after its entire
begin/commit transaction finishes.

## Admission

The planner compares successive windows of the existing cumulative timing and
activity counters. Active and inactive timing remain separate, and their means
are combined with the activity ratio in that window. Four fresh samples of each
activity state present in a window are required. Two windows whose estimated
costs differ by at most 25% establish a usable estimate. This is a noise filter,
not a statistical confidence interval. Historical samples therefore cannot hide
a new expensive phase inside a lifetime average. The initial placement still
uses the existing supplied/precomputed costs; it never executes speculative ticks.

Every actor resident on the proposed source and target must have a usable cost.
An unrelated rare actor on another worker does not block the move. A never-seen
or rarely sampled phase can delay migration of its own worker, intentionally.

The existing placement score ranks candidates. Admission separately requires a
positive conservative saving: half the estimated reduction in the busiest
worker's active cost, minus any added communication pressure. Removed
communication and heuristic bonuses do not count as measured savings. The old
active-load-only fallback cannot bypass this check in multi-clock mode.

The saving must also exceed four times `rebalance_min_gain` multiplied by the
recent observed host nanoseconds per retired reference cycle (and 2% of the
modeled busiest-worker cost). This accounts for polling/coordinator work absent
from model tick samples: a small fraction of cheap model work need not produce
a useful wall-time improvement. Unknown host rates reject admission. The factor
four is the same heuristic safety margin used for amortization, not a measured
confidence bound; no workload name, tick-cost threshold, or domain frequency is
special-cased. The host-rate window advances after unsuccessful planning and
restarts at each public run call.

The predicted saving must repay four times the estimated overhead over at most
eight confidence intervals. That overhead includes the measured planning time,
the largest observed request-to-commit elapsed time, a calibrated timer allowance
for sparse sampling, and eight actor edges of estimated cache warmup work. The
last two terms are allowances, not measurements of cache misses or attribution
of model execution to profiling. Request-to-commit time includes work that can
overlap on other workers, so charging all of it is conservative.

All recurring costs and benefit windows use reference-clock cycles of physical
time. One-off costs use host nanoseconds. The available window is also clipped
by this public run call's remaining batch/time/domain-cycle limit, excluding the
admitted batch window. Summed domain edge rates deliberately underestimate the
physical duration represented by coincident batches. Budget calculation occurs
only when the planner's existing gate opens.

## Feedback and overhead

After a move, the planner waits for four confidence intervals of physical
progress before considering another move. It compares host nanoseconds per
retired reference cycle against the pre-move rate. A measured improvement greater
than 2% resets the check interval; a failed or noisy result increases it. Failed
planning attempts also double their interval, up to 32 times the normal
confidence interval. This prevents cheap, balanced workloads from repeatedly
paying for full candidate enumeration. Cold sampling waits do not exponentially
back off before any usable estimates exist.

The minimum confidence interval is four existing sparse-sampler intervals, or
`rebalance_check_interval_cycles`, whichever is larger. Existing per-actor
cooldowns and reverse-move cooldowns still apply. Only another migration waits
for feedback: workers continue independently, with no execution barrier,
rollback, or speculative work.

A public run call restarts host-rate feedback, excluding wall time spent stopped
between calls. Sampling windows and bounded backoff survive segmentation.
Termination can shorten any predicted benefit window; admission cannot know
future unit-initiated termination.

With `profile_clock_scheduler`, the existing profile additionally reports
`migration_plans`, `migration_planning_ns`, `migration_handoff_ns`,
`migration_feedback_good`, and `migration_feedback_bad`. These counters cover
all planning attempts and committed handoffs, unlike the sampled sweep fields.
The benchmark emits them with its existing `sample_` prefix. Handoff elapsed
and planning elapsed must not be added to sampled actor time as independent CPU
costs.

## Limitations and validation

This policy cannot guarantee that every move is profitable. Host scheduling,
coordinator saturation, cache placement, unknown work on other workers, and
phase changes can invalidate the estimate. Feedback is an observed before/after
comparison, not a causal performance proof. Tiny segmented calls may never
accumulate an in-call feedback window. Cheap models may remain faster in serial
or static execution even when dynamic execution makes no migrations.

Deterministic tests cover fresh conditional windows, rare/inactive activity,
phase changes, unknown costs, communication penalties, short benefit windows,
saturating arithmetic, reference-frequency invariance, negative feedback and
stopped-time exclusion. The live-sampling test permits conservative rejection
on slow or instrumented hosts; controlled samples separately require unit and
bridge admission and real handoff. Existing serial/parallel state, forced bridge
handoff, trace identity, termination and resume differential tests remain applicable.

## Measurements on 2026-09-22

The [raw results](/benchmarks/multiclock-140/measurements.tar.gz) contain 780
fresh-process, interleaved runs. An initial policy matrix used seven scenarios,
1/2/4 workers, serial/static/dynamic execution, baseline/candidate, three
repetitions, at both 20k and 80k batches (420 runs), plus 90 separately profiled
runs. A principled final revision added the host-rate gain floor described above,
then repeated three representative scenarios at both budgets plus profiling
(270 runs). Both policy iterations and their patches are retained; the initial
results must not be mislabeled as final-policy measurements.

All runs pass unit-tick, send/receive, checksum, work-digest, FIFO/lane-digest and
overflow comparisons with the serial oracle. Throughput runs disable profiling
in both variants. Baseline is freshly built main
`0c8b6f2379f9c8bec09a7cbc258c8df72604b7e1`, GCC 12.2 Release, same dependencies.
The final candidate is that revision plus the archived
[runtime patch](/benchmarks/multiclock-140/candidate-runtime.patch). Processes
inherit CPU mask `0,2,4,6,8,10,12,14` (distinct physical cores); workers are not
individually pinned. Metadata includes topology, compiler, CMake caches and
binary hashes. This compares the immediately previous runtime, not historical
`b3bef22` or `4e97c5e` measurements.

**The final timing window was externally contended.** Chronon builds/tests were
paused, but host inspection found two unrelated processes each using about six
CPUs. They were not stopped. Unchanged baseline light 4-pair dynamic execution
at 80k batches changed from a 31.91 ms median in the initial window to 731.65 ms
in the final window. This variation prevents a clean causal speedup claim.
The final data validates hardware equivalence and conservative migration
behavior under contention; it is not an isolated performance certification.

Final four-worker run medians below are milliseconds, with min–max in
parentheses over three runs. Ranges are not statistical confidence intervals.
`p/d/w/s` mean pairs/domains/work/skew. Serial and static use the candidate.

### 20k batches

| Scenario | Serial | Static | Previous dynamic | Candidate dynamic | Moves previous → candidate |
|---|---:|---:|---:|---:|---:|
| p16-d8-w4000-s1 | 225.15 | 152.61 (142.90–182.69) | 144.17 (120.06–204.45) | 156.15 (151.84–205.25) | 1 → 0 |
| p16-d8-w4000-s16 | 309.62 | 303.15 (219.36–307.28) | 218.38 (178.26–313.26) | 156.47 (153.77–234.30) | 1 → 0 |
| p4-d2-w0-s1 | 1.39 | 34.57 (26.25–40.57) | 146.68 (8.44–202.05) | 21.38 (4.43–32.40) | 5 → 0 |

### 80k batches

| Scenario | Serial | Static | Previous dynamic | Candidate dynamic | Moves previous → candidate |
|---|---:|---:|---:|---:|---:|
| p16-d8-w4000-s1 | 900.49 | 615.32 (576.80–623.96) | 723.62 (630.31–875.51) | 587.25 (521.54–720.09) | 3 → 0 |
| p16-d8-w4000-s16 | 1238.07 | 956.29 (615.50–1212.01) | 1422.31 (1301.93–1436.41) | 711.19 (665.64–771.92) | 4 → 1 |
| p4-d2-w0-s1 | 5.65 | 45.88 (44.60–51.56) | 731.65 (352.61–1293.04) | 65.73 (64.00–90.38) | 5 → 0 |

The final policy admits no light-case moves at four workers in either budget;
it still admits a long skewed move. Controlled unit and bridge planner tests
exercise positive admission and actual ownership handoff. Migration remains
available for useful moves. Nevertheless, final long light dynamic remains slower than
candidate static despite zero moves. Sampling, dynamic ownership checks,
coordinator/polling work and external host scheduling are not eliminated by a
migration policy. These measurements do not separate their causal contributions.

Initialization and complete in-process wall time remain separate (milliseconds,
medians, four-worker dynamic):

| Budget | Scenario | Previous init | Candidate init | Previous total | Candidate total |
|---|---|---:|---:|---:|---:|
| 20k | p4-d2-w0-s1 | 0.095 | 0.092 | 146.901 | 21.585 |
| 20k | p16-d8-w4000-s1 | 2.860 | 2.909 | 147.179 | 159.184 |
| 20k | p16-d8-w4000-s16 | 2.856 | 2.926 | 221.362 | 159.532 |
| 80k | p4-d2-w0-s1 | 0.091 | 0.088 | 731.851 | 65.932 |
| 80k | p16-d8-w4000-s1 | 2.885 | 2.942 | 726.724 | 590.328 |
| 80k | p16-d8-w4000-s16 | 2.862 | 2.907 | 1425.304 | 714.257 |

The separate final 80k diagnostic runs provide exact migration attempt/time
counters alongside sampled execution fields (four workers, medians):

| Scenario | Plans | Planning µs | Handoff elapsed µs | Good / bad feedback |
|---|---:|---:|---:|---:|
| p16-d8-w4000-s1 | 3 | 41.89 | 0.00 | 0 / 0 |
| p16-d8-w4000-s16 | 5 | 44.92 | 148.71 | 0 / 0 |
| p4-d2-w0-s1 | 6 | 25.24 | 0.00 | 0 / 0 |

Profiling changes timing and sometimes decisions; these figures are not additive
explanations of unprofiled differences. No observed feedback is not evidence of
benefit: a move can finish too near the run limit for its observation window.
The initial placement algorithm is unchanged; `partition_ns` and allocation
counters are preserved in the raw data.

The archived initial policy's full seven-case matrix also records useful
counterexamples: long balanced-heavy and skewed dynamic improved over their
same-revision static medians, but were 4.7% and 2.1% slower than previous dynamic.
Its light 4-pair run retained one move with negative feedback and remained slower
than static. That led to the final host-overhead bound, not a workload-specific
cutoff. Broader isolated performance calibration and sampling/coordinator
optimizations remain necessary; this PR does not claim universal speedup.
