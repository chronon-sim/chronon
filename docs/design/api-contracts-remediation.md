# API contract remediation

Baseline: `8563cea58fb0d84306c3252be1fd68044c07afbc` (main, 0.5.1).
Working branch: `fix/api-contracts-determinism`.

## Acceptance

- Preserve modeled timing, message order, admission, cancellation and architectural
  state across sequential/parallel execution, worker counts, repeated runs,
  segmentation, clock domains and dynamic migration.
- Compare baseline and candidate on the same machine, compiler, Release flags,
  dependencies, source workloads, seeds and CPU affinity. Use interleaved paired
  repetitions after warmup. Keep all samples and per-scenario state digests.
- Each measured scenario must demonstrate throughput at least 99% of baseline.
  Uncertain measurements fail acceptance; averages across scenarios must not hide
  a regression. Allocation-instrumented runs are not throughput measurements.
- GitHub PR checks must run correctness, deterministic differential tests,
  sanitizers, package consumption, documentation examples and the performance gate.
  Record the tested base/head commits and retain raw artifacts.
- Compatibility spellings must retain their existing behavior. New canonical
  APIs must not silently change old models' admission or scheduling semantics.

## Work items

- [x] Lifecycle: explicit, idempotent finalization between runs; truthful unit
      state; initialization/finalization failure behavior; app integration.
- [x] Identity: factory instance names, hierarchical lookup and diagnostics agree.
- [x] Observation: automatic unit-local timestamps and documented clock capabilities;
      explicit-clock tracing stays separate from the legacy cycle-time backend.
- [x] Boundary: protect scheduler tick hooks and progress installation; validate
      topology ownership/configuration phases; document adapter integration APIs.
- [x] Configuration: shared default values, one YAML-to-runtime conversion,
      canonical execution policy and polling terminology, conflict rejection.
- [x] Ports: named rate/depth values, consistent receive defaults, deterministic
      fan-in depth conflict handling, documented aliases and compatibility no-ops.
- [x] Ownership: instance-scoped port discovery; exclusive owned observation
      session with cleanup on success/failure and rejection of overlapping sessions.
- [x] Structure: extract cold lifecycle operations and internal progress descriptors;
      remove ObservationManager/YAML dependency from TickSimulation's public header.
- [x] Documentation: executable README, capability matrix and migration guide.
- [ ] Validation: baseline/candidate deterministic and performance artifacts,
      complete PR checks on final head, no automatic merge.

Implementation checks above record code completion; the final acceptance item
remains open until the final revision passes every required gate. The tested
commit and final acceptance record are maintained in
[PR #150](https://github.com/chronon-sim/chronon/pull/150), without changing the
revision after its measurements.

## Implementation decisions

`run()` remains a resumable advancement operation. Finalization belongs to an
explicit end-of-session operation, rather than occurring after each run segment.
No performance or determinism claim is complete until measured on the final PR
head. Changes to modeled behavior require an explicit regression demonstrating
the contract repair, rather than weakening existing equivalence checks.

## Measurement record

- `c210b2e` diagnostics: floor and sequential port cases passed. The four-worker,
  single-clock, fixed-placement scheduler case failed (median speedup 0.98237,
  one-sided 95% lower bound 0.97634). State digests matched throughout.
- Moving cold metadata out of the hot simulation layout and removing the extra
  Unit directory pointer improved the same case's median to 0.99928. Its lower
  bound was 0.98043, so the result remained uncertain and failed acceptance.
- Measurement starts with 51 pairs and a calibrated two-second target for every
  case. An uncertain first result may extend once to a fixed total of 201 pairs,
  retaining every initial sample. Both predeclared looks use a one-sided 97.5%
  bound, allocating the nominal 5% false-acceptance budget across the two looks.
  A clear regression stops without extension; uncertainty at the cap fails.
  The 0.99 threshold is unchanged. Failed diagnostics remain recorded.
- Observation capability is deliberately explicit: the existing process backend
  supports one managed observed session at a time. This change makes ownership
  and failure behavior safe; it does not introduce concurrent observation backends
  or remove the multiclock timestamp safety gate.

- `edd9e5e` local measurement: floor passed (median 0.997695, lower95 0.997471);
  sequential scheduler poll0 failed (median 0.989076, lower95 0.988867). This
  candidate's full matrix was stopped after the completed failure; all samples
  are retained. Its complete correctness Actions passed.
- Inlining the per-unit dispatch helper, with identical tick/activity logic,
  improved that sequential case to median 1.016249 and lower95 1.015771. The
  four-worker case remained uncertain (median 1.001907, lower95 0.986720).
  These diagnostic figures use the earlier single-look protocol and are not
  final acceptance. The fixed extension and stricter per-look confidence above
  were specified before measuring the next candidate.
- Session bookkeeping now uses a count and an explicit registration flag,
  avoiding an allocation on observation-free initialization. Lease management
  methods are private to the simulation instead of expanding the public API.

- Static lifetime regression: a static simulation finalized after the process
  observer's destruction and segfaulted after main returned. The regressions
  check queued-log and annotated-timeline draining after static destruction, plus
  finalizer logging when
  explicitly finalized before main returns. The observer service is constructed
  before the simulation; lazily interned metadata retains process lifetime so
  static finalizers and backend drains cannot use destroyed format/track tables.
  Logs after the main thread has retired its TLS producer may be dropped, as in
  the existing producer-lifetime contract; explicit finalization avoids this.

- `e5a7e07` completed local cases: six passed, while the four-worker fixed
  scheduler with polling interval 1 remained uncertain at the fixed 201-pair
  cap (median 0.994983, lower97.5 0.989525). Every state digest matched. The
  remaining matrix was stopped and retained before building the next fix.
- A direct untraced static-worker tick path did not establish acceptance
  (201-pair diagnostic median 0.991881, lower97.5 0.989350); it was reverted.
- Parallel launches now compose `bulk(schedule(scheduler), ...)` directly,
  avoiding the nested connection in `starts_on(..., bulk(just(), ...))`.
  Worker callbacks declare `noexcept` because they already capture exceptions,
  request peer stop, join, and rethrow on the caller. Static, dynamic and clock
  launches use the same boundary. Measured acceptance is still required.
- The direct `bulk(schedule(...))` plus nonthrowing callback diagnostic passed
  the short four-worker case at 51 pairs (median 1.005811, lower97.5 0.997469),
  with every state digest equal. This is one diagnostic case; the full final-head
  matrix and Actions remain required.
- `2638e65` completed local floor and sequential poll0 cases passed, but
  sequential poll1 showed a clear regression at 51 pairs (median 0.988636,
  lower97.5 0.987451, upper97.5 0.989490). Digests matched. The matrix was
  stopped and retained before further changes; all correctness Actions passed.
- `runUntil` now selects the immutable sequential/parallel dispatch once,
  preserving each predicate boundary, advancement count, and lifecycle check.
  Regressions cover predicates that advance the simulation or finalize it, on
  verified sequential and parallel paths; the same tests passed before changing
  dispatch. Final-head performance and correctness acceptance is still pending.
- `38b93a0` completed nine local cases: eight passed, while four-worker dynamic
  poll0 remained uncertain at 201 pairs (median 0.993022, lower97.5 0.984796).
  State digests matched. The incomplete matrix and every sample were retained.
- Calibration had targeted two seconds using one tiny linear estimate; dynamic
  poll0 actually measured a median of about 1.25 seconds. Calibration now checks
  each scaled workload (at most five rounds), compares both states, and saves
  all round durations. The existing billion-cycle cap is explicit in artifacts.
  The 51/201-pair rule, per-look confidence and 0.99 threshold are unchanged.
- Dynamic workers cache owner-private sampling state and the immutable unit list
  within a burst. They still observe activity opt-in after every tick and publish
  progress in the same order. Dynamic wakeup/opt-in regressions passed before
  this optimization and are included in the full suite afterward.
- The burst-cache plus feedback-calibration diagnostic passed dynamic poll0 at
  51 pairs (median 1.006356, lower97.5 0.993970), with equal states throughout.
  Calibration reached roughly 2.1 seconds after four rounds. This remains one
  diagnostic; a fresh complete final-head matrix and Actions are required.
- `8cf7821` completed fourteen local cases: thirteen passed, including all
  single-clock cases and multiclock sequential polling configurations. Multiclock
  four-worker static poll0 failed at 51 pairs (median 0.940839, lower97.5
  0.927774). Every state comparison matched; the incomplete matrix was retained.
- Specializing the immutable clock migration policy removed its repeated runtime
  branches, but the diagnostic remained uncertain at 201 pairs (median 0.997154,
  lower97.5 0.981661). This result failed acceptance and is retained.
- CPU sampling placed most execution time in the clock actor polling loop,
  including admission loads. Domain admission now occupies a separate cache line
  from coordinator-private retirement scratch. Invocation counters also occupy
  their own cache line, away from the flags and captures read by all workers.
  These storage changes retain the same loads, stores, memory orders, bridge
  transactions and migration boundaries. All 135 regressions passed; measured
  performance acceptance remains required.
- With migration-policy specialization and cache-line isolation, all six local
  multiclock/four-worker diagnostics passed at 51 pairs each. The smallest
  lower97.5 bound was 1.012501 (dynamic poll1); static poll0 reached median
  1.656348 and lower97.5 1.621825. Every state comparison matched. These targeted
  diagnostics are explicitly an incomplete matrix; full final-head validation
  is still required.
- The retained `8cf7821` CI matrix completed eighteen cases before cancellation:
  thirteen passed and five failed. In addition to three multiclock failures,
  two-worker single-clock dynamic poll0 and poll1 failed at 201 pairs
  (lower97.5 0.987581 and 0.985595). The CI host was AMD EPYC; local measurements
  use Intel physical cores. These failures cannot be replaced by local passes.
- `56fb154` local two-worker dynamic poll0 and poll1 diagnostics passed at 51
  pairs (lower97.5 1.007197 and 1.037221), with equal states. Runtime code remains
  unchanged while the exact candidate still requires complete CI validation.
- The gate now returns immediately after a scenario's final negative decision,
  uploading its evidence without waiting for unrelated scenarios. An uncertain
  first look still extends to 201 before this decision. An early exit records an
  incomplete matrix; a successful full gate still requires every scenario.
  Eighteen gate tests include CLI sampling, retained samples, early rejection,
  extension before rejection, and successful partial versus full matrices.
- `10872ca` failed both complete-matrix attempts after fixed 201-pair decisions:
  local multiclock sequential poll0 had median 0.988559 and lower97.5 0.987942;
  CI two-worker single-clock dynamic poll1 had median 0.989927 and lower97.5
  0.986792. Ten prior local cases and eight prior CI cases passed. Every state
  comparison matched; both early-exit artifacts explicitly mark incomplete matrices.
- Sequential single-domain clock batches now resolve their runtime once and reuse
  it for CDC selection, unit execution and retirement. Small dynamic graphs use
  invocation-local ranking/readiness buffers, following the predecessor-cache
  design; larger graphs retain vector capacity. Readiness resets on every call
  and ranking is written before sorting. Execution and migration order are unchanged.
- Poisoned-boundary and forced-migration equivalence coverage now includes 2/4
  workers and 4/8/9/12 producer-consumer pairs. The expanded test passed before
  these optimizations; all 135 regressions passed afterward. Three 51-pair local
  diagnostics passed with equal states: multiclock sequential poll0 lower97.5
  0.999495, two-worker dynamic poll1 1.034698, and four-worker dynamic poll1
  1.018149. These are incomplete diagnostic matrices; the CI AMD host and complete
  final-head matrices still require validation.
- `40eeebf` CI passed eight cases, then clearly failed two-worker dynamic poll1
  at 51 pairs (median 0.980516, lower97.5 0.973446, upper97.5 0.985863).
  All state comparisons matched. Its local matrix was stopped and retained after
  eleven passing cases, including multiclock sequential poll0 (lower 1.000324).
  Neither the local passes nor the earlier targeted diagnostics establish CI
  acceptance; the sampling rule and threshold remain unchanged.
- Short dynamic invocations now reuse the immutable, published ownership lists
  while their generation matches, retaining the before/after generation check
  and atomic owner scan after migration. Clock actor discovery still includes
  bridges through the existing scan. Runtime sampling preparation avoids the
  per-unit scan only while the existing activity summary proves no unit has
  opted in; constructor binding, interval changes and sleep operations publish
  that summary. Claim resets, sampling decisions and progress ordering remain
  unchanged. A boundary interval/sleep/wake regression passed before the change;
  all 135 regressions passed afterward. Three local 51-pair diagnostics passed
  with equal states: two-worker dynamic poll1 lower97.5 1.050485, four-worker
  dynamic poll0 0.993081, and four-worker dynamic poll1 1.021420. These targeted
  results do not replace complete final-head local and CI acceptance.
- `b6fae75` passed all applicable correctness and documentation Actions. Its CI
  performance host was AMD EPYC 9V45, whereas the preceding two runs used EPYC
  7763. After four passing cases, static two-worker poll0 clearly failed at 51
  pairs (median 0.972262, lower97.5 0.964632, upper97.5 0.978448); all states
  matched. The local matrix was stopped and retained after six passing cases,
  with minimum lower bound 0.990461. Neither matrix establishes acceptance.
- The performance workflow provides an explicit manual profiling mode for exact
  baseline/candidate commits. It captures CPU samples on the CI hardware for
  static long calls and static/dynamic short calls, comparing workload states
  and retaining raw samples, annotated instructions, build metadata and hashes.
  Profiling artifacts are explicitly incomplete and never constitute throughput
  acceptance. Normal PR validation keeps the same full matrix and strict gate.
- The first two manual profile runs used process affinity but omitted the gate's
  worker-pinning and CPU-warmup environment flags. Their raw data is retained,
  with that limitation; they cannot attribute the acceptance failures. Profiling
  now passes those flags explicitly through `sudo` and records them in metadata.
  It also archives both executables and validates whole-executable instruction
  annotations, avoiding version-dependent demangled symbol filters. Very brief
  scheduled tasks can be under-sampled by software CPU-clock timers.
- Generated code independently shows an outlined per-unit dispatch lambda inside
  `executeClusterOneCycle_`. That lambda is now explicitly inlined, retaining its
  clock, activity and exception behavior while removing a per-unit call boundary.
  GCC/Clang accept the attribute; all 135 regressions and 18 gate tests pass.
  The gate measures previously failing parallel paths first and records the case
  order. All 29 case definitions, the threshold and statistical rules are unchanged.
- Four local diagnostics of the inlined dispatch passed at 51 pairs with equal
  states: two-worker static poll0 lower97.5 1.329728, two-worker dynamic poll1
  1.045107, four-worker dynamic poll1 1.022083, and four-worker static poll1
  0.999736. These incomplete diagnostics require fresh full local/CI matrices on
  the final committed revision; they do not establish hardware-independent gains.
- `bd1b0ca` validation was interrupted for a CI formatting failure: older
  clang-format versions misparsed the explicitly typed GNU-attributed lambda
  parameter. Using `auto*` produces stable formatting under versions 14 and 20.
  All 135 regressions pass, and all three local benchmark executables are
  byte-identical before/after this spelling change. The interrupted local matrix
  retains two passing cases and partial samples; fresh final-head validation is
  still required, with no observed performance failure discarded.
- `61c1073` passed all correctness/documentation Actions, but CI static
  two-worker poll64 failed at the fixed 201-pair endpoint on AMD EPYC 9V74:
  median 0.984037, lower97.5 0.980185, upper97.5 0.987005. Seven preceding CI
  cases passed, including the previous static poll0 and dynamic poll1 failures.
  Every state matched. Its local matrix was stopped after twelve passes;
  dynamic four-worker poll0 required 201 pairs and passed with lower 0.992809.
  Both incomplete matrices and all raw samples are retained.
- Small static worker invocations now use the existing invocation-local
  predecessor buffer, as dynamic and clock workers already do. This removes
  retained vector transfer/reset/return work and one cache-selection template
  axis. Large graphs retain their existing reusable storage. Every invocation
  still clears the same predecessor slots, including the synthetic floor slot;
  dependency checks, unit order and progress publication are unchanged.
  All 135 regressions pass, including boundary/poisoned-scratch equivalence, and
  all 18 gate tests pass. Fresh performance measurements are still required.
  Static poll64 now runs first to expose the latest failure promptly; all 29
  workload definitions and acceptance rules remain unchanged.
- `1e35ba5` failed CI static two-worker poll64 on Intel Xeon Platinum 8573C
  at 51 pairs: median 0.978230, lower97.5 0.967329, upper97.5 0.986175. Every
  state matched. Its local matrix was stopped after three passes; static
  four-worker poll64 had lower 1.066559. This does not establish that the cache
  simplification resolves the CI regression; the raw failure is retained.
- Diagnostic profiling now includes static poll64 and probes hardware cycle
  sampling, retaining the probe output and the selected event/frequency. If the
  runner does not expose a PMU, software CPU-clock sampling remains explicitly
  labeled with its short-task sampling limitation. This profiling-only update
  does not change runtime code or retry performance acceptance on failed code.
- The pinned/warmed CI diagnostic of `1e35ba5` used the same Xeon 8573C model
  and preserved all states. Its PMU probe printed `<not supported>` but returned
  success; perf silently sampled `task-clock:uH`. The retained reports identify
  the actual software event. The workflow now rejects unsupported/countless
  probe output and records the event reported for every profile, rather than
  claiming the requested event was sampled.
- Local hardware-cycle and CI software profiles both include the generic
  cluster dispatcher among the hot functions. Generated code gives it a larger
  call frame after tick-guard inlining. Ordinary static workers now select unit
  tracing once per invocation and directly reuse `executeUnitCycle_` when unit
  tracing is off. The traced path retains the generic dispatcher, and all tick
  activity checks, unit order, idle handling and progress stores are unchanged.
  This differs from the earlier rejected per-cluster conditional experiment by
  specializing the immutable trace choice outside the worker loop. No measured
  improvement is claimed before fresh regression and performance validation.
- `47b3552` passed all correctness/documentation Actions and the complete CI
  29-case performance matrix on Xeon 8573C (minimum lower97.5 0.990387).
  Its local matrix passed 26 cases, then failed four-worker backpressure at the
  fixed 201-pair endpoint: median 0.990282, lower97.5 0.987602, upper97.5
  0.992464. All states matched. The CI pass does not replace that local failure;
  both raw artifacts are retained and final acceptance remains incomplete.
- Local hardware-cycle profiling of the failing backpressure workload places
  substantial time in dependency checks and predecessor refreshes. Static
  workers now reuse a readiness bound derived from already-acquired predecessor
  progress, as dynamic workers do. The bound includes the synthetic floor,
  saturates on addition and resets every invocation; a missing proof takes the
  original dependency check. Cluster order, idle behavior and progress stores
  are unchanged. The final cycle avoids constructing an unused bound. Small
  graphs use local slots and larger graphs retain storage covered by poisoned
  boundary regressions. Performance improvement still requires measurement.
  Parallel backpressure runs first; all 29 workload definitions and acceptance
  rules remain unchanged.
- `b551eab` passed correctness/documentation Actions, but its CI matrix on
  Xeon 8370C passed 15 cases and clearly failed two-worker multiclock static
  poll1 at 51 pairs: median 0.973342, lower97.5 0.969182, upper97.5 0.976456.
  Every state matched. The local matrix was stopped and retained after 14
  passes, including single-clock dynamic poll0 at its fixed 201-pair endpoint
  (lower97.5 0.992482). Neither incomplete matrix establishes acceptance.
- Local hardware-cycle profiles of static multiclock short invocations identify
  the general cluster dispatcher among the sampled functions. Static clock
  workers now reuse a shared inline clock-unit operation directly, preserving
  activity checks, edge markers, trace edge completion and exception behavior.
  Dynamic workers retain per-unit sampling through the general dispatcher;
  coordinator admission, bridge operations and progress stores are unchanged.
  The previously failing static multiclock poll1 case runs first for feedback;
  all 29 workloads and statistical acceptance rules are unchanged. Fresh
  final-head measurements are required; profiles are not throughput acceptance.
