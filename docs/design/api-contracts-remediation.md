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
