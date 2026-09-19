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
remains open until the final revision passes every required gate.

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
- Final measurement protocol therefore uses 51 pairs and a calibrated two-second
  target for **every** case. The 0.99 threshold and confidence level are unchanged;
  failed diagnostics are retained, not replaced by selectively successful reruns.
- Observation capability is deliberately explicit: the existing process backend
  supports one managed observed session at a time. This change makes ownership
  and failure behavior safe; it does not introduce concurrent observation backends
  or remove the multiclock timestamp safety gate.
