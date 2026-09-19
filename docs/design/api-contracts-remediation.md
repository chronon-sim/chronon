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

- [ ] Lifecycle: explicit, idempotent finalization between runs; truthful unit
      state; initialization/finalization failure behavior; app integration.
- [ ] Identity: factory instance names, hierarchical lookup and diagnostics agree.
- [ ] Observation: automatic unit-local timestamps and explicit clock support;
      eliminate silent zero timestamps without adding work to disabled tick paths.
- [ ] Boundary: restrict scheduler hooks and transport setup to internal access;
      validate topology ownership and configuration phases.
- [ ] Configuration: shared defaults/validation, canonical execution policy and
      compatibility decoding, explicit polling terminology.
- [ ] Ports: named rate/depth/edge limits, consistent receive defaults, explicit
      fan-in capacity conflict handling, documented compatibility aliases/no-ops.
- [ ] Ownership: instance-scoped port discovery and observation lifetime with
      repeat/overlapping simulation coverage.
- [ ] Structure: separate cold control responsibilities and public headers from
      implementation details; retain hot-path specialization.
- [ ] Documentation: executable canonical examples, capability matrix and migration
      guide; preserve supported compatibility tests.
- [ ] Validation: baseline/candidate deterministic and performance artifacts,
      complete PR checks on final head, no automatic merge.

## Implementation decisions

`run()` remains a resumable advancement operation. Finalization belongs to an
explicit end-of-session operation, rather than occurring after each run segment.
No performance or determinism claim is complete until measured on the final PR
head. Changes to modeled behavior require an explicit regression demonstrating
the contract repair, rather than weakening existing equivalence checks.
