# Scheduler-owned observation I/O

This completes the pipeline unification on `feat/scheduler-observation-backend`.
The earlier [SAGE diagnosis](observation-service-sage-diagnosis.md) remains a
historical performance comparison; it predates this change.

## Execution and lifetime

`HostServices` owns both ingress registrations and a shared blocking-I/O lane.
The scheduler is neither copyable nor movable: its destruction detaches its
registrations. Callers can transfer a `unique_ptr<HostServices>` when needed.
`ObservationBackend` and `ClockTraceRecorder` no longer own threads or separate
consumer implementations. Scheduler workers copy bounded batches (at most 256
records; ordinary records also have a 256 KiB byte limit). A preallocated
`HostIOJob` dispatches each batch to the shared lane, which runs pending jobs
round-robin. Each output has at most one outstanding job and one handoff buffer.
No task allocation occurs during dispatch.

When the initial batch is full, native clock output reuses its handoff buffer on
the I/O lane for up to 16 batches per job (including the worker's initial batch),
stopping sooner when no work is available. Partial initial batches return without
an extra scan. Its ingress registration stays paused throughout, so there is still
one queue consumer and no concurrent access to that buffer. Each continuation
uses the same snapshot/frontier rules as worker ingress. The job then yields to
other outputs; it never waits for producers. This avoids a worker/I/O round trip
for every 256 records when native trace output is backlogged, without adding
buffers or increasing the worker's record budget. Native `service_*` statistics
measure scheduler polls only, excluding I/O continuations and final draining.

Opening files, sorting, formatting, encoding, compression, writing, flushing,
closing, native manifests and native statistics reports all execute on this lane.
The standalone scheduler timeline export also uses it. The executor marks the job
slot reusable before enabling its ingress registration again; producer readiness
published during an outstanding write remains latched.

Blocking file calls can stall the shared I/O lane. They do not occupy simulation
workers, and bounded producer queues retain their configured drop/wait policy.
The lane does not preempt a filesystem call. I/O callbacks must not wait for
another job on the same lane or recursively stop their own backend.

Standalone backends create a `HostServices` driver that polls the identical
bounded ingress on its I/O lane, sleeping up to 50 microseconds when idle. This
supports standalone examples without retaining a second consumer algorithm.
Ordinary output supports both `enable_reordering=true` and `false`.
Stopping and restarting a standalone backend retains its driver. Attaching the
stopped backend to an external scheduler joins and releases the old standalone
driver before registering with the new scheduler.
Repeated attachment to the same scheduler reuses the registration and I/O job,
so stopped sessions do not add empty slots to the scheduler's polling rotation.

Shutdown detaches ingress, fences the outstanding job, drains bounded batches and
submits finalization. Jobs retain the executor state, allowing observers to close
after their simulation has been destroyed. Failed output releases waiting
producers; ordinary backends retain their first error, discard the failed run's
unread records, and support restart. Failure in one close operation does not skip
closing the other sinks.

Native final drain takes a fresh snapshot after producers stop. Reusing a partial
runtime snapshot can incorrectly conclude that queues are empty after finishing
its empty suffix, leaving later publications unread. The multithread self-loop
and scaling trace comparisons caught this during the refactor. Runtime polling
also schedules a fresh scan after an empty suffix, preserving readiness for
publications outside the older snapshot.

## API migration

The experimental C++ `scheduler_service` switches are removed. YAML `true` is
accepted for configurations created during the experiment; `false` reports a
migration error instead of silently selecting another path. New configurations
omit the key. SAGE removes `--perfetto-service` and `--perfetto-thread`; ordinary
`--perfetto` uses the unified scheduler pipeline. Historical A/B scripts require
the saved binaries with their recorded hashes.

`backend.attachScheduler(sim.hostServices())` remains available for independently
owned ordinary backends. Simulation-owned ordinary and native recorders attach
automatically before startup. A manager backend started before simulation
initialization retains its existing scheduler and continues through the same
standalone service pipeline. Initialize the simulation before `startBackend()`
to share the simulation scheduler; `SimulationApp` already does so.
`HostIOJob` callbacks are noexcept and own error publication;
callers must detach ingress and fence I/O before destroying callback state.

`ObservationYAMLConfig::service_buffer_bytes` is appended after the existing
fields to preserve positional aggregate initialization. The legacy
`ObservationBackend::Config::poll_interval` member remains for source compatibility
and has no effect. Class layouts changed, so downstream binaries must be rebuilt;
this does not preserve binary ABI compatibility.

## Validation

Evidence is under `out/observation-service-io-20260920/`. The final smoke runs
use six workers; these are correctness checks under concurrent build/test load,
not isolated performance measurements. No new performance percentage is claimed.

| Check | Result |
| --- | --- |
| Chronon Release (GCC 12) | 164/164 passed, including package consumers and real Perfetto import |
| ASan + UBSan + leak detection | 31/31 passed |
| ThreadSanitizer | 31/31 passed |
| Deterministic outstanding-I/O lifetime test | Passed again in Release, ASan/UBSan/leaks and TSan after strengthening the blocked-write handoff |
| SAGE native clock integration (Clang 20) | 13/13 internal cases passed |
| Nucleus six-worker correctness | Dhrystone 401,268 cycles; Coremark 1,037,093 cycles; both exit 0 |
| SAGE six-worker correctness | Vector and copy, each compressed/uncompressed, match all baseline model metrics, output buffers, native manifests and text records; zero drops |
| Independent Perfetto import | Vector: 262,638 events / 206,084 flows; copy: 795,600 events / 624,388 flows; both encodings match |

Nucleus CSV comparisons include every row and column. The only differences from
the saved baseline are the last-row `simulation.obs_info_emitted` counts:
Dhrystone 60→62 and Coremark 56→64. All periodic cells and final model counters
match. This observation/self-report count was already host-timing-dependent in
the earlier evaluation; the differences are retained in `final/correctness.json`.

The targeted tests cover 1/4 workers, static/dynamic scheduling, burst/sparse
records, slow callbacks, 4 KiB producer queues, large payloads, a 64 KiB reorder
arena, immediate output, standalone driving, native frontier ordering, output
failure and restart. `test_host_services` blocks an I/O callback while polling a
second service, verifies shared thread identity, then destroys the scheduler
while a callback remains blocked and completes both that write and a later flush.
The ordinary sink test also verifies that the final reorder tail stays on the
same I/O thread.

Final logs, binary/source hashes and the SAGE companion patch are saved alongside
the smoke commands and trace validation results. These measurements were collected
before publication.
