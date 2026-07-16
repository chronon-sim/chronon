# Non-Cancelable OutPort Evaluation

## Scope

`CHRONON_ENABLE_OUTPORT_CANCELLATION` defaults to `ON`. Models that prove they
never call `OutPort::cancelInFlight()` may set it to `OFF`, which removes:

- the per-send cancellation epoch acquire;
- the epoch stamp in MPSC staging;
- the cancellation pointer and epoch snapshot in every `PortEnvelope`;
- the receiver/arbiter epoch checks.

`PortEnvelope<uint64_t>` is 48 bytes with cancellation enabled and 32 bytes
when disabled. Receiver-side selective cancellation remains available. Exact
OutPort cancellation is intentionally not reimplemented with a cycle timestamp
because that cannot distinguish sends before and after a mid-cycle cancel.

## Clean Nucleus A/B

- Nucleus: `master` at `defdb1f`, with no `cancelInFlight()` call sites
- Chronon base: `main` at `ec78cdb`
- Separate Release binaries configured with cancellation `ON` and `OFF`
- Six pinned physical cores: `0,2,4,6,12,14`
- `--no-observe`, `CHRONON_EXPERIMENTAL_TRANSITIVE_DEP_PRUNE=1`
- One process at a time, five interleaved runs per side

| Workload | ON mean | OFF mean | Wall-time reduction | Throughput gain |
|---|---:|---:|---:|---:|
| Dhrystone O3 GCC 12 | 3658.6 ms | 3425.0 ms | 6.38% | 6.82% |
| CoreMark O2 GCC 13.2 | 6272.2 ms | 5915.2 ms | 5.69% | 6.04% |

Dhrystone completed at 409,668 cycles with full benchmark variable validation
in all runs. CoreMark completed at 1,139,377 cycles with
`crcfinal=0x6751` in all runs. The comprehensive-test output was identical
between the two builds.

## Verification

- Complete Release build and test suite with cancellation `ON`: 42/42 passed
- Complete Release build and test suite with cancellation `OFF`: 42/42 passed
- Cancellation-specific tests run only for the supported `ON` contract
- Envelope-size assertion and all receiver-side selective-cancellation tests
  run in the `OFF` configuration
