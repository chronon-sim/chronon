# Delay-One Broadcast Fabric Evaluation

## Scope

This branch adds a reusable Chronon transport for a complete delay-one
broadcast bus. Payload is stored once per producer cycle; the original port
connections remain scheduler dependencies but stop carrying duplicate queue
entries.

The API is intentionally opt-in and validates the full `P x C`, delay-one
topology before mutating any connection. Consumers must drain once per local
cycle. Bounded queue semantics, selective cancellation, and
`OutPort::cancelInFlight()` are outside the specialization.

## Clean Nucleus A/B

- Nucleus: `master` at `defdb1f`
- Chronon baseline: `main` at `ec78cdb`
- Six pinned physical cores: `0,2,4,6,12,14`
- Release build, `--no-observe`
- `CHRONON_EXPERIMENTAL_TRANSITIVE_DEP_PRUNE=1`
- One process at a time, five interleaved runs per side
- Same Nucleus binary supports queue/fabric selection through
  `NUCLEUS_EXPERIMENTAL_WAKEUP_FABRIC`

| Workload | Queue mean | Fabric mean | Wall-time reduction | Throughput gain |
|---|---:|---:|---:|---:|
| Dhrystone O3 GCC 12 | 3491.6 ms | 3131.6 ms | 10.31% | 11.50% |
| CoreMark O2 GCC 13.2 | 6099.8 ms | 5673.8 ms | 6.98% | 7.51% |

Dhrystone completed at 409,668 cycles with the full benchmark variable
validation on every run. CoreMark completed at 1,139,377 cycles with
`crcfinal=0x6751` on both sides. The clean Nucleus master comprehensive test
had identical output and cycle count with the fabric on and off; its existing
section-49 failure was present in both modes and is not attributed to this
change.

## Verification

- Complete Chronon Release build
- Complete Chronon test suite: 43/43 passed
- Focused ordering, sparse-cycle, topology rejection, ring reuse, slow-consumer,
  skipped-cycle, and release/acquire concurrency coverage
- Clean Nucleus Dhrystone and CoreMark output/cycle equivalence
