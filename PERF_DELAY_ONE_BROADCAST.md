# Delay-One Broadcast Fabric Evaluation

## Scope

This branch adds a reusable Chronon transport for a complete delay-one
broadcast bus. Payload is stored once per producer cycle; the original port
connections remain scheduler dependencies but stop carrying duplicate queue
entries.

The API is intentionally opt-in and validates the full `P x C`, delay-one
topology before mutating any connection. Publication preserves delay-one port
wakeups; consumers drain on every scheduled tick and may jump over idle cycles.
The declared dependency edges expose the finite cycle-ring headroom to bound
producer run-ahead. Bounded queue semantics, selective cancellation, and
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
| Dhrystone O3 GCC 12 | 3432.2 ms | 3158.8 ms | 7.97% | 8.66% |
| CoreMark O2 GCC 13.2 | 6416.8 ms | 5907.8 ms | 7.93% | 8.62% |

Dhrystone completed at 409,668 cycles with the full benchmark variable
validation on every run. CoreMark completed at 1,139,377 cycles with
`crcfinal=0x6751` on both sides. The clean Nucleus master comprehensive test
had identical output and cycle count with the fabric on and off; its existing
section-49 failure was present in both modes and is not attributed to this
change.

These figures were remeasured after preserving delay-one consumer wakeups and
exporting finite ring headroom to the scheduler. Reverse capacity dependencies
are added only when that headroom is tighter than the global lookahead floor.

## Verification

- Complete Chronon Release build
- Complete Chronon test suite: 43/43 passed
- Focused ordering, sparse-cycle, topology rejection, ring reuse, slow-consumer,
  activity wakeup, global cycle guards, and release/acquire concurrency coverage
- Clean Nucleus Dhrystone and CoreMark output/cycle equivalence
