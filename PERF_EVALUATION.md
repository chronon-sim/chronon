# Direct SPSC adapter A/B evaluation

Decision: retain; this is a clear isolated winner.

- Focused registered-edge push/pop/admission microbench: legacy adapter about
  14.4 ns/message; direct SPSC about 2.7--2.8 ns/message (about 5.2x faster).
- Differential, wraparound, discard/cancellation, and 225/225 epoch-free determinism
  coverage passed.
- Same candidate binary, gate off/on, six pairs: direct path about 5.7% faster, 6/6.
- Independent original Chronon main vs candidate, clean Nucleus `master@defdb1f`,
  CoreMark, pinned CPUs, `--no-observe`, six pairs: 6636.8 ms vs 6337.0 ms,
  about 4.5% faster, 6/6.

The runtime environment gate is for A/B and rollback; the direct path is default-on
in this branch.
