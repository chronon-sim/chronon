# Typed MPSC consumer dispatch A/B evaluation

Decision: retain; this is a clear isolated winner.

- Focused empty 16-connection consumer-arbitration microbench: erased dispatch
  averaged 39.66 ns/arbitration; typed dispatch 18.72 ns (about 2.12x faster).
- Queue hardening, mixed-delay determinism, and 225/225 epoch-free determinism passed.
- Same candidate binary, gate off/on, six pairs: 6426 ms vs 6216 ms, about 3.3%
  faster, 6/6.
- Independent original Chronon main vs candidate, clean Nucleus `master@defdb1f`,
  CoreMark, pinned CPUs, `--no-observe`, six pairs: 6519.8 ms vs 6306.2 ms,
  about 3.3% faster, 5/6.

The runtime environment gate is for A/B and rollback; typed dispatch is default-on
in this branch.
