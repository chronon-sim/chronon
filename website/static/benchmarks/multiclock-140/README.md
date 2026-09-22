# Multi-clock migration measurements

`measurements.tar.gz` contains `initial/{short,long,profile}` and
`final/{short,long,profile}` directories with raw CSV, per-process commands and
results, summaries and full metadata. All 780 runs passed the runner's serial
hardware-state comparisons. Three repetitions provide ranges, not confidence
intervals or causal attribution.

Baseline: freshly built main `0c8b6f2379f9c8bec09a7cbc258c8df72604b7e1`.
Initial candidate: baseline plus `initial-runtime.patch` (510 runs, seven
throughput scenarios and three profiled scenarios).
Final candidate: baseline plus `candidate-runtime.patch` (270 runs, three
representative scenarios). The final policy adds the observed host-rate gain
floor; do not attribute the initial matrix to the final policy.
Compiler: GCC 12.2, Release. No warming/model ticks before measurement.
CPU mask: `0,2,4,6,8,10,12,14`, inherited by workers without individual pinning.

**Final timing is externally contended.** Chronon builds/tests were paused, but
host inspection found two unrelated processes using about six CPUs each. They
were not stopped. Unchanged baseline timing varies drastically between windows;
these data cannot establish an isolated causal speedup. Hardware comparisons
and conservative admission behavior remain testable. See the policy document
for ranges, initialization, total time and remaining counterexamples.

Metadata records topology, binary SHA-256 and the candidate CMake cache. The
baseline binary was copied out of that same Release build before modifying
runtime sources, so its recorded cache lookup is null. The baseline is not
historical `b3bef22` or `4e97c5e`.

For each revision, build `chronon_multiclock_benchmark`. Run from the candidate:

```sh
python3 scripts/run_multiclock_benchmark.py --scaling \
  --binary CANDIDATE --baseline-binary BASELINE \
  --baseline-revision 0c8b6f2379f9c8bec09a7cbc258c8df72604b7e1 \
  --steps 20000 --threads 1,2,4 --repetitions 3 \
  --cpus 0,2,4,6,8,10,12,14 --output-dir short
```

Repeat with `--steps 80000 --output-dir long`. The final throughput matrices
also pass `--scenarios p4-d2-w0-s1,p16-d8-w4000-s1,p16-d8-w4000-s16`.
Both separate diagnostic matrices add `--profile` and that scenario selection,
and use `--steps 80000 --output-dir profile`.
