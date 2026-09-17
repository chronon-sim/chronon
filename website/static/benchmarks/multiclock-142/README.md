# Issue #142 measurements

3616 fresh-process measurements. Final candidate runtime: 9a9bd5b.
Baseline runtime: 000ebb3 (7c0766d with the common benchmark/diagnostic harness).
Source: https://github.com/chronon-sim/chronon

## Reconstructing the archived sources

The original source commits are historical identifiers and need not be present
in a fresh clone after rebasing. Both trees can be reconstructed using the
patches alongside this README:

1. Fetch public ancestor `7c0766d14ee9ed10323e75b4d82d1aa6a71e650a` from
   `https://github.com/chronon-sim/chronon.git`.
2. Apply `baseline-harness.patch` to that ancestor for the baseline.
3. Apply `candidate-runtime.patch` on top of the baseline for the candidate.

| Version | Original commit | Reconstructed Git tree |
| --- | --- | --- |
| Baseline | `000ebb383ed9a75f6a495a339797b6c6df8a6a8c` | `301c12587628e51b8c50443ebdda2544dc640f72` |
| Candidate | `9a9bd5bcb916cfd7fe4797ef04077b752d6b5993` | `4ec37e3b74c7d70af5e20fb758ba63f4c1ae7890` |

These patches and the unchanged measurement archive are covered by `SHA256SUMS`.
The report's Reproduce section contains fetch, worktree, patch, build and runner
commands. The two source patches are separate downloads, not additions to the
original measurement archive.

## Measurement contents

Each measurement directory has raw.csv, runs.jsonl (exact invocation and result),
summary.json (min/median/max and resources), and metadata.json (seed, CPU topology,
affinity, binary SHA-256 and available CMake caches). Intermediate experiments
were built before their source commit; map them to the implementation commits
below. Final measurements were made from the clean 9a9bd5b tree.

The two rejected prototype patches are included for reproducibility. Apply a
patch to its listed base commit, then build the same benchmark. The readiness
trial combines ready-through caching, stable ownership and ring arithmetic;
the final implementation retains only the latter two. Allocation counts include
wrapped scalar/array C++ new and exclude aligned new, malloc and shared libraries.
Do not compare diagnostic timings as uninstrumented throughput.

Native fixtures contain text records, independent-serial-checked reference TSV,
manifest, stats and native Perfetto output. Each imports 4771 events and 2644
flows without loss or Trace Processor errors. Full throughput output files are
not bundled; recorded commands regenerate them in fresh output directories.

The repository report website/docs/guides/multiclock-scaling.md explains limitations,
rejected experiments, longer-run follow-up, initialization and recorder costs.

## Source of each candidate

- alignment: 9a9bd5b + prototypes/alignment.patch
- baseline-profile: 000ebb3
- calendar: 71eb725
- final-16: 9a9bd5b
- final-extended: 9a9bd5b
- final-long: 9a9bd5b
- final-lossy: 9a9bd5b
- final-original: 9a9bd5b
- final-profile: 9a9bd5b
- final-trace: 9a9bd5b
- groups: d3f2712
- ownership: bbda6d5
- readiness: d3f2712 + prototypes/readiness.patch
- serial: 37e6918
