# Experimental Benchmarks

This directory holds exploratory benchmark code for Chronon framework performance
work. It is intentionally not built with the default examples or tests.

Build with a Release configuration:

```bash
cmake -S . -B build_bench -DCMAKE_BUILD_TYPE=Release -DCHRONON_BUILD_BENCHMARKS=ON
cmake --build build_bench --target bench_synthetic_framework bench_boom bench_synthetic_determinism -j
```

Run examples:

```bash
cd build_bench
./bench/bench_synthetic_framework --topo=mesh --units=256 --arith=256 --fp=64
./bench/bench_boom --cores=4 --cycles=100000 --threads=1,2,4,8
./bench/bench_boom --cores=4 --cycles=100000 --timeline=chronon_timeline_boom.json
./bench/bench_synthetic_determinism
```

Treat results as benchmark-lab data, not as product examples. The synthetic
suite is useful for decomposing scheduler, port, and lookahead overhead; it is
not a replacement for trace-driven or execution-driven CPU-model evaluation.
