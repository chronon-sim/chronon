# Chronon benchmarks

The small queue benchmarks in this directory isolate individual transport hot
paths. `chronon_representative_workload_benchmark` complements them with a full
`TickSimulation`: variable-cost units execute real memory-dependent work while
typed ports carry physically different payload sizes through a generated graph.

## Representative workload benchmark

The workload combines three established benchmark ideas:

- PHOLD's parameterized control of logical processes, communication rate,
  lookahead, and event population;
- Graph500's seeded scalable graph generation, untimed input generation, and
  separate correctness validation; and
- repeated, interleaved timing with median, percentiles, and coefficient of
  variation instead of selecting the best run.

The default `nucleus` profile is based on the port types in Nucleus. Its payload
classes are 8, 16, 32, 64, 144, and 256 bytes. The 144-byte class exactly models
Nucleus `InstData`; the smaller classes cover scalars, smart pointers, wakeups,
flushes, and fetched-instruction records. The default weights are explicit in
the output and can be replaced without changing the benchmark implementation.

Each unit has independent compute and memory components:

1. a per-unit baseline sampled from a lognormal-like distribution; and
2. per-cycle jitter around that baseline;
3. a separately sampled, bounded-lognormal resident footprint; and
4. a seeded memory access pattern: random read/modify/write, cache-line
   streaming, dependent pointer chasing, or a hot/cold read/modify/write mix.

The compute distributions are truncated at a configurable normal quantile
(2.58 by default) so the profile has Nucleus-like skew without an artificial
extreme tail. Generated
unit baselines and final per-cycle samples are additionally capped at 4096 work
iterations, so compounded variance cannot bypass the execution bound. Footprint
variance uses the same bounded integer-only generator and is independently
configurable. Each footprint is a real 64-byte-aligned allocation. Its contents
are initialized on that unit's first tick so default warmup gives the operating
system a worker-local first-touch opportunity. The timed work is never an empty
spin loop: every iteration performs a modeled load, and all patterns except
pointer chasing also perform a store. Hot/cold units direct the configured
fraction of accesses (90% by default) to a seeded region that is 1/4, 1/8, or
1/16 of that unit's full footprint.

Each generated channel has a typed `OutPort`, one or more destinations, a delay,
a send-rate schedule, and optional bursts. Receivers have independently varied
drain rates. Failed sends remain pending and are retried, making queue capacity
real architectural backpressure rather than packet loss. The generator creates
fan-in, fan-out, hotspots, delay-zero DAG edges, and longer-delay edges.

### Build and run

```bash
cmake -S . -B build-release \
  -DCMAKE_BUILD_TYPE=Release \
  -DCHRONON_BUILD_BENCHMARKS=ON
cmake --build build-release \
  --target chronon_representative_workload_benchmark -j$(nproc)

taskset -c 16-31 \
  ./build-release/benchmark/chronon_representative_workload_benchmark \
  --profile nucleus --seed 20260719 --threads 1,2,4,8,16 \
  --warmup 8192 --cycles 50000 --repetitions 5
```

CPU affinity is deliberately external. On hybrid or SMT machines, select a
homogeneous set of physical cores and record that set with the result. Do not
compare a run that mixes core classes with one that does not.

Profiles provide stable starting points:

| Profile | Primary stress |
| --- | --- |
| `nucleus` | mixed unit cost, topology, payload, and moderate traffic |
| `scheduler` | variable unit work without connections |
| `scheduler-floor` | port-free independent clusters for scheduler floor/progress overhead |
| `memory` | mixed locality over a large, connection-free bounded-lognormal footprint |
| `port` | high message rate with almost no unit work |
| `hotspot` | high-indegree MPSC destinations |
| `broadcast` | wide fan-out and transparent shared broadcast eligibility |
| `backpressure` | small queues, bursts, slow receivers, and reliable retry |
| `saturation` | adversarial queue saturation and strict cross-mode validation |
| `random` | seeded randomization of the main scenario parameters |

`scheduler-floor` replaces the former `examples/bench_parallel_scheduler`
program. Its Unit type owns no ports and runs only a fixed serial arithmetic
chain, while retaining the representative runner's warmup, interleaved
repetitions, robust timing statistics, and cross-worker digest checks.

### Standalone microbenchmarks

All profiling-only executables live under `benchmark/` and are enabled by
`CHRONON_BUILD_BENCHMARKS`. They intentionally complement rather than replace
the correctness tests under `test/`:

| Executable | Isolated signal |
| --- | --- |
| `chronon_direct_spsc_benchmark` | legacy/direct local cost and real two-thread DirectSPSC envelope handoff for 8/64/144/256-byte payloads |
| `chronon_mpsc_lane_frontier_benchmark` | sparse and active-lane MPSC frontier scaling |
| `chronon_typed_mpsc_dispatch_benchmark` | typed InPort empty-fan-in dispatch cost |
| `chronon_weighted_dependency_reduction_benchmark` | dependency reducer construction and retained fan-in |
| `chronon_counter_periodic_benchmark` | counter update and periodic snapshot overhead |

The two-thread SPSC cases supersede the former
`examples/bench_message_queue`; they exercise the current
`DirectSPSCQueueAdapter<PortEnvelope<T>>` production layout and admission path.

Run `--help` for individual overrides. Useful examples:

```bash
# Generate a fresh random scenario, printing the resolved replay seed.
./build-release/benchmark/chronon_representative_workload_benchmark \
  --profile random --random-seed --describe-only

# Replay three deterministic random scenarios derived from one base seed.
./build-release/benchmark/chronon_representative_workload_benchmark \
  --profile random --seed 4815162342 --scenario-count 3 --threads 1,8

# Replay only derived scenario index 2 (the benchmark prints this normalized form).
./build-release/benchmark/chronon_representative_workload_benchmark \
  --profile random --seed 4815162342 --scenario-offset 2 \
  --scenario-count 1 --describe-only

# Port-heavy 144-byte traffic only.
./build-release/benchmark/chronon_representative_workload_benchmark \
  --profile port --payload-weights 0,0,0,0,1,0 --threads 1,8

# LLC/DRAM pressure with an explicit 1 MiB median footprint and mixed locality.
./build-release/benchmark/chronon_representative_workload_benchmark \
  --profile memory --working-set 1048576 --working-set-sigma 650 \
  --memory-pattern-weights 45,20,20,15 --threads 1,8,16
```

### Reproducibility and validation

Scenario generation uses a counter-based SplitMix64 mapping and integer-only
bounded-lognormal approximation. It does not use C++ standard random
distributions, whose generation algorithms are implementation-defined. Work
and send schedules are generated before timing; the hot path only reads those
tables. Each derived `random` scenario resamples its compute, footprint,
locality, traffic, and graph parameter envelopes from its derived seed; explicit
CLI overrides are then reapplied to every scenario. Generated per-unit
footprints are summed exactly and rejected above a 256 MiB aggregate maximum
before simulation storage is allocated.
Finite input queues reserve receive scratch only for payload classes that are
actually connected to each unit. That scratch, bounded shared FIFOs, and
worst-case per-connection cross-thread transport lanes share a conservative 256
MiB aggregate cap. Worker sweeps are capped at 256 so malformed CLI input cannot
request an impractical thread pool.
Scenario sweeps, repetitions, cycle counts, drain limits, and median unit work
also have explicit execution bounds so a mistyped but syntactically valid
integer cannot create an effectively unbounded benchmark run. Lookahead and
fixed-delay overrides share the default transport ring's headroom budget, so
transport setup cannot resize every cross-thread lane from an extreme CLI value.
Unit, channel, fan-out edge, and generated schedule counts also have explicit
pre-allocation limits; `--help` reports the user-facing topology limits.

Every scenario prints:

- the resolved seed and generator version;
- a fingerprint over all generated units, footprints, memory patterns, edges,
  payload classes, delays, work schedules, and send schedules;
- a complete replay command.

The generator unit test pins a known fingerprint. Changing generated output
requires an intentional generator-version bump and fixture update.

For every timed run the benchmark also checks:

- the exact expected unit-tick count;
- message conservation, including bounded-MPSC ingress lanes;
- zero physical transport overflow; and
- an identical final state digest across repetitions and worker counts.

An equivalence failure exits nonzero. This is intentional: a faster run that
changes cycle-visible backpressure or message ordering is not a valid Chronon
performance result. `saturation` is specifically intended to find these
boundary-condition bugs.

Scenario creation, graph connection, simulation initialization, deterministic
cost placement, and warmup are excluded from wall time. Repetition order is
seeded and interleaved across worker counts. The report includes median,
10th/90th percentiles, coefficient of variation, simulated cycles/s,
unit-ticks/s, memory operations/s, messages/s, payload GiB/s, blocked-send
percentage, and speedup. `Mcycles/s` is the primary global simulation
throughput and does not scale with the number of units; `Munit-tick/s` retains
the aggregate unit-work rate for comparing scenarios with different unit
counts.

Primary design references:

- [ROSS PHOLD parameters](https://ross-org.github.io/setup/running-sim.html)
- [Fujimoto et al., *The PHOLD Benchmark: A Critical Review*](https://informs-sim.org/wsc17papers/includes/files/019.pdf)
- [Graph500 benchmark specification](https://graph500.org/?page_id=12)
- [Google Benchmark timing and repetition guidance](https://github.com/google/benchmark/blob/main/docs/user_guide.md)
- [WG21 P3791R0, reproducible random-number generation](https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2025/p3791r0.html)
- [Mytkowicz et al., *Producing Wrong Data Without Doing Anything Obviously Wrong*](https://sape.inf.usi.ch/publications/asplos09.html)
- [Kalibera and Jones, *Rigorous Benchmarking in Reasonable Time*](https://kar.kent.ac.uk/33611/)
