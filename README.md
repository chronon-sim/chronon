# Chronon

**High-Performance Tick-Based Simulation Framework**

Chronon is a fast, multithreaded simulation framework designed for CPU microarchitecture modeling. Named after the hypothetical quantum of time, Chronon provides the building blocks for cycle-accurate simulation with automatic parallelization support.

Written in C++20; stdexec powers parallel execution. See the [performance guide](website/docs/guides/performance.md) for workload-specific measurements.

## Features

- **Tick-Based Architecture**: Model each unit as a synchronous `tick()` state machine
- **Sender-Based Scheduling**: Modern sender/receiver pattern for parallel execution
- **Hierarchical Tree Structure**: Organize simulation units in a flexible tree hierarchy
- **Automatic Port System**: Automatic port registration with connection delays, backpressure, and queue optimization
- **Automatic Parallelization**: Dependency-driven lookahead scheduling with `stdexec::static_thread_pool`
- **Unified Observability**: Macro-free API with automatic ID assignment (counters, traces, logs)
- **YAML Configuration**: Factory-driven unit instantiation from configuration files
- **SimulationApp**: Unified entry point with built-in CLI, YAML overrides, and observation lifecycle
- **Pipeline Utilities**: StageReg, SingleStageReg, and StagePipeline for efficient pipeline modeling

## Basic Usage

<!-- quickstart:begin -->
```cpp
#include "chronon/Simulation.hpp"

using namespace chronon;

class Producer : public TickableUnit {
public:
    OutPort<int> out{this, "out", SendRate{1}};

    Producer() : TickableUnit("producer") {}

    void tick() override {
        if (next_ < 100 && out.send(next_)) ++next_;
    }

private:
    int next_ = 0;
};

class Consumer : public TickableUnit {
public:
    InPort<int> in{this, "in", QueueDepth{16}};
    int sum = 0;

    Consumer() : TickableUnit("consumer") {}

    void tick() override {
        if (auto value = in.tryReceive()) sum += *value;
    }
};

int main() {
    TickSimulationConfig config;
    config.num_threads = 2;
    config.setExecutionPolicy(ExecutionPolicy::Auto);

    TickSimulation sim(config);
    auto* producer = sim.createUnit<Producer>();
    auto* consumer = sim.createUnit<Consumer>();
    sim.connect(producer->out, consumer->in, 1);

    sim.initialize();
    sim.run(101);
    sim.finalize();
    return consumer->sum == 4950 ? 0 : 1;
}
```
<!-- quickstart:end -->

The example is maintained in [`examples/quickstart.cpp`](examples/quickstart.cpp) and checked by CTest.
Use `chronon/Simulation.hpp` for model code, `chronon/Observation.hpp` for counters and traces,
and `chronon/Application.hpp` for YAML applications. `chronon/Chronon.hpp` remains the full umbrella.

## Quick Start

```bash
# GCC (default)
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
ctest --output-on-failure

# Clang
mkdir build-clang && cd build-clang
cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang-20 -DCMAKE_CXX_COMPILER=clang++-20
make -j$(nproc)
ctest --output-on-failure
```

## Use Chronon from another project

Chronon supports `add_subdirectory`, CPM, and an installed CMake package. All
three expose `chronon::core` and `chronon::observe`.

With [CPM.cmake](https://github.com/cpm-cmake/CPM.cmake) loaded in the parent project:

```cmake
CPMAddPackage(
    NAME chronon
    GITHUB_REPOSITORY chronon-sim/chronon
    GIT_TAG main # Pin a release tag or full commit for reproducible builds.
    OPTIONS "CHRONON_BUILD_TESTS OFF" "CHRONON_BUILD_BENCHMARKS OFF"
)
target_link_libraries(my_sim PRIVATE chronon::core)
```

Set `CPM_SOURCE_CACHE` to an absolute shared cache directory to reuse downloads
across build trees. Use `CPM_chronon_SOURCE` to develop against a local checkout;
CPM applies this override before its download/cache lookup:

```bash
cmake -S . -B build -DCPM_SOURCE_CACHE="$HOME/.cache/CPM"
cmake -S . -B build -DCPM_chronon_SOURCE=/absolute/path/to/chronon
```

Chronon reuses the parent's CPM instance and compatible dependencies already
registered with CPM, including a pre-downloaded Perfetto SDK. Nested CPM
consumers are supported. Use the dependency versions specified by Chronon's
CMakeLists when preloading dependencies; arbitrary version combinations are
not a compatibility guarantee.

For installation, build and install Chronon before configuring the consumer:

```bash
cmake -S . -B build-install -DCMAKE_BUILD_TYPE=Release -DCHRONON_INSTALL=ON
cmake --build build-install -j
cmake --install build-install --prefix /absolute/path/to/chronon-install
```

```cmake
find_package(chronon CONFIG REQUIRED)
target_link_libraries(my_sim PRIVATE chronon::core)
```

Configure the consumer with `-DCMAKE_PREFIX_PATH=/absolute/path/to/chronon-install`.
Installation honors `CMAKE_INSTALL_LIBDIR`, `CMAKE_INSTALL_INCLUDEDIR`, and
`CMAKE_INSTALL_BINDIR` from GNUInstallDirs. Keep these paths relative for
`--prefix` and relocation support. For a custom library directory that CMake
does not search automatically, set `chronon_DIR` to
`<prefix>/<libdir>/cmake/chronon` and `stdexec_DIR` to
`<prefix>/<libdir>/cmake/stdexec`. System dependencies (yaml-cpp, fmt, zlib and
threads) must also be available to the consumer.

## YAML-Driven Simulation with SimulationApp

For YAML-driven simulations, use `SimulationApp` for a minimal entry point with full CLI support:

```cpp
#include "chronon/Application.hpp"

int main(int argc, char* argv[]) {
    return chronon::SimulationApp("CPU Pipeline Simulator")
        .setDefaultConfig("cpu_pipeline.yaml")
        .setConfigSearchPaths({".", "../examples", "examples"})
        .run(argc, argv);
}
```

This provides automatic CLI support:
```bash
./my_simulator config.yaml                    # Run with config
./my_simulator --threads=4 --run-cycles=1000000  # Override settings
./my_simulator -p simulation.num_workers=8    # YAML path override
./my_simulator --no-observe                   # Disable observation
./my_simulator --help                         # Show all options
```

## Project Structure

| Location | Responsibility |
|---|---|
| `src/chronon/` | Supported entry headers |
| `src/sender/` | Simulation runtime, ports, configuration and modeling utilities |
| `src/observe/` | Counters, traces and logs |
| `src/tree/`, `src/params/`, `src/time/` | Model hierarchy, parameters and clocks |
| `examples/`, `test/` | Executable models and regression coverage |
| `website/docs/` | Guides and generated API reference |

## Documentation

Detailed design documents are available in [`website/docs/guides/`](website/docs/guides/):

| Document | Description |
|----------|-------------|
| [Architecture Overview](website/docs/guides/architecture.md) | Core sender-based architecture and component relationships |
| [Scheduling and Parallelization](website/docs/guides/scheduling.md) | Dependency analysis, lookahead scheduling, and partitioning |
| [Simulation Observability](website/docs/guides/observability.md) | Unified observability with macro-free API (counters, traces, logs) |
| [Unit, Port, and Configuration](website/docs/guides/units-and-simulation.md) | Unit lifecycle, port system, and simulation execution |
| [Configuration](website/docs/guides/configuration.md) | YAML-driven unit instantiation, parameters, and SimulationApp |
| [Scheduler Timeline](website/docs/guides/scheduler-timeline.md) | Perfetto timeline export for scheduler execution analysis |

## Dependencies

- C++20 compiler (GCC 12+, Clang 20+)
- CMake 3.25+
- stdexec (included via CPM)
- Perfetto SDK (included via CPM, for timeline.pftrace output)
- yaml-cpp (for YAML configuration)
- fmt (for observability formatting)

## License

Copyright (c) 2026 EHTech (Beijing) Co., Ltd.

This project is licensed under the **Mozilla Public License 2.0** (`MPL-2.0`).

See [LICENSE](LICENSE) for the full license text.

Third-party dependencies and their licenses are listed in [THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES).

## Contributing

Contributions are welcome! Please ensure all tests pass before submitting a pull request.
