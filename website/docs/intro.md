---
sidebar_position: 1
sidebar_label: "Getting Started"
slug: /intro
---

# Getting Started with Chronon

Chronon is a C++20 framework for cycle-accurate CPU microarchitecture simulation.
Define units with `tick()`, connect typed ports, then let the simulation advance time.
Chronon selects sequential or dependency-driven parallel execution from the configuration and topology.

## Build and run

Requirements: GCC 12+ or Clang 20+, CMake 3.25+, and the system dependencies
listed in the repository README. CMake downloads stdexec and the Perfetto SDK.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
./build/examples/chronon_quickstart
ctest --test-dir build --output-on-failure
```

## A complete model

This is the compiled `examples/quickstart.cpp` example. The producer sends values
0 through 99; the consumer receives them after a one-cycle connection delay.
Only a successful send advances producer state. The return value checks the sum.

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

`SendRate` bounds sends per unit cycle; `QueueDepth` bounds the destination FIFO.
`run()` advances time and can be called repeatedly. `finalize()` ends the session
and reports finalizer errors. See [API contracts](guides/api-contracts) for ownership
and failure behavior.

## Choose an entry point

| Header | Use |
|---|---|
| `chronon/Simulation.hpp` | Units, ports, connections, clocks and simulation |
| `chronon/Observation.hpp` | Counters, logs and timeline events |
| `chronon/Application.hpp` | Parameters, factories and YAML/CLI applications |
| `chronon/Chronon.hpp` | Full umbrella, including existing aliases and modeling utilities |

Start with [units and simulation](guides/units-and-simulation), then add
[observability](guides/observability) or [YAML configuration](guides/configuration)
when the model needs them. [Explicit clock domains](guides/multiclock-cdc) cover
multiclock models and clock-domain crossings. See the
[performance guide](guides/performance) for measurements with workload conditions,
and the [API reference](/docs/api/) for supported interfaces.
