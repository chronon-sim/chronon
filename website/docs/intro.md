---
sidebar_position: 1
sidebar_label: "Getting Started"
slug: /intro
---

# Getting Started with Chronon

Chronon is a high-performance tick-based simulation framework for CPU microarchitecture modeling, written in C++20 with stdexec-powered automatic parallelization.

## Key Features

| Feature | Description |
|---------|-------------|
| **Tick-Based Architecture** | State machine `tick()` execution model |
| **Port Communication** | Type-safe `send()` / `tryReceive()` with automatic mode selection |
| **Auto Parallelization** | Dependency-driven scheduling with stdexec thread pool |
| **Performance** | ~90+ Mcycles/sec throughput |
| **YAML Configuration** | Factory-driven unit instantiation |
| **Observability** | Counters, traces, logs with macro-free API |

## Quick Start

### Build

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
ctest --output-on-failure
```

### Minimal Example

```cpp
#include "chronon/Chronon.hpp"
using namespace chronon;

class MyUnit : public TickableUnit {
    OutPort<int> out{this, "out"};
    int count_ = 0;
public:
    MyUnit() : TickableUnit("my_unit") {}

    void tick() override {
        if (out.canSend()) out.send(count_++);
    }

    bool isCompleted() const override { return count_ >= 100; }
};

int main() {
    TickSimulation sim;
    sim.createUnit<MyUnit>();
    sim.initialize();
    sim.run(1000);
}
```

### Producer-Consumer with Observability

```cpp
#include "chronon/Chronon.hpp"
using namespace chronon;

inline const auto DATA_FLOW = Category<"data_flow", "Data flow events">{};

class Producer : public TickableUnit, public ObservableUnit {
    OutPort<int> out{this, "out"};
    Counter produced_{this, "produced", "Items produced"};
    int value_ = 0;
public:
    Producer() : TickableUnit("producer") {}
    bool isCompleted() const override { return value_ >= 1000; }

    void tick() override {
        if (out.canSend()) {
            out.send(value_++);
            ++produced_;
            trace<"Produced: {}">(DATA_FLOW, value_);
        }
    }
};

class Consumer : public TickableUnit, public ObservableUnit {
    InPort<int> in{this, "in"};
    Counter consumed_{this, "consumed", "Items consumed"};
public:
    Consumer() : TickableUnit("consumer") {}

    void tick() override {
        if (auto value = in.tryReceive(localCycle())) {
            ++consumed_;
        }
    }
};

int main() {
    TickSimulationConfig config;
    config.num_threads = 8;
    config.enable_parallel = true;

    TickSimulation sim(config);
    auto* producer = sim.createUnit<Producer>();
    auto* consumer = sim.createUnit<Consumer>();
    sim.connect(producer->out, consumer->in, 1);

    sim.initialize();
    sim.run(2000);
}
```

### YAML-Driven Simulation

```cpp
#include "chronon/Chronon.hpp"

int main(int argc, char* argv[]) {
    return chronon::SimulationApp("My Simulator")
        .setDefaultConfig("config.yaml")
        .run(argc, argv);
}
```

## Requirements

- C++20 compiler (GCC 12+, Clang 20+)
- CMake 3.25+
- stdexec and the Perfetto SDK (downloaded via CPM during CMake configuration)
- yaml-cpp (provided by your system/package manager)

## Next Steps

- [Architecture Overview](guides/architecture) — understand the component structure
- [Units and Simulation](guides/units-and-simulation) — deep dive into the execution model
- [Port System](guides/port-system) — learn about inter-unit communication
- [API Reference](/docs/api/) — browse the C++ API documentation
