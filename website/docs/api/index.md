---
sidebar_label: "API Reference"
sidebar_position: "1"
---

# API Reference

Auto-generated C++ API documentation from source code using Doxygen.

## Namespaces

| Namespace | Description |
|-----------|-------------|
| `chronon` | Main public API — all commonly-used types |
| `chronon::sender` | Core simulation engine internals |
| `chronon::observe` | Observability system (counters, traces, logs) |
| `chronon::params` | Parameter and configuration system |

## Core Classes

| Class | Header | Description |
|-------|--------|-------------|
| `TickableUnit` | `chronon/Unit.hpp` | Base class for tick-driven simulation units |
| `TickSimulation` | `chronon/Simulation.hpp` | Main simulation engine with parallel execution |
| `OutPort<T>` | `chronon/Port.hpp` | Output port for sending data |
| `InPort<T>` | `chronon/Port.hpp` | Input port for receiving data |
| `Connection<T>` | `chronon/Port.hpp` | Port-to-port connection with configurable delay |
| `Counter` | `chronon/Observe.hpp` | Per-unit performance counter |
| `ObservableUnit` | `chronon/Observe.hpp` | Mixin for units with observability |
| `Param<T>` | `chronon/Params.hpp` | Self-registering parameter wrapper |
| `SimulationApp` | `chronon/SimulationApp.hpp` | Unified CLI entry point |

## Single Include

```cpp
#include "chronon/Chronon.hpp"
using namespace chronon;
```

:::info
Full Doxygen-generated API documentation will appear here once the documentation build pipeline is configured. Run `scripts/build-docs.sh api` to generate.
:::
