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
| `EventCounter` | `chronon/Observe.hpp` | Per-unit aggregate counter with optional timeline marks |
| `DerivedCounter` | `chronon/Observe.hpp` | Backend-computed metric derived from `EventCounter` snapshots |
| `ObservableUnit` | `chronon/Observe.hpp` | Mixin for units with observability |
| `TimelineLane` / `TimelineSpan` | `chronon/Observe.hpp` | Perfetto instants and occupancy spans |
| Pipeline trace primitives | `chronon/Observe.hpp` | Typed one-cycle pipeline slices for model-level `observe::pipeline` wrappers |
| `Param<T>` | `chronon/Params.hpp` | Self-registering parameter wrapper |
| `SimulationApp` | `chronon/SimulationApp.hpp` | Unified CLI entry point |

## Browse

- [**Namespaces**](Namespaces/Namespaces) — `chronon`, `chronon::sender`, `chronon::observe`, `chronon::params`
- [**Classes**](Classes/Classes) — all documented classes and structs

## Single Include

```cpp
#include "chronon/Chronon.hpp"
using namespace chronon;
```
