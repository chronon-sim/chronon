---
sidebar_label: "API Reference"
sidebar_position: "1"
---

# API Reference

Choose an entry header by task. Names below are available in `chronon`.

| Header | Primary interfaces |
|---|---|
| `chronon/Simulation.hpp` | `TickableUnit`, `TickSimulation`, `TickSimulationConfig`, `ExecutionPolicy`, `OutPort<T>`, `InPort<T>`, `Connection<T>`, `SendRate`, `QueueDepth`; explicit clocks and CDC |
| `chronon/Observation.hpp` | `ObservableUnit`, `EventCounter`, `DerivedCounter`, `Category`, `TimelineLane`, `TimelineSpan` and structured event arguments |
| `chronon/Application.hpp` | `Param<T>`, `ParameterSet`, `AutoRegisteredUnit`, `SimulationBuilder`, `SimulationApp` |
| `chronon/Chronon.hpp` | Full umbrella, including pipeline utilities, adapter interfaces and historical aliases |

The focused headers are conveniences over the same types, not alternate APIs or
independent link libraries. Continue linking `chronon::core`; current template and
class layouts still require internal headers and observation dependencies.
Existing umbrella includes and qualified subnamespace names remain valid.

## Model, extension and implementation boundaries

**Model code** defines `tick()`, sends and receives through ports, configures the
simulation, and advances or finalizes it. Start with the [quickstart](../intro).
Use `TickSimulation` and `TickSimulationConfig` consistently in new model code;
`Simulation` and `SimulationConfig` remain umbrella aliases for existing clients.

**Extension code** can integrate factories, tree bindings, custom transport
adapters and placement inspection. `PortDirectory`, `PortBindingRegistry`,
`registerConnection()` and the queue-installation methods serve that layer;
they are not required to build an ordinary model. Their ownership and phase
constraints are described in [API contracts](../guides/api-contracts).

**Implementation code** includes scheduler progress records, invocation scratch,
clock-runtime records and wait policies. Installed internal headers support C++
templates and class definitions; installation alone does not promise a stable
extension API or binary layout. These implementation files are excluded from the
default generated reference.

## Browse generated declarations

The API sidebar lists namespaces and classes when documentation is generated
from the current source revision.

The generated reference includes advanced interfaces; the entry table above
identifies the recommended starting points. Legacy option behavior is documented
in [Compatibility names](../guides/api-contracts#compatibility-names).
