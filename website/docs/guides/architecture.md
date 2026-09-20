---
sidebar_label: "Architecture Overview"
---

# Architecture Overview

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              TickSimulation                                   │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                         User Space                                      │ │
│  │                                                                         │ │
│  │   ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐            │ │
│  │   │ Unit A  │───►│ Unit B  │───►│ Unit C  │───►│ Unit D  │            │ │
│  │   │ tick()  │    │ tick()  │    │ tick()  │    │ tick()  │            │ │
│  │   └─────────┘    └─────────┘    └─────────┘    └─────────┘            │ │
│  │        │              │              │              │                  │ │
│  │        └──────────────┴──────────────┴──────────────┘                  │ │
│  │                              │                                          │ │
│  │                    Ports (InPort/OutPort)                              │ │
│  │                                                                         │ │
│  └─────────────────────────────────┬──────────────────────────────────────┘ │
│                                    │                                         │
│  ┌─────────────────────────────────┴──────────────────────────────────────┐ │
│  │                       Framework Core                                    │ │
│  │                                                                         │ │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐     │ │
│  │  │ Dependency       │  │ Cycle            │  │ Weighted         │     │ │
│  │  │ Graph Builder    │──│ Analyzer         │──│ Partitioner      │     │ │
│  │  └──────────────────┘  └──────────────────┘  └──────────────────┘     │ │
│  │                                 │                                       │ │
│  │  ┌──────────────────────────────┴───────────────────────────────────┐  │ │
│  │  │              Execution Paths (Sequential / Epoch-Free)            │  │ │
│  │  └───────────────────────────────────────────────────────────────────┘  │ │
│  └─────────────────────────────────┼───────────────────────────────────────┘ │
│                                    │                                         │
│  ┌─────────────────────────────────┴───────────────────────────────────────┐ │
│  │                    stdexec::static_thread_pool                           │ │
│  └──────────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────────┘
```

See [API Contracts and Migration](api-contracts.md) for the public/internal
boundary, ownership rules and capability matrix.

## Component Relationships

```
                            TickSimulation
                                    │
                ┌───────────────────┼───────────────────┐
                │                   │                   │
                ▼                   ▼                   ▼
          Unit Registry      Connection Registry    Partitioner
                │                   │                   │
                │                   ▼                   │
                │           DependencyGraph ◄──────────┘
                │                   │
                │                   ▼
                │           CycleAnalyzer
                │                   │
                │         ┌─────────┼─────────┐
                │         ▼         ▼         ▼
                │    Independent  Loose    Tight
                │      Groups    Cycles   Cycles
                │         │         │         │
                │         └─────────┼─────────┘
                │                   ▼
                │         WeightedPartitioner
                │                   │
                └───────────────────┼───────────────────┐
                                    ▼                   │
                   stdexec::static_thread_pool  ◄───────┘
```

## Source map

Start model code with the [public entry headers](../api/). Follow this map when
working on the framework; paths are relative to `src/`.

| Responsibility | Location |
|---|---|
| Model, observation and application entry points | `chronon/` |
| Unit lifecycle and simulation execution | `sender/core/` |
| Typed ports, connections, queues and CDC | `sender/port/` |
| Dependency analysis, partitioning and profiling | `sender/schedule/` |
| Pipeline registers, phases and arbitration | `sender/util/` |
| YAML loading, unit factories and CLI entry | `sender/config/`, `sender/factory/`, `sender/app/` |
| Counters, timeline events, logs and output | `observe/` |
| Unit hierarchy and parameter declarations | `tree/`, `params/` |

### Reading TickSimulation

`TickSimulation` owns the simulation session. Its public reference groups
construction/topology, execution/termination, and inspection/adapter integration.
The implementation files below share that owner; they are not separate public
scheduler or lifecycle objects. Paths are relative to `src/sender/core/`.

| Concern | Start here |
|---|---|
| Model options and execution policy | `TickSimulationConfig.hpp` |
| Unit construction and typed connections | `TickSimulation.hpp` |
| Session lifetime, finalization and tree bindings | `TickSimulationLifecycle.cpp` |
| Initialization, execution selection and sequential ticks | `TickSimulation.cpp` |
| Single-clock progress and parallel workers | `TickSimulationParallel.cpp` |
| Clock topology, sequential clock events and CDC draining | `TickSimulationClocks.cpp` |
| Parallel clock execution and migration | `TickSimulationClockParallel.cpp`, `TickSimulationClockMigration.cpp` |
| Placement and dependency preparation | `TickSimulationPartition.cpp`, `TickSimulationClusters.cpp`, `TickSimulationDependencies.cpp` |
| Dynamic placement planning, sampling and worker coordination | `TickSimulationPlanning.cpp`, `TickSimulationDynamicSampling.cpp`, `TickSimulationDynamicRuntime.cpp`, `TickSimulationDynamicRebalance.cpp` |
| Internal progress and clock state | `TickSimulationDescriptors.hpp`, `TickSimulationClockRuntime.hpp` |

See [scheduling](scheduling.md) for execution semantics and
[API contracts](api-contracts.md) for lifecycle and ownership constraints.

## Key Components

| Component | Purpose |
|-----------|---------|
| `TickableUnit` | Base class for simulation units with `tick()` method |
| `TickSimulation` | Simulation driver with parallel execution |
| `OutPort<T>` / `InPort<T>` | Type-safe communication ports |
| `Connection<T>` | Connects ports with configurable delay |
| `DependencyGraph` | Captures unit interconnections (Floyd-Warshall all-pairs) |
| `CycleAnalyzer` | Tarjan SCC + Johnson's cycle detection and classification |
| `WeightedPartitioner` | Cost-aware graph partitioner for thread assignment |
| `ObservableUnit` | Mixin for counters, traces, logs |
| `ParameterSet` | Self-registering YAML-serializable parameters |
| `SimulationApp` | Unified CLI entry point |
| `CrashHandler` | Signal handler and emergency observer flush on crash |
| `TickException` | Exception wrapping unit name + cycle from tick() crash |
