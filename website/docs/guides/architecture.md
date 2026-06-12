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
│  │  │              Execution Modes (Sequential / Barrier / Lookahead)   │  │ │
│  │  └───────────────────────────────────────────────────────────────────┘  │ │
│  └─────────────────────────────────┼───────────────────────────────────────┘ │
│                                    │                                         │
│  ┌─────────────────────────────────┴───────────────────────────────────────┐ │
│  │                    stdexec::static_thread_pool                           │ │
│  └──────────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────────┘
```

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

## Directory Structure

```
src/
├── chronon/                             # Unified namespace headers
│   └── Chronon.hpp                      # Master include (all you need!)
│
├── sender/                              # Framework root
│   ├── core/                            # Core abstractions
│   │   ├── Unit.hpp                     # Unit base class
│   │   ├── TickableUnit.hpp             # Tick-based unit interface
│   │   ├── PhasedTickableUnit.hpp       # Phase-based tick dispatch
│   │   ├── TickSimulation.hpp           # Simulation driver with stdexec
│   │   ├── TickSimulationConfig.hpp     # Configuration structure
│   │   ├── TerminationRequest.hpp       # Termination conditions
│   │   ├── CrashHandler.hpp             # Signal handler + emergency flush
│   │   └── CrashHandler.cpp             # Crash handler implementation
│   │
│   ├── port/                            # Port system
│   │   ├── Port.hpp                     # Combined InPort/OutPort header
│   │   ├── InPort.hpp                   # InPort<T>
│   │   ├── OutPort.hpp                  # OutPort<T>
│   │   ├── Connection.hpp               # Connection with delay
│   │   ├── MessageQueue.hpp             # Thread-safe timestamped queue
│   │   └── PortDirectory.hpp            # Port lookup by path
│   │
│   ├── schedule/                        # Scheduling infrastructure
│   │   ├── DependencyGraph.hpp          # Graph construction + Floyd-Warshall
│   │   ├── CycleAnalyzer.hpp            # Tarjan SCC + Johnson's cycle detection
│   │   ├── WeightedPartitioner.hpp      # Cost-aware graph partitioner
│   │   ├── TickCostProfiler.hpp         # Per-unit tick cost measurement
│   │   ├── PlatformBenchmark.hpp        # Atomic sync cost measurement
│   │   ├── CostProfileCache.hpp         # Cached profiling results
│   │   ├── SimulatedAnnealingPartitioner.hpp  # SA-based partitioner
│   │   └── SchedulerTimelineTrace.hpp   # Scheduler timeline recording (Perfetto)
│   │
│   ├── util/                            # Utilities
│   │   ├── Graph.hpp                    # Graph algorithms
│   │   ├── StageReg.hpp                 # Stage register (multi-pipe)
│   │   ├── SingleStageReg.hpp           # Stage register (single entry)
│   │   ├── StagePipeline.hpp            # Multi-stage pipeline abstraction
│   │   ├── StageForward.hpp             # Stage forwarding utilities
│   │   ├── PipelinePhase.hpp            # Phase tag types
│   │   ├── PriorityArbiter.hpp          # N-pipe priority arbiter
│   │   └── VersionedRegister.hpp        # Lock-free temporal register
│   │
│   ├── config/                          # Configuration
│   │   ├── SenderConfigLoader.hpp       # YAML parsing
│   │   ├── SenderSimulationBuilder.hpp  # Build orchestration
│   │   ├── SenderUnitConfig.hpp         # Per-unit YAML config structs
│   │   └── YAMLOverride.hpp             # CLI -p override support
│   │
│   ├── factory/                         # Factory system
│   │   └── SenderFactory.hpp            # Unit factories + auto-registration
│   │
│   └── app/                             # Application support
│       ├── SimulationApp.hpp            # Unified entry point
│       └── SimulationApp.cpp
│
├── observe/                             # Observability system
│   ├── Observation.hpp                  # Preferred include
│   ├── Counter.hpp                      # Counter storage
│   ├── LocalCounter.hpp                 # Per-unit Counter class
│   ├── DerivedCounter.hpp               # Computed counters (hit rate, IPC)
│   ├── FormatRegistry.hpp               # Pre-registered format strings
│   ├── ObservableUnit.hpp               # Mixin for units
│   ├── ObservationContext.hpp           # Per-unit observation context
│   ├── ObservationManager.hpp           # Central coordinator
│   ├── ObservationBackend.hpp           # Backend thread + output
│   ├── ObservationQueue.hpp             # Lock-free SPSC queue
│   ├── ObservationFilter.hpp            # Category + temporal filtering
│   ├── PerfettoTraceWriter.hpp          # Minimal Perfetto protobuf writer (.pftrace)
│   ├── TimelineData.hpp                 # Recorded timeline slice storage
│   ├── ReorderBuffer.hpp                # Cycle-ordered event reordering
│   ├── SPSCQueue.hpp                    # SPSC ring buffer
│   ├── ThreadContext.hpp                # Per-thread observation state
│   └── ThreadContextManager.hpp         # Thread context registry
│
├── tree/                                # TreeNode hierarchy
│   └── TreeNode.hpp                     # Tree structure
│
└── params/                              # Parameter system
    ├── Param.hpp                        # Self-registering parameters
    ├── ParameterSet.hpp                 # Parameter collections
    └── UnitConstructorMacros.hpp        # CHRONON_UNIT_CONSTRUCTOR macro
```

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
