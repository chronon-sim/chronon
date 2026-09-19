---
sidebar_label: "API Contracts and Migration"
---

# API Contracts and Migration

Model code defines synchronous `tick()` behavior, sends through `OutPort`, and
receives through `InPort`. `TickSimulation` owns time advancement, topology,
transport selection, and the lifetime of managed units. Use
`#include "chronon/Chronon.hpp"` for the supported umbrella API.

## Lifecycle and ownership

Construct units and connections, bind tree nodes with `sim.bindTreeNode(unit,
node)`, then initialize. The tree must outlive the simulation. The YAML builder
and `SimulationApp` arrange this lifetime automatically. Port lookup uses
`sim.portDirectory()`; separately built simulations can use identical names.

`run()` and the other advancement methods are resumable. They do not finalize
units after each segment. Call `sim.finalize()` after the last segment to receive
finalizer errors. It finalizes each successfully initialized unit exactly once
in creation order, continuing if a hook throws and reporting the first error.
Destruction makes the same best-effort attempt. A finalized simulation cannot
run again. Initialization failures cannot be retried on the same instance.

`UnitState` changes from `Created` to `Initialized` after a successful initialize
hook and to `Finalized` before the finalizer is entered. Factory instances use
the YAML instance key for `name()`, lookup, and crash diagnostics. `fullPath()`
adds the tree hierarchy. Rename or topology changes after initialization are
errors. Model code implements `tick()`; `executeTick`, `executeTickAlwaysActive`,
and `advanceIdleTick` are protected scheduler hooks.

## Execution configuration

```cpp
TickSimulationConfig config;
config.num_threads = 4;
config.setExecutionPolicy(ExecutionPolicy::Auto);
config.setPollingIntervalCycles(64);
```

```yaml
simulation:
  num_workers: 4
  execution_policy: auto
  polling_interval_cycles: 64
```

`Auto` requests epoch-free parallel execution where safety and topology permit
it, otherwise sequential execution. `Sequential` forces the reference path.
`executionPolicy()` reports the request; the simulation's resolved execution
state and `parallelFallbackReason()` describe the selected backend.

The interval controls host predicate and sequential termination polling.
It does not add a barrier or epoch to the persistent parallel workers.
YAML configuration converts through `SimulationYAMLConfig::toRuntimeConfig()`;
shared numeric and boolean defaults come from `TickSimulationConfig`.
The historical worker defaults remain distinct: YAML uses four workers and C++
uses hardware concurrency. Set the count explicitly when comparing them.
YAML declaration order determines unit order; manually assembled configurations
use declaration order followed by lexically sorted unlisted names.

## Port limits and timing

```cpp
OutPort<Data> out{this, "out", SendRate{2}};
InPort<Data> in{this, "in", QueueDepth{16}};
```

| Concept | Contract |
|---|---|
| `SendRate` / `sendRate()` | Source sends per owning unit cycle |
| `QueueDepth` / `queueDepth()` | Model-visible destination FIFO entries |
| `InPort::capacity()` | Physical storage exposed by the selected adapter |
| Connection `delay` | Same-cycle eligibility at zero, N-cycle eligibility at N |
| YAML `destination_depth` | Shared destination depth override; fan-in overrides must agree |
| YAML edge `rate` | Registered edge admission rate; distinct from source port rate |

An omitted destination depth preserves the input port's configured depth.
An explicit override retains the old `capacity` behavior. Conflicting fan-in
overrides fail before any override is applied. This avoids connection-order
dependent depth changes. Physical queues remain a scheduler implementation
choice. `tryReceive()` uses the owning unit's local cycle; the overload taking a
cycle remains available for standalone adapters and explicit queries.

Always check `send()`'s result before advancing producer state. A `canSend()`
check alone does not replace checking successful delivery.

## Observation capabilities

| Mode | Supported observation contract |
|---|---|
| Managed single-clock simulation | `ObservableUnit` gets the owning local cycle automatically |
| `SimulationApp` / YAML observation | Simulation owns an exclusive process backend session |
| Direct C++ observation | `sim.configureObservation(config)` acquires the same session; attach contexts before initialization |
| Multiple simulations without observation | Independent port directories and lifetimes |
| Overlapping managed observed sessions | Rejected before existing contexts are replaced |
| Explicit clock domains | `configureClockTrace` and `clockEvent`, using exact physical time |
| Explicit clock domains plus legacy ObservationManager | Rejected; legacy cycle timestamps cannot represent this contract |

The observed simulation finalizes units, stops the backend before unit destruction,
and releases contexts afterward. Failed YAML builds also release their session.
Do not call `reset`, `shutdown`, or `initialize` on the singleton while a managed
session owns it. The singleton remains a compatibility interface for standalone
observation clients; those clients must manage context and producer lifetimes.

## Compatibility names

| Existing spelling | Canonical use / retained behavior |
|---|---|
| `enable_parallel`, `enable_lookahead`, `enable_epoch_free_lookahead` | `setExecutionPolicy` / YAML `execution_policy`; any old false switch still forces sequential |
| `epoch_size` / `--epoch-size` | Polling interval, not an execution epoch |
| YAML `capacity` on a connection | `destination_depth`; both names may appear only with equal values |
| Integral port constructor limits | `SendRate{n}` for output, `QueueDepth{n}` for input |
| `sendImmediate()` | Alias of `send()`; it still respects the connection delay |
| `PortPolicy` values | Compatibility tags using the same selective flush engine |
| `resetSelectiveCancellation()` | Compatibility no-op; receiver-owned cancellation retires automatically |
| `PortDirectory::instance()` / one-argument `Unit::setTreeNode` | Standalone compatibility; managed code uses `sim.bindTreeNode` |

Do not mix YAML `execution_policy` with legacy execution booleans. Conflicting
polling-interval aliases are also rejected. Queue installation and progress
methods are adapter integration interfaces, not model-time operations.

## Implementation boundaries and validation

`TickSimulationLifecycle.cpp` contains cold lifetime and inspection operations;
`TickSimulationConfig.hpp` contains model configuration; runtime progress
descriptors live in `TickSimulationDescriptors.hpp`. Transport implementations
remain specialized on the existing tick path. The core still links observation
support and exposes some adapter types for compatibility; these are not a
promise of interchangeable backends or a stable binary layout.

PR validation compares immutable baseline and candidate commits using identical
workloads, compiler settings, seeds and CPU affinity. State digests must match
on every repetition. Each performance scenario must independently establish
throughput at least 99% of baseline using the stated confidence bound. Noisy
measurements fail the gate. Raw samples, executable hashes and test logs are
retained as Actions artifacts. Passing the finite test matrix establishes the
tested workloads' contract, not a universal performance claim for all models.
