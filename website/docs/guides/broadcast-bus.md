---
sidebar_label: "BroadcastBus (Planned)"
---

# BroadcastBus Implementation

:::caution Planned Feature
`BroadcastBus` is a planned component that has not yet been implemented. This document describes the intended design. Do not attempt to use `BroadcastBus` in current code — it will not compile.
:::

## Overview

The `BroadcastBus` is a planned passive connector component for event-driven N-to-M message broadcasting in the Chronon simulation framework. It will dramatically reduce YAML configuration boilerplate for broadcast patterns.

## Architecture

### Design Principles

1. **Passive Infrastructure** - Inherits from `Unit` (not `TickableUnit`), has no tick() overhead
2. **Event-Driven Forwarding** - Uses `IPortReceiveOperation<T>` for async message delivery
3. **Zero-Cycle Latency** - When used with delay=0 connections, messages forward in same cycle
4. **Type-Safe** - Full template type checking via Chronon's port system

### Message Flow

```wavedrom
{ "signal": [
  { "name": "clk",                     "wave": "p......" },
  {},
  { "name": "sender.send()",           "wave": "x=x....", "data": ["data"] },
  { "name": "bus.in (delay=0)",        "wave": "x=x....", "data": ["fwd"] },
  { "name": "bus.out0 → recv0 (d=1)",  "wave": "x.=x...", "data": ["data"] },
  { "name": "bus.out1 → recv1 (d=1)",  "wave": "x.=x...", "data": ["data"] },
  { "name": "bus.out2 → recv2 (d=1)",  "wave": "x.=x...", "data": ["data"] }
],
  "head": { "text": "BroadcastBus: sender → bus (delay=0) → receivers (delay=1)" },
  "config": { "hscale": 2 }
}
```

### Key Components

#### BroadcastForwarder

Inner class that implements `IPortReceiveOperation<T>`. Registered with each input port as a pending receiver. When messages arrive, automatically broadcasts to all outputs.

#### Port Layout

- **Inputs**: Dynamically created as `in0`, `in1`, ..., `in{N-1}`
- **Outputs**: Dynamically created as `out0`, `out1`, ..., `out{M-1}`

## Planned Implementation Files

When implemented, `BroadcastBus` will be located at:

1. **Header**: `src/sender/util/BroadcastBus.hpp`
   - Template class `BroadcastBus<T>`
   - `BroadcastBusParams` parameter set
   - Auto-registration via `AutoRegisteredUnit<T>`

2. **Export**: `src/chronon/Util.hpp`
   - Will export `BroadcastBus` and `BroadcastBusParams` to `chronon::` namespace

## Usage

### YAML Configuration

#### 1. Define Bus Units

```yaml
unit:
    wakeup_bus:
        type: BroadcastBus
        params:
            num_inputs: 10    # Number of senders
            num_outputs: 11   # Number of receivers
            capacity: 10     # Per-input queue capacity

    flush_bus:
        type: BroadcastBus
        params:
            num_inputs: 3
            num_outputs: 24
            capacity: 3
```

#### 2. Connect Senders to Bus Inputs

```yaml
unit:
    exe0:
        type: ExecutePipe
        port:
            out_wakeup:
                to: wakeup_bus.in0
                delay: 0  # 0-cycle to bus
```

#### 3. Connect Bus Outputs to Receivers (Global Port Section)

```yaml
port:
    # Wakeup bus outputs
    - from: wakeup_bus.out0
      to: iq0.in_wakeup
      delay: 1  # 1-cycle from bus to receiver
    - from: wakeup_bus.out1
      to: iq1.in_wakeup
      delay: 1
    # ... (repeat for all outputs)
```

### C++ Direct Instantiation

```cpp
#include "chronon/Chronon.hpp"
using namespace chronon;

// Create wakeup bus
auto* wakeup_bus = sim.createUnit<BroadcastBus<WakeupSignal>>(
    10,   // num_inputs
    11,   // num_outputs
    128   // capacity
);

// Connect sender
auto* exe0 = sim.createUnit<ExecutePipe>(...);
exe0->getOutputPort("out_wakeup")->connectTo(
    wakeup_bus->getInput(0), 0);  // delay=0

// Connect receivers
auto* iq0 = sim.createUnit<IssueQueue>(...);
wakeup_bus->getOutput(0)->connectTo(
    iq0->getInputPort("in_wakeup"), 1);  // delay=1
```

## References

- Chronon sender framework: `src/sender/`
- Port system: `src/sender/port/Port.hpp`
- Factory system: `src/sender/factory/SenderFactory.hpp`
- [Port System](port-system.md) — inter-unit communication
- [Configuration](configuration.md) — YAML-driven unit instantiation
