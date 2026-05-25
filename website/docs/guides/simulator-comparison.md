---
sidebar_label: "Simulator Comparison"
---

# Chronon vs. gem5 vs. GPGPU-Sim: Scheduling & Execution Model Comparison

This document compares the scheduling and execution models of three architecture simulators: **Chronon**, **gem5**, and **GPGPU-Sim**. The goal is to highlight architectural design choices and their implications for simulation performance, scalability, and modeling fidelity.

## Executive Summary

| Aspect | Chronon | gem5 | GPGPU-Sim |
|--------|---------|------|-----------|
| **Paradigm** | Synchronous tick-based | Discrete event simulation (DES) | Cycle-driven component ticking |
| **Time advance** | Every unit ticks every cycle | Time jumps to next event | Every component ticks every cycle |
| **Parallelism** | Dataflow-driven multi-thread (stdexec) | Multi-queue event parallelism | Single-threaded |
| **Language** | C++20 | C++ / Python | C++ / CUDA |
| **Target** | CPU microarchitecture | Full-system (CPU, memory, network) | GPU (shader cores, memory) |

## 1. Execution Model

### 1.1 Chronon: Synchronous Tick-Based State Machines

Chronon uses a **synchronous, tick-based execution model** where every `TickableUnit` implements a `tick()` method that executes once per simulation cycle.

```wavedrom
{ "signal": [
  { "name": "clk",    "wave": "p........" },
  {},
  { "name": "Unit A", "wave": "=.=.=.=.=", "data": ["tick","tick","tick","tick","tick"] },
  { "name": "Unit B", "wave": "=.=.=.=.=", "data": ["tick","tick","tick","tick","tick"] },
  { "name": "Unit C", "wave": "=.=.=.=.=", "data": ["tick","tick","tick","tick","tick"] }
],
  "head": { "text": "Chronon: every unit ticks every cycle" },
  "config": { "hscale": 1.5 }
}
```

Key characteristics:
- **Deterministic**: Every unit executes every cycle in a well-defined order
- **State-machine style**: Units maintain state as member variables; `tick()` reads inputs, updates state, produces outputs
- **Explicit cycle counting**: Each unit tracks its own `localCycle()` for message timing
- **Epoch-based grouping**: Cycles are grouped into epochs (default 64 cycles) for synchronization amortization

This model closely mirrors RTL simulation (SystemVerilog `always @(posedge clk)`) but at a higher abstraction level.

### 1.2 gem5: Discrete Event Simulation (DES)

gem5 uses a **discrete event simulation** engine where simulation time only advances when events occur.

```wavedrom
{ "signal": [
  { "name": "curTick",    "wave": "x2..x3..x4......x", "data": ["100","150","300"] },
  {},
  { "name": "CPU event",  "wave": "x=..x...x=......x", "data": ["process","process"] },
  { "name": "Mem event",  "wave": "x...x=..x.......x", "data": ["process"] },
  { "name": "Bus event",  "wave": "x...x...x=..=...x", "data": ["hi-pri","lo-pri"] }
],
  "head": { "text": "gem5: time jumps between events, idle periods skipped" },
  "config": { "hscale": 1.5 }
}
```

Key characteristics:
- **Sparse execution**: Only components with pending events execute; idle components consume zero simulation time
- **Event-driven**: `SimObject`s schedule `Event` callbacks at future ticks via `schedule(event, when)`
- **Priority-ordered**: Events at the same tick execute in priority order (e.g., `CPU_Tick_Pri=50` before `Stat_Event_Pri=90`)
- **Two-level event queue**: Events organized into time bins (linked list), with in-bin priority ordering

This is efficient for systems with many idle components (e.g., large memory hierarchies) but imposes overhead for densely-active pipelines.

### 1.3 GPGPU-Sim: Cycle-Driven Component Ticking

GPGPU-Sim uses a **cycle-driven model** with **four independent clock domains** (CORE, L2, DRAM, ICNT), each ticking at its own configured frequency. A central `next_clock_domain()` function determines which domains fire on each simulation step.

```
gpgpu_sim::cycle() {
    mask = next_clock_domain();       // Which domains tick this step?

    if (mask & ICNT)  icnt_cycle();            // 1. Interconnect responses
    if (mask & DRAM)  dram_cycle();            // 2. DRAM scheduling
    if (mask & L2)    cache_cycle();           // 3. L2 cache operations
                      icnt_transfer();         // 4. Move flits through network
    if (mask & CORE) {
        for each cluster: core_cycle();        // 5. Warp scheduling + execute
        issue_block2core();                    // 6. Distribute CTAs round-robin
    }
}
```

Key characteristics:
- **Sequential component ticking**: All components execute in a fixed, hardcoded order within each cycle
- **No dependency analysis**: Execution order is determined by the order of `cycle()` calls in the main loop, not by data dependencies
- **Clock domain support**: Four independent clock domains (core, L2, DRAM, interconnect) with configurable periods — e.g., L2 at half core frequency means it ticks once every two core ticks
- **Single-threaded**: The entire simulation runs in one thread with no parallelism
- **Pluggable warp schedulers**: GTO (Greedy-Then-Oldest), LRR (Local Round Robin), RRR (Relative Round Robin), Oldest, Two-Level, SWL (Sliding Window Limiting)

## 2. Scheduling and Dependency Management

### 2.1 Chronon: Graph-Based Dependency Analysis

Chronon performs **static dependency analysis** at initialization time to determine optimal scheduling:

```
                    DependencyGraph
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
         Floyd-Warshall  Tarjan   Johnson's
         (all-pairs     (SCC      (all simple
          shortest       detection) cycles)
          path)
              │          │          │
              └──────────┼──────────┘
                         ▼
                   CycleAnalyzer
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
          Tight        Loose     Independent
          Cycles       Cycles      Groups
        (delay=0)    (delay>0)  (no connection)
              │          │          │
              └──────────┼──────────┘
                         ▼
               WeightedPartitioner
                    ┌────┼────┐
                    ▼    ▼    ▼
             Sequential Barrier  Lookahead
```

**Cycle classification** determines scheduling strategy:

| Cycle Type | Total Delay | Strategy | Example |
|------------|-------------|----------|---------|
| Tight | = 0 | Delta cycles (sequential, convergence-based) | Decode ↔ Rename with zero latency |
| Loose | > 0 | Lookahead (units run ahead within delay window) | Fetch → Decode with 3-cycle queue |
| Independent | ∞ (no path) | Fully parallel | Separate cores |

**Lookahead scheduling** allows units connected by non-zero-delay edges to advance independently:

```
A ──(delay=3)──► B ──(delay=2)──► A
Total cycle delay = 5

Safe boundaries:
  cycle_B ≤ cycle_A + 3   (B can be at most 3 cycles ahead of A)
  cycle_A ≤ cycle_B + 2   (A can be at most 2 cycles ahead of B)

Both A and B can execute in parallel within these bounds.
```

### 2.2 gem5: Implicit Event Ordering

gem5 has **no explicit dependency graph**. Causality is enforced implicitly through event timestamps:

```
Component A at tick 100:
  process() {
      // Do work
      schedule(responseEvent, curTick() + latency);  // Schedule future event
      port.sendTimingReq(packet);                     // Send to connected port
  }

Component B receives the request:
  recvTimingReq(packet) {
      schedule(processEvent, curTick() + pipeline_delay);
  }
```

- Dependencies are **dynamic**: created at runtime when one component schedules events or sends packets to another
- No static analysis; no lookahead optimization
- Event queue provides total ordering guarantee: events at earlier ticks always execute first
- When multiple events share a tick, priority constants enforce ordering

### 2.3 GPGPU-Sim: Hardcoded Execution Order

GPGPU-Sim uses a **fixed, manually-defined execution order** with no automated dependency management:

```cpp
// In gpu-sim.cc — order is hardcoded; clock domain mask selects active phases
void gpgpu_sim::cycle() {
    int mask = next_clock_domain();  // Bitmask: CORE=0x01, L2=0x02, DRAM=0x04, ICNT=0x08

    if (mask & ICNT) {
        // Phase 1: Deliver memory responses to cores
        icnt_cycle();
    }
    if (mask & DRAM) {
        // Phase 2: DRAM scheduling (tRCD/tCAS/tRP timing, bank conflicts)
        for (auto& part : m_memory_partition_unit)
            part->dram_cycle();
    }
    if (mask & L2) {
        // Phase 3: L2 cache hit/miss processing
        for (auto& sub : m_memory_sub_partition)
            sub->cache_cycle(gpu_sim_cycle);
    }
    icnt_transfer();  // Phase 4: Advance network-on-chip flits

    if (mask & CORE) {
        // Phase 5: Shader cores — warp scheduling, issue, execute, writeback
        for (auto& cluster : m_cluster)
            cluster->core_cycle();
        issue_block2core();  // Phase 6: Round-robin CTA distribution
    }
}
```

- Execution order is part of the model: data flows from ICNT responses → DRAM → L2 → ICNT → cores, changing order affects correctness
- No automatic dependency detection or optimization
- Adding new components requires manual integration into the cycle loop and careful ordering
- Each `core_cycle()` triggers the pluggable `scheduler_unit::cycle()` which calls `order_warps()` and `issue_warp()` to feed execution pipelines (SP, DP, SFU, MEM, INT, TENSOR)

## 3. Parallelization Strategy

### 3.1 Chronon: Cost-Aware Graph Partitioning

Chronon uses a sophisticated **four-phase partitioning algorithm** (WeightedPartitioner) to distribute units across threads:

```
Phase 1: LPT (Longest Processing Time first)
  → Sort units by measured tick cost, assign to lightest thread
  → 4/3-OPT approximation for multiprocessor scheduling

Phase 2: FM Refinement
  → Move units from heaviest to lightest thread (up to 5 passes)
  → Considers sync cost changes from each move

Phase 3: Pairwise Swap
  → Try swapping units between all thread pairs
  → Escapes local minima from Phase 1-2

Phase 4: Multi-Unit Relocate
  → Remove unit pairs from heaviest thread, distribute to lightest
  → Handles cases where no single move/swap improves makespan
```

**Synchronization cost model** penalizes cross-thread edges based on delay:

| Connection Delay | Sync Cost Factor | Rationale |
|-----------------|------------------|-----------|
| 0 | 100× | Same-cycle: must co-locate |
| 1 | 1× | Tight spin-waiting every cycle |
| N > 1 | 1/N | Higher delay = less frequent sync |

**Three parallel execution modes**:

| Mode | Mechanism | When Used |
|------|-----------|-----------|
| Barrier | `stdexec::bulk` with per-cycle sync | Tight connections present |
| Lookahead | Progress atomics, units advance within safe boundaries | No tight connections |
| Cluster-aware | Tight clusters on same thread, lookahead between clusters | Mixed topology |

**Dynamic rebalancing** at epoch boundaries (configurable interval, default 8192 cycles) adapts to changing workload patterns by migrating tight clusters between threads.

### 3.2 gem5: Multi-Queue Thread Parallelism

gem5 supports parallelism through **multiple event queues**, each processed by a dedicated thread:

```
Thread 0: EventQueue[0] ──serviceOne()──► process events
Thread 1: EventQueue[1] ──serviceOne()──► process events
Thread 2: EventQueue[2] ──serviceOne()──► process events
              │                │               │
              └────────────────┼───────────────┘
                          Barrier.wait()
                      (every simQuantum ticks)
```

- **Simulation quantum**: Threads synchronize at fixed intervals (`simQuantum` ticks); no thread advances beyond the quantum boundary without all others catching up
- **Cross-thread events**: Asynchronous event insertion via `asyncInsert()` with mutex protection
- **Coarse-grained**: Parallelism is at the SimObject level — entire subsystems (cores, memory controllers) are assigned to queues
- **Limited scalability**: The quantum synchronization model creates barrier overhead; typically used for multi-core simulation where each core gets its own event queue

### 3.3 GPGPU-Sim: No Parallelism

GPGPU-Sim is **entirely single-threaded**:
- All components (shader cores, interconnect, memory partitions) execute sequentially within each cycle
- No thread pool, no parallel execution, no dependency-driven scheduling
- GPU parallelism is *simulated* (multiple warps, multiple cores) but not *exploited* by the simulator
- Performance scales only by reducing model complexity or simulation scope
- The AccelSim variant adds trace-driven acceleration but the core timing simulator remains single-threaded

## 4. Inter-Component Communication

### 4.1 Chronon: Typed Ports with Delay-Based Queuing

```
OutPort<Instruction> ──(delay=3)──► InPort<Instruction>
         │                                    │
    send(instr)                      tryReceive(cycle)
         │                                    │
         ▼                                    ▼
  Connection<Instruction>             MessageQueue
  (generation tracking,          (organized by arrival
   cancellation support)          cycle = send_cycle + delay)
```

Features:
- **Type-safe**: `OutPort<T>` connects only to `InPort<T>`
- **Delay-based delivery**: Messages sent at cycle `T` arrive at cycle `T + delay`
- **Three queue modes**: SingleThread (no sync), LockFree (SPSC atomics), MultiProducer (per-producer staging)
- **Backpressure**: `canSend()` preflight checks both per-cycle capacity and destination availability
- **All-or-nothing fanout**: Multi-destination sends either all succeed or all fail
- **Message cancellation**: `cancelInFlight()` with generation tracking — stale messages dropped without queue scanning
- **Per-cycle admission**: InPort limits how many messages are accepted per cycle (independent of queue depth)

### 4.2 gem5: Packet-Based Port Protocol

```
RequestPort ◄──────────────────► ResponsePort
     │         sendTimingReq()         │
     │         sendTimingResp()        │
     │         sendAtomic()            │
     │         sendFunctional()        │
     │                                 │
     ▼                                 ▼
  CPU/Cache                      Cache/Memory
```

Three communication modes serve different simulation needs:
- **Atomic**: Instantaneous, returns latency estimate — used for fast-forwarding and warming
- **Functional**: Non-timing, used for debugging and state inspection
- **Timing**: Full cycle-accurate with backpressure (`sendTimingReq()` can return `false`, requiring retry)

gem5 ports carry `Packet` objects with command type, address, data, and metadata. The protocol is request/response oriented (not unidirectional stream-based like Chronon).

### 4.3 GPGPU-Sim: Explicit Queues and Interconnect

```
Shader Core ──► icnt_push() ──► Interconnect ──► icnt_pop() ──► Memory Partition
                                     │
                              (BookSim / custom)
                                     │
Memory Partition ──► icnt_push() ──► Interconnect ──► icnt_pop() ──► Shader Core
```

- **Interconnect-mediated**: Most inter-component communication goes through a pluggable network model (InterSim2 for full routing or LOCAL_XBAR for simplified crossbar)
- **Explicit buffers**: Components manage their own FIFO pipelines (e.g., `m_icnt_L2_queue`, `m_L2_dram_queue`, `m_dram_L2_queue`, `m_L2_icnt_queue`)
- **No type safety**: Communication via raw pointers to `mem_fetch` objects; `icnt_push(src, dst, data, size)` is untyped
- **Credit-based arbitration**: Memory sub-partitions use credit-based flow control with private/shared credits to prevent starvation on shared DRAM channels
- **Warp-level communication**: Within a shader core, instruction data flows through pipeline stages via explicit register file reads, scoreboard tracking, and `register_set` objects

## 5. Timing Model

### 5.1 Chronon: Connection Delay as Fundamental Primitive

Chronon's timing model is built on **port connection delays**:

```cpp
// Delay is specified at connection time
fetch_out.connect(&decode_in, 1);    // 1-cycle latency
decode_out.connect(&exec_in, 0);     // 0-cycle (inline, same-cycle delivery)
l1_out.connect(&l2_in, 10);          // 10-cycle latency (models bus/NoC)
```

- Delay determines both **timing semantics** and **scheduling constraints**
- `delay=0` forces same-thread execution (delta cycle convergence, like SystemVerilog)
- `delay>0` enables lookahead parallelism (units can advance independently within the delay window)
- Units see time through `localCycle()` — a monotonically increasing counter per unit

### 5.2 gem5: Event Timestamps

```cpp
// Timing through event scheduling
schedule(event, curTick() + latency);     // Absolute tick
schedule(event, clockEdge(Cycles(3)));    // Clock-domain-aware
```

- Time is global (`curTick()`) with 64-bit tick resolution
- Clock domains convert between cycles and ticks via `clockPeriod()`
- No explicit connection delays — latency is modeled by event scheduling within each component
- Supports multiple frequency domains (CPU clock, bus clock, memory clock)

### 5.3 GPGPU-Sim: Clock Domains and Pipeline Stages

```cpp
// Four clock domains with independent periods
// next_clock_domain() selects which domains tick each step
core_time  += core_period;    // Shader core clock (e.g., 1.4 GHz)
icnt_time  += icnt_period;    // Interconnect clock
dram_time  += dram_period;    // DRAM clock (e.g., 924 MHz)
l2_time    += l2_period;      // L2 cache clock

// Domain fires when its time equals the global minimum
// e.g., L2 period = 2× core period → L2 ticks every other core tick
```

- Multiple clock domains with independent frequencies; the ratio between periods determines relative tick rates
- Timing modeled through explicit pipeline stage counters within each component
- DRAM timing uses detailed models (tRCD, tCAS, tRP, bank conflicts, row buffer management)
- Execution pipelines model latency via `register_set` objects with configurable throughput
- No abstract delay primitive — latency is hardcoded in component logic

## 6. The Hardware Principle: Why `delay > 0` Enables Parallelism

Chronon's parallelization is not merely a software optimization — it directly exploits a fundamental property of synchronous digital circuits: **the sample-and-hold semantics of D flip-flops (DFFs)**.

### 6.1 DFF Sample-and-Hold in Hardware

In a synchronous digital circuit, at the clock rising edge, every DFF simultaneously samples its input and latches the result. The latched output remains **stable for the entire clock cycle**, regardless of what happens to the input after the edge:

```wavedrom
{ "signal": [
  { "name": "clk",       "wave": "p......." },
  { "name": "D (input)", "wave": "x=.=..=x", "data": ["A","B","C"] },
  { "name": "Q (latch)", "wave": "x.=.=..=", "data": ["A","B","C"] }
],
  "head": { "text": "DFF: Q samples D at posedge, holds stable until next edge" },
  "edge": [ "a+b", "c+d" ],
  "config": { "hscale": 2 }
}
```

The downstream combinational logic reads from **Q** (previous cycle's latched value), not from the live, potentially changing **D**. Two independent logic cones reading from different DFF outputs can be evaluated in **any order** — or in **parallel** — because their inputs are frozen snapshots from the previous cycle.

This is the reason RTL simulators (Verilator, VCS) can parallelize evaluation of independent `always @(posedge clk)` blocks. **Execution order is irrelevant when reads and writes are separated by a register boundary.**

### 6.2 Chronon Models DFF Boundaries as `delay`

Chronon's `delay` parameter on port connections directly corresponds to the number of pipeline register stages (DFFs) between two units:

```wavedrom
{ "signal": [
  { "name": "clk",              "wave": "p........" },
  {},
  ["Unit A",
    { "name": "A.tick()",       "wave": "=.=.=.=.=", "data": ["T-3","T-2","T-1","T","T+1"] },
    { "name": "A.send()",       "wave": "x=x......", "data": ["val"] }
  ],
  {},
  ["3× DFF",
    { "name": "DFF stage 1",    "wave": "x.=x.....", "data": ["val"] },
    { "name": "DFF stage 2",    "wave": "x..=x....", "data": ["val"] },
    { "name": "DFF stage 3",    "wave": "x...=x...", "data": ["val"] }
  ],
  {},
  ["Unit B",
    { "name": "B.tryReceive()", "wave": "x...=x...", "data": ["val"] },
    { "name": "B.tick()",       "wave": "=.=.=.=.=", "data": ["T-3","T-2","T-1","T","T+1"] }
  ]
],
  "head": { "text": "delay=3: A sends at T-3, B receives at T — 3 DFF stages of isolation" },
  "config": { "hscale": 2 }
}
```

Unit B's `tryReceive(T)` reads the value A sent at cycle T-3. This value is already "latched" in the MessageQueue. A's `tick()` at cycle T **cannot** change what B reads at cycle T, so A and B can execute in any order (parallelizable). A can run up to 3 cycles ahead of B (lookahead = delay).

For `delay = 0`, there is **no DFF between the two units** — this is pure combinational feedback:

```wavedrom
{ "signal": [
  { "name": "clk",                  "wave": "p........" },
  {},
  { "name": "A.tick() delta 0",     "wave": "x=x=x....", "data": ["run","run"] },
  { "name": "A→B (delay=0)",        "wave": "x.=x=x...", "data": ["v0","v1"] },
  { "name": "B.tick() delta 1",     "wave": "x..=x=x..", "data": ["run","run"] },
  { "name": "B→A (delay=0)",        "wave": "x...=x=x.", "data": ["v0","v1"] },
  {},
  { "name": "converged",            "wave": "0.....1.0" }
],
  "head": { "text": "delay=0: combinational loop — delta cycle convergence within one cycle" },
  "config": { "hscale": 2 }
}
```

No DFF isolation — A's output feeds B's input within the **same cycle**. Evaluation order matters, requiring delta cycle iteration to convergence (analogous to SystemVerilog `always @(*)`). These units must execute on the same thread.

### 6.3 Why gem5 and GPGPU-Sim Cannot Exploit This

Neither gem5 nor GPGPU-Sim formalizes register-stage boundaries, so neither can automatically derive which computations are independent:

| Aspect | gem5 | GPGPU-Sim |
|--------|------|-----------|
| **DFF / register boundary concept** | None. Events have timestamps and priorities, but no explicit register-stage isolation between SimObjects | None. Components share mutable state through queues with no sample-and-hold semantics |
| **What guarantees causality** | Total ordering of events by `(tick, priority)` in the EventQueue | Hardcoded `cycle()` call order: ICNT → DRAM → L2 → ICNT → Core |
| **Can you reorder execution?** | Swapping same-tick event priorities may change results, because `process()` can dynamically `schedule()` new events at the *same tick* — creating read-after-write dependencies invisible to static analysis | Swapping `core_cycle()` and `dram_cycle()` causes cores to see unprocessed DRAM responses — no isolation boundary prevents this |
| **Root cause of non-parallelizability** | Events at the same tick can create and consume other events at the same tick — there is no "latch" separating writes from reads | All components read/write shared state (queues, buffers) within the same cycle step — the execution order *is* the isolation mechanism |

### 6.4 The Key Insight

> **Chronon models the `delay` parameter as pipeline register stages (DFFs), transforming the hardware property "DFF isolation guarantees execution-order independence" into automatically derivable software parallelism. gem5 and GPGPU-Sim lack an equivalent register-boundary abstraction, so they cannot automatically determine which computations can safely execute out of order or in parallel.**

This is not an accidental optimization — it is the direct consequence of choosing a simulation model that preserves the fundamental timing semantics of synchronous digital logic.

## 7. Design Philosophy Comparison

### 7.1 Abstraction Level

```
                    Low-level                              High-level
                    (closer to RTL)                       (closer to functional)
                         │                                      │
  GPGPU-Sim ─────────────┤                                      │
  (pipeline stages,       │                                      │
   explicit buffers)      │                                      │
                          │                                      │
         Chronon ─────────┤                                      │
         (tick-based,     │                                      │
          typed ports)    │                                      │
                          │                                      │
                          │         gem5 ───────────────────────┤
                          │         (event-driven,              │
                          │          multi-mode ports)          │
```

- **GPGPU-Sim**: Closest to hardware — pipeline stages, scoreboard, register files modeled explicitly
- **Chronon**: Balanced — tick-based like RTL but with higher-level port abstractions and automatic scheduling
- **gem5**: Most flexible — event-driven model supports both fast functional and detailed timing simulation

### 7.2 Extensibility

| Dimension | Chronon | gem5 | GPGPU-Sim |
|-----------|---------|------|-----------|
| Adding a component | Implement `TickableUnit::tick()`, declare ports | Create `SimObject`, register events, implement port protocols | Add `cycle()` calls to main loop, manage buffers manually |
| Configuration | YAML with auto-registered parameters | Python scripting with SimObject parameters | Configuration file with flat key-value pairs |
| Dependency handling | Automatic via port connections | Manual via event scheduling | Manual via code ordering |
| Output modes | Automatic: counters, traces, timeline | SimObject stats, gem5 stat system | Custom printf-based statistics |

### 7.3 Performance Trade-offs

| Aspect | Chronon | gem5 | GPGPU-Sim |
|--------|---------|------|-----------|
| **Throughput** | ~90+ Mcycles/sec (minimal), ~11 Mcycles/sec (pipeline) | ~0.1-1 MIPS (detailed), ~10-100 MIPS (atomic) | ~1-10 Kcycles/sec (detailed GPU) |
| **Scaling** | Multi-thread with cost-aware partitioning | Limited multi-queue parallelism | Single-threaded only |
| **Idle efficiency** | Every unit ticks every cycle (potential waste) | Sparse — only active components consume time | Every component ticks every cycle |
| **Memory overhead** | Per-unit message queues | Per-event allocation/deallocation | Explicit per-component buffers |
| **Sync overhead** | Epoch barriers + lookahead atomics (~100 ns/epoch) | Quantum barriers + async event mutex | None (single-threaded) |

## 8. Architectural Implications

### When to Choose Each Model

**Chronon** is best suited for:
- Densely-active CPU pipeline simulation where most components are busy every cycle
- Models requiring deterministic, reproducible results across different thread counts
- Teams wanting automatic parallelization without manual event management
- Rapid prototyping with YAML-driven configuration

**gem5** is best suited for:
- Full-system simulation (OS boot, I/O devices, network)
- Studies requiring fast-forwarding (atomic mode) and detailed warmup (timing mode)
- Large memory hierarchies with many idle components
- Research requiring the extensive existing gem5 model library

**GPGPU-Sim** is best suited for:
- Detailed GPU microarchitecture studies (warp scheduling, memory coalescing)
- CUDA application performance analysis
- GPU memory system research
- Studies where single-threaded simulation speed is acceptable

### Fundamental Design Trade-offs

```
              Sparse Execution              Dense Execution
              (only active units)           (all units every cycle)
                    │                              │
                    │     gem5                      │  Chronon, GPGPU-Sim
                    │  (event-driven)              │  (tick-driven)
                    │                              │
    Advantage:     Skip idle cycles          Predictable, parallelizable
    Disadvantage:  Event overhead,           Wastes time on idle units,
                   hard to parallelize       cannot skip quiescent periods
```

```
              Static Scheduling             Dynamic Scheduling
              (predetermined order)         (runtime decisions)
                    │                              │
                    │  Chronon                     │  gem5
                    │  (dependency graph at init)  │  (events scheduled dynamically)
                    │                              │
                    │  GPGPU-Sim                   │
                    │  (hardcoded order)           │
                    │                              │
    Advantage:     Low runtime overhead,     Flexible, adapts to workload
                   enables lookahead
    Disadvantage:  Cannot adapt topology     Event queue management overhead
                   at runtime
```

## 9. Summary Table

| Feature | Chronon | gem5 | GPGPU-Sim |
|---------|---------|------|-----------|
| Execution model | Tick-based (`tick()` per unit per cycle) | Event-driven (`process()` per event) | Cycle-driven (`cycle()` per component per cycle) |
| Time representation | `localCycle()` per unit (64-bit) | Global `curTick()` (64-bit, typically ns) | `gpu_sim_cycle` global counter |
| Dependency analysis | Floyd-Warshall + Tarjan SCC + Johnson's cycles | None (implicit via event ordering) | None (hardcoded order) |
| Parallelization | stdexec thread pool + weighted partitioning | Multi-queue with quantum sync | None (single-threaded) |
| Communication | Typed ports with delay-based queues | Packet-based request/response ports | Interconnect + explicit buffers |
| Backpressure | `canSend()` preflight + per-cycle admission | `sendTimingReq()` returns false → retry | Explicit buffer full checks |
| Message cancellation | Generation-tracked `cancelInFlight()` | Event squashing / packet invalidation | Warp mask invalidation |
| Configuration | YAML + `ParameterSet` auto-registration | Python scripting + SimObject params | Config file + command-line flags |
| Clock domains | Single (uniform tick) | Multiple (clock domain crossing) | Multiple (core/DRAM/interconnect) |
| Delta cycles | Yes (zero-delay convergence) | No (events are always ≥ 1 tick apart) | No |
| Dynamic rebalancing | Yes (epoch-boundary migration) | No | No |
| Simulation modes | Tick-based only | Atomic / Timing / Functional | Cycle-accurate only |
