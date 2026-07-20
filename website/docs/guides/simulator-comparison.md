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
  { "name": "clk",    "wave": "p..." },
  {},
  { "name": "Unit A", "wave": "====", "data": ["tick","tick","tick","tick","tick"] },
  { "name": "Unit B", "wave": "====", "data": ["tick","tick","tick","tick","tick"] },
  { "name": "Unit C", "wave": "====", "data": ["tick","tick","tick","tick","tick"] },
  {}
],
  "head": { "text": "Chronon: every unit ticks every cycle" },
  "config": { "hscale": 3 }
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
  { "name": "execution_time",      "wave": "=..=.=.=..=..=.", "data": ["100","200","300","350","500","600"] },
  {},
  ["Event Queue",
    { "name": "CPU",      "wave": "2..x.2.x..x..2.", "data": ["load","ifu","alu2"], "node": "..a..f.g.....h..." },
    { "name": "Cache",    "wave": "x..3.x.x..3..x.", "data": ["evict","snoop"],           "node": "...b.c....i...." },
    { "name": "MemCtrl",  "wave": "x..x.x.4..x..x.", "data": ["forward"],                    "node": "e......d......." }
  ],
  {},
  { "name": "cycle",     "wave": "=.........=....", "data": ["1","2","3","4","5","6"] }
],
  "edge": ["a~>b", "c~>d", "e~>f", "e~>i", "g~>h"],
  "head": { "text": "gem5 DES: units schedule events for each other on one shared queue" },
  "config": { "hscale": 1.5 }
}
```

Key characteristics:
- **Sparse execution**: Only components with pending events execute; idle components consume zero simulation time
- **Event-driven**: `SimObject`s schedule `Event` callbacks at future ticks via `schedule(event, when)`
- **Priority-ordered**: Events at the same tick/cycle execute in fixed priority order (e.g., `CPU_Tick_Pri=50` before `Stat_Event_Pri=90`, `evict` before `forward`)
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
              Cluster-aware solver
                         │
                         ▼
                     Lookahead
```

**Cycle classification** determines scheduling strategy:

| Cycle Type | Total Delay | Strategy | Example |
|------------|-------------|----------|---------|
| Tight | = 0 | Invalid combinational feedback | Decode ↔ Rename with zero latency |
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

### 3.1 Chronon: Cluster-Aware Graph Partitioning

Chronon's cluster-aware partitioning distributes units across threads using the configured solver. `SA` is the default initial solver; the optional `Weighted` solver uses this four-phase algorithm:

```
Phase 1: LPT (Longest Processing Time first)
  → Sort units by unit cost, assign to lightest thread
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

**Two execution paths**:

| Mode | Mechanism | When Used |
|------|-----------|-----------|
| Sequential | Direct ordered ticks with no worker synchronization | Parallelism is disabled, not beneficial, or cannot pass the safety gate |
| Epoch-free lookahead | Cluster-aware workers advance against progress atomics within safe bounds | The topology and port headroom satisfy the epoch-free gate |

**Dynamic rebalancing** at scheduler fence points (configurable interval, default 8192 cycles) adapts to changing workload patterns by migrating tight clusters between threads.

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
- **Three queue modes**: SingleThread (no sync), LockFree (SPSC atomics), MultiProducer (direct per-Connection SPSC lanes)
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
- `delay=0` gives same-cycle delivery on acyclic paths; zero-delay feedback is rejected at initialization
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

For `delay = 0`, there is **no DFF between the two units**. Acyclic paths can be
evaluated in topological order, but feedback loops are invalid topology:

```wavedrom
{ "signal": [
  { "name": "clk",                  "wave": "p........" },
  {},
  { "name": "A.tick()",             "wave": "x=x=x....", "data": ["run","run"] },
  { "name": "A→B (delay=0)",        "wave": "x.=x=x...", "data": ["v0","v1"] },
  { "name": "B.tick()",             "wave": "x..=x=x..", "data": ["run","run"] },
  { "name": "B→A (delay=0)",        "wave": "x...=x=x.", "data": ["v0","v1"] },
  {},
  { "name": "init error",           "wave": "0.1.....0" }
],
  "head": { "text": "delay=0 feedback loop — rejected at initialization" },
  "config": { "hscale": 2 }
}
```

No DFF isolation means A's output can feed B's input within the **same cycle** on
an acyclic path. If there is feedback, evaluation order becomes part of the
observable semantics. Chronon rejects such zero-delay cycles rather than choosing
an arbitrary unit order.

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

- **GPGPU-Sim**: Closest to hardware — pipeline stages, scoreboard, register files modeled explicitly
- **Chronon**: Balanced — tick-based like RTL but with higher-level port abstractions and automatic scheduling, providing bit-accuracy for key data
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
| **Sync overhead** | Direct-predecessor progress atomics; no global or epoch barrier | Quantum barriers + async event mutex | None (single-threaded) |

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

## 9. Related Work: Parallel Architecture Simulation

Beyond gem5 and GPGPU-Sim, many projects have tackled parallel architecture simulation. This section surveys the major approaches and contrasts them with Chronon's static-analysis-driven parallelism.

### 9.1 Parallelizing gem5

Several projects add parallelism to gem5's inherently sequential event queue:

- [**parti-gem5**](https://arxiv.org/abs/2308.09445) (SAMOS 2023) parallelizes gem5's detailed timing mode by assigning each simulated core to a host thread with *temporal decoupling* — threads run independently within a configurable synchronization quantum, then synchronize at barriers. Achieves up to 42.7x speedup for a 120-core ARM MPSoC but introduces up to 15% timing error from the decoupling window.
- [**par-gem5**](https://ieeexplore.ieee.org/document/10137178/) (DATE 2023) applies the same temporal decoupling idea to gem5's atomic (fast-forward) mode, reaching ~25x speedup on 128 ARM cores.
- [**dist-gem5**](https://ieeexplore.ieee.org/document/7975287) (ISPASS 2017) takes a multi-process approach: separate gem5 instances simulate different nodes of a distributed system, communicating via conservative PDES synchronization over TCP/MPI. Merged into upstream gem5.

All three rely on *temporal decoupling* or *spatial distribution* — they tolerate timing inaccuracy in exchange for speed, but perform no static analysis of component dependencies to determine safe parallelization boundaries.

### 9.2 Simulators Designed for Parallel Execution

| Simulator | Venue | Parallelization Approach | Source |
|-----------|-------|--------------------------|--------|
| **SST** (Sandia) | [SC 2006](https://doi.org/10.1145/1188455.1188618) | Component-based conservative PDES; link latencies provide lookahead; MPI + threads | [GitHub](https://github.com/sstsimulator/sst-core) |
| **ZSim** (Stanford) | [ISCA 2013](https://doi.org/10.1145/2508148.2485963) | PIN-based bound-weave: parallel "bound" phase runs application code, sequential "weave" resolves memory timing | [GitHub](https://github.com/s5z/zsim) |
| **Sniper** (Ghent) | [SC 2011](https://doi.org/10.1145/2063384.2063454) | Interval core model — jumps between miss events instead of simulating every cycle; built on Graphite's distributed infrastructure | [GitHub](https://github.com/snipersim/snipersim) |
| **Manifold** (Georgia Tech) | [ISPASS 2014](https://doi.org/10.1109/ISPASS.2014.6844466) | Component-based PDES with MPI transparent parallelism | [GitHub](https://github.com/gtcasl/manifold) |
| **SlackSim** | [SIGARCH 2009](https://doi.org/10.1145/1577129.1577134) | Each simulated core in its own pthread; bounded time slack between cores reduces synchronization while keeping error low (~2.1%) | [GitHub](https://github.com/dlkumar/SlackSim) |
| **HORNET** (Cornell/MIT) | [IEEE TCAD 2012](https://doi.org/10.1109/TCAD.2012.2184760) | Cycle-level NoC simulator with switchable cycle-accurate and periodic-sync modes; >5x on 6 host cores | [Paper](https://cwfletcher.github.io/content/research/2012.tcad.hornet.paper.pdf) |
| **Prophet** | [IEEE TPDS 2017](https://doi.org/10.1109/TPDS.2017.2700307) | Instruction-oriented (vs. cycle-oriented): each instruction is simulated from its own perspective, enabling speculative decoupling; scales to 4096 cores | [IEEE](https://ieeexplore.ieee.org/document/7917344/) |
| **MGPUSim** | [ISCA 2019](https://doi.org/10.1145/3307650.3322230) | Go-based GPU simulator with conservative parallel event processing | [GitHub](https://github.com/sarchlab/mgpusim) |

Separately, [**FireSim**](https://github.com/firesim/firesim) ([ISCA 2018](https://doi.org/10.1109/ISCA.2018.00014)) sidesteps software simulation entirely by running cycle-exact RISC-V RTL on cloud FPGAs, achieving orders-of-magnitude speedup through hardware parallelism.

Epoch-based and sampling-based approaches form another family: [**EPSim**](https://ieeexplore.ieee.org/document/8579541/) (IEEE Access 2018) identifies execution intervals with no functional/timing interaction and runs them in parallel (12.8x on 16 cores); [**Pac-Sim**](https://dl.acm.org/doi/full/10.1145/3680548) (ACM TACO 2024) uses online predictors to sample representative regions, reaching up to 523x speedup at the cost of statistical rather than exact results.

### 9.3 How Chronon Differs

The table below compares the parallelization strategy of each major approach against Chronon:

| Technique | Used By | Tradeoff vs. Chronon |
|-----------|---------|----------------------|
| **Temporal decoupling** (run ahead within a quantum, sync at barriers) | parti-gem5, par-gem5, Simics | Introduces timing error proportional to quantum size (parti-gem5: up to 15%; SlackSim: ~2.1%); quantum is a global knob requiring manual tuning, not topology-aware |
| **Distributed multi-process PDES** (MPI between separate simulator instances) | dist-gem5, SST, Graphite, BigSim | Coarse-grained; inter-process communication overhead; designed for multi-node clusters, not intra-core pipeline parallelism |
| **Bound-weave** (parallel application execution + sequential timing resolution) | ZSim | Application-level parallelism only; the timing "weave" phase is sequential — cannot parallelize the microarchitecture model itself |
| **Interval / analytical core model** (skip cycles between miss events) | Sniper | Trades cycle-accuracy for speed; not a parallelization of the timing model but an abstraction that avoids simulating it |
| **Bounded slack** (allow cores to diverge in simulated time by a fixed window) | SlackSim | Closest to Chronon's lookahead, but slack bounds are configured manually per-experiment, not derived from topology |
| **Link-latency-based conservative PDES** | SST, Manifold | Most architecturally similar — link delays provide lookahead. But synchronization is dynamic (runtime null-message protocol), not statically analyzed; no cost-aware thread partitioning |
| **Epoch / sampling parallelism** | EPSim, Pac-Sim, LoopPoint | Orthogonal — parallelizes across time (independent intervals), not across components within a cycle. Can be combined with Chronon |
| **FPGA acceleration** | FireSim, RAMP Gold | Not software simulation; requires FPGA hardware and RTL-level models |

Among all the frameworks above, **SST** and **Manifold** are architecturally closest to Chronon — all three use component connection delays as the basis for parallel scheduling. The differences lie in *how* that delay information is exploited. The following comparison is based on SST-core source and Manifold's published algorithms.

#### Chronon vs. SST vs. Manifold

| Aspect | SST | Manifold | Chronon |
|--------|-----|----------|---------|
| **Graph analysis** | Computes a single global min-latency across all cross-partition links (`findSyncInterval`); no per-pair analysis, no cycle detection | Computes global or pairwise lookahead during link setup; no cycle detection or topological analysis | Floyd-Warshall all-pairs shortest paths + Tarjan SCC + Johnson's cycle enumeration; classifies every component pair as tight / loose / independent |
| **Zero-delay connections** | Prohibited between Components (links must have latency > 0); zero-delay interaction requires SubComponent function calls within the same Component tree | Half-tick minimum for inter-component links; intra-LP links can be zero but require same-LP placement | First-class `delay=0` connections on acyclic paths; zero-delay feedback cycles are rejected during initialization |
| **Partitioning** | 5 built-in strategies; `simple` partitioner maximizes cross-cut latency via pairwise swap; balances component *count*, not computational *cost* | Manual only — user specifies LP assignment per component in driver code | Cluster-aware solver: models per-unit compute cost + sync cost scaled by delay (100x penalty for delay=0, 1/N for delay=N); default SA solver and optional Weighted solver minimize max thread execution time |
| **Synchronization protocol** | Barrier-based with skip-ahead: `MPI_Allreduce` computes global min next-activity time, then advances by the static lookahead period; no null messages | Chandy-Misra-Bryant null-message protocol with multiple variants (basic CMB, tick-optimized, Forecast Null-Message for dynamic lookahead, LBTS barrier, quantum) | No runtime protocol. Lookahead bounds are precomputed from the dependency graph; progress tracking uses cache-line-aligned atomics. Each unit checks only its direct predecessors' progress — no global barrier or null-message exchange |
| **Lookahead exploitation** | Single global value (min cross-partition latency); all components sync at the same interval regardless of local topology | Global or pairwise; FNM variant allows runtime forecast-based dynamic lookahead per LP pair | Per-unit direct-edge lookahead: each unit independently advances based on its immediate predecessors' progress. Transitive closure penalties do not propagate — if A→B (delay=5) and B→C (delay=3), C uses 3 from B directly, not min(5,3) from the transitive path |
| **Execution model adaptation** | Fixed: always event-driven PDES regardless of topology | Fixed: always null-message PDES (choice of 5 variants, but always PDES) | Adaptive: selects Sequential when parallelism is not worthwhile or safe; otherwise uses epoch-free lookahead with cluster-aware placement |
| **Queue implementation** | Fixed per-link type (polling or callback); no adaptation to thread placement | Fixed per-link; event delivery via LP-local event queue | Topology-aware polymorphism: `InPort` auto-selects LockFreeQueue (SPSC, zero mutex) for single-source or MultiProducerQueue (per-source SPSC rings with deterministic k-way merge) for multi-source, based on thread assignment |
| **Dynamic rebalancing** | None — partitioning is one-time at initialization | None — LP assignment is fixed | Continuous: per-unit tick costs, dependency topology, and wait attribution are sampled; at scheduler fence points, candidate whole-cluster moves are scored and committed only if gain exceeds minimum |
| **Parallelism granularity** | MPI rank (inter-process) + pthreads (intra-rank); typically one simulated core or memory controller per thread | MPI rank; one LP per MPI process; typically pairs of cores+caches per LP | Shared-memory threads via stdexec thread pool; parallelism down to individual pipeline stages within a single core |
| **Configuration** | Python scripting — powerful but requires learning SST's Python module binding, ELI macros, and multi-file project structure | C++ driver code only — no declarative configuration; user writes `main()` to instantiate components, connect links, and assign LPs by hand | Declarative YAML — parameters are auto-registered via `ParameterSet`; a single `.yaml` file describes the full system topology, delays, and queue sizes |
| **Component authoring boilerplate** | Substantial: register with `SST_ELI_REGISTER_COMPONENT` macro, declare params/ports/stats via ELI macros, implement `setup()`/`finish()` lifecycle, write separate Python module for config-time visibility | Moderate: inherit from `Component`, implement `Tick()` / `Tock()`, manually register clock handler and link handlers; no macro registration but no auto-discovery either | Minimal: inherit from `TickableUnit`, implement `tick()`, declare ports as member variables — dependency analysis, scheduling, and stat collection are automatic |
| **Build & dependency setup** | Multi-repo: build sst-core first, then sst-elements, then custom element libraries as separate shared-object plugins; requires MPI and Boost | Autotools-based; requires manual `configure`/`make` with dependency on SystemC and MPI; no package manager integration | Single CMake build; all dependencies fetched via `FetchContent` or system packages; no MPI required |
| **Error diagnostics** | Runtime errors from the PDES engine (deadlocked null-message, unresolved link) can be cryptic; debugging requires understanding of MPI ranks and event queues | Minimal runtime diagnostics; errors typically manifest as segfaults or silent hangs in the null-message protocol; no built-in topology validation | Compile-time type-safe port checking; runtime diagnostics include cycle-level traces, topology visualization, dependency graph export, and descriptive error messages for misconfigured connections |
| **Documentation & community** | Active open-source community with tutorials, annual workshops (SST Micro), and Sandia support; documentation covers core well but element libraries vary | Academic project (Georgia Tech); last commit ~2016; minimal documentation beyond the ISPASS 2014 paper; effectively unmaintained | Actively developed; integrated documentation site with API reference, tutorials, and architecture guides |

#### Why these differences matter

1. **Zero-delay support enables accurate acyclic pipeline modeling.** Real CPU pipelines have combinational paths between stages (e.g., bypass networks, stall signals). SST cannot model these as links — they must be collapsed into a single Component with internal function calls, losing the compositional port abstraction. Manifold's half-tick workaround introduces artificial timing granularity. Chronon handles acyclic `delay=0` paths natively and rejects zero-delay feedback cycles that would otherwise depend on arbitrary evaluation order.

2. **Per-unit direct-edge lookahead admits more parallelism than global sync intervals.** SST's single global `max_period` is bottlenecked by the shortest cross-partition link in the entire system. A 1-cycle link between two components forces *all* components to synchronize every cycle, even if most have 10+ cycle lookahead. Chronon's per-unit progress tracking means each unit advances at its own safe rate — a tight pair syncs frequently while the rest of the system runs ahead freely.

3. **Cost-aware partitioning outperforms count-balanced or manual placement.** SST's `simple` partitioner balances component count and maximizes cross-cut latency, but ignores that a branch predictor's `tick()` costs 10x less than an OOO scheduler's. If both end up on the same thread, the imbalance is invisible to the partitioner. Manifold has no automatic partitioner at all. Chronon's cluster-aware partitioning can use supplied per-unit tick costs and a delay-scaled sync cost model to directly minimize the metric that determines simulation throughput: max thread execution time.

4. **Adaptive execution mode avoids unnecessary overhead.** A deeply pipelined single-core model with mostly `delay=0` connections has little exploitable parallelism. SST and Manifold would still run the full PDES machinery (barrier reduction or null-message exchange) for negligible benefit. Chronon detects this topology and falls back to Sequential mode — zero synchronization overhead, deterministic, and often faster than parallel execution with its attendant scheduling cost.

5. **Dynamic rebalancing compensates for phase-dependent workload shifts.** Architecture simulations exhibit phase behavior: a memory-intensive phase may bottleneck memory-side units while a compute-intensive phase shifts load to execution-side units. SST and Manifold's static partitioning cannot adapt. Chronon samples per-unit costs, dependency pressure, and wait attribution continuously, then applies whole-cluster moves at scheduler fence points to preserve tight-cycle locality while tracking workload shifts.

6. **User-friendliness determines adoption velocity.** SST's power comes at the cost of a steep learning curve: users must master Python-side ELI registration macros, multi-repo builds (sst-core → sst-elements → custom plugins), and MPI configuration before simulating a single cycle. Manifold is worse — no declarative configuration exists; the user writes a C++ `main()` that manually instantiates every component, connects every link, and assigns every LP, with no automatic partitioning or topology validation. Chronon replaces all of this with a single YAML file: parameters are auto-registered via `ParameterSet`, ports are type-checked at compile time, dependency analysis and thread partitioning are fully automatic, and the build is a single CMake invocation with no MPI dependency. The practical consequence is that adding a new pipeline stage in SST requires touching 4-5 files (C++ component, ELI macros, Python config, build scripts, and possibly a custom element `Makefile.am`); in Manifold it requires editing the driver `main()` and manually updating LP assignments; in Chronon it requires one `TickableUnit` subclass and one YAML stanza.

### 9.4 Broader Parallel Simulation Landscape

Beyond SST and Manifold, four other projects tackle subsets of the problems Chronon addresses. This section provides a detailed comparison with **P-GAS/CRAW/P** (parallel many-core simulation), **Virtual Time III** (unified conservative–optimistic synchronization), **POSE/CharmDES** (adaptive optimistic PDES on Charm++), and **DDA-DES** (data-dependence-driven event parallelism).

#### Overview

| Aspect | P-GAS/CRAW/P | Virtual Time III (UVT) | POSE/CharmDES | DDA-DES | Chronon |
|--------|-------------|----------------------|---------------|---------|---------|
| **Origin** | ICT, CAS (Beijing) | LLNL (Jefferson & Barnes) | UIUC PPL (Wilmarth & Kale) | ODU (Jensen & Leathrum) | — |
| **Key papers** | [PADS 2010](https://ieeexplore.ieee.org/document/5471655/) (P-GAS), [Euro-Par 2012](https://link.springer.com/chapter/10.1007/978-3-642-32820-6_12) (CRAW/P) | [WSC 2017](https://ieeexplore.ieee.org/document/8247832), [TOMACS 2022–2024](https://dl.acm.org/doi/10.1145/3505248) (Parts 1–3) | [ICPP 2004](https://ieeexplore.ieee.org/document/1327899/) (POSE), [PADS 2018–2019](https://experts.illinois.edu/en/publications/adaptive-methods-for-irregular-parallel-discrete-event-simulation/) (CharmROSS) | [PADS 2025](https://doi.org/10.1145/3726301.3728416) | — |
| **Domain** | Many-core NoC (64–256 cores, mesh topology) | General PDES (domain-agnostic) | General PDES (network simulation focus) | General DES (domain-agnostic) | CPU microarchitecture |
| **Execution paradigm** | Conservative PDES (null-message or quantum barrier) | Unified conservative + optimistic | Optimistic (Time Warp) with adaptive window | Serial measurement of parallelism potential | Tick-based with static lookahead |
| **Implementation maturity** | Research prototype (QMill simulator) | Theoretical framework; experiments on ROSS | Open-source library (Charm++ `src/libs/ck-libs/pose/`); commercial CharmDES product | Measurement study only; no parallel runtime | Production framework |

#### Dependency Analysis

| Aspect | P-GAS/CRAW/P | Virtual Time III | POSE/CharmDES | DDA-DES | Chronon |
|--------|-------------|-----------------|---------------|---------|---------|
| **Static topology analysis** | None — simulated mesh topology is known a priori from architecture spec; manual square-tile grouping of routers into LPs | None — UVT is a synchronization protocol, not a topology analyzer | None — "currently POSE has no way to directly specify event dependencies"; all dependencies runtime-discovered via message passing | Yes — pre-processing phase statically analyzes event graph to build DDD/IDD/EC lookup table from state-variable read/write sets | Yes — Floyd-Warshall all-pairs shortest paths + Tarjan SCC + Johnson's cycle enumeration at initialization |
| **Graph algorithms** | None | None | None | Event-graph traversal for direct data dependencies (DDD) and indirect data dependencies (IDD) through scheduling chains | Floyd-Warshall, Tarjan, Johnson's; CycleAnalyzer classifies every component pair |
| **Dependency granularity** | Per-LP (4-router groups) | Per-LP channel (senderCVT array tracks per-source lower bounds) | Per-poser (each Charm++ chare tracks its own event queue) | Per-event-type pair (lookup table for all event-type combinations) | Per-unit pair (tight/loose/independent classification) |
| **When analysis runs** | Never (manual) | Continuously (CVT updated on every message arrival) | Continuously (rollback history drives leash) | Pre-processing (static) + runtime lookup (dynamic) | Once at initialization; re-examined at rebalancing epochs |

#### Synchronization Protocol

| Aspect | P-GAS/CRAW/P | Virtual Time III | POSE/CharmDES | DDA-DES | Chronon |
|--------|-------------|-----------------|---------------|---------|---------|
| **Protocol type** | P-GAS: CMB null-message with "zero-or-one lookahead." CRAW/P: quantum barrier with dual granularity (Q=1 for routers, Q=8+ for cores) | Per-event switching: events below CVT processed conservatively (irreversible handler, no checkpointing); events above CVT processed optimistically (reversible handler, state saving + anti-messages) | Pure optimistic (Time Warp) with per-object speculative window ("time leash"): adapt4/adapt5 dynamically adjust leash size based on rollback history | N/A — no parallel execution engine; serial simulation with instrumentation | No runtime protocol — lookahead bounds precomputed from dependency graph; per-unit progress tracking via cache-line-aligned atomics |
| **Rollback support** | None (conservative only) | Yes — standard Time Warp rollback with state restoration and anti-messages for optimistic events; LLNL's [Backstroke](https://github.com/LLNL/backstroke) tool can auto-generate reverse code | Yes — anti-messages + state checkpointing; user must provide both forward and reverse event handlers | None (only proven-safe events execute) | None (deterministic; no speculation) |
| **Lookahead mechanism** | P-GAS: custom zero-or-one lookahead tied to Godson-T's REQ/ACK flow control — lookahead=0 when awaiting ACK, =1 when pair completes. CRAW/P: implicit via quantum size | Channel-based (CMB tradition): each LP maintains senderCVT array with per-source timestamp lower bounds; CVT = min(senderCVT); null messages propagate lookahead. Model author must specify lookahead values on channels | None — no lookahead computation. The analogous concept is the speculative time leash, which is an upper bound on how far ahead of GVT a poser may execute, not a lower bound on future message timestamps | Minimum scheduling delays on event-graph edges used to refine independence analysis, but not "lookahead" in the PDES sense | Per-unit direct-edge lookahead: each unit checks only its immediate predecessors' progress atomics; precomputed from connection delay graph |
| **Key invariant** | P-GAS: LP advances only when min(neighbor timestamps + lookahead) > local clock. CRAW/P: all threads barrier every Q cycles | GVT ≤ CVT ≤ TVT ≤ LVT (four control variables per LP; conservative events below CVT, optimistic between CVT and TVT) | GVT ≤ OVT ≤ OVT + timeLeash (per-poser; leash contracts on rollback, expands on success) | Ready events are those with no event conflict (EC) against all earlier events in the pending set | cycle_B ≤ cycle_A + delay(A→B) for every connected pair |

#### Zero-Delay Handling

| Aspect | P-GAS/CRAW/P | Virtual Time III | POSE/CharmDES | DDA-DES | Chronon |
|--------|-------------|-----------------|---------------|---------|---------|
| **Zero-delay support** | Not natively — P-GAS's zero-or-one lookahead is a workaround for Godson-T's zero-delay ACK protocol; CRAW/P absorbs zero-delay by forcing router threads to Q=1 (cycle-by-cycle sync) | Yes — zero-delay messages cause CVT to stall (cannot advance past current time via that channel), so events at or above current time are simply processed optimistically. "The framework naturally falls back to optimistic execution precisely where conservative execution would deadlock" | Partially — `POSE_invoke` supports offset=0 (same-timestamp delivery), but zero-delay cycles between objects cause rollback cascades; ordering of same-timestamp events requires manual sequence numbers or `DETERMINISTIC_EVENTS` flag | Yes — data-dependence analysis can identify parallelism between simultaneous events if they share no state variables | Yes for acyclic `delay=0` paths; zero-delay feedback cycles are rejected |
| **Implication** | Minimum effective latency is 1 cycle for RR links; no true combinational modeling across partition boundaries | Zero-delay degrades to optimistic execution (correct but incurs rollback overhead) | Zero-delay cycles may cause cascading rollbacks; model author bears responsibility for correct ordering | Zero-delay parallelism limited by shared state variables | Zero-delay is a scheduling constraint (same-thread), not a correctness problem |

#### Partitioning and Load Balancing

| Aspect | P-GAS/CRAW/P | Virtual Time III | POSE/CharmDES | DDA-DES | Chronon |
|--------|-------------|-----------------|---------------|---------|---------|
| **Initial placement** | Manual — topology-based square-tile grouping; CRAW/P separates cores and routers into different thread types | Not addressed — UVT is purely a synchronization protocol | Manual — user assigns integer handle per poser; Charm++ default round-robin across PEs | Not addressed — no parallel runtime | Automatic — cluster-aware solver (`SA` by default, `Weighted` optional) |
| **Cost model** | CRAW/P: per-tile simulation time measured at barriers; standard deviation across threads used to detect imbalance | None | Per-poser metrics: event count, rollback count, avg rollback offset, avg events per step. Load score: 100 if at GVT frontier, 90 if within spec window, 80 otherwise, 50 if idle. PE load = sum of local object scores | N/A | Deterministic unit costs by default, optional measured unit costs, plus delay-scaled sync cost (100× for delay=0, 1/N for delay=N); minimizes max thread execution time |
| **Rebalancing** | CRAW/P: at barrier points every T cycles, one thread redistributes tiles; interval T set dynamically to keep overhead < 2%. ALWP achieves 14–42% speedup over static | None | Every 51st GVT iteration (configurable `LB_SKIP`): centralized coordinator collects PE loads, identifies overloaded PEs, migrates posers to underloaded PEs. Communication-aware: only migrates objects whose `remoteComm > localComm`, preferentially to the PE with highest communication affinity. Migration happens during GVT quiescence | N/A | Continuous: per-unit tick costs, dependency pressure, and wait attribution sampled at runtime; at scheduler fence points (default interval 8192 cycles), candidate whole-cluster moves are scored and committed only if gain exceeds minimum |
| **Migration granularity** | Per-tile (1 core + 1 router) | N/A | Per-poser (individual Charm++ chare); leverages Charm++ object migration infrastructure | N/A | Per-tight-cluster (preserves co-located zero-delay groups) |

#### Adaptive Execution Mode

| Aspect | P-GAS/CRAW/P | Virtual Time III | POSE/CharmDES | DDA-DES | Chronon |
|--------|-------------|-----------------|---------------|---------|---------|
| **Mode selection** | Fixed: P-GAS always uses null-message; CRAW/P always uses dual-quantum barrier. The "adaptive" in ALWP/CRAW/P refers to partitioning, not synchronization mode | Per-event: each event independently processed conservatively or optimistically based on CVT comparison. The choice is topology-derived (CVT computed from channel lookahead), but the mechanism is runtime, not init-time | Per-object: each poser's time leash (adapt4/adapt5) adapts independently based on its own rollback history. Objects with frequent rollbacks get tighter leashes; objects with no rollbacks get wider ones | N/A | Per-topology: selects Sequential when parallel overhead exceeds benefit or the epoch-free safety gate rejects the topology; otherwise selects epoch-free lookahead. Dynamic rebalancing changes placement, not synchronization protocol |
| **What drives adaptation** | Workload imbalance (measured per-tile simulation time) | Lookahead availability: channels with good lookahead → conservative; channels with no/poor lookahead → optimistic | Rollback frequency: adapt4 sets leash = avg rollback offset on rollback, expands on success. adapt5 uses sliding window of 16 most recent committed event timestamp differences, discards outliers | Data-dependence structure of event types (static) | Topology structure: dependency graph connectivity and delay distribution (static); workload phase shifts (dynamic) |

#### Performance and Scale

| Aspect | P-GAS/CRAW/P | Virtual Time III | POSE/CharmDES | DDA-DES | Chronon |
|--------|-------------|-----------------|---------------|---------|---------|
| **Peak results** | P-GAS: 10.9× avg (13.6× max) on 16 cores for 64-core mesh (SPLASH-2). CRAW/P: 28–67% improvement over static partition for 256-core mesh | 40% higher event rate vs pure optimistic on PHOLD (2M LPs); strong scaling to 524K processes (dragonfly network); 504B events/sec on 1.97M cores (Blue Gene/Q, with ROSS) | POSE: "modest speedups" on fine-grained events (2–6 μs granularity). CharmROSS: 1.4–5× with dynamic load balancing; 40% higher event rate on PHOLD at 2M processes | Mean ready-event set size 1.5–110 across models (serial measurement; no parallel speedup reported) | ~90+ Mcycles/sec (minimal model), ~11 Mcycles/sec (pipeline model) |
| **Parallelism granularity** | Inter-core / inter-router (LP = group of routers or memory controllers); shared-memory only | Inter-LP (MPI + threads); tested at extreme scale (millions of processes) | Inter-poser (Charm++ chares); MPI + SMP threads; over-decomposition (many more posers than PEs) | Intra-event-set (event-level; finer than LP-level) | Intra-pipeline (down to individual pipeline stages within a single core); shared-memory threads via stdexec |
| **Target architecture simulated** | Homogeneous many-core with mesh NoC (Godson-T) | General (PHOLD, dragonfly network) | General (BigNetSim network simulation) | General DES models (ring, torus, mesh topologies) | Heterogeneous CPU pipeline (out-of-order cores, caches, memory) |

#### Limitations

| Limitation | P-GAS/CRAW/P | Virtual Time III | POSE/CharmDES | DDA-DES | Chronon |
|-----------|-------------|-----------------|---------------|---------|---------|
| **No static dependency analysis** | Yes — manual topology decomposition only; no graph algorithms for partitioning | Yes — lookahead is model-specified, not automatically derived | Yes — no event dependency specification; "POSE has no way to directly specify event dependencies" | No — this is its strength; static DDD/IDD/EC analysis on event graph | No — Floyd-Warshall + Tarjan + Johnson's |
| **No cost-aware partitioning** | Partially — CRAW/P measures per-tile time but uses simple heuristics, not formal optimization | Yes — UVT addresses synchronization only | Partially — load scoring exists but is coarse (100/90/80/50 buckets), not weighted by measured execution cost | N/A (no partitioning) | No — four-phase weighted optimization |
| **Model author burden** | Low (uses existing GAS simulator) | High — must provide both irreversible and reversible event handlers (or use Backstroke auto-generation) | High — must write anti-methods for rollback, `pup()` for checkpointing, manage `etrans.pl` translation step | Low (annotate event-graph SV read/write sets) | Low — implement `tick()`, declare ports |
| **Accuracy tradeoff** | CRAW/P: quantum-based relaxation introduces timing error (< 10% at Q=64, but up to 84% with static partition at Q=64) | Optimistic events may be rolled back — correct but wastes work proportional to rollback rate | Rollback waste; adapt4/5 aim to minimize but cannot eliminate | None (only proven-safe events execute) | None (deterministic, no speculation, no timing error) |
| **Scaling limitation** | Shared-memory only (single host); tested up to 16 host cores | Scales to millions of processes, but GVT remains a global synchronization point | GVT is a global coordination point; load balancer hard-coded to 128 PEs (`sortArray[128]`) | O(k²) pairwise comparisons for k-th event in pending set | Shared-memory only; lookahead bounded by topology |
| **Domain restriction** | Tied to Godson-T/QMill ecosystem; tested only on homogeneous mesh | Domain-agnostic but no public implementation available (theoretical/algorithmic contribution) | General-purpose but conservative strategy "NOT MAINTAINED"; primarily validated on network simulation | No parallel runtime — measurement study only (PADS 2025) | Architecture simulation (tick-based models) |

#### Why These Differences Matter for Architecture Simulation

1. **Static analysis vs. runtime discovery determines parallelization overhead.** P-GAS/CRAW/P, UVT, and POSE all discover parallelization opportunities at runtime — through null-message exchange, CVT tracking, or rollback-driven leash adjustment. This incurs per-cycle or per-event overhead that scales with system size. Chronon and DDA-DES both perform static pre-analysis (dependency graph and event-graph respectively), paying a one-time initialization cost and incurring near-zero runtime discovery overhead. For architecture simulation where the component topology is fixed throughout execution, static analysis is strictly superior: the topology does not change, so there is nothing to discover at runtime.

2. **Conservative determinism vs. optimistic speculation determines reproducibility.** UVT and POSE process events speculatively and roll back on causality violations. This means (a) wasted work proportional to rollback frequency, (b) non-deterministic execution order across runs, and (c) model authors must implement reversible event handlers — a significant correctness burden. P-GAS/CRAW/P avoids rollback but introduces timing error from quantum relaxation (up to 84% in pathological cases). Chronon and DDA-DES never speculate: Chronon's lookahead bounds are proven safe from the dependency graph; DDA-DES's ready events are proven independent from data-dependence analysis. Both guarantee bitwise-deterministic results regardless of thread count — a critical property for architecture simulation where researchers compare cycle counts across design changes.

3. **Per-unit lookahead outperforms global or per-LP synchronization.** P-GAS/CRAW/P uses a single quantum for all components of each type (Q=1 for routers, Q=8 for cores). UVT computes CVT per-LP but requires null-message propagation to advance it. POSE has no lookahead at all — its leash is a speculation bound, not a safety guarantee. Chronon computes per-unit direct-edge lookahead from the connection delay graph: each unit independently advances based on its immediate predecessors' progress. A fetch unit with a 3-cycle queue to decode advances freely while decode and rename (connected at delay=0) synchronize tightly — no global barrier or message exchange needed.

4. **Architecture-aware partitioning requires both topology and cost information.** CRAW/P's insight of separating core and router threads by type is valuable but domain-specific (assumes a mesh NoC topology). UVT and DDA-DES do not address partitioning at all. POSE's load balancer uses coarse event-count-based scoring (100/90/80/50 buckets) without considering per-event execution cost. Chronon's partition input combines topology, optional measured unit costs, and delay-scaled synchronization costs in a unified objective function, then applies topology-agnostic optimization — it works for any DAG of TickableUnits, whether modeling a pipeline, a cache hierarchy, or a NoC.

5. **DDA-DES identifies a complementary form of parallelism.** DDA-DES's ready-event discovery is orthogonal to spatial decomposition: it finds events that are data-independent regardless of which component they belong to. For architecture simulation, this suggests a potential hybrid approach — Chronon's topology-based spatial parallelism could be augmented with event-level parallelism within each thread's assigned units. However, DDA-DES currently lacks a parallel runtime and its O(k²) pairwise comparison cost at runtime may limit practical applicability for cycle-level simulation where event sets are large and event granularity is small.

6. **UVT's per-event conservative/optimistic switching is theoretically elegant but practically mismatched for cycle-accurate simulation.** UVT's four-variable framework (GVT ≤ CVT ≤ TVT ≤ LVT) provides the most general synchronization theory in the PDES literature, subsuming both conservative and optimistic as special cases. However, cycle-accurate architecture simulation has a fixed, known topology with statically-derivable lookahead on every connection — exactly the scenario where conservative execution is provably sufficient everywhere. UVT's ability to fall back to optimistic execution "where conservative would deadlock" is unnecessary when the dependency graph is fully analyzed at initialization. The overhead of maintaining four control variables per LP, exchanging null messages, and implementing reversible handlers adds complexity without benefit for this domain.

## 10. Summary Table

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
| Delta cycles | No (zero-delay feedback is rejected) | No (events are always ≥ 1 tick apart) | No |
| Dynamic rebalancing | Yes (scheduler-fence migration) | No | No |
| Simulation modes | Tick-based only | Atomic / Timing / Functional | Cycle-accurate only |
