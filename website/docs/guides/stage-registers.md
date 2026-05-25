---
sidebar_label: "Stage Registers"
---

# Stage Registers

Stage-level pipeline register abstractions with built-in write tracking. These use two-slot ping-pong buffering internally and solve three common problems in pipeline models:

1. **Ghost entries** — forgetting `retain<P>()` leaves stale data in the write slot that reappears 2 cycles later
2. **Unsafe dual-write** — external `bool[]` arrays tracking whether a stage was written are manually maintained and frequently wrong
3. **Boilerplate** — every stage forward requires ~15 lines of valid/retain/consume/set logic

## Quick Selection

| Type | Entries | Best For |
|------|---------|----------|
| `StageReg<T, N>` | N parallel pipes | Superscalar pipeline stages (e.g., 4-wide addr/data pipes) |
| `SingleStageReg<T>` | 1 | Single-entry stages (fill pipeline, snoop pipeline) |
| `StagePipeline<S...>` | Group | Batch `beginCycle` / `flushIf` / `reset` across multiple stages |

## Design Principle

Each physical pipeline stage is an independent `StageReg` member with its own write tracking:

```cpp
StageReg<Data, NUM_PIPES> a0_, a1_, a2_;
// a2_.written(pipe) — built-in tracking, no external bool arrays needed
```

This eliminates all external boolean tracking arrays that are a frequent source of ghost-entry bugs.

---

## StageReg

Per-stage pipeline register array with built-in write tracking for N parallel pipes.

### Cycle Discipline

Every `StageReg` must call `beginCycle<P>()` exactly once at the start of each tick. This clears all write slots and resets the `written_[]` array, establishing the safe D-FF default: **any pipe not explicitly written this cycle will be empty next cycle**.

```cpp
template<ValidPhase P>
void tickPipelines_() {
    a0_.beginCycle<P>();  // Clears write slots + resets tracking
    a1_.beginCycle<P>();
    a2_.beginCycle<P>();

    // Process stages downstream-first...
    processA2_<P>();
    processA1_<P>();
    processA0_<P>();
}
```

### Write Tracking

`written(pipe)` returns `true` if `write<P>()` or `retain<P>()` was called for that pipe this cycle. This replaces all external boolean arrays:

```cpp
// Before: manual tracking prone to bugs
if (a2_retained_[pipe]) {
    addr_pipe_[pipe].retain<P>(AddrStage::A1);
    a1_retained_[pipe] = true;      // Easy to forget!
    continue;
}

// After: self-tracking, impossible to forget
if (a2_.written(pipe)) {
    a1_.retain<P>(pipe);            // a1_.written(pipe) is now true
    continue;
}
```

### API

```cpp
// Cycle boundary (call once per tick per stage)
template<ValidPhase P> void beginCycle();

// Read side (stable within cycle)
template<ValidPhase P> bool valid(size_t pipe) const;
template<ValidPhase P> const T& read(size_t pipe) const;
template<ValidPhase P> T& read(size_t pipe);

// Write side (visible next cycle, at most once per pipe per cycle)
template<ValidPhase P> void write(size_t pipe, T data);   // asserts !written_[pipe]
template<ValidPhase P> void retain(size_t pipe);           // asserts !written_[pipe]
template<ValidPhase P> T consume(size_t pipe);             // does NOT set written_

// Write tracking
bool written(size_t pipe) const;

// Bulk operations
template<ValidPhase P, typename Func> size_t forEachValidConsume(Func&& fn);
template<ValidPhase P, typename Pred> size_t flushIf(Pred&& pred);
void reset();

static constexpr size_t size();
```

**Important**: `write()` and `retain()` assert that the pipe hasn't been written yet this cycle. `consume()` does NOT set `written_` — the caller forwards data via `write()` on the downstream stage.

---

## SingleStageReg

Same API as `StageReg` but for single-entry stages (no pipe index). Uses a single `bool written_` instead of an array.

```cpp
SingleStageReg<FillData> fill_t3_, fill_t4_, fill_t5_;

template<ValidPhase P>
void processFill_() {
    fill_t4_.forEachValidConsume<P>([&](auto& data) {
        fill_t5_.write<P>(data);
    });
    if (fill_t3_.valid<P>()) {
        fill_t4_.write<P>(fill_t3_.read<P>());
    }
}
```

### API

```cpp
template<ValidPhase P> void beginCycle();

template<ValidPhase P> bool valid() const;
template<ValidPhase P> const T& read() const;
template<ValidPhase P> T& read();

template<ValidPhase P> void write(T data);
template<ValidPhase P> void retain();
template<ValidPhase P> T consume();

bool written() const;

template<ValidPhase P, typename Func> size_t forEachValidConsume(Func&& fn);
template<ValidPhase P, typename Func> bool ifValidConsume(Func&& fn); // compatibility alias
void reset();
```

---

## StagePipeline

Groups multiple stage registers for batch operations. Stages can be heterogeneous (mix of `StageReg` and `SingleStageReg`).

```cpp
StageReg<AddrData, 4> a0_, a1_, a2_;
StagePipeline addr_pipe_{a0_, a1_, a2_};

template<ValidPhase P>
void tickPipelines_() {
    addr_pipe_.beginCycle<P>();     // Calls beginCycle on a0_, a1_, a2_
    // ...
    addr_pipe_.flushIf<P>(pred);   // Flush matching entries across all stages
}
```

### API

```cpp
template<typename... Stages>
class StagePipeline {
    explicit StagePipeline(Stages&... s);

    template<ValidPhase P> void beginCycle();
    template<ValidPhase P, typename Pred> void flushIf(Pred&& pred);
    void reset();
};
```

---

## StageForward Helpers

Generic forwarding templates that encapsulate the consume-from-source, write-to-destination pattern with stall detection.

### simpleForward / simpleForwardAll

Stall-aware forward: if the destination already has a write pending, retains in the source instead.

```cpp
// Forward a single pipe (returns true if forwarded, false if stalled)
simpleForward<P>(a1_, a2_, pipe);

// Forward all N pipes
simpleForwardAll<P, NUM_PIPES>(a1_, a2_);
```

Replaces this boilerplate:

```cpp
// BEFORE (15 lines per stage)
for (size_t pipe = 0; pipe < NUM_PIPES; ++pipe) {
    if (!addr_pipe_[pipe].valid<P>(AddrStage::A1)) continue;
    if (a2_retained_[pipe]) {
        addr_pipe_[pipe].retain<P>(AddrStage::A1);
        a1_retained_[pipe] = true;
        continue;
    }
    auto data = addr_pipe_[pipe].consume<P>(AddrStage::A1);
    addr_pipe_[pipe].set<P>(AddrStage::A2, data);
}

// AFTER (1 line)
simpleForwardAll<P, NUM_PIPES>(a1_, a2_);
```

### processForward

Forward with per-entry processing lambda. The lambda is called on the consumed data before writing to the destination.

```cpp
// Forward R0→R1 with RST qualification
processForward<P>(r0_, r1_, pipe, [](size_t p, auto& data) {
    data.rst_qualified = true;
});
```

### convertForward

Cross-type forward where the source and destination have different data types.

```cpp
// Forward AddrPipeData → StorePipeData
convertForward<P>(a2_, r0_, pipe, [](size_t p, auto&& addr_data) {
    return toStorePipeData(std::move(addr_data));
});
```

### API

```cpp
template<ValidPhase P, typename Src, typename Dst>
bool simpleForward(Src& src, Dst& dst, size_t pipe);

template<ValidPhase P, size_t N, typename Src, typename Dst>
void simpleForwardAll(Src& src, Dst& dst);

template<ValidPhase P, typename Src, typename Dst, typename Fn>
bool processForward(Src& src, Dst& dst, size_t pipe, Fn&& fn);

template<ValidPhase P, typename Src, typename Dst, typename Convert>
bool convertForward(Src& src, Dst& dst, size_t pipe, Convert&& convert);
```

---

## Best Practices

### 1. One StageReg Per Pipeline Stage

Each physical pipeline stage should be an independent `StageReg`:

```cpp
// GOOD: independent stages (self-tracking, ghost-safe)
StageReg<Data, NUM_PIPES> a0_, a1_, a2_;
```

### 2. Always beginCycle Before Processing

Call `beginCycle<P>()` for every `StageReg` / `SingleStageReg` **before** any stage processing logic. This ensures:

- Write slots are clear (no ghost data from 2 cycles ago)
- Write tracking is reset (no stale `written_` flags)

Use `StagePipeline` to batch this:

```cpp
StagePipeline all_stages_{a0_, a1_, a2_, d0_, d1_, d2_};

template<ValidPhase P>
void tickPipelines_() {
    all_stages_.beginCycle<P>();   // One call for all stages
    // ...
}
```

### 3. Process Downstream First

Process stages from downstream to upstream (reverse pipeline order). This ensures downstream stalls (visible via `written()`) are detected before upstream stages try to forward:

```cpp
processA2_<P>();   // May set a2_.written(pipe) via retain
processA1_<P>();   // Checks a2_.written(pipe) before forwarding
processA0_<P>();   // Checks a1_.written(pipe) before forwarding
```

### 4. Use written() for Backpressure, Not External Booleans

The `written()` method is the single source of truth for whether a stage has been claimed this cycle. It subsumes all external tracking:

| Old Pattern | New Pattern |
|-------------|-------------|
| `a2_retained_[pipe]` | `a2_.written(pipe)` |
| `a1_retained_[pipe]` | `a1_.written(pipe)` |
| `r0_forwarded_to_r1_[pipe]` | `r1_.written(pipe)` |
| `r0_retained_[pipe]` | `r0_.written(pipe)` |
| `i1_pipe_written_[pipe]` | `load_i1_.written(pipe) \|\| store_i1_.written(pipe)` |

Note that `r0_forwarded_to_r1_` and `r1_retained_` collapse into the same check: `r1_.written(pipe)`. Both conditions mean "R1's write slot is occupied", which is exactly what `written()` tracks.

### 5. consume() Does Not Set written()

`consume()` moves data out of the read slot but does NOT mark the pipe as written. This is intentional — the producer (upstream stage) consumes, then the consumer (downstream stage) writes. Only `write()` and `retain()` set `written_`.

```cpp
// Correct pattern: consume from source, write to destination
auto data = a1_.consume<P>(pipe);   // a1_.written(pipe) stays false
a2_.write<P>(pipe, data);           // a2_.written(pipe) becomes true
```

### 6. Prefer Forwarding Helpers for Simple Stages

Use `simpleForwardAll` for trivial stages that just pass data through:

```cpp
// Trivial forward (e.g., TLB continuation stage)
template<ValidPhase P>
void processA1_() {
    simpleForwardAll<P, NUM_PIPES>(a1_, a2_);
}
```

Use `processForward` when you need to modify data in-flight. Use `convertForward` for cross-type conversions (e.g., AddrPipeData → StorePipeData at A2→R0 boundary).

### 7. Side Buffers Stay Separate

Not everything is a pipeline stage. Side buffers, retry registers, and staging areas that don't follow the ping-pong discipline should remain as plain structs:

```cpp
// These are NOT pipeline stages — keep as-is
std::array<D2RetryEntry, NUM_PIPES> d2_mb_rd_retry_{};    // Side register
std::array<DataPipeData, NUM_PIPES> i1_to_d0_staging_{};  // Handoff buffer
```

---

## Migration Guide

### Step 1: Add StageReg Members

Replace array-of-arrays pipeline declarations with individual `StageReg` members:

```cpp
StageReg<Data, NUM_PIPES> stage0_, stage1_, stage2_;
```

### Step 2: Delete External Boolean Arrays

Remove all `std::array<bool, NUM_PIPES>` tracking arrays. Replace all references with `.written(pipe)`.

### Step 3: Add beginCycle Calls

Add `beginCycle<P>()` calls at the top of `tickPipelines_`. Remove scattered `clear<P>()` and `.fill(false)` calls.

### Step 4: Rewrite Process Functions

Replace `pipe_[p].valid<P>(STAGE)` → `stage_.valid<P>(p)`, `pipe_[p].consume<P>(STAGE)` → `stage_.consume<P>(p)`, etc. Use forwarding helpers where appropriate.

### Step 5: Verify

After each migration step, build and run regression tests. Pipeline register changes are subtle — verify instruction counts and IPC match the pre-migration baseline.

---

## Related Docs

- [Pipeline Registers](pipeline-registers.md) — overview and removed types reference
- [Units and Simulation](units-and-simulation.md) — `TickableUnit` and phase dispatch
