---
sidebar_label: "PriorityArbiter"
---

# PriorityArbiter

Declarative priority arbiter for N-pipe resource arbitration. Replaces hand-unrolled priority chains with a configurable data structure that handles winner selection, bank conflict detection, overflow routing, and idle-fill — all with zero heap allocation on the hot path.

## Problem

Superscalar pipeline models frequently need priority arbitration: multiple request sources (issue queue, replay queue, store tag lookup, cache fill, etc.) compete for a fixed number of shared pipeline slots each cycle. The naive implementation is a hand-unrolled priority chain with per-pipe winner selection, pairwise bank conflict checks, and overflow routing. This typically produces 300-400 lines of imperative code per arbitration point, is error-prone to modify, and obscures the actual priority policy.

## Design Principles

1. **Declarative configuration** — priority chains, overflow rules, bank conflict predicates, and pass rules are configured once at construction time
2. **Zero allocation** — all internal storage uses `std::array`; no `std::vector`, `std::function`, or heap allocation on the per-tick path
3. **Structured results** — winners, losers (with reason), and contention counts are available for observe integration without callbacks
4. **Composable** — works with any `enum class` source type; not tied to any specific pipeline model

---

## Types

### ArbRequest

A request submitted to a specific pipe for arbitration.

```cpp
template <typename SourceEnum>
struct ArbRequest {
    SourceEnum source{};       // Which source is requesting
    uint8_t    entry_id = 0xFF; // Buffer-specific ID (LID, STID, etc.)
    uint64_t   addr = 0;       // Address for bank conflict detection
    bool       valid = false;
    uint64_t   tag = 0;        // Opaque attribute for logging (uid, etc.)
};
```

### ArbWinner

Result for a single pipe after arbitration.

```cpp
template <typename SourceEnum>
struct ArbWinner {
    SourceEnum source{};
    uint8_t    entry_id = 0xFF;
    uint8_t    pipe_id = 0;
    uint64_t   tag = 0;
    bool       valid = false;
    bool       bank_conflict = false;  // Won priority but blocked by conflict
};
```

When `bank_conflict` is true and `valid` is false, the request won priority selection but was subsequently blocked by a bank conflict with a higher-priority pipe. The default bank-conflict priority is lower pipe index first; it can be changed with `setBankConflictPriority()`.

### ArbLoser

Information about a request that did not win.

```cpp
enum class LoseReason : uint8_t {
    LOWER_PRIORITY,    // Lost to a higher-priority source on the same pipe
    BANK_CONFLICT,     // Won priority but blocked by bank conflict
    OVERFLOW_FAILED    // (reserved for future use)
};

template <typename SourceEnum>
struct ArbLoser {
    SourceEnum source{};
    uint8_t    entry_id = 0xFF;
    uint8_t    target_pipe = 0;
    uint64_t   tag = 0;
    LoseReason reason{};
};
```

### ArbResult

Complete arbitration result for all pipes.

```cpp
template <typename SourceEnum, std::size_t NumPipes, std::size_t MaxLosers = 16>
struct ArbResult {
    std::array<ArbWinner<SourceEnum>, NumPipes> winners{};
    std::array<ArbLoser<SourceEnum>, MaxLosers> losers{};
    uint8_t loser_count = 0;
    std::array<uint8_t, NumPipes> contention{};  // # requests per pipe

    const ArbWinner<SourceEnum>& operator[](std::size_t pipe) const;
    void clear() noexcept;
};
```

---

## PriorityArbiter Class

```cpp
template <typename SourceEnum,
          std::size_t NumPipes,
          std::size_t MaxSourcesPerPipe = 8,
          std::size_t MaxLosers = 16,
          std::size_t MaxOverflows = 8,
          std::size_t MaxPassRules = 4,
          std::size_t MaxDynOverrides = 4,
          std::size_t MaxIdleFillSources = 4,
          std::size_t MaxIdleFillRequests = 8>
class PriorityArbiter;
```

All capacity limits are compile-time template parameters with sensible defaults. Increase them if your use case exceeds the defaults.

### Configuration API (call once at construction)

```cpp
// Set priority chain for a pipe (first element = highest priority)
void setPipePriority(std::size_t pipe, std::initializer_list<SourceEnum> priority);

// Overflow: if `source` loses on `from_pipe`, re-submit to `to_pipe`
void setOverflow(SourceEnum source, std::size_t from_pipe, std::size_t to_pipe);

// Bank conflict predicate: returns true if two addresses conflict
void setBankConflictFn(bool(*fn)(uint64_t, uint64_t));

// Select which pipe wins on bank conflict (default: LowerPipeIndex)
void setBankConflictPriority(BankConflictPriority priority);

// Pass rule: `lower` may proceed despite conflict with `higher`
void addPassRule(SourceEnum higher, SourceEnum lower);

// Dynamic override: when *cond is true, `promoted` beats `demoted`
// Uses bool* (not std::function) for zero overhead
void addDynamicOverride(SourceEnum promoted, SourceEnum demoted, const bool* cond);

// Register a source eligible for idle-fill
void addIdleFillSource(SourceEnum source);
```

### Per-Tick API

```cpp
// Reset all per-tick state (call at start of each tick)
void clearRequests() noexcept;

// Mark a pipe as pre-occupied (e.g., retained pipeline entry from previous cycle)
void setOccupied(std::size_t pipe, uint64_t addr, SourceEnum source);

// Submit request for a specific pipe
void addRequest(std::size_t pipe, ArbRequest<SourceEnum> req);

// Submit idle-fill request (assigned to any empty pipe after main arbitration)
void addIdleFillRequest(ArbRequest<SourceEnum> req);

// Run arbitration; returns const ref valid until next clearRequests()
const ArbResult<SourceEnum, NumPipes>& arbitrate();

// Access last result without re-running
const ArbResult<SourceEnum, NumPipes>& result() const noexcept;
```

---

## Algorithm

The `arbitrate()` method runs four phases in sequence:

### 1. Apply Dynamic Overrides

For each active override (where `*cond == true`), swap the promoted and demoted sources in all priority chains so the promoted source has higher priority.

### 2. Select Winners (pipe 0 → N-1)

Pipes are processed in index order. For each non-occupied pipe, walk the priority chain and pick the first valid request as the winner. All other matching requests become losers. Losers with matching overflow rules are re-submitted to the target pipe (which hasn't been processed yet if `to_pipe > from_pipe`).

**Key property**: Because pipes are processed in order, overflow from pipe 0 to pipe 1 naturally injects into pipe 1's request list before pipe 1 is evaluated. This enables waterfall assignment patterns.

### 3. Bank Conflict Resolution

Pairwise check across all winners and occupied pipes. Lower-index pipe has higher priority by default; call `setBankConflictPriority(BankConflictPriority::HigherPipeIndex)` to reverse the lane priority. If two addresses conflict (per the bank conflict predicate), the lower-priority winner is invalidated unless a pass rule allows it to proceed.

Pass rules model architectural exceptions where certain source types may bypass bank conflicts (e.g., replay loads passing new loads).

### 4. Idle Fill

Any pipe with no winner and not occupied is filled from the idle-fill request queue. Requests are consumed in submission order.

---

## Usage Example

### Source Definition

```cpp
enum class ArbSource : uint8_t {
    IQ,        // Issue queue (new operations)
    LRQ,       // Load replay queue
    RST,       // Resolved store tracker (tag lookups)
    FILL,      // Cache fill
    MB_RD      // Merge buffer read (AMO)
};
```

### Configuration (constructor)

```cpp
PriorityArbiter<ArbSource, 4> arbiter;

// All pipes: FILL > IQ > LRQ > RST
arbiter.setPipePriority(0, {ArbSource::FILL, ArbSource::IQ, ArbSource::LRQ, ArbSource::RST});
arbiter.setPipePriority(1, {ArbSource::FILL, ArbSource::IQ, ArbSource::LRQ, ArbSource::RST});
arbiter.setPipePriority(2, {ArbSource::IQ, ArbSource::LRQ, ArbSource::RST});
arbiter.setPipePriority(3, {ArbSource::IQ, ArbSource::LRQ, ArbSource::RST});

// Bank conflict: same tag bank (bit 6)
arbiter.setBankConflictFn([](uint64_t a, uint64_t b) {
    return (a & 0x40) == (b & 0x40);
});

// LRQ replays can pass IQ loads despite bank conflict
arbiter.addPassRule(ArbSource::IQ, ArbSource::LRQ);

// RST beats IQ/LRQ when store-priority flag is set
arbiter.addDynamicOverride(ArbSource::RST, ArbSource::IQ, &rst_pri_over_ld_);
arbiter.addDynamicOverride(ArbSource::RST, ArbSource::LRQ, &rst_pri_over_ld_);

// MB_RD fills idle pipes after main arbitration
arbiter.addIdleFillSource(ArbSource::MB_RD);
```

### Per-Tick Usage

```cpp
template<ValidPhase P>
void arbitrateForPipelines_() {
    arbiter.clearRequests();

    // Mark occupied pipes (retained D0 entries from previous cycle)
    for (size_t lane = 0; lane < NUM_PIPES; ++lane) {
        if (d0_.valid<P>(lane)) {
            arbiter.setOccupied(lane, d0_.read<P>(lane).pa_line, ArbSource::IQ);
        }
    }

    // Distribute candidates to free pipes in priority order
    for (size_t lane = 0; lane < NUM_PIPES; ++lane) {
        if (d0_.valid<P>(lane)) continue;

        if (fill_ready) {
            arbiter.addRequest(lane, {ArbSource::FILL, 0, 0, true});
            fill_ready = false;
            continue;
        }
        if (iq_idx < iq_count) {
            arbiter.addRequest(lane, {ArbSource::IQ, sidx, addr, true, uid});
            ++iq_idx;
            continue;
        }
        // ... LRQ, RST similarly
    }

    // Run arbitration
    const auto& result = arbiter.arbitrate();

    // Process results
    for (size_t lane = 0; lane < NUM_PIPES; ++lane) {
        if (result[lane].valid) {
            // Inject winner into pipeline
        }
    }

    // Log contention (zero overhead when observe disabled)
    for (size_t p = 0; p < NUM_PIPES; ++p) {
        if (result.contention[p] > 1) {
            debug<"arb contention: pipe={} count={}">(p, result.contention[p]);
        }
    }
}
```

---

## Observation Integration

The arbiter is a pure data-structure utility with no observer callbacks. The owning unit queries the structured result and calls its own logging methods:

```cpp
// Winners
for (size_t p = 0; p < NUM_PIPES; ++p) {
    if (result[p].valid)
        debug<"arb win: pipe={} src={} tag={}">(p, (int)result[p].source, result[p].tag);
}

// Losers
for (uint8_t i = 0; i < result.loser_count; ++i) {
    auto& l = result.losers[i];
    debug<"arb lose: pipe={} src={} reason={}">(
        l.target_pipe, (int)l.source, (int)l.reason);
}
```

Zero overhead when observation is disabled — the `debug<>` calls short-circuit via `shouldLog<>()`.

---

## Implementation Details

### Zero Allocation

All storage is `std::array`-based with compile-time capacity:

| Storage | Default Capacity |
|---------|-----------------|
| Priority chains per pipe | `MaxSourcesPerPipe` (8) |
| Overflow rules | `MaxOverflows` (8) |
| Pass rules | `MaxPassRules` (4) |
| Dynamic overrides | `MaxDynOverrides` (4) |
| Idle-fill sources | `MaxIdleFillSources` (4) |
| Idle-fill requests | `MaxIdleFillRequests` (8) |
| Losers | `MaxLosers` (16) |
| Requests per pipe | `MaxSourcesPerPipe` (8) |

### Dynamic Override Mechanism

Uses `const bool*` instead of `std::function` — the arbiter reads the pointed-to bool each tick. This allows external logic (e.g., RST pressure detection) to flip a flag that the arbiter observes with zero indirection overhead.

### Overflow Propagation

Overflow rules are evaluated during winner selection. Since pipes are processed in index order (0→1→2→3), an overflow rule `from_pipe=1, to_pipe=2` injects the request into pipe 2's request list before pipe 2 is evaluated. This enables multi-hop overflow chains (e.g., LRQ overflows 1→2→3).

---

## Implementation Files

| File | Description |
|------|-------------|
| `src/sender/util/PriorityArbiter.hpp` | Header-only implementation (~350 lines) |
| `src/chronon/Util.hpp` | Re-exports types to `chronon::` namespace |

## Related Docs

- [Stage Registers](stage-registers.md) — pipeline register abstractions that work alongside the arbiter
- [Units and Simulation](units-and-simulation.md) — `TickableUnit` and phase dispatch
- [BroadcastBus](broadcast-bus) — planned broadcast utility for N-to-M messaging
