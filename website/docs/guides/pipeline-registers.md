---
sidebar_label: "Pipeline Registers"
---

# Pipeline Registers

Chronon provides stage-level pipeline register abstractions for tick-based units.

## Quick Selection

| Type | Entries | Best For |
|------|---------|----------|
| `StageReg<T, N>` | N parallel pipes | Superscalar pipeline stages (e.g., 4-wide addr/data pipes) |
| `SingleStageReg<T>` | 1 | Single-entry stages (fill pipeline, snoop pipeline) |
| `StagePipeline<S...>` | Group | Batch `beginCycle` / `flushIf` / `reset` across multiple stages |

See **[Stage Registers](stage-registers.md)** for the full API documentation, examples, and forwarding helpers.

---

## Internal Implementation

`StageReg` and `SingleStageReg` use two-slot ping-pong buffers internally. The phase template parameter (`Phase0` / `Phase1`) selects read/write slots at compile time, eliminating pointer indirection.

---

## Removed Types (2026-03)

The following types were removed after the migration to `StageReg` / `SingleStageReg`:

- `PipelineReg<T>` — manual tick-based single-entry register
- `PipelineRegMulti<T, N>` — manual tick-based multi-entry register
- `PipelineControl` — `PipelineStallController` and `PipelineFlushController`
- `TrackedPipelineReg<T>` / `TrackedPipelineRegArray<T, N>` — write-tracking wrappers
- `pipeline_tick()`, `pipeline_reset()`, `pipeline_tick_with_stall()` — helper functions

---

## Related Docs

- [Stage Registers](stage-registers.md) — full `StageReg` / `SingleStageReg` API and forwarding helpers
- [Units and Simulation](units-and-simulation.md) — `TickableUnit` and phase dispatch
- [API Reference](/docs/api/) — full API listing
- [Changelog](changelog.md) — breaking changes and migration notes
