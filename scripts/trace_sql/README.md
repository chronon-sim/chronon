# Canned trace_processor queries for chronon timelines

Offline SQL analysis of `timeline.pftrace` with [Perfetto's
`trace_processor`](https://perfetto.dev/docs/analysis/trace-processor)
(`curl -LO https://get.perfetto.dev/trace_processor && chmod +x trace_processor`).

```bash
trace_processor -q scripts/trace_sql/per_stage_latency.sql out/latest/timeline.pftrace
```

All timestamps and durations are simulation **cycles** (the timeline's custom
cycle clock is paired 1:1 with the trace clock). The queries are model-agnostic:
they classify by track path (the unit hierarchy) and by the low-cardinality
event names emitted through `TimelineLane`, so they work on any chronon model
that uses the timeline API.

| Query | Question it answers |
|---|---|
| `per_stage_latency.sql` | How long does a flow (instruction/transaction uid) take between each pair of connected stages? |
| `latency_histogram.sql` | Distribution of span durations per event name (e.g. `miss`, `mul`) |
| `occupancy.sql` | How busy is each lane track — busy fraction, slice counts, mean duration |
| `stall_attribution.sql` | Which lanes/events accumulate cycles beyond their per-name minimum (stall attribution) |
| `counter_stats.sql` | Min/avg/max of every counter track (`TimelineCounter` samples and counter snapshots) |

Tip: `trace_processor --httpd timeline.pftrace` serves the trace to
ui.perfetto.dev for interactive exploration alongside these queries.
