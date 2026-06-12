-- Span-duration distribution per event name.
--
-- TimelineLane event names are low-cardinality compile-time literals
-- ("miss", "mul", ...), so duration statistics aggregate cleanly by kind.
-- Instants (dur = 0) are excluded.
--
-- Usage: trace_processor -q latency_histogram.sql timeline.pftrace

SELECT
    name,
    COUNT(*) AS spans,
    AVG(dur) AS avg_cycles,
    MIN(dur) AS min_cycles,
    MAX(dur) AS max_cycles,
    SUM(dur) AS total_cycles
FROM slice
WHERE dur > 0
GROUP BY name
ORDER BY total_cycles DESC;

-- Full histogram (one row per distinct duration) — feed to plotting:
-- SELECT name, dur AS cycles, COUNT(*) AS n
-- FROM slice WHERE dur > 0
-- GROUP BY name, dur ORDER BY name, dur;
