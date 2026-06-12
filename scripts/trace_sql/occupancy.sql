-- Lane occupancy: how busy is each lane track over the traced window.
--
-- busy_frac is the fraction of the trace span during which the lane held an
-- open slice (per-slot lanes are separate tracks, so a lane group's slot
-- utilisation reads directly from its children).
--
-- Usage: trace_processor -q occupancy.sql timeline.pftrace

WITH RECURSIVE track_path(id, path) AS (
    SELECT id, name FROM track WHERE parent_id IS NULL
    UNION ALL
    SELECT t.id, tp.path || '.' || t.name
    FROM track t
    JOIN track_path tp ON t.parent_id = tp.id
),
bounds AS (
    SELECT MIN(ts) AS t0, MAX(ts + dur) AS t1 FROM slice
)
SELECT
    tp.path AS track,
    COUNT(*) AS spans,
    SUM(s.dur) AS busy_cycles,
    1.0 * SUM(s.dur) / (SELECT t1 - t0 FROM bounds) AS busy_frac,
    AVG(s.dur) AS avg_span_cycles
FROM slice s
JOIN track_path tp ON s.track_id = tp.id
WHERE s.dur > 0
GROUP BY track
ORDER BY busy_cycles DESC;
