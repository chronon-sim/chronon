-- Stall attribution: cycles spent beyond each event kind's best case.
--
-- For every (track, name), the minimum observed duration approximates the
-- unstalled latency of that operation on that lane; everything above it is
-- attributed as stall. Sorting by stall_cycles surfaces where backpressure
-- and contention actually cost time.
--
-- Usage: trace_processor -q stall_attribution.sql timeline.pftrace

WITH RECURSIVE track_path(id, path) AS (
    SELECT id, name FROM track WHERE parent_id IS NULL
    UNION ALL
    SELECT t.id, tp.path || '.' || t.name
    FROM track t
    JOIN track_path tp ON t.parent_id = tp.id
),
base AS (
    SELECT track_id, name, MIN(dur) AS base_cycles
    FROM slice
    WHERE dur > 0
    GROUP BY track_id, name
)
SELECT
    tp.path AS track,
    s.name,
    COUNT(*) AS spans,
    b.base_cycles,
    SUM(s.dur - b.base_cycles) AS stall_cycles,
    SUM(CASE WHEN s.dur > b.base_cycles THEN 1 ELSE 0 END) AS stalled_spans,
    AVG(s.dur) AS avg_cycles,
    MAX(s.dur) AS max_cycles
FROM slice s
JOIN base b ON s.track_id = b.track_id AND s.name = b.name
JOIN track_path tp ON s.track_id = tp.id
WHERE s.dur > 0
GROUP BY track, s.name
ORDER BY stall_cycles DESC;
