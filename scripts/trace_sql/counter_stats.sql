-- Counter-track statistics: TimelineCounter samples and counter snapshots.
--
-- Counter tracks nest under their owning unit, so the hierarchical path
-- identifies them unambiguously (e.g. sim.cpu0.lsu.lsq_occupancy).
--
-- Usage: trace_processor -q counter_stats.sql timeline.pftrace

WITH RECURSIVE track_path(id, path) AS (
    SELECT id, name FROM track WHERE parent_id IS NULL
    UNION ALL
    SELECT t.id, tp.path || '.' || t.name
    FROM track t
    JOIN track_path tp ON t.parent_id = tp.id
)
SELECT
    tp.path AS counter,
    COUNT(*) AS samples,
    MIN(c.value) AS min_value,
    AVG(c.value) AS avg_value,
    MAX(c.value) AS max_value
FROM counter c
JOIN counter_track ct ON c.track_id = ct.id
JOIN track_path tp ON tp.id = ct.id
GROUP BY counter
ORDER BY counter;
