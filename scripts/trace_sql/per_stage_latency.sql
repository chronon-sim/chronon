-- Per-stage latency between pipeline stages connected by flows.
--
-- Every flow edge links one uid's event on stage A to its next event on
-- stage B (chronon stamps flow(uid) on lane events). Grouping the edges by
-- the two tracks' hierarchical paths gives the latency distribution of each
-- stage transition, in cycles.
--
-- Usage: trace_processor -q per_stage_latency.sql timeline.pftrace

WITH RECURSIVE track_path(id, path) AS (
    SELECT id, name FROM track WHERE parent_id IS NULL
    UNION ALL
    SELECT t.id, tp.path || '.' || t.name
    FROM track t
    JOIN track_path tp ON t.parent_id = tp.id
),
located AS (
    SELECT s.id, s.ts, s.name, tp.path
    FROM slice s
    JOIN track_path tp ON s.track_id = tp.id
)
SELECT
    src.path AS from_track,
    dst.path AS to_track,
    COUNT(*) AS edges,
    AVG(dst.ts - src.ts) AS avg_cycles,
    MIN(dst.ts - src.ts) AS min_cycles,
    MAX(dst.ts - src.ts) AS max_cycles
FROM flow f
JOIN located src ON f.slice_out = src.id
JOIN located dst ON f.slice_in = dst.id
GROUP BY from_track, to_track
ORDER BY edges DESC;
