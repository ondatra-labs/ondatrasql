-- Batch query for run_type decisions (dependency-aware)
-- Computes run_type for ALL models in one query instead of N individual queries
-- Args: 1) VALUES list like ('t1','h1','k1'),('t2','h2','k2')
-- Logic: table kind uses dependency tracking to skip when nothing changed
WITH model_input AS (
    SELECT * FROM (VALUES %s) AS t(target, current_hash, kind, is_fetch)
),
-- Get ALL latest commits in one scan (instead of N individual lookups)
latest_commits AS (
    SELECT
        commit_extra_info->>'model' AS model,
        commit_extra_info->>'sql_hash' AS prev_hash,
        commit_extra_info->>'depends' AS depends_raw,
        snapshot_id,
        -- Partition on lowercased model so case-variant commits are deduped
        -- to a single row (matches the case-insensitive lookup story).
        ROW_NUMBER() OVER (PARTITION BY LOWER(commit_extra_info->>'model')
                          ORDER BY snapshot_id DESC) AS rn
    FROM snapshots()
    WHERE commit_extra_info->>'model' IS NOT NULL
),
model_status AS (
    SELECT
        m.target,
        m.current_hash,
        m.kind,
        m.is_fetch,
        COALESCE(lc.prev_hash, '') AS prev_hash,
        lc.depends_raw,
        TRY_CAST(lc.depends_raw AS VARCHAR[]) AS depends_array,
        CASE
            WHEN lc.depends_raw IS NOT NULL AND TRY_CAST(lc.depends_raw AS VARCHAR[]) IS NULL THEN true
            ELSE false
        END AS depends_invalid,
        lc.snapshot_id AS model_snapshot_id
    FROM model_input m
    LEFT JOIN latest_commits lc ON LOWER(lc.model) = LOWER(m.target) AND lc.rn = 1
),
-- Unnest table dependencies for models with valid depends arrays
table_deps AS (
    SELECT DISTINCT
        ms.target,
        UNNEST(ms.depends_array) AS dep
    FROM model_status ms
    WHERE ms.kind = 'table'
      AND ms.depends_array IS NOT NULL
      AND len(ms.depends_array) > 0
),
-- Check each dependency's latest snapshot against the model's snapshot
dep_status AS (
    SELECT
        td.target,
        td.dep,
        CASE
            WHEN lc.snapshot_id IS NULL THEN 'unverifiable'
            WHEN lc.snapshot_id > ms.model_snapshot_id THEN 'changed'
            ELSE 'unchanged'
        END AS status
    FROM table_deps td
    JOIN model_status ms ON LOWER(ms.target) = LOWER(td.target)
    LEFT JOIN latest_commits lc ON LOWER(lc.model) = LOWER(td.dep) AND lc.rn = 1
),
-- Summarize dependency status per table model
table_dep_summary AS (
    SELECT
        target,
        COUNT(*) AS dep_count,
        COUNT(*) FILTER (WHERE status = 'unverifiable') AS unverifiable_dep_count,
        COUNT(*) FILTER (WHERE status = 'changed') AS changed_dep_count,
        MIN(CASE WHEN status = 'changed' THEN dep END) AS first_changed_dep,
        MIN(CASE WHEN status = 'unverifiable' THEN dep END) AS first_unverifiable_dep
    FROM dep_status
    GROUP BY target
)
SELECT
    ms.target,
    ms.current_hash,
    ms.kind,
    ms.prev_hash,
    CASE
        -- Non-table kinds: standard logic
        WHEN ms.kind != 'table' AND ms.prev_hash = '' THEN 'backfill'
        WHEN ms.kind != 'table' AND ms.prev_hash != ms.current_hash THEN 'backfill'
        WHEN ms.kind != 'table' THEN 'incremental'
        -- Table kind: dependency-aware skip logic
        WHEN ms.prev_hash = '' THEN 'backfill'
        WHEN ms.prev_hash != ms.current_hash THEN 'backfill'
        -- @kind: table @fetch pulls from external APIs whose state isn't
        -- visible to the runtime — never skip, always do a full re-fetch.
        WHEN ms.is_fetch THEN 'full'
        WHEN ms.depends_raw IS NULL THEN 'full'
        WHEN ms.depends_invalid THEN 'full'
        WHEN tds.dep_count IS NULL OR tds.dep_count = 0 THEN 'skip'
        WHEN tds.unverifiable_dep_count > 0 THEN 'full'
        WHEN tds.changed_dep_count > 0 THEN 'full'
        ELSE 'skip'
    END AS run_type,
    CASE
        -- Non-table kinds
        WHEN ms.kind != 'table' AND ms.prev_hash = '' THEN 'first run'
        WHEN ms.kind != 'table' AND ms.prev_hash != ms.current_hash THEN 'sql changed'
        -- For incremental kinds (append/merge/scd2/tracked) the SQL being
        -- unchanged doesn't mean nothing happens — the model still runs
        -- to ingest new source rows. (Bug 15)
        WHEN ms.kind != 'table' THEN 'incremental run'
        -- Table kind
        WHEN ms.prev_hash = '' THEN 'first run'
        WHEN ms.prev_hash != ms.current_hash THEN 'sql changed'
        WHEN ms.is_fetch THEN 'fetch source'
        WHEN ms.depends_raw IS NULL THEN 'missing depends metadata'
        WHEN ms.depends_invalid THEN 'invalid depends metadata'
        WHEN tds.dep_count IS NULL OR tds.dep_count = 0 THEN 'no dependencies'
        WHEN tds.unverifiable_dep_count > 0 THEN 'unverifiable dep: ' || tds.first_unverifiable_dep
        WHEN tds.changed_dep_count > 0 THEN 'dep changed: ' || tds.first_changed_dep
        ELSE 'deps unchanged'
    END AS run_reason
FROM model_status ms
LEFT JOIN table_dep_summary tds ON tds.target = ms.target
