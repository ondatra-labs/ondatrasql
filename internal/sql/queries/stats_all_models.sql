-- All models with latest run stats
WITH latest AS (
    SELECT
        commit_extra_info->>'model' as model,
        COALESCE(commit_extra_info->>'kind', 'table') as kind,
        commit_extra_info->>'run_type' as run_type,
        CAST(commit_extra_info->>'rows_affected' AS BIGINT) as rows,
        CAST(commit_extra_info->>'duration_ms' AS INTEGER) as duration,
        CASE
            WHEN snapshot_time::DATE = CURRENT_DATE THEN strftime(snapshot_time, '%%H:%%M')
            ELSE strftime(snapshot_time, '%%m-%%d')
        END as last_run,
        ROW_NUMBER() OVER (PARTITION BY commit_extra_info->>'model' ORDER BY snapshot_id DESC) as rn
    FROM {{catalog}}.snapshots()
    WHERE commit_extra_info->>'model' IS NOT NULL
)
SELECT model, kind, run_type, rows, duration, last_run
FROM latest
WHERE rn = 1
ORDER BY model
