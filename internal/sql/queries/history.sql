-- Show run history (last 20 runs)
SELECT
    snapshot_id as "ID",
    strftime(snapshot_time, '%Y-%m-%d %H:%M:%S') as "Time",
    commit_extra_info->>'model' as "Model",
    commit_extra_info->>'kind' as "Kind",
    commit_extra_info->>'run_type' as "Type",
    CAST(commit_extra_info->>'rows_affected' AS INTEGER) as "Rows",
    CAST(commit_extra_info->>'duration_ms' AS INTEGER) as "ms",
    LEFT(commit_extra_info->>'dag_run_id', 20) as "Run ID"
FROM {{catalog}}.snapshots()
WHERE commit_extra_info->>'model' IS NOT NULL
ORDER BY snapshot_id DESC
LIMIT 20
