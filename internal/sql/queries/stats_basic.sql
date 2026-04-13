-- Basic project statistics (model count, runs, rows, avg duration, last run)
-- Model count uses LOWER() so case-variant commits dedup to one model,
-- matching the case-insensitive lookup story.
SELECT
    COUNT(DISTINCT LOWER(commit_extra_info->>'model')) as models,
    COUNT(*) as total_runs,
    COALESCE(SUM(CAST(commit_extra_info->>'rows_affected' AS BIGINT)), 0) as total_rows,
    COALESCE(AVG(CAST(commit_extra_info->>'duration_ms' AS DOUBLE)), 0) as avg_duration,
    CASE
        WHEN MAX(snapshot_time)::DATE = CURRENT_DATE THEN strftime(MAX(snapshot_time), '%H:%M')
        ELSE strftime(MAX(snapshot_time), '%Y-%m-%d')
    END as last_run
FROM snapshots()
WHERE commit_extra_info->>'model' IS NOT NULL
