-- @kind: table
-- @constraint: not_null(repo)
-- One row per repository and ISO week (Monday, UTC) with merged pull
-- requests: how many were merged and the median hours from opening to merge.
SELECT
    repo,
    date_trunc('week', timezone('UTC', merged_at))::DATE                         AS week,
    COUNT(*)                                                                     AS merged_prs,
    ROUND(MEDIAN(date_diff('second', created_at, merged_at)) / 3600.0, 1)::DOUBLE AS median_hours_to_merge
FROM raw.forgejo_pulls
WHERE merged_at IS NOT NULL
GROUP BY ALL
