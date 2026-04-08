-- Model count breakdown by kind
SELECT
    COALESCE(commit_extra_info->>'kind', 'table') as kind,
    -- LOWER() to match case-insensitive lookup story (case-variant commits
    -- count as one model).
    COUNT(DISTINCT LOWER(commit_extra_info->>'model')) as cnt
FROM {{catalog}}.snapshots()
WHERE commit_extra_info->>'model' IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC
