-- Model count breakdown by kind
SELECT
    COALESCE(commit_extra_info->>'kind', 'table') as kind,
    COUNT(DISTINCT commit_extra_info->>'model') as cnt
FROM {{catalog}}.snapshots()
WHERE commit_extra_info->>'model' IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC
