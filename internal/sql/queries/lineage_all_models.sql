-- All distinct model names for lineage overview
SELECT DISTINCT commit_extra_info->>'model'
FROM {{catalog}}.snapshots()
WHERE commit_extra_info->>'model' IS NOT NULL
ORDER BY 1
