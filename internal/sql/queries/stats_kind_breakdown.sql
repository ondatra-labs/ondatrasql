-- Model count breakdown by kind
SELECT
    COALESCE(commit_extra_info->>'kind', 'table') as kind,
    -- LOWER() to match case-insensitive lookup story (case-variant commits
    -- count as one model).
    COUNT(DISTINCT LOWER(commit_extra_info->>'model')) as cnt
FROM snapshots()
WHERE commit_extra_info->>'model' IS NOT NULL
GROUP BY 1
-- Tiebreak on kind name so equal-count rows render in a stable order
-- across runs; without this, --json stats reshuffles the breakdown
-- and breaks consumers that diff successive runs.
ORDER BY 2 DESC, 1 ASC
