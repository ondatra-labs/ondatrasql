-- All distinct model names for lineage overview.
-- Distinct on lowercased name so case-variant commits collapse to one row,
-- matching the case-insensitive lookup story. The original-case display
-- value comes from FIRST_VALUE on the most recent commit.
WITH latest AS (
    SELECT
        commit_extra_info->>'model' AS model,
        ROW_NUMBER() OVER (PARTITION BY LOWER(commit_extra_info->>'model') ORDER BY snapshot_id DESC) AS rn
    FROM snapshots()
    WHERE commit_extra_info->>'model' IS NOT NULL
)
SELECT model FROM latest WHERE rn = 1 ORDER BY LOWER(model)
