-- ===============================================================
-- OndatraSQL global variables
--
-- Computed once at session init via subqueries. Same value for
-- the entire pipeline run. Use for derived state that doesn't
-- change between models.
--
-- Available everywhere via getvariable('name').
--
-- Edit freely -- this file is yours.
-- ===============================================================

-- Example: total number of models tracked in DuckLake
-- SET VARIABLE total_models = (SELECT COUNT(DISTINCT commit_extra_info->>'model') FROM snapshots() WHERE commit_extra_info->>'model' IS NOT NULL);

-- Example: timestamp of last pipeline run
-- SET VARIABLE last_pipeline_run = (SELECT MAX(snapshot_time) FROM snapshots());
