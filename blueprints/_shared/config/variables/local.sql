-- ===============================================================
-- OndatraSQL per-model variables
--
-- Runs once per model, before validation. The variable
-- 'current_model' is already set (e.g. 'staging.orders').
--
-- These variables are available in macros via getvariable().
-- Delta warning macros use prev_model_snapshot and curr_snapshot
-- to detect changes between runs via table_changes().
--
-- Edit freely -- this file is yours.
-- ===============================================================

-- Snapshot of the PREVIOUS commit for this model (second-to-last).
-- After commit, the latest snapshot IS the current run. We need the one before that.
-- Returns 0 if this is the first or second run (no meaningful previous state).
SET VARIABLE prev_model_snapshot = COALESCE(
  (SELECT snapshot_id FROM (
    SELECT snapshot_id, ROW_NUMBER() OVER (ORDER BY snapshot_id DESC) AS rn
    FROM snapshots()
    WHERE LOWER(commit_extra_info->>'model') = LOWER(getvariable('current_model'))
  ) WHERE rn = 2), 0);

-- Current snapshot (the one we just committed).
SET VARIABLE curr_snapshot =
  (SELECT id FROM current_snapshot())::BIGINT;
