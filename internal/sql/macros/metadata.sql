-- Metadata introspection macros for OndatraSQL
-- Loaded at session startup (PHASE 3, after DuckLake attach)
-- These query {{catalog}}.snapshots() for commit history

-- Get the SQL hash from the latest commit for a model
CREATE OR REPLACE MACRO ondatra_get_sql_hash(p_model) AS
  (SELECT commit_extra_info->>'sql_hash'
   FROM {{catalog}}.snapshots()
   WHERE commit_extra_info->>'model' = p_model
   ORDER BY snapshot_id DESC
   LIMIT 1);

-- Get the columns JSON from the latest commit for a model
CREATE OR REPLACE MACRO ondatra_get_prev_columns(p_model) AS
  (SELECT commit_extra_info->>'columns'
   FROM {{catalog}}.snapshots()
   WHERE commit_extra_info->>'model' = p_model
   ORDER BY snapshot_id DESC
   LIMIT 1);

-- Get the full commit info for a model's latest commit
CREATE OR REPLACE MACRO ondatra_get_commit_info(p_model) AS
  (SELECT commit_extra_info
   FROM {{catalog}}.snapshots()
   WHERE commit_extra_info->>'model' = p_model
   ORDER BY snapshot_id DESC
   LIMIT 1);

-- Get current snapshot ID (before materialize creates a new one)
CREATE OR REPLACE MACRO ondatra_current_snapshot() AS
  COALESCE((SELECT id FROM {{catalog}}.current_snapshot()), 0);

-- Get all models that depend on the given target
CREATE OR REPLACE MACRO ondatra_get_downstream(p_model) AS TABLE
  WITH latest_commits AS (
    SELECT
      commit_extra_info->>'model' AS model,
      commit_extra_info->>'depends' AS depends,
      ROW_NUMBER() OVER (PARTITION BY commit_extra_info->>'model' ORDER BY snapshot_id DESC) AS rn
    FROM {{catalog}}.snapshots()
    WHERE commit_extra_info->>'model' IS NOT NULL
  )
  SELECT DISTINCT model
  FROM latest_commits
  WHERE rn = 1
    AND depends LIKE '%"' || p_model || '"%';
