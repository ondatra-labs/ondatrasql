-- Metadata introspection macros for OndatraSQL
-- Loaded at session startup (PHASE 3, after DuckLake attach)
-- These query {{catalog}}.snapshots() for commit history.
--
-- Model name lookups are CASE-INSENSITIVE: DuckDB folds unquoted
-- identifiers to lowercase, but commit metadata stores whatever case the
-- file path produced. Without LOWER() on both sides, a model committed as
-- `Raw.Orders` would be invisible to a `raw.orders` lookup, breaking
-- describe/lineage/run-type decisions for any non-lowercase path.

-- Get the SQL hash from the latest commit for a model
CREATE OR REPLACE MACRO ondatra_get_sql_hash(p_model) AS
  (SELECT commit_extra_info->>'sql_hash'
   FROM {{catalog}}.snapshots()
   WHERE LOWER(commit_extra_info->>'model') = LOWER(p_model)
   ORDER BY snapshot_id DESC
   LIMIT 1);

-- Get the columns JSON from the latest commit for a model
CREATE OR REPLACE MACRO ondatra_get_prev_columns(p_model) AS
  (SELECT commit_extra_info->>'columns'
   FROM {{catalog}}.snapshots()
   WHERE LOWER(commit_extra_info->>'model') = LOWER(p_model)
   ORDER BY snapshot_id DESC
   LIMIT 1);

-- Get the full commit info for a model's latest commit
CREATE OR REPLACE MACRO ondatra_get_commit_info(p_model) AS
  (SELECT commit_extra_info
   FROM {{catalog}}.snapshots()
   WHERE LOWER(commit_extra_info->>'model') = LOWER(p_model)
   ORDER BY snapshot_id DESC
   LIMIT 1);

-- Get current snapshot ID (before materialize creates a new one)
CREATE OR REPLACE MACRO ondatra_current_snapshot() AS
  COALESCE((SELECT id FROM {{catalog}}.current_snapshot()), 0);

-- NOTE: per-model snapshot lookup is built inline in
-- GetPreviousSnapshot (internal/backfill/detect.go) so it can pick the
-- prod catalog when running in sandbox. A macro hard-codes {{catalog}}
-- at load time and would always read from sandbox, which is empty.

-- Get all models that depend on the given target.
-- Uses ILIKE so the depends-array substring search is case-insensitive,
-- matching the LOWER()-based lookups above. Without it, a parent
-- committed as `Raw.Orders` would be invisible to lookups for `raw.orders`.
CREATE OR REPLACE MACRO ondatra_get_downstream(p_model) AS TABLE
  WITH latest_commits AS (
    SELECT
      commit_extra_info->>'model' AS model,
      commit_extra_info->>'depends' AS depends,
      ROW_NUMBER() OVER (PARTITION BY LOWER(commit_extra_info->>'model') ORDER BY snapshot_id DESC) AS rn
    FROM {{catalog}}.snapshots()
    WHERE commit_extra_info->>'model' IS NOT NULL
  )
  SELECT DISTINCT model
  FROM latest_commits
  WHERE rn = 1
    AND depends ILIKE '%"' || p_model || '"%';
