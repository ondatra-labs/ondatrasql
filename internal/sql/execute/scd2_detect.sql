-- SCD2: Batch detect changes and deletions in one round-trip
-- Args: target, tmp_table, unique_key, unique_key, tmp_table, unique_key, unique_key, change_where,
--       unique_key, target, unique_key, unique_key, tmp_table
-- Creates both temp tables in a single execution

CREATE TEMP TABLE scd2_changes AS
WITH current_rows AS (
    SELECT * FROM %s WHERE is_current IS true
)
-- New rows (not in current)
SELECT t.*, 'new' AS _change_type FROM %s t
WHERE t.%s NOT IN (SELECT %s FROM current_rows)
UNION ALL
-- Changed rows (in current but values differ)
SELECT t.*, 'changed' AS _change_type FROM %s t
JOIN current_rows c ON t.%s = c.%s
WHERE %s;

CREATE TEMP TABLE scd2_deleted AS
SELECT %s FROM %s WHERE is_current IS true
AND %s NOT IN (SELECT %s FROM %s);
