-- SCD2: Find deleted keys (in current but not in source)
-- Args: unique_key, target, unique_key, unique_key, tmp_table
CREATE TEMP TABLE scd2_deleted AS
SELECT %s FROM %s WHERE is_current = true
AND %s NOT IN (SELECT %s FROM %s)
