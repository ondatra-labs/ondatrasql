-- sandbox_diff_count_scd2.sql - Count rows in sandbox that differ from prod (SCD2)
-- Excludes SCD2 metadata columns (valid_from_snapshot, valid_to_snapshot, is_current)
-- Args: %[1]s = sandbox catalog, %[2]s = prod catalog, %[3]s = target table

SELECT COUNT(*) FROM (
    SELECT * EXCLUDE (valid_from_snapshot, valid_to_snapshot, is_current)
    FROM %[1]s.%[3]s
    EXCEPT
    SELECT * EXCLUDE (valid_from_snapshot, valid_to_snapshot, is_current)
    FROM %[2]s.%[3]s
)
