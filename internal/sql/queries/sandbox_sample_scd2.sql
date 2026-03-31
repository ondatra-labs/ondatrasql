-- sandbox_sample_scd2.sql - Sample rows that differ between sandbox and prod (SCD2)
-- Excludes SCD2 metadata columns (valid_from_snapshot, valid_to_snapshot, is_current)
-- Args: %[1]s = sandbox catalog, %[2]s = prod catalog, %[3]s = target table, %[4]s = limit

SELECT * FROM (
    SELECT * EXCLUDE (valid_from_snapshot, valid_to_snapshot, is_current)
    FROM %[1]s.%[3]s
    EXCEPT
    SELECT * EXCLUDE (valid_from_snapshot, valid_to_snapshot, is_current)
    FROM %[2]s.%[3]s
) LIMIT %[4]s
