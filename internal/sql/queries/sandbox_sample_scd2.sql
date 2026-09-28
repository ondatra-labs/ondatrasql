-- sandbox_sample_scd2.sql - Sample rows that differ between sandbox and prod (SCD2)
-- Compares current versions only, without the SCD2 metadata columns. Closed
-- versions are history both sides share; including them hides a change whose
-- new values match an older version.
-- Args: %[1]s = sandbox catalog, %[2]s = prod catalog, %[3]s = target table, %[4]s = limit

SELECT * FROM (
    SELECT * EXCLUDE (valid_from_snapshot, valid_to_snapshot, is_current)
    FROM %[1]s.%[3]s WHERE is_current IS true
    EXCEPT
    SELECT * EXCLUDE (valid_from_snapshot, valid_to_snapshot, is_current)
    FROM %[2]s.%[3]s WHERE is_current IS true
) LIMIT %[4]s
