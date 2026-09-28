-- sandbox_diff_count_scd2.sql - Count rows in sandbox that differ from prod (SCD2)
-- Compares current versions only, without the SCD2 metadata columns. Closed
-- versions are history both sides share; including them hides a change whose
-- new values match an older version.
-- Args: %[1]s = sandbox catalog, %[2]s = prod catalog, %[3]s = target table

SELECT COUNT(*) FROM (
    SELECT * EXCLUDE (valid_from_snapshot, valid_to_snapshot, is_current)
    FROM %[1]s.%[3]s WHERE is_current IS true
    EXCEPT
    SELECT * EXCLUDE (valid_from_snapshot, valid_to_snapshot, is_current)
    FROM %[2]s.%[3]s WHERE is_current IS true
)
