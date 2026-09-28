-- sandbox_diff_count_scd2.sql - Count rows in sandbox that differ from prod (SCD2)
-- Compares current versions only, without the SCD2 metadata columns. Closed
-- versions are history both sides share; including them hides a change whose
-- new values match an older version. The columns are filtered rather than
-- EXCLUDEd because a prod target built before valid_from_at/valid_to_at
-- existed lacks them.
-- Args: %[1]s = sandbox catalog, %[2]s = prod catalog, %[3]s = target table

SELECT COUNT(*) FROM (
    SELECT COLUMNS(c -> c NOT IN ('valid_from_snapshot', 'valid_to_snapshot', 'is_current', 'valid_from_at', 'valid_to_at'))
    FROM %[1]s.%[3]s WHERE is_current IS true
    EXCEPT
    SELECT COLUMNS(c -> c NOT IN ('valid_from_snapshot', 'valid_to_snapshot', 'is_current', 'valid_from_at', 'valid_to_at'))
    FROM %[2]s.%[3]s WHERE is_current IS true
)
