-- sandbox_diff_count.sql - Count rows in sandbox that differ from prod
-- Args: %[1]s = sandbox catalog, %[2]s = prod catalog, %[3]s = target table

SELECT COUNT(*) FROM (
    SELECT * FROM %[1]s.%[3]s EXCEPT SELECT * FROM %[2]s.%[3]s
)
