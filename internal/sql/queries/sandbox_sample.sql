-- sandbox_sample.sql - Sample rows that differ between sandbox and prod
-- Args: %[1]s = sandbox catalog, %[2]s = prod catalog, %[3]s = target table, %[4]s = limit

SELECT * FROM (
    SELECT * FROM %[1]s.%[3]s EXCEPT SELECT * FROM %[2]s.%[3]s
) LIMIT %[4]s
