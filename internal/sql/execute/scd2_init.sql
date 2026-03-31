-- scd2_init.sql - Create SCD2 table with history tracking columns
-- Args: %[1]s = target table, %[2]s = column list, %[3]d = current snapshot, %[4]s = temp table

CREATE OR REPLACE TABLE %[1]s AS
SELECT %[2]s,
       %[3]d::BIGINT AS valid_from_snapshot,
       CAST(NULL AS BIGINT) AS valid_to_snapshot,
       true AS is_current
FROM %[4]s
