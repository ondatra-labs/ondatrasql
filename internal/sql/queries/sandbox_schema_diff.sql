-- sandbox_schema_diff.sql - Compare schema between sandbox and prod
-- Args: %[1]s = sandbox catalog, %[2]s = prod catalog, %[3]s = target table

WITH sandbox_cols AS (
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_catalog = '%[1]s' AND table_name = '%[3]s'
),
prod_cols AS (
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_catalog = '%[2]s' AND table_name = '%[3]s'
)
SELECT
    COALESCE(s.column_name, p.column_name) as column_name,
    p.data_type as prod_type,
    s.data_type as sandbox_type,
    CASE
        WHEN p.column_name IS NULL THEN 'added'
        WHEN s.column_name IS NULL THEN 'removed'
        WHEN p.data_type != s.data_type THEN 'type_changed'
        ELSE 'unchanged'
    END as change_type
FROM sandbox_cols s
FULL OUTER JOIN prod_cols p ON s.column_name = p.column_name
WHERE p.column_name IS NULL
   OR s.column_name IS NULL
   OR p.data_type != s.data_type
ORDER BY change_type, column_name
