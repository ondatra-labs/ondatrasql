-- Schema introspection macros for OndatraSQL
-- Loaded at session startup (PHASE 1, before DuckLake attach)

-- Type normalization: converts DuckDB type aliases to canonical form
-- E.g., INT -> INTEGER, FLOAT8 -> DOUBLE, BOOL -> BOOLEAN
-- MUST be defined first as other macros depend on it
CREATE OR REPLACE MACRO ondatra_normalize_type(p_type) AS
  CASE UPPER(TRIM(p_type))
    WHEN 'INT' THEN 'INTEGER'
    WHEN 'INT4' THEN 'INTEGER'
    WHEN 'SIGNED' THEN 'INTEGER'
    WHEN 'INT8' THEN 'BIGINT'
    WHEN 'LONG' THEN 'BIGINT'
    WHEN 'INT2' THEN 'SMALLINT'
    WHEN 'SHORT' THEN 'SMALLINT'
    WHEN 'INT1' THEN 'TINYINT'
    WHEN 'FLOAT4' THEN 'FLOAT'
    WHEN 'REAL' THEN 'FLOAT'
    WHEN 'FLOAT8' THEN 'DOUBLE'
    WHEN 'NUMERIC' THEN 'DOUBLE'
    WHEN 'STRING' THEN 'VARCHAR'
    WHEN 'CHAR' THEN 'VARCHAR'
    WHEN 'BPCHAR' THEN 'VARCHAR'
    WHEN 'BOOL' THEN 'BOOLEAN'
    ELSE UPPER(TRIM(p_type))
  END;

-- Type promotion check: returns true if old_type can be safely widened to new_type
-- Only allows widening conversions that preserve data (e.g., INTEGER -> BIGINT)
-- Note: DuckDB's can_cast_implicitly() is too permissive, so we use explicit rules
CREATE OR REPLACE MACRO ondatra_is_type_promotable(p_old_type, p_new_type) AS
  CASE
    -- Same type (after normalization) is always promotable
    WHEN ondatra_normalize_type(p_old_type) = ondatra_normalize_type(p_new_type) THEN true
    -- Integer widening paths
    WHEN ondatra_normalize_type(p_old_type) = 'TINYINT' AND ondatra_normalize_type(p_new_type) IN ('SMALLINT', 'INTEGER', 'BIGINT', 'HUGEINT', 'FLOAT', 'DOUBLE') THEN true
    WHEN ondatra_normalize_type(p_old_type) = 'SMALLINT' AND ondatra_normalize_type(p_new_type) IN ('INTEGER', 'BIGINT', 'HUGEINT', 'FLOAT', 'DOUBLE') THEN true
    WHEN ondatra_normalize_type(p_old_type) = 'INTEGER' AND ondatra_normalize_type(p_new_type) IN ('BIGINT', 'HUGEINT', 'FLOAT', 'DOUBLE') THEN true
    WHEN ondatra_normalize_type(p_old_type) = 'BIGINT' AND ondatra_normalize_type(p_new_type) IN ('HUGEINT', 'DOUBLE') THEN true
    -- Float widening
    WHEN ondatra_normalize_type(p_old_type) = 'FLOAT' AND ondatra_normalize_type(p_new_type) = 'DOUBLE' THEN true
    -- All other cases are not safe promotions
    ELSE false
  END;

-- Get columns with types for a table (schema.table format)
-- Returns normalized types using ondatra_normalize_type()
CREATE OR REPLACE MACRO ondatra_get_columns(p_schema, p_table) AS TABLE
  SELECT column_name || '|' || ondatra_normalize_type(data_type) AS col_info
  FROM information_schema.columns
  WHERE table_catalog = current_catalog() AND table_schema = p_schema AND table_name = p_table
  ORDER BY ordinal_position;

-- Get columns with types for a table (table name only)
-- Returns normalized types using ondatra_normalize_type()
CREATE OR REPLACE MACRO ondatra_get_columns_simple(p_table) AS TABLE
  SELECT column_name || '|' || ondatra_normalize_type(data_type) AS col_info
  FROM information_schema.columns
  WHERE table_name = p_table
  ORDER BY ordinal_position;

-- Get columns with types for a table (catalog.schema.table format)
-- Returns normalized types using ondatra_normalize_type()
CREATE OR REPLACE MACRO ondatra_get_columns_full(p_catalog, p_schema, p_table) AS TABLE
  SELECT column_name || '|' || ondatra_normalize_type(data_type) AS col_info
  FROM information_schema.columns
  WHERE table_catalog = p_catalog AND table_schema = p_schema AND table_name = p_table
  ORDER BY ordinal_position;

-- Check if table exists (schema.table format)
-- Uses current_catalog() to scope the check to the active catalog.
-- This prevents sandbox mode from finding prod tables.
CREATE OR REPLACE MACRO ondatra_table_exists(p_schema, p_table) AS
  (SELECT COUNT(*) FROM information_schema.tables
   WHERE table_catalog = current_catalog() AND table_schema = p_schema AND table_name = p_table) > 0;

-- Check if table exists (table name only)
CREATE OR REPLACE MACRO ondatra_table_exists_simple(p_table) AS
  (SELECT COUNT(*) FROM information_schema.tables
   WHERE table_name = p_table) > 0;

-- Check if table exists (catalog.schema.table format)
CREATE OR REPLACE MACRO ondatra_table_exists_full(p_catalog, p_schema, p_table) AS
  (SELECT COUNT(*) FROM information_schema.tables
   WHERE table_catalog = p_catalog AND table_schema = p_schema AND table_name = p_table) > 0;

-- Get column names only (for MERGE operations)
CREATE OR REPLACE MACRO ondatra_get_column_names(p_table) AS TABLE
  SELECT column_name
  FROM information_schema.columns
  WHERE table_name = p_table
  ORDER BY ordinal_position;

-- Get column names (schema.table format)
CREATE OR REPLACE MACRO ondatra_get_column_names_schema(p_schema, p_table) AS TABLE
  SELECT column_name
  FROM information_schema.columns
  WHERE table_catalog = current_catalog() AND table_schema = p_schema AND table_name = p_table
  ORDER BY ordinal_position;

-- Get column names (catalog.schema.table format)
CREATE OR REPLACE MACRO ondatra_get_column_names_full(p_catalog, p_schema, p_table) AS TABLE
  SELECT column_name
  FROM information_schema.columns
  WHERE table_catalog = p_catalog AND table_schema = p_schema AND table_name = p_table
  ORDER BY ordinal_position;

-- Check if column exists in table
CREATE OR REPLACE MACRO ondatra_column_exists(p_table, p_column) AS
  (SELECT COUNT(*) FROM information_schema.columns
   WHERE table_name = p_table AND column_name = p_column) > 0;

-- Get column type (returns normalized type)
CREATE OR REPLACE MACRO ondatra_column_type(p_table, p_column) AS
  (SELECT ondatra_normalize_type(data_type) FROM information_schema.columns
   WHERE table_name = p_table AND column_name = p_column);

-- CDC macros using DuckLake native table_changes function
-- These are more efficient than EXCEPT-based comparisons
-- table_changes returns: original columns + snapshot_id, rowid, change_type
-- change_type: 'insert', 'update_preimage', 'update_postimage', 'delete'

-- CDC: Find new/changed rows since snapshot (DuckLake native)
-- p_table must be schema.table format (e.g., 'staging.orders')
-- Returns inserts and updated rows (post-update state)
CREATE OR REPLACE MACRO ondatra_cdc_changes(p_table, p_snapshot) AS TABLE
  SELECT * EXCLUDE (snapshot_id, rowid, change_type)
  FROM query(
    printf('SELECT * FROM {{catalog}}.table_changes(''%s'', %d, (SELECT id FROM {{catalog}}.current_snapshot()))',
           p_table, p_snapshot + 1)
  )
  WHERE change_type IN ('insert', 'update_postimage');

-- CDC: Return empty result set (schema-preserving)
-- Used when no changes detected but we need column structure
CREATE OR REPLACE MACRO ondatra_cdc_empty(p_table) AS TABLE
  SELECT * FROM query(printf('SELECT * FROM %s WHERE false', p_table));

-- CDC: Find deleted rows since snapshot (DuckLake native)
-- Returns rows that were deleted between snapshots
CREATE OR REPLACE MACRO ondatra_cdc_deletes(p_table, p_snapshot) AS TABLE
  SELECT * EXCLUDE (snapshot_id, rowid, change_type)
  FROM query(
    printf('SELECT * FROM {{catalog}}.table_changes(''%s'', %d, (SELECT id FROM {{catalog}}.current_snapshot()))',
           p_table, p_snapshot + 1)
  )
  WHERE change_type = 'delete';

-- CDC: Find updated rows (pre-update state) since snapshot
-- Useful for SCD2 to get the old values before update
CREATE OR REPLACE MACRO ondatra_cdc_updates_before(p_table, p_snapshot) AS TABLE
  SELECT * EXCLUDE (snapshot_id, rowid, change_type)
  FROM query(
    printf('SELECT * FROM {{catalog}}.table_changes(''%s'', %d, (SELECT id FROM {{catalog}}.current_snapshot()))',
           p_table, p_snapshot + 1)
  )
  WHERE change_type = 'update_preimage';

-- CDC: Count of changed rows since snapshot (DuckLake native)
-- Returns a single row with counts of inserts, updates, deletes
CREATE OR REPLACE MACRO ondatra_cdc_summary(p_table, p_snapshot) AS TABLE
  SELECT
    COUNT(*) FILTER (WHERE change_type = 'insert') AS inserts,
    COUNT(*) FILTER (WHERE change_type = 'update_postimage') AS updates,
    COUNT(*) FILTER (WHERE change_type = 'delete') AS deletes
  FROM query(
    printf('SELECT change_type FROM {{catalog}}.table_changes(''%s'', %d, (SELECT id FROM {{catalog}}.current_snapshot()))',
           p_table, p_snapshot + 1)
  );

-- Table diff: Find all differences between two tables (useful for testing/validation)
-- Returns rows that exist in only one of the tables with a _source column
CREATE OR REPLACE MACRO ondatra_table_diff(p_table1, p_table2) AS TABLE
  SELECT * FROM query(
    printf('SELECT *, ''%s'' AS _source FROM (%s EXCEPT %s)
            UNION ALL
            SELECT *, ''%s'' AS _source FROM (%s EXCEPT %s)',
           p_table1, p_table1, p_table2, p_table2, p_table2, p_table1)
  );

-- Row count: Simple count wrapper (useful for dynamic table names)
CREATE OR REPLACE MACRO ondatra_row_count(p_table) AS
  (SELECT * FROM query(printf('SELECT COUNT(*)::BIGINT FROM %s', p_table)));

-- Check if table has any rows (faster than counting all rows)
CREATE OR REPLACE MACRO ondatra_has_rows(p_table) AS
  (SELECT * FROM query(printf('SELECT EXISTS(SELECT 1 FROM %s LIMIT 1)', p_table)));

-- Get min/max of a column (useful for incremental state)
CREATE OR REPLACE MACRO ondatra_min_value(p_table, p_column) AS
  (SELECT * FROM query(printf('SELECT MIN(%s) FROM %s', p_column, p_table)));

CREATE OR REPLACE MACRO ondatra_max_value(p_table, p_column) AS
  (SELECT * FROM query(printf('SELECT MAX(%s) FROM %s', p_column, p_table)));

-- Get distinct count of a column (useful for data quality checks)
CREATE OR REPLACE MACRO ondatra_distinct_count(p_table, p_column) AS
  (SELECT * FROM query(printf('SELECT COUNT(DISTINCT %s)::BIGINT FROM %s', p_column, p_table)));

-- Get null count of a column (useful for data quality checks)
CREATE OR REPLACE MACRO ondatra_null_count(p_table, p_column) AS
  (SELECT * FROM query(printf('SELECT COUNT(*)::BIGINT FROM %s WHERE %s IS NULL', p_table, p_column)));

-- Get duplicate count based on columns (useful for uniqueness validation)
-- Returns count of rows where the specified columns have duplicate values
CREATE OR REPLACE MACRO ondatra_duplicate_count(p_table, p_columns) AS
  (SELECT * FROM query(
    printf('SELECT COALESCE(SUM(cnt - 1), 0)::BIGINT FROM (SELECT COUNT(*) AS cnt FROM %s GROUP BY %s HAVING COUNT(*) > 1)',
           p_table, p_columns)
  ));

-- Batch type check: verify multiple type pairs are promotable
-- Takes comma-separated pairs like 'INTEGER,BIGINT,FLOAT,DOUBLE'
-- Returns true if all pairs (1->2, 3->4, etc.) are promotable
-- Uses parallel unnest to avoid list_zip unnamed struct issues (DuckDB 1.5+)
CREATE OR REPLACE MACRO ondatra_batch_type_check(p_pairs) AS
  NOT EXISTS (
    WITH arr AS (
      SELECT string_split(p_pairs, ',') AS types
    ),
    flat AS (
      SELECT unnest(types) AS type_name, unnest(range(len(types))) AS idx FROM arr
    ),
    type_pairs AS (
      SELECT
        MAX(CASE WHEN idx % 2 = 0 THEN type_name END) AS old_type,
        MAX(CASE WHEN idx % 2 = 1 THEN type_name END) AS new_type
      FROM flat
      GROUP BY idx // 2
    )
    SELECT 1 FROM type_pairs
    WHERE NOT ondatra_is_type_promotable(TRIM(old_type), TRIM(new_type))
  );

-- Get all column types for a table as a list (useful for batch comparisons)
-- Returns a single string with all column:type pairs separated by |
CREATE OR REPLACE MACRO ondatra_schema_string(p_table) AS
  (SELECT string_agg(column_name || ':' || ondatra_normalize_type(data_type), '|' ORDER BY ordinal_position)
   FROM information_schema.columns
   WHERE table_name = p_table);

-- Compare two tables and return summary of differences
-- Returns counts of inserts, deletes, and matching rows
CREATE OR REPLACE MACRO ondatra_compare_tables(p_table1, p_table2) AS TABLE
  SELECT * FROM query(
    printf('SELECT
      (SELECT COUNT(*) FROM %s EXCEPT SELECT COUNT(*) FROM %s) AS in_first_only,
      (SELECT COUNT(*) FROM %s EXCEPT SELECT COUNT(*) FROM %s) AS in_second_only,
      (SELECT COUNT(*) FROM %s INTERSECT SELECT COUNT(*) FROM %s) AS matching',
      p_table1, p_table2, p_table2, p_table1, p_table1, p_table2)
  );

-- SQL hash macro: normalizes SQL and computes md5 hash
-- Removes comments, normalizes whitespace, lowercases
-- Used when SQL string is available (e.g., for validation)
CREATE OR REPLACE MACRO ondatra_sql_hash(p_sql) AS
  md5(
    lower(
      regexp_replace(
        regexp_replace(
          regexp_replace(p_sql, '--[^\n]*', '', 'g'),  -- Remove -- comments
          '\s+', ' ', 'g'                              -- Normalize whitespace
        ),
        '^\s+|\s+$', '', 'g'                           -- Trim
      )
    )
  );

-- Schema hash macro: computes hash of table schema
-- Orders columns alphabetically for deterministic hash
-- Returns first 32 hex chars (16 bytes) like Go version
-- Uses normalized types for consistency
CREATE OR REPLACE MACRO ondatra_schema_hash(p_table) AS
  substring(
    md5(
      (SELECT string_agg(column_name || ':' || ondatra_normalize_type(data_type), ',' ORDER BY column_name)
       FROM information_schema.columns
       WHERE table_name = p_table)
    ), 1, 32
  );

-- Schema hash for schema.table format
CREATE OR REPLACE MACRO ondatra_schema_hash_qualified(p_schema, p_table) AS
  substring(
    md5(
      (SELECT string_agg(column_name || ':' || ondatra_normalize_type(data_type), ',' ORDER BY column_name)
       FROM information_schema.columns
       WHERE table_catalog = current_catalog() AND table_schema = p_schema AND table_name = p_table)
    ), 1, 32
  );
