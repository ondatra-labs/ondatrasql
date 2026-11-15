-- ===============================================================
-- OndatraSQL audit macros
--
-- Audits run INSIDE the materialize transaction, after data is
-- written but before commit. If any audit returns a row,
-- error() aborts the transaction -- schema changes, data writes,
-- and commit metadata all roll back atomically.
--
-- Convention: ondatra_audit_{name}(t, ...) AS TABLE
--   t = table name as string (the target table, post-write)
--   Returns: 0 rows = pass, 1+ rows with error message = fail
--
-- Edit freely -- this file is yours.
-- ===============================================================


-- ── Row count ─────────────────────────────────────────────────

-- Fails if row count doesn't meet the threshold.
-- op: >=, >, <=, <, =
-- @audit: row_count(>=, 1)
-- @audit: row_count(=, 1000)
CREATE OR REPLACE MACRO ondatra_audit_row_count(t, op, n) AS TABLE
  SELECT printf('row_count %s %d failed: actual count is %d', op, n,
    (SELECT COUNT(*) FROM query(printf('SELECT * FROM %s', t))))
  WHERE CASE op
    WHEN '>=' THEN (SELECT COUNT(*) FROM query(printf('SELECT * FROM %s', t))) < n
    WHEN '>'  THEN (SELECT COUNT(*) FROM query(printf('SELECT * FROM %s', t))) <= n
    WHEN '<=' THEN (SELECT COUNT(*) FROM query(printf('SELECT * FROM %s', t))) > n
    WHEN '<'  THEN (SELECT COUNT(*) FROM query(printf('SELECT * FROM %s', t))) >= n
    WHEN '='  THEN (SELECT COUNT(*) FROM query(printf('SELECT * FROM %s', t))) != n
    ELSE true END;


-- ── Freshness ─────────────────────────────────────────────────

-- Fails if MAX(col) is older than the interval.
-- interval_str: DuckDB interval, e.g. '24 HOUR', '7 DAY'
-- @audit: freshness(updated_at, 24h)
CREATE OR REPLACE MACRO ondatra_audit_freshness(t, col, interval_str) AS TABLE
  SELECT printf('freshness(%s) failed: max is %s (threshold: %s)',
    col,
    COALESCE((SELECT MAX(c)::TIMESTAMP::VARCHAR FROM query(printf('SELECT %s AS c FROM %s', col, t))), 'NULL'),
    interval_str)
  WHERE (SELECT MAX(c)::TIMESTAMP FROM query(printf('SELECT %s AS c FROM %s', col, t))) IS NULL
     OR (SELECT MAX(c)::TIMESTAMP FROM query(printf('SELECT %s AS c FROM %s', col, t))) < NOW() - INTERVAL (interval_str);


-- ── Statistical aggregates ────────────────────────────────────

-- Fails if AVG(col) is outside [low, high].
-- @audit: mean_between(amount, 50, 500)
CREATE OR REPLACE MACRO ondatra_audit_mean_between(t, col, low, high) AS TABLE
  SELECT printf('mean(%s) BETWEEN failed: actual mean is %s (expected [%s, %s])',
    col,
    COALESCE((SELECT AVG(c)::VARCHAR FROM query(printf('SELECT %s AS c FROM %s', col, t))), 'NULL'),
    low::VARCHAR, high::VARCHAR)
  WHERE (SELECT AVG(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) IS NULL
     OR (SELECT AVG(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) < low
     OR (SELECT AVG(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) > high;

-- Fails if AVG(col) doesn't meet the comparison.
-- @audit: mean(amount, >=, 100)
CREATE OR REPLACE MACRO ondatra_audit_mean(t, col, op, val) AS TABLE
  SELECT printf('mean(%s) %s %s failed: actual mean is %s',
    col, op, val::VARCHAR,
    COALESCE((SELECT AVG(c)::VARCHAR FROM query(printf('SELECT %s AS c FROM %s', col, t))), 'NULL'))
  WHERE (SELECT AVG(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) IS NULL
     OR CASE op
    WHEN '>=' THEN (SELECT AVG(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) < val
    WHEN '>'  THEN (SELECT AVG(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) <= val
    WHEN '<=' THEN (SELECT AVG(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) > val
    WHEN '<'  THEN (SELECT AVG(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) >= val
    WHEN '='  THEN (SELECT AVG(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) != val
    ELSE false END;

-- Fails if MEDIAN(col) doesn't meet the comparison.
-- @audit: median(amount, >=, 100)
CREATE OR REPLACE MACRO ondatra_audit_median(t, col, op, val) AS TABLE
  SELECT printf('median(%s) %s %s failed: actual median is %s',
    col, op, val::VARCHAR,
    COALESCE((SELECT MEDIAN(c)::VARCHAR FROM query(printf('SELECT %s AS c FROM %s', col, t))), 'NULL'))
  WHERE (SELECT MEDIAN(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) IS NULL
     OR CASE op
    WHEN '>=' THEN (SELECT MEDIAN(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) < val
    WHEN '<=' THEN (SELECT MEDIAN(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) > val
    WHEN '='  THEN (SELECT MEDIAN(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) != val
    ELSE false END;

-- Fails if STDDEV(col) exceeds the threshold.
-- @audit: stddev(amount, 50)
CREATE OR REPLACE MACRO ondatra_audit_stddev(t, col, max_val) AS TABLE
  SELECT printf('stddev(%s) < %s failed: actual stddev is %s',
    col, max_val::VARCHAR,
    COALESCE((SELECT STDDEV(c)::VARCHAR FROM query(printf('SELECT %s AS c FROM %s', col, t))), 'NULL'))
  WHERE (SELECT STDDEV(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) IS NULL
     OR (SELECT STDDEV(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) >= max_val;

-- Fails if MIN(col) doesn't meet the comparison.
-- @audit: min(amount, >=, 0)
CREATE OR REPLACE MACRO ondatra_audit_min(t, col, op, val) AS TABLE
  SELECT printf('min(%s) %s %s failed: actual min is %s',
    col, op, val::VARCHAR,
    COALESCE((SELECT MIN(c)::VARCHAR FROM query(printf('SELECT %s AS c FROM %s', col, t))), 'NULL'))
  WHERE (SELECT MIN(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) IS NULL
     OR CASE op
    WHEN '>=' THEN (SELECT MIN(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) < val
    WHEN '>'  THEN (SELECT MIN(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) <= val
    WHEN '<=' THEN (SELECT MIN(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) > val
    WHEN '<'  THEN (SELECT MIN(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) >= val
    WHEN '='  THEN (SELECT MIN(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) != val
    ELSE false END;

-- Fails if MAX(col) doesn't meet the comparison.
-- @audit: max(amount, <=, 1000000)
CREATE OR REPLACE MACRO ondatra_audit_max(t, col, op, val) AS TABLE
  SELECT printf('max(%s) %s %s failed: actual max is %s',
    col, op, val::VARCHAR,
    COALESCE((SELECT MAX(c)::VARCHAR FROM query(printf('SELECT %s AS c FROM %s', col, t))), 'NULL'))
  WHERE (SELECT MAX(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) IS NULL
     OR CASE op
    WHEN '>=' THEN (SELECT MAX(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) < val
    WHEN '>'  THEN (SELECT MAX(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) <= val
    WHEN '<=' THEN (SELECT MAX(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) > val
    WHEN '<'  THEN (SELECT MAX(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) >= val
    WHEN '='  THEN (SELECT MAX(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) != val
    ELSE false END;

-- Fails if SUM(col) doesn't meet the comparison.
-- @audit: sum(amount, >=, 0)
CREATE OR REPLACE MACRO ondatra_audit_sum(t, col, op, val) AS TABLE
  SELECT printf('sum(%s) %s %s failed: actual sum is %s',
    col, op, val::VARCHAR,
    COALESCE((SELECT SUM(c)::VARCHAR FROM query(printf('SELECT %s AS c FROM %s', col, t))), 'NULL'))
  WHERE (SELECT SUM(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) IS NULL
     OR CASE op
    WHEN '>=' THEN (SELECT SUM(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) < val
    WHEN '>'  THEN (SELECT SUM(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) <= val
    WHEN '<=' THEN (SELECT SUM(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) > val
    WHEN '<'  THEN (SELECT SUM(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) >= val
    WHEN '='  THEN (SELECT SUM(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) != val
    ELSE false END;


-- ── Outlier detection ─────────────────────────────────────────

-- Fails if any row's z-score exceeds the threshold.
-- @audit: zscore(amount, 3)
CREATE OR REPLACE MACRO ondatra_audit_zscore(t, col, threshold) AS TABLE
  SELECT printf('zscore(%s) < %s failed: outlier detected', col, threshold::VARCHAR)
  WHERE (SELECT AVG(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) IS NULL
     OR EXISTS(
        SELECT 1 FROM query(printf('SELECT %s AS c FROM %s', col, t))
        WHERE ABS((c - (SELECT AVG(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))))
          / NULLIF((SELECT STDDEV(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))), 0)) >= threshold);

-- Fails if the percentile doesn't meet the comparison.
-- pct must be a fraction 0-1 (0.95 = 95th percentile).
-- @audit: percentile(amount, 0.95, <=, 1000)
CREATE OR REPLACE MACRO ondatra_audit_percentile(t, col, pct, op, val) AS TABLE
  SELECT printf('percentile(%s, %s) %s %s failed: actual is %s',
    col, pct::VARCHAR, op, val::VARCHAR,
    COALESCE((SELECT PERCENTILE_CONT(pct) WITHIN GROUP (ORDER BY c)::VARCHAR
      FROM query(printf('SELECT %s AS c FROM %s', col, t))), 'NULL'))
  WHERE CASE op
    WHEN '>=' THEN (SELECT PERCENTILE_CONT(pct) WITHIN GROUP (ORDER BY c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) < val
    WHEN '>'  THEN (SELECT PERCENTILE_CONT(pct) WITHIN GROUP (ORDER BY c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) <= val
    WHEN '<=' THEN (SELECT PERCENTILE_CONT(pct) WITHIN GROUP (ORDER BY c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) > val
    WHEN '<'  THEN (SELECT PERCENTILE_CONT(pct) WITHIN GROUP (ORDER BY c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) >= val
    WHEN '='  THEN (SELECT PERCENTILE_CONT(pct) WITHIN GROUP (ORDER BY c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) != val
    ELSE false END;

-- Fails if ENTROPY(col) doesn't meet the comparison.
-- Low entropy may indicate data quality issues (e.g. a column that's mostly one value).
-- @audit: entropy(status, >=, 1.0)
CREATE OR REPLACE MACRO ondatra_audit_entropy(t, col, op, val) AS TABLE
  SELECT printf('entropy(%s) %s %s failed: actual entropy is %s',
    col, op, val::VARCHAR,
    COALESCE((SELECT ENTROPY(c)::VARCHAR FROM query(printf('SELECT %s AS c FROM %s', col, t))), 'NULL'))
  WHERE CASE op
    WHEN '>=' THEN (SELECT ENTROPY(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) < val
    WHEN '<=' THEN (SELECT ENTROPY(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) > val
    ELSE false END;

-- Fails if approximate distinct count doesn't meet the threshold.
-- Uses HyperLogLog -- fast on large tables.
-- @audit: approx_distinct(customer_id, >=, 1000)
CREATE OR REPLACE MACRO ondatra_audit_approx_distinct(t, col, op, n) AS TABLE
  SELECT printf('approx_distinct(%s) %s %d failed: approx count is %d',
    col, op, n,
    (SELECT APPROX_COUNT_DISTINCT(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))))
  WHERE CASE op
    WHEN '>=' THEN (SELECT APPROX_COUNT_DISTINCT(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) < n
    WHEN '<=' THEN (SELECT APPROX_COUNT_DISTINCT(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) > n
    WHEN '='  THEN (SELECT APPROX_COUNT_DISTINCT(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) != n
    ELSE false END;


-- ── Reconciliation ────────────────────────────────────────────

-- Fails if this table and other_table have different row counts.
-- @audit: reconcile_count(staging.orders_backup)
CREATE OR REPLACE MACRO ondatra_audit_reconcile_count(t, other) AS TABLE
  SELECT printf('reconcile_count failed: %s has %d rows, %s has %d rows',
    t, (SELECT COUNT(*) FROM query(printf('SELECT * FROM %s', t))),
    other, (SELECT COUNT(*) FROM query(printf('SELECT * FROM %s', other))))
  WHERE (SELECT COUNT(*) FROM query(printf('SELECT * FROM %s', t)))
     != (SELECT COUNT(*) FROM query(printf('SELECT * FROM %s', other)));

-- Fails if SUM(col) differs from SUM(other_col) in other_table.
-- Uses IS DISTINCT FROM so NULL sums (empty tables) are caught correctly.
-- @audit: reconcile_sum(amount, staging.totals, total)
CREATE OR REPLACE MACRO ondatra_audit_reconcile_sum(t, col, other_table, other_col) AS TABLE
  SELECT printf('reconcile_sum failed: %s.%s = %s, %s.%s = %s',
    t, col, COALESCE((SELECT SUM(c)::VARCHAR FROM query(printf('SELECT %s AS c FROM %s', col, t))), 'NULL'),
    other_table, other_col, COALESCE((SELECT SUM(c)::VARCHAR FROM query(printf('SELECT %s AS c FROM %s', other_col, other_table))), 'NULL'))
  WHERE (SELECT SUM(c) FROM query(printf('SELECT %s AS c FROM %s', col, t)))
     IS DISTINCT FROM (SELECT SUM(c) FROM query(printf('SELECT %s AS c FROM %s', other_col, other_table)));


-- ── Schema introspection ──────────────────────────────────────

-- Fails if column doesn't exist in the table (DuckLake-compatible via DESCRIBE).
-- @audit: column_exists(customer_id)
CREATE OR REPLACE MACRO ondatra_audit_column_exists(t, col) AS TABLE
  SELECT printf('column_exists failed: column %s not found in %s', col, t)
  WHERE NOT EXISTS(SELECT 1 FROM query(printf('DESCRIBE %s', t)) WHERE column_name = col);

-- Fails if column type doesn't match (case-insensitive comparison).
-- @audit: column_type(amount, DECIMAL)
CREATE OR REPLACE MACRO ondatra_audit_column_type(t, col, expected_type) AS TABLE
  SELECT printf('column_type failed: %s.%s is %s (expected %s)',
    t, col,
    COALESCE((SELECT UPPER(column_type) FROM query(printf('DESCRIBE %s', t)) WHERE column_name = col LIMIT 1), 'NOT FOUND'),
    UPPER(expected_type))
  WHERE COALESCE((SELECT UPPER(column_type) FROM query(printf('DESCRIBE %s', t)) WHERE column_name = col LIMIT 1), '')
     != UPPER(expected_type);


-- ── Golden file comparison ────────────────────────────────────

-- Fails if table content differs from a CSV file (bidirectional EXCEPT).
-- @audit: golden(expected/orders.csv)
CREATE OR REPLACE MACRO ondatra_audit_golden(t, csv_path) AS TABLE
  SELECT printf('golden failed: %d rows differ from %s',
    (SELECT COUNT(*) FROM (
      SELECT * FROM query(printf('SELECT * FROM %s', t)) EXCEPT SELECT * FROM read_csv_auto(csv_path)
      UNION ALL
      SELECT * FROM read_csv_auto(csv_path) EXCEPT SELECT * FROM query(printf('SELECT * FROM %s', t))
    )), csv_path)
  WHERE EXISTS(SELECT * FROM query(printf('SELECT * FROM %s', t)) EXCEPT SELECT * FROM read_csv_auto(csv_path))
     OR EXISTS(SELECT * FROM read_csv_auto(csv_path) EXCEPT SELECT * FROM query(printf('SELECT * FROM %s', t)));


-- ── Delta detection ────────────────────────────────────────────
-- Delta patterns (row_count_delta, sum_delta, mean_delta, checksum) are
-- available as WARNINGS only -- not audits. They use table_changes() which
-- cannot run inside transactions (DuckDB limitation: subqueries in table
-- functions). Warnings run after commit, where table_changes() works.
--
-- See the warning macros section below for delta patterns.
-- For DORA Art. 10 compliance, warnings (detection + logging) are sufficient.
