-- ===============================================================
-- OndatraSQL constraint macros
--
-- Constraints run BEFORE data is inserted into the target table.
-- They validate the temp table (model output). If any constraint
-- returns a row, the model is aborted -- bad data never enters.
--
-- Convention: ondatra_constraint_{name}(t, ...) AS TABLE
--   t = table name as string (the temp table with model output)
--   Returns: 0 rows = pass, 1+ rows with error message = fail
--
-- Edit freely -- this file is yours.
-- ===============================================================


-- ── NULL & emptiness ──────────────────────────────────────────

-- Rejects rows where col is NULL.
-- @constraint: not_null(customer_id)
CREATE OR REPLACE MACRO ondatra_constraint_not_null(t, col) AS TABLE
  SELECT printf('NOT NULL failed: %s has %d NULL values', col, COUNT(*))
  FROM query(printf('SELECT %s AS c FROM %s', col, t))
  WHERE c IS NULL HAVING COUNT(*) > 0;

-- Rejects rows where col is NULL or blank (empty string after trim).
-- @constraint: not_empty(name)
CREATE OR REPLACE MACRO ondatra_constraint_not_empty(t, col) AS TABLE
  SELECT printf('NOT EMPTY failed: %s has %d empty/NULL values', col, COUNT(*))
  FROM query(printf('SELECT %s AS c FROM %s', col, t))
  WHERE c IS NULL OR TRIM(c::VARCHAR) = '' HAVING COUNT(*) > 0;

-- Fails if col has no non-NULL values at all (entire column is NULL).
-- @constraint: at_least_one(email)
CREATE OR REPLACE MACRO ondatra_constraint_at_least_one(t, col) AS TABLE
  SELECT printf('AT_LEAST_ONE failed: %s has no non-NULL values', col)
  WHERE (SELECT COUNT(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) = 0;

-- Fails if NULL percentage exceeds max_pct.
-- @constraint: null_percent(email, 10)
CREATE OR REPLACE MACRO ondatra_constraint_null_percent(t, col, max_pct) AS TABLE
  SELECT printf('NULL_PERCENT failed: %s has %.1f%% NULLs (max %d%%)', col,
    (SELECT 100.0 * SUM(CASE WHEN c IS NULL THEN 1 ELSE 0 END) / COUNT(*)
     FROM query(printf('SELECT %s AS c FROM %s', col, t))),
    max_pct)
  WHERE (SELECT 100.0 * SUM(CASE WHEN c IS NULL THEN 1 ELSE 0 END) / COUNT(*)
    FROM query(printf('SELECT %s AS c FROM %s', col, t))) >= max_pct;


-- ── Uniqueness ────────────────────────────────────────────────

-- Rejects if col has any duplicate values.
-- @constraint: unique(order_id)
CREATE OR REPLACE MACRO ondatra_constraint_unique(t, col) AS TABLE
  SELECT printf('UNIQUE failed: %s has duplicates (value %s appears %d times)', col, c::VARCHAR, COUNT(*))
  FROM query(printf('SELECT %s AS c FROM %s', col, t))
  GROUP BY c HAVING COUNT(*) > 1 LIMIT 1;

-- Rejects if combination of columns has duplicates.
-- Pass column names as a single quoted, comma-separated string.
-- @constraint: composite_unique('date, region')
CREATE OR REPLACE MACRO ondatra_constraint_composite_unique(t, cols) AS TABLE
  SELECT printf('COMPOSITE UNIQUE failed: (%s) has duplicates', cols)
  WHERE EXISTS(SELECT 1 FROM query(printf('SELECT %s FROM %s GROUP BY ALL HAVING COUNT(*) > 1', cols, t)));

-- Combines NOT NULL + UNIQUE -- standard primary key semantics.
-- @constraint: primary_key(id)
CREATE OR REPLACE MACRO ondatra_constraint_primary_key(t, col) AS TABLE
  SELECT printf('PRIMARY KEY failed: %s has %d NULLs and %d duplicates', col,
    (SELECT COUNT(*) FROM query(printf('SELECT %s AS c FROM %s', col, t)) WHERE c IS NULL),
    (SELECT COALESCE(SUM(cnt - 1), 0) FROM (SELECT COUNT(*) AS cnt FROM query(printf('SELECT %s AS c FROM %s', col, t)) GROUP BY c HAVING COUNT(*) > 1)))
  WHERE EXISTS(SELECT 1 FROM query(printf('SELECT %s AS c FROM %s', col, t)) WHERE c IS NULL)
     OR EXISTS(SELECT 1 FROM query(printf('SELECT %s AS c FROM %s', col, t)) GROUP BY c HAVING COUNT(*) > 1);


-- ── Value checks ──────────────────────────────────────────────

-- Rejects rows where col violates the comparison.
-- op: >=, >, <=, <, =, !=
-- @constraint: compare(amount, >=, 0)
-- @constraint: compare(status, =, 'active')
CREATE OR REPLACE MACRO ondatra_constraint_compare(t, col, op, val) AS TABLE
  SELECT printf('%s %s %s failed: %d violations', col, op, val::VARCHAR, COUNT(*))
  FROM query(printf('SELECT %s AS c FROM %s', col, t))
  WHERE CASE op
    WHEN '>=' THEN c < val WHEN '>' THEN c <= val
    WHEN '<=' THEN c > val WHEN '<' THEN c >= val
    WHEN '='  THEN c != val WHEN '!=' THEN c = val
    ELSE false END
  HAVING COUNT(*) > 0;

-- Rejects rows where col is outside [low, high].
-- @constraint: between(age, 0, 150)
CREATE OR REPLACE MACRO ondatra_constraint_between(t, col, low, high) AS TABLE
  SELECT printf('BETWEEN failed: %s has %d values outside [%s, %s]', col, COUNT(*), low::VARCHAR, high::VARCHAR)
  FROM query(printf('SELECT %s AS c FROM %s', col, t))
  WHERE c < low OR c > high HAVING COUNT(*) > 0;

-- Rejects rows where col is not in the comma-separated list of allowed values.
-- Pass values as a single quoted string: 'val1,val2,val3'
-- @constraint: in_list(status, 'active,pending,completed')
CREATE OR REPLACE MACRO ondatra_constraint_in_list(t, col, vals_str) AS TABLE
  SELECT printf('IN failed: %s has %d values not in (%s)', col, COUNT(*), vals_str)
  FROM query(printf('SELECT %s AS c FROM %s', col, t))
  WHERE c::VARCHAR NOT IN (SELECT TRIM(unnest) FROM unnest(string_split(vals_str, ',')))
  HAVING COUNT(*) > 0;

-- Rejects rows where col IS in the comma-separated list of forbidden values.
-- @constraint: not_in(status, 'deleted,banned,suspended')
CREATE OR REPLACE MACRO ondatra_constraint_not_in(t, col, vals_str) AS TABLE
  SELECT printf('NOT IN failed: %s has %d forbidden values', col, COUNT(*))
  FROM query(printf('SELECT %s AS c FROM %s', col, t))
  WHERE c::VARCHAR IN (SELECT TRIM(unnest) FROM unnest(string_split(vals_str, ',')))
  HAVING COUNT(*) > 0;

-- Fails if col has fewer than 2 distinct values (e.g. all rows identical).
-- @constraint: not_constant(category)
CREATE OR REPLACE MACRO ondatra_constraint_not_constant(t, col) AS TABLE
  SELECT printf('NOT_CONSTANT failed: %s has only %d distinct value(s)', col,
    (SELECT COUNT(DISTINCT c) FROM query(printf('SELECT %s AS c FROM %s', col, t))))
  WHERE (SELECT COUNT(DISTINCT c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) < 2;


-- ── Pattern matching ──────────────────────────────────────────

-- Rejects rows where col doesn't match SQL LIKE pattern.
-- @constraint: like(email, %@%)
CREATE OR REPLACE MACRO ondatra_constraint_like(t, col, pattern) AS TABLE
  SELECT printf('LIKE failed: %s has %d values not matching pattern', col, COUNT(*))
  FROM query(printf('SELECT %s AS c FROM %s', col, t))
  WHERE c NOT LIKE pattern HAVING COUNT(*) > 0;

-- Rejects rows where col matches the forbidden LIKE pattern.
-- @constraint: not_like(name, %test%)
CREATE OR REPLACE MACRO ondatra_constraint_not_like(t, col, pattern) AS TABLE
  SELECT printf('NOT LIKE failed: %s has values matching forbidden pattern', col)
  FROM query(printf('SELECT %s AS c FROM %s', col, t))
  WHERE c LIKE pattern HAVING COUNT(*) > 0;

-- Rejects rows where col doesn't fully match the regex.
-- Uses regexp_full_match (entire string must match, no anchors needed).
-- @constraint: matches(phone, \+[0-9]{10,15})
CREATE OR REPLACE MACRO ondatra_constraint_matches(t, col, regex) AS TABLE
  SELECT printf('MATCHES failed: %s has %d values not matching regex', col, COUNT(*))
  FROM query(printf('SELECT %s AS c FROM %s', col, t))
  WHERE NOT regexp_full_match(c::VARCHAR, regex) HAVING COUNT(*) > 0;


-- ── Format validation ─────────────────────────────────────────

-- Rejects rows with invalid email addresses (regex-based).
-- @constraint: email(contact_email)
CREATE OR REPLACE MACRO ondatra_constraint_email(t, col) AS TABLE
  SELECT printf('EMAIL failed: %s has %d invalid email addresses', col, COUNT(*))
  FROM query(printf('SELECT %s AS c FROM %s', col, t))
  WHERE c IS NOT NULL AND NOT regexp_full_match(c::VARCHAR, '[A-Za-z0-9._%%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}')
  HAVING COUNT(*) > 0;

-- Rejects rows with invalid UUIDs (uses TRY_CAST, not regex).
-- @constraint: uuid(transaction_id)
CREATE OR REPLACE MACRO ondatra_constraint_uuid(t, col) AS TABLE
  SELECT printf('UUID failed: %s has %d invalid UUIDs', col, COUNT(*))
  FROM query(printf('SELECT %s AS c FROM %s', col, t))
  WHERE c IS NOT NULL AND TRY_CAST(c AS UUID) IS NULL
  HAVING COUNT(*) > 0;

-- Rejects rows where col can't be cast to the expected DuckDB type.
-- Works for any type: DATE, TIMESTAMP, INTEGER, DECIMAL, UUID, etc.
-- @constraint: valid_type(order_date, DATE)
CREATE OR REPLACE MACRO ondatra_constraint_valid_type(t, col, expected_type) AS TABLE
  SELECT printf('valid_type failed: %s has %d values that cannot cast to %s', col, COUNT(*), expected_type)
  FROM query(printf('SELECT TRY_CAST(%s AS %s) AS casted, %s AS original FROM %s', col, expected_type, col, t))
  WHERE original IS NOT NULL AND casted IS NULL
  HAVING COUNT(*) > 0;


-- ── String length ─────────────────────────────────────────────

-- Rejects rows where string length is outside [low, high].
-- @constraint: length_between(postal_code, 4, 10)
CREATE OR REPLACE MACRO ondatra_constraint_length_between(t, col, low, high) AS TABLE
  SELECT printf('LENGTH BETWEEN failed: %s has %d values with length outside [%d, %d]', col, COUNT(*), low, high)
  FROM query(printf('SELECT %s AS c FROM %s', col, t))
  WHERE LENGTH(c::VARCHAR) < low OR LENGTH(c::VARCHAR) > high HAVING COUNT(*) > 0;

-- Rejects rows where string length is not exactly n.
-- @constraint: length_eq(country_code, 2)
CREATE OR REPLACE MACRO ondatra_constraint_length_eq(t, col, n) AS TABLE
  SELECT printf('LENGTH = %d failed: %s has %d violations', n, col, COUNT(*))
  FROM query(printf('SELECT %s AS c FROM %s', col, t))
  WHERE LENGTH(c::VARCHAR) != n HAVING COUNT(*) > 0;


-- ── Referential integrity ─────────────────────────────────────

-- Rejects rows where col has values not found in ref_table.ref_col (foreign key).
-- NULLs in col are ignored (nullable foreign keys are allowed).
-- @constraint: references(customer_id, staging.customers, id)
CREATE OR REPLACE MACRO ondatra_constraint_references(t, col, ref_table, ref_col) AS TABLE
  SELECT printf('REFERENCES failed: %s has orphan values not in %s.%s', col, ref_table, ref_col)
  WHERE EXISTS(
    SELECT 1 FROM query(printf('SELECT %s AS c FROM %s', col, t))
    WHERE c IS NOT NULL AND c NOT IN (SELECT c FROM query(printf('SELECT %s AS c FROM %s', ref_col, ref_table))));


-- ── Sequence & distribution ───────────────────────────────────

-- Rejects if integer col has gaps > 1 (expects consecutive sequence).
-- @constraint: sequential(line_number)
CREATE OR REPLACE MACRO ondatra_constraint_sequential(t, col) AS TABLE
  SELECT printf('SEQUENTIAL failed: %s has gap of %d at value %s', col, gap::INTEGER, c::VARCHAR)
  FROM (SELECT c, c - LAG(c) OVER (ORDER BY c) AS gap
        FROM query(printf('SELECT %s AS c FROM %s', col, t)))
  WHERE gap > 1 LIMIT 1;

-- Rejects if integer col has gaps > step.
-- @constraint: sequential_step(batch_id, 10)
CREATE OR REPLACE MACRO ondatra_constraint_sequential_step(t, col, step) AS TABLE
  SELECT printf('SEQUENTIAL(%d) failed: %s has gap at %s', step, col, c::VARCHAR)
  FROM (SELECT c, c - LAG(c) OVER (ORDER BY c) AS gap
        FROM query(printf('SELECT %s AS c FROM %s', col, t)))
  WHERE gap > step LIMIT 1;

-- Rejects if distinct value count doesn't meet the threshold.
-- op: >=, >, <=, <, =
-- @constraint: distinct_count(region, >=, 3)
CREATE OR REPLACE MACRO ondatra_constraint_distinct_count(t, col, op, n) AS TABLE
  SELECT printf('DISTINCT_COUNT failed: %s has %d distinct values', col,
    (SELECT COUNT(DISTINCT c) FROM query(printf('SELECT %s AS c FROM %s', col, t))))
  WHERE CASE op
    WHEN '>=' THEN (SELECT COUNT(DISTINCT c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) < n
    WHEN '>'  THEN (SELECT COUNT(DISTINCT c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) <= n
    WHEN '<=' THEN (SELECT COUNT(DISTINCT c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) > n
    WHEN '<'  THEN (SELECT COUNT(DISTINCT c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) >= n
    WHEN '='  THEN (SELECT COUNT(DISTINCT c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) != n
    ELSE false END;

-- Rejects if duplicate percentage exceeds max_pct.
-- @constraint: duplicate_percent(email, 5)
CREATE OR REPLACE MACRO ondatra_constraint_duplicate_percent(t, col, max_pct) AS TABLE
  SELECT printf('DUPLICATE_PERCENT failed: %s has %.1f%% duplicates', col,
    (SELECT (COUNT(*) - COUNT(DISTINCT c)) * 100.0 / NULLIF(COUNT(*), 0)
     FROM query(printf('SELECT %s AS c FROM %s', col, t))))
  WHERE (SELECT (COUNT(*) - COUNT(DISTINCT c)) * 100.0 / NULLIF(COUNT(*), 0)
    FROM query(printf('SELECT %s AS c FROM %s', col, t))) >= max_pct;


-- ── Interval & temporal ───────────────────────────────────────

-- Rejects if any date/time intervals overlap (cross-join comparison).
-- @constraint: no_overlap(start_date, end_date)
CREATE OR REPLACE MACRO ondatra_constraint_no_overlap(t, start_col, end_col) AS TABLE
  SELECT printf('NO_OVERLAP failed: (%s, %s) has overlapping intervals', start_col, end_col)
  WHERE EXISTS(
    SELECT 1
    FROM query(printf('SELECT %s AS s, %s AS e FROM %s', start_col, end_col, t)) a,
         query(printf('SELECT %s AS s, %s AS e FROM %s', start_col, end_col, t)) b
    WHERE a.s < b.s AND a.e > b.s);


-- ── Numeric safety ────────────────────────────────────────────

-- Rejects rows with NaN values (floating-point edge case).
-- @constraint: no_nan(amount)
CREATE OR REPLACE MACRO ondatra_constraint_no_nan(t, col) AS TABLE
  SELECT printf('no_nan failed: %s has %d NaN values', col, COUNT(*))
  FROM query(printf('SELECT %s AS c FROM %s', col, t))
  WHERE isnan(c) HAVING COUNT(*) > 0;

-- Rejects rows with infinite values (floating-point edge case).
-- @constraint: finite(amount)
CREATE OR REPLACE MACRO ondatra_constraint_finite(t, col) AS TABLE
  SELECT printf('finite failed: %s has non-finite values', col)
  FROM query(printf('SELECT %s AS c FROM %s', col, t))
  WHERE NOT isfinite(c) HAVING COUNT(*) > 0;


-- ── Conditional ───────────────────────────────────────────────

-- Rejects rows where col is NULL but the condition is true.
-- The condition is raw SQL evaluated against the table.
-- @constraint: required_if(delivery_date, status = 'delivered')
CREATE OR REPLACE MACRO ondatra_constraint_required_if(t, col, condition_sql) AS TABLE
  SELECT printf('REQUIRED_IF failed: %s is NULL where %s', col, condition_sql)
  WHERE EXISTS(SELECT 1 FROM query(printf('SELECT * FROM %s WHERE (%s) AND %s IS NULL', t, condition_sql, col)));

-- Arbitrary SQL expression check. Rejects rows where the expression is false.
-- The expression is raw SQL -- full power of DuckDB available.
-- @constraint: check(price, price > 0 AND price < 1000000)
-- @constraint: check(dates, start_date <= end_date)
CREATE OR REPLACE MACRO ondatra_constraint_check(t, col, expr_sql) AS TABLE
  SELECT printf('CHECK failed: %s has rows violating (%s)', col, expr_sql)
  WHERE EXISTS(SELECT 1 FROM query(printf('SELECT * FROM %s WHERE NOT (%s)', t, expr_sql)));
