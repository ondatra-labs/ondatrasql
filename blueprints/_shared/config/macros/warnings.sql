-- ===============================================================
-- OndatraSQL warning macros -- complete definitions for testing
-- Convention: ondatra_warning_{name}(t, ...) AS TABLE
-- Returns: 0 rows = pass, 1+ rows with warning message = logged
-- Runs AFTER commit -- never blocks, only informs.
-- ===============================================================

-- 1. Freshness warning
-- Usage: @warning: freshness(updated_at, 24h)
CREATE OR REPLACE MACRO ondatra_warning_freshness(t, col, interval_str) AS TABLE
  SELECT printf('freshness warning: %s max is %s (threshold: %s)',
    col,
    COALESCE((SELECT MAX(c)::TIMESTAMP::VARCHAR FROM query(printf('SELECT %s AS c FROM %s', col, t))), 'NULL'),
    interval_str)
  WHERE (SELECT MAX(c)::TIMESTAMP FROM query(printf('SELECT %s AS c FROM %s', col, t))) IS NULL
     OR (SELECT MAX(c)::TIMESTAMP FROM query(printf('SELECT %s AS c FROM %s', col, t))) < NOW() - INTERVAL (interval_str);

-- 2. Row count warning
-- Usage: @warning: row_count(>=, 100)
CREATE OR REPLACE MACRO ondatra_warning_row_count(t, op, n) AS TABLE
  SELECT printf('row_count warning: %s %d -- actual is %d', op, n,
    (SELECT COUNT(*) FROM query(printf('SELECT * FROM %s', t))))
  WHERE CASE op
    WHEN '>=' THEN (SELECT COUNT(*) FROM query(printf('SELECT * FROM %s', t))) < n
    WHEN '>'  THEN (SELECT COUNT(*) FROM query(printf('SELECT * FROM %s', t))) <= n
    WHEN '<=' THEN (SELECT COUNT(*) FROM query(printf('SELECT * FROM %s', t))) > n
    WHEN '<'  THEN (SELECT COUNT(*) FROM query(printf('SELECT * FROM %s', t))) >= n
    WHEN '='  THEN (SELECT COUNT(*) FROM query(printf('SELECT * FROM %s', t))) != n
    ELSE true END;

-- ── Delta warnings (compare current vs previous run) ──────────
-- These use table_changes() which requires:
--   - getvariable('prev_model_snapshot') set by runner per model
--   - getvariable('curr_snapshot') set at session init
--   - USE lake.{schema} set by runner before warning dispatch
-- table_changes() takes bare table name -- split_part extracts it.
-- Skips on first run (prev_model_snapshot = 0).
--
-- table_changes() returns change_type per row:
--   insert, delete, update_preimage, update_postimage
-- We count only real changes (exclude update_preimage to avoid
-- double-counting updates). rowid is preserved across compaction
-- so file reorganization does not produce false positives.

-- 3. Row count delta warning
-- @warning: row_count_delta(20)
CREATE OR REPLACE MACRO ondatra_warning_row_count_delta(t, max_pct) AS TABLE
  SELECT printf('row_count_delta warning: %d inserts, %d updates, %d deletes out of %d rows (%.1f%%)',
    ins, upd, del, curr_cnt, (ins + upd + del) * 100.0 / curr_cnt)
  FROM (SELECT
    COUNT(*) FILTER (WHERE change_type = 'insert') AS ins,
    COUNT(*) FILTER (WHERE change_type = 'update_postimage') AS upd,
    COUNT(*) FILTER (WHERE change_type = 'delete') AS del
    FROM table_changes(
      split_part(t, '.', 2),
      (getvariable('prev_model_snapshot') + 1)::BIGINT,
      getvariable('curr_snapshot')::BIGINT))
  CROSS JOIN (SELECT COUNT(*) AS curr_cnt FROM query(printf('SELECT * FROM %s', t)))
  WHERE getvariable('prev_model_snapshot') > 0
    AND curr_cnt > 0
    AND (ins + upd + del) * 100.0 / curr_cnt > max_pct;

-- 4. Sum delta warning (by change count ratio)
-- @warning: sum_delta(amount, 10)
CREATE OR REPLACE MACRO ondatra_warning_sum_delta(t, col, max_pct) AS TABLE
  SELECT printf('sum_delta(%s) warning: %d inserts, %d updates, %d deletes out of %d rows',
    col, ins, upd, del, curr_cnt)
  FROM (SELECT
    COUNT(*) FILTER (WHERE change_type = 'insert') AS ins,
    COUNT(*) FILTER (WHERE change_type = 'update_postimage') AS upd,
    COUNT(*) FILTER (WHERE change_type = 'delete') AS del
    FROM table_changes(
      split_part(t, '.', 2),
      (getvariable('prev_model_snapshot') + 1)::BIGINT,
      getvariable('curr_snapshot')::BIGINT))
  CROSS JOIN (SELECT COUNT(*) AS curr_cnt FROM query(printf('SELECT * FROM %s', t)))
  WHERE getvariable('prev_model_snapshot') > 0
    AND curr_cnt > 0
    AND (ins + upd + del) * 100.0 / curr_cnt > max_pct;

-- 5. Mean delta warning (by change count ratio)
-- @warning: mean_delta(amount, 15)
CREATE OR REPLACE MACRO ondatra_warning_mean_delta(t, col, max_pct) AS TABLE
  SELECT printf('mean_delta(%s) warning: %d inserts, %d updates, %d deletes out of %d rows',
    col, ins, upd, del, curr_cnt)
  FROM (SELECT
    COUNT(*) FILTER (WHERE change_type = 'insert') AS ins,
    COUNT(*) FILTER (WHERE change_type = 'update_postimage') AS upd,
    COUNT(*) FILTER (WHERE change_type = 'delete') AS del
    FROM table_changes(
      split_part(t, '.', 2),
      (getvariable('prev_model_snapshot') + 1)::BIGINT,
      getvariable('curr_snapshot')::BIGINT))
  CROSS JOIN (SELECT COUNT(*) AS curr_cnt FROM query(printf('SELECT * FROM %s', t)))
  WHERE getvariable('prev_model_snapshot') > 0
    AND curr_cnt > 0
    AND (ins + upd + del) * 100.0 / curr_cnt > max_pct;

-- 6. Approaching limit
-- Usage: @warning: approaching_limit(amount)
-- Reads getvariable('alert_amount_threshold') from variables.sql
CREATE OR REPLACE MACRO ondatra_warning_approaching_limit(t, col) AS TABLE
  SELECT printf('approaching limit: %s max is %s (threshold: %s, 90%% = %s)',
    col,
    (SELECT MAX(c)::VARCHAR FROM query(printf('SELECT %s AS c FROM %s', col, t))),
    getvariable('alert_amount_threshold')::VARCHAR,
    (getvariable('alert_amount_threshold') * 0.9)::VARCHAR)
  WHERE (SELECT MAX(c) FROM query(printf('SELECT %s AS c FROM %s', col, t)))
     > getvariable('alert_amount_threshold') * 0.9;

-- 7. NULL percentage warning
-- Usage: @warning: null_percent(email, 5)
CREATE OR REPLACE MACRO ondatra_warning_null_percent(t, col, max_pct) AS TABLE
  SELECT printf('null_percent warning: %s has %.1f%% NULLs (threshold: %d%%)', col,
    (SELECT 100.0 * SUM(CASE WHEN c IS NULL THEN 1 ELSE 0 END) / COUNT(*)
     FROM query(printf('SELECT %s AS c FROM %s', col, t))),
    max_pct)
  WHERE (SELECT 100.0 * SUM(CASE WHEN c IS NULL THEN 1 ELSE 0 END) / COUNT(*)
    FROM query(printf('SELECT %s AS c FROM %s', col, t))) >= max_pct;

-- 8. Not null warning (logs NULL count without blocking)
-- Usage: @warning: not_null(customer_id)
CREATE OR REPLACE MACRO ondatra_warning_not_null(t, col) AS TABLE
  SELECT printf('not_null warning: %s has %d NULL values', col, COUNT(*))
  FROM query(printf('SELECT %s AS c FROM %s', col, t))
  WHERE c IS NULL HAVING COUNT(*) > 0;

-- 9. Unique warning (logs duplicates without blocking)
-- Usage: @warning: unique(order_id)
CREATE OR REPLACE MACRO ondatra_warning_unique(t, col) AS TABLE
  SELECT printf('unique warning: %s has duplicates (value %s appears %d times)', col, c::VARCHAR, COUNT(*))
  FROM query(printf('SELECT %s AS c FROM %s', col, t))
  GROUP BY c HAVING COUNT(*) > 1 LIMIT 1;

-- 10. Min value warning
-- Usage: @warning: min(amount, >, 0)
CREATE OR REPLACE MACRO ondatra_warning_min(t, col, op, val) AS TABLE
  SELECT printf('min(%s) %s %s warning: actual min is %s',
    col, op, val::VARCHAR,
    COALESCE((SELECT MIN(c)::VARCHAR FROM query(printf('SELECT %s AS c FROM %s', col, t))), 'NULL'))
  WHERE (SELECT MIN(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) IS NULL
     OR CASE op
    WHEN '>=' THEN (SELECT MIN(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) < val
    WHEN '>'  THEN (SELECT MIN(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) <= val
    WHEN '<=' THEN (SELECT MIN(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) > val
    WHEN '<'  THEN (SELECT MIN(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) >= val
    ELSE false END;

-- 11. Compare warning (row-level value check)
-- Usage: @warning: compare(amount, >=, 0)
CREATE OR REPLACE MACRO ondatra_warning_compare(t, col, op, val) AS TABLE
  SELECT printf('%s %s %s warning: %d violations', col, op, val::VARCHAR, COUNT(*))
  FROM query(printf('SELECT %s AS c FROM %s', col, t))
  WHERE CASE op
    WHEN '>=' THEN c < val WHEN '>' THEN c <= val
    WHEN '<=' THEN c > val WHEN '<' THEN c >= val
    WHEN '='  THEN c != val WHEN '!=' THEN c = val
    ELSE false END
  HAVING COUNT(*) > 0;

-- 12. Mean warning
-- Usage: @warning: mean(amount, >=, 100)
CREATE OR REPLACE MACRO ondatra_warning_mean(t, col, op, val) AS TABLE
  SELECT printf('mean(%s) %s %s warning: actual mean is %s',
    col, op, val::VARCHAR,
    COALESCE((SELECT AVG(c)::VARCHAR FROM query(printf('SELECT %s AS c FROM %s', col, t))), 'NULL'))
  WHERE CASE op
    WHEN '>=' THEN (SELECT AVG(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) < val
    WHEN '<'  THEN (SELECT AVG(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) >= val
    ELSE false END;

-- 13. Low entropy warning (data quality signal)
-- Usage: @warning: low_entropy(status, 1.0)
-- Logs if column entropy drops below threshold -- may indicate data quality issues
CREATE OR REPLACE MACRO ondatra_warning_low_entropy(t, col, min_entropy) AS TABLE
  SELECT printf('low_entropy warning: %s has entropy %.2f (threshold: %s)',
    col,
    COALESCE((SELECT ENTROPY(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))), 0),
    min_entropy::VARCHAR)
  WHERE (SELECT ENTROPY(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) < min_entropy;

-- 9. Cardinality warning (unexpected number of distinct values)
-- Usage: @warning: cardinality(customer_id, >=, 100)
CREATE OR REPLACE MACRO ondatra_warning_cardinality(t, col, op, n) AS TABLE
  SELECT printf('cardinality warning: %s has %d distinct values',
    col, (SELECT APPROX_COUNT_DISTINCT(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))))
  WHERE CASE op
    WHEN '>=' THEN (SELECT APPROX_COUNT_DISTINCT(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) < n
    WHEN '<=' THEN (SELECT APPROX_COUNT_DISTINCT(c) FROM query(printf('SELECT %s AS c FROM %s', col, t))) > n
    ELSE false END;
