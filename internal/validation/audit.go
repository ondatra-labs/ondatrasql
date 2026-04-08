// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package validation

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// percentileFractionRe matches the fraction in `percentile(col, N)` so we
// can validate it before pattern dispatch — DuckDB's PERCENTILE_CONT only
// accepts 0–1, but the percentage convention is intuitive enough that
// users will write `95` and hit a cryptic Binder Error otherwise. (Bug 32)
var percentileFractionRe = regexp.MustCompile(`(?i)^percentile\(\s*\w+\s*,\s*([0-9.]+)\s*\)`)

// AuditToSQL converts an audit directive to a validation SQL query.
// The returned query should return 0 rows if the audit passes.
// On failure, returns a descriptive error message using DuckDB's printf().
//
// `table` is the target table reference for current-data clauses (e.g.
// `raw.zzz_snap`). `historicalTable` is the reference used inside time-travel
// `AT (VERSION => N)` clauses. In normal (prod) runs the two are identical.
// In sandbox mode the caller passes a catalog-prefixed name like
// `lake.raw.zzz_snap` so historical audits can read prod's snapshot history
// while the current-data side still reads the sandbox materialization.
// `prevSnapshot` is the previous snapshot ID for historical comparisons.
func AuditToSQL(directive, table, historicalTable string, prevSnapshot int64) (string, error) {
	directive = strings.TrimSpace(directive)
	if directive == "" {
		return "", fmt.Errorf("empty audit")
	}
	if historicalTable == "" {
		historicalTable = table
	}

	// Bug 32: catch percentile fractions outside [0,1] before pattern
	// dispatch so the user gets a clear, actionable parse-time error
	// instead of a downstream "PERCENTILEs can only take parameters in
	// the range [0, 1]" Binder Error at run time.
	if pm := percentileFractionRe.FindStringSubmatch(directive); pm != nil {
		if f, err := strconv.ParseFloat(pm[1], 64); err == nil {
			if f < 0 || f > 1 {
				return "", fmt.Errorf(
					"percentile fraction must be in [0, 1]: got %s — use a fraction like 0.95, not the percentage 95",
					pm[1])
			}
		}
	}

	patterns := []struct {
		regex   *regexp.Regexp
		handler func(matches []string, table, histTable string, prev int64) string
	}{
		// row_count >= N
		{
			regexp.MustCompile(`(?i)^row_count\s*(>=|<=|>|<|=)\s*(\d+)$`),
			func(m []string, t string, _ string, _ int64) string {
				op, n := m[1], m[2]
				invOp := invertOp(op)
				return fmt.Sprintf(
					`SELECT printf('row_count %%s %%s failed: actual count is %%d', '%s', '%s', (SELECT COUNT(*) FROM %s))
					WHERE (SELECT COUNT(*) FROM %s) %s %s`,
					op, n, t, t, invOp, n)
			},
		},
		// row_count_change < N%
		{
			regexp.MustCompile(`(?i)^row_count_change\s*<\s*(\d+)%%?$`),
			func(m []string, t, h string, prev int64) string {
				pct := m[1]
				if prev == 0 {
					return "SELECT 1 WHERE 0" // No previous snapshot, pass
				}
				// `t` = current data (sandbox in sandbox mode); `h` =
				// historical reference (catalog-prefixed in sandbox so the
				// AT VERSION clause finds prod's snapshot).
				return fmt.Sprintf(
					`SELECT printf('row_count_change failed: changed %%.1f%%%% (threshold: <%%s%%%%)',
						ABS((SELECT COUNT(*) FROM %s) - (SELECT COUNT(*) FROM %s AT (VERSION => %d))) * 100.0
						/ NULLIF((SELECT COUNT(*) FROM %s AT (VERSION => %d)), 0), '%s')
					WHERE ABS((SELECT COUNT(*) FROM %s) - (SELECT COUNT(*) FROM %s AT (VERSION => %d))) * 100.0
						/ NULLIF((SELECT COUNT(*) FROM %s AT (VERSION => %d)), 0) >= %s`,
					t, h, prev, h, prev, pct, t, h, prev, h, prev, pct)
			},
		},
		// freshness(col, 24h)
		{
			regexp.MustCompile(`(?i)^freshness\((\w+),\s*(\d+)([hd])\)$`),
			func(m []string, t string, _ string, _ int64) string {
				col, n, unit := m[1], m[2], m[3]
				var interval, unitName string
				if unit == "d" {
					interval, unitName = n+" DAY", "days"
				} else {
					interval, unitName = n+" HOUR", "hours"
				}
				return fmt.Sprintf(
					`SELECT printf('freshness failed: %%s max value is %%s (threshold: %%s %%s)', '%s',
						(SELECT MAX(%s)::TIMESTAMP::VARCHAR FROM %s), '%s', '%s')
					WHERE (SELECT MAX(%s)::TIMESTAMP FROM %s) < NOW() - INTERVAL '%s'`,
					col, col, t, n, unitName, col, t, interval)
			},
		},
		// mean(col) BETWEEN x AND y
		{
			regexp.MustCompile(`(?i)^mean\((\w+)\)\s+BETWEEN\s+(.+)\s+AND\s+(.+)$`),
			func(m []string, t string, _ string, _ int64) string {
				col, low, high := m[1], m[2], m[3]
				return fmt.Sprintf(
					`SELECT printf('mean(%%s) BETWEEN failed: actual mean is %%.4f (expected [%%s, %%s])', '%s',
						(SELECT AVG(%s) FROM %s), '%s', '%s')
					WHERE (SELECT AVG(%s) FROM %s) < %s OR (SELECT AVG(%s) FROM %s) > %s`,
					col, col, t, low, high, col, t, low, col, t, high)
			},
		},
		// mean(col) >= x (and other comparisons)
		{
			regexp.MustCompile(`(?i)^mean\((\w+)\)\s*(>=|<=|>|<|=)\s*(.+)$`),
			func(m []string, t string, _ string, _ int64) string {
				col, op, val := m[1], m[2], m[3]
				invOp := invertOp(op)
				return fmt.Sprintf(
					`SELECT printf('mean(%%s) %%s %%s failed: actual mean is %%.4f', '%s', '%s', '%s', (SELECT AVG(%s) FROM %s))
					WHERE (SELECT AVG(%s) FROM %s) %s %s`,
					col, op, val, col, t, col, t, invOp, val)
			},
		},
		// stddev(col) < X
		{
			regexp.MustCompile(`(?i)^stddev\((\w+)\)\s*<\s*(.+)$`),
			func(m []string, t string, _ string, _ int64) string {
				col, val := m[1], m[2]
				return fmt.Sprintf(
					`SELECT printf('stddev(%%s) < %%s failed: actual stddev is %%.4f', '%s', '%s', (SELECT STDDEV(%s) FROM %s))
					WHERE (SELECT STDDEV(%s) FROM %s) >= %s`,
					col, val, col, t, col, t, val)
			},
		},
		// min(col) >= X
		{
			regexp.MustCompile(`(?i)^min\((\w+)\)\s*(>=|<=|>|<|=)\s*(.+)$`),
			func(m []string, t string, _ string, _ int64) string {
				col, op, val := m[1], m[2], m[3]
				invOp := invertOp(op)
				return fmt.Sprintf(
					`SELECT printf('min(%%s) %%s %%s failed: actual min is %%s', '%s', '%s', '%s', (SELECT MIN(%s)::VARCHAR FROM %s))
					WHERE (SELECT MIN(%s) FROM %s) %s %s`,
					col, op, val, col, t, col, t, invOp, val)
			},
		},
		// max(col) <= X
		{
			regexp.MustCompile(`(?i)^max\((\w+)\)\s*(>=|<=|>|<|=)\s*(.+)$`),
			func(m []string, t string, _ string, _ int64) string {
				col, op, val := m[1], m[2], m[3]
				invOp := invertOp(op)
				return fmt.Sprintf(
					`SELECT printf('max(%%s) %%s %%s failed: actual max is %%s', '%s', '%s', '%s', (SELECT MAX(%s)::VARCHAR FROM %s))
					WHERE (SELECT MAX(%s) FROM %s) %s %s`,
					col, op, val, col, t, col, t, invOp, val)
			},
		},
		// sum(col) < X
		{
			regexp.MustCompile(`(?i)^sum\((\w+)\)\s*(>=|<=|>|<|=)\s*(.+)$`),
			func(m []string, t string, _ string, _ int64) string {
				col, op, val := m[1], m[2], m[3]
				invOp := invertOp(op)
				return fmt.Sprintf(
					`SELECT printf('sum(%%s) %%s %%s failed: actual sum is %%s', '%s', '%s', '%s', (SELECT SUM(%s)::VARCHAR FROM %s))
					WHERE (SELECT SUM(%s) FROM %s) %s %s`,
					col, op, val, col, t, col, t, invOp, val)
			},
		},
		// zscore(col) < N
		{
			regexp.MustCompile(`(?i)^zscore\((\w+)\)\s*<\s*(.+)$`),
			func(m []string, t string, _ string, _ int64) string {
				col, n := m[1], m[2]
				return fmt.Sprintf(
					`SELECT printf('zscore(%%s) < %%s failed: found outlier with zscore %%.2f', '%s', '%s',
						ABS((%s - (SELECT AVG(%s) FROM %s)) / NULLIF((SELECT STDDEV(%s) FROM %s), 0)))
					FROM %s WHERE ABS((%s - (SELECT AVG(%s) FROM %s)) / NULLIF((SELECT STDDEV(%s) FROM %s), 0)) >= %s LIMIT 1`,
					col, n, col, col, t, col, t, t, col, col, t, col, t, n)
			},
		},
		// percentile(col, 0.95) < X
		{
			regexp.MustCompile(`(?i)^percentile\((\w+),\s*([0-9.]+)\)\s*(>=|<=|>|<|=)\s*(.+)$`),
			func(m []string, t string, _ string, _ int64) string {
				col, pct, op, val := m[1], m[2], m[3], m[4]
				invOp := invertOp(op)
				return fmt.Sprintf(
					`SELECT printf('percentile(%%s, %%s) %%s %%s failed: actual is %%s', '%s', '%s', '%s', '%s',
						(SELECT PERCENTILE_CONT(%s) WITHIN GROUP (ORDER BY %s) FROM %s)::VARCHAR)
					WHERE (SELECT PERCENTILE_CONT(%s) WITHIN GROUP (ORDER BY %s) FROM %s) %s %s`,
					col, pct, op, val, pct, col, t, pct, col, t, invOp, val)
			},
		},
		// reconcile_count(other_table)
		{
			regexp.MustCompile(`(?i)^reconcile_count\((.+)\)$`),
			func(m []string, t string, _ string, _ int64) string {
				other := m[1]
				return fmt.Sprintf(
					`SELECT printf('reconcile_count failed: %%s has %%d rows, %%s has %%d rows', '%s',
						(SELECT COUNT(*) FROM %s), '%s', (SELECT COUNT(*) FROM %s))
					WHERE (SELECT COUNT(*) FROM %s) != (SELECT COUNT(*) FROM %s)`,
					t, t, other, other, t, other)
			},
		},
		// reconcile_sum(col, other.col)
		{
			regexp.MustCompile(`(?i)^reconcile_sum\((\w+),\s*(\S+)\.(\w+)\)$`),
			func(m []string, t string, _ string, _ int64) string {
				col, other, otherCol := m[1], m[2], m[3]
				return fmt.Sprintf(
					`SELECT printf('reconcile_sum failed: %%s.%%s = %%s, %%s.%%s = %%s', '%s', '%s',
						(SELECT SUM(%s)::VARCHAR FROM %s), '%s', '%s', (SELECT SUM(%s)::VARCHAR FROM %s))
					WHERE (SELECT SUM(%s) FROM %s) != (SELECT SUM(%s) FROM %s)`,
					t, col, col, t, other, otherCol, otherCol, other, col, t, otherCol, other)
			},
		},
		// column_exists(col) - introspection via DESCRIBE (DuckLake-compatible)
		{
			regexp.MustCompile(`(?i)^column_exists\((\w+)\)$`),
			func(m []string, t string, _ string, _ int64) string {
				col := m[1]
				return fmt.Sprintf(
					`SELECT 'column_exists failed: column %s not found in %s'
					WHERE NOT EXISTS(SELECT 1 FROM (DESCRIBE %s) WHERE column_name = '%s')`,
					col, t, t, col)
			},
		},
		// column_type(col, TYPE) - introspection via DESCRIBE (DuckLake-compatible)
		// TYPE can be e.g. DECIMAL, DECIMAL(18,2), VARCHAR(255)
		{
			regexp.MustCompile(`(?i)^column_type\((\w+),\s*([\w()., ]+)\)$`),
			func(m []string, t string, _ string, _ int64) string {
				col, typ := m[1], strings.TrimSpace(m[2])
				upperTyp := strings.ToUpper(typ)
				return fmt.Sprintf(
					`SELECT 'column_type failed: %s.%s is ' ||
						COALESCE((SELECT UPPER(column_type) FROM (DESCRIBE %s) WHERE column_name = '%s' LIMIT 1), 'NOT FOUND') ||
						' (expected %s)'
					WHERE COALESCE((SELECT UPPER(column_type) FROM (DESCRIBE %s) WHERE column_name = '%s' LIMIT 1), '') != '%s'`,
					t, col, t, col, upperTyp, t, col, upperTyp)
			},
		},
		// golden('path/to/expected.csv')
		{
			regexp.MustCompile(`(?i)^golden\('([^']+)'\)$`),
			func(m []string, t string, _ string, _ int64) string {
				path := m[1]
				// Compare with CSV file - return diff count
				return fmt.Sprintf(
					`SELECT printf('golden failed: %%d rows differ from %%s',
						(SELECT COUNT(*) FROM (SELECT * FROM %s EXCEPT SELECT * FROM read_csv_auto('%s')
						 UNION ALL SELECT * FROM read_csv_auto('%s') EXCEPT SELECT * FROM %s)), '%s')
					WHERE EXISTS(SELECT * FROM %s EXCEPT SELECT * FROM read_csv_auto('%s'))
					   OR EXISTS(SELECT * FROM read_csv_auto('%s') EXCEPT SELECT * FROM %s)`,
					t, path, path, t, path, t, path, path, t)
			},
		},
		// distribution(col) STABLE
		{
			regexp.MustCompile(`(?i)^distribution\((\w+)\)\s+STABLE(?:\(([0-9.]+)\))?$`),
			func(m []string, t, h string, prev int64) string {
				col := m[1]
				threshold := "0.1"
				if len(m) > 2 && m[2] != "" {
					threshold = m[2]
				}
				if prev == 0 {
					return "SELECT 1 WHERE 0" // No previous snapshot, pass
				}
				// `t` = current data; `h` = historical reference for AT VERSION.
				return fmt.Sprintf(
					`WITH curr AS (SELECT %s, COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () AS pct FROM %s GROUP BY %s),
					prev AS (SELECT %s, COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () AS pct FROM %s AT (VERSION => %d) GROUP BY %s)
					SELECT printf('distribution(%%s) STABLE failed: value %%s changed by %%.1f%%%% (threshold: %%s)', '%s',
						c.%s::VARCHAR, ABS(COALESCE(c.pct, 0) - COALESCE(p.pct, 0)) * 100, '%s')
					FROM curr c LEFT JOIN prev p ON c.%s = p.%s
					WHERE ABS(COALESCE(c.pct, 0) - COALESCE(p.pct, 0)) > %s LIMIT 1`,
					col, t, col, col, h, prev, col, col, col, threshold, col, col, threshold)
			},
		},
	}

	// All patterns use (?i) for case-insensitive keyword matching.
	// Captures preserve original case (column names, literals, paths).
	for _, p := range patterns {
		if matches := p.regex.FindStringSubmatch(directive); matches != nil {
			return p.handler(matches, table, historicalTable, prevSnapshot), nil
		}
	}

	return "", fmt.Errorf("unknown audit format: %s", directive)
}

// splitSchemaTable splits a "schema.table" target into its parts.
// Returns ("main", table) if no schema is specified.
func splitSchemaTable(target string) (string, string) {
	parts := strings.SplitN(target, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "main", target
}

// AuditsToTransactionalSQL wraps the AuditsToBatchSQL output so that any
// failing audit raises a DuckDB error() and aborts the surrounding
// transaction. Used by materialize() to fold audits inside the same
// BEGIN/COMMIT as the data write — a failing audit then rolls back the
// ALTER, the INSERT, and the commit metadata together, eliminating the
// "metadata ahead of physical state" divergence that the old
// post-commit + rollback() pattern was prone to after a crash window.
//
// Returns "" if there are no audits (caller can skip the check entirely).
// Parse errors are returned alongside the SQL so callers can surface them
// the same way they did with AuditsToBatchSQL.
//
// Implementation note: each individual audit query already returns at
// most one row containing the error message (or no rows on success), so
// the wrapper just collects all messages with string_agg and calls
// error() if any are present. error() is a scalar that throws on
// evaluation; wrapping it in a SELECT means the throw fires only when
// at least one audit produced a row, and the message contains every
// failing audit's text.
func AuditsToTransactionalSQL(directives []string, table, historicalTable string, prevSnapshot int64) (string, []error) {
	batchSQL, parseErrors := AuditsToBatchSQL(directives, table, historicalTable, prevSnapshot)
	if batchSQL == "" {
		return "", parseErrors
	}
	// CTE form: aggregate all failing-audit messages once, then call
	// error() only if string_agg produced a non-NULL value (i.e. at
	// least one audit failed). Two notes:
	//   1. The `AS r(audit_msg)` column-rename is needed because the
	//      individual audit queries don't share a stable column name
	//      across UNION ALL branches.
	//   2. string_agg over zero matching rows returns NULL, so the
	//      WHERE on the outer SELECT cleanly skips the error() call
	//      when every audit passed.
	wrapped := fmt.Sprintf(
		"WITH audit_failures AS ("+
			"SELECT string_agg(audit_msg, '; ') AS msg FROM (%s) AS r(audit_msg) "+
			"WHERE audit_msg IS NOT NULL"+
			") "+
			"SELECT error('audit failed: ' || msg) FROM audit_failures WHERE msg IS NOT NULL",
		batchSQL,
	)
	return wrapped, parseErrors
}

// AuditsToBatchSQL converts multiple audit directives to a single batched SQL query.
// Returns a query that returns all audit violations in one round-trip.
// Each row in the result contains an error message for a failed audit.
// If all audits pass, the query returns no rows.
//
// `historicalTable` lets the caller redirect time-travel `AT (VERSION => N)`
// clauses to a different reference (e.g. `lake.raw.foo` in sandbox mode so
// historical audits read prod's snapshot history). If empty, defaults to
// `table`.
func AuditsToBatchSQL(directives []string, table, historicalTable string, prevSnapshot int64) (string, []error) {
	if len(directives) == 0 {
		return "", nil
	}

	var queries []string
	var parseErrors []error

	for _, directive := range directives {
		sql, err := AuditToSQL(directive, table, historicalTable, prevSnapshot)
		if err != nil {
			parseErrors = append(parseErrors, fmt.Errorf("audit parse error: %w", err))
			continue
		}
		// Wrap each audit query as a subquery that returns error_msg
		// The original queries already return printf() message or nothing
		queries = append(queries, fmt.Sprintf("(%s)", sql))
	}

	if len(queries) == 0 {
		return "", parseErrors
	}

	// Combine all queries with UNION ALL
	// Each subquery returns at most one row with the error message
	batchSQL := strings.Join(queries, "\nUNION ALL\n")

	return batchSQL, parseErrors
}
