// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package validation converts constraint/audit directives to SQL.
package validation

import (
	"fmt"
	"regexp"
	"strings"
)

// ConstraintToSQL converts a constraint directive to a validation SQL query.
// The returned query should return 0 rows if the constraint passes.
// On failure, returns a descriptive error message using DuckDB's printf().
// The table parameter is the temporary model table name (e.g., "tmp_model").
func ConstraintToSQL(directive, table string) (string, error) {
	directive = strings.TrimSpace(directive)
	if directive == "" {
		return "", fmt.Errorf("empty constraint")
	}

	// Try each pattern in order
	// NOTE: All handlers return a printf() message with details on failure
	patterns := []struct {
		regex   *regexp.Regexp
		handler func(matches []string, table string) string
	}{
		// col PRIMARY KEY -> NOT NULL + UNIQUE
		{
			regexp.MustCompile(`(?i)^(\w+)\s+PRIMARY\s+KEY$`),
			func(m []string, t string) string {
				col := m[1]
				return fmt.Sprintf(
					`SELECT printf('PRIMARY KEY failed: %%s has %%d NULL values and %%d duplicate values', '%s',
						(SELECT COUNT(*) FROM %s WHERE %s IS NULL),
						(SELECT COALESCE(SUM(cnt - 1), 0) FROM (SELECT COUNT(*) as cnt FROM %s GROUP BY %s HAVING COUNT(*) > 1)))
					WHERE EXISTS(SELECT 1 FROM %s WHERE %s IS NULL)
					   OR EXISTS(SELECT 1 FROM %s GROUP BY %s HAVING COUNT(*) > 1)`,
					col, t, col, t, col, t, col, t, col)
			},
		},
		// col NOT NULL
		{
			regexp.MustCompile(`(?i)^(\w+)\s+NOT\s+NULL$`),
			func(m []string, t string) string {
				col := m[1]
				return fmt.Sprintf(
					`SELECT printf('NOT NULL failed: %%s has %%d NULL values', '%s', COUNT(*))
					FROM %s WHERE %s IS NULL HAVING COUNT(*) > 0`,
					col, t, col)
			},
		},
		// col UNIQUE
		{
			regexp.MustCompile(`(?i)^(\w+)\s+UNIQUE$`),
			func(m []string, t string) string {
				col := m[1]
				return fmt.Sprintf(
					`SELECT printf('UNIQUE failed: %%s has %%d duplicate values (first duplicate: %%s appears %%d times)', '%s',
						(SELECT COALESCE(SUM(cnt - 1), 0) FROM (SELECT COUNT(*) as cnt FROM %s GROUP BY %s HAVING COUNT(*) > 1)),
						%s::VARCHAR, COUNT(*))
					FROM %s GROUP BY %s HAVING COUNT(*) > 1 LIMIT 1`,
					col, t, col, col, t, col)
			},
		},
		// (col1, col2) UNIQUE - composite unique
		{
			regexp.MustCompile(`(?i)^\(([^)]+)\)\s+UNIQUE$`),
			func(m []string, t string) string {
				cols := m[1]
				return fmt.Sprintf(
					`SELECT printf('UNIQUE failed: (%%s) has duplicate values', '%s')
					FROM %s GROUP BY %s HAVING COUNT(*) > 1 LIMIT 1`,
					cols, t, cols)
			},
		},
		// col NOT EMPTY
		{
			regexp.MustCompile(`(?i)^(\w+)\s+NOT\s+EMPTY$`),
			func(m []string, t string) string {
				col := m[1]
				return fmt.Sprintf(
					`SELECT printf('NOT EMPTY failed: %%s has %%d empty/NULL values', '%s', COUNT(*))
					FROM %s WHERE %s IS NULL OR TRIM(%s) = '' HAVING COUNT(*) > 0`,
					col, t, col, col)
			},
		},
		// col >= N (and other comparisons)
		// Bug 28: val may contain single quotes (e.g. 'active'). Escape them
		// for the printf-arg context, but pass val unchanged into the actual
		// WHERE clause where it's a SQL literal already.
		{
			regexp.MustCompile(`^(\w+)\s*(>=|<=|>|<|=|!=)\s*(.+)$`),
			func(m []string, t string) string {
				col, op, val := m[1], m[2], m[3]
				invOp := invertOp(op)
				escapedVal := escapeSQLString(val)
				return fmt.Sprintf(
					`SELECT printf('%%s %%s %%s failed: found %%d violations (example: %%s)', '%s', '%s', '%s', COUNT(*), MIN(%s)::VARCHAR)
					FROM %s WHERE %s %s %s HAVING COUNT(*) > 0`,
					col, op, escapedVal, col, t, col, invOp, val)
			},
		},
		// col BETWEEN x AND y
		{
			regexp.MustCompile(`(?i)^(\w+)\s+BETWEEN\s+(.+)\s+AND\s+(.+)$`),
			func(m []string, t string) string {
				col, low, high := m[1], m[2], m[3]
				return fmt.Sprintf(
					`SELECT printf('BETWEEN failed: %%s has %%d values outside [%%s, %%s]', '%s', COUNT(*), '%s', '%s')
					FROM %s WHERE %s < %s OR %s > %s HAVING COUNT(*) > 0`,
					col, low, high, t, col, low, col, high)
			},
		},
		// col IN ('a', 'b') or col IN (a, b) — auto-quotes unquoted strings
		{
			regexp.MustCompile(`(?i)^(\w+)\s+IN\s+\((.+)\)$`),
			func(m []string, t string) string {
				col := m[1]
				vals := quoteINValues(m[2])
				return fmt.Sprintf(
					`SELECT 'IN failed: %s has ' || COUNT(*) || ' values not in (%s)'
					FROM %s WHERE %s NOT IN (%s) HAVING COUNT(*) > 0`,
					col, escapeSQLString(vals), t, col, vals)
			},
		},
		// col NOT IN ('x') or col NOT IN (x) — auto-quotes unquoted strings
		{
			regexp.MustCompile(`(?i)^(\w+)\s+NOT\s+IN\s+\((.+)\)$`),
			func(m []string, t string) string {
				col := m[1]
				vals := quoteINValues(m[2])
				return fmt.Sprintf(
					`SELECT 'NOT IN failed: %s has ' || COUNT(*) || ' forbidden values'
					FROM %s WHERE %s IN (%s) HAVING COUNT(*) > 0`,
					col, t, col, vals)
			},
		},
		// col LIKE 'pattern'
		{
			regexp.MustCompile(`(?i)^(\w+)\s+LIKE\s+'([^']+)'$`),
			func(m []string, t string) string {
				col, pattern := m[1], m[2]
				return fmt.Sprintf(
					`SELECT printf('LIKE failed: %%s has %%d values not matching ''%%s''', '%s', COUNT(*), '%s')
					FROM %s WHERE %s NOT LIKE '%s' HAVING COUNT(*) > 0`,
					col, pattern, t, col, pattern)
			},
		},
		// col NOT LIKE 'pattern'
		{
			regexp.MustCompile(`(?i)^(\w+)\s+NOT\s+LIKE\s+'([^']+)'$`),
			func(m []string, t string) string {
				col, pattern := m[1], m[2]
				return fmt.Sprintf(
					`SELECT printf('NOT LIKE failed: %%s has %%d values matching forbidden pattern ''%%s''', '%s', COUNT(*), '%s')
					FROM %s WHERE %s LIKE '%s' HAVING COUNT(*) > 0`,
					col, pattern, t, col, pattern)
			},
		},
		// col REFERENCES table(pk) - foreign key
		{
			regexp.MustCompile(`(?i)^(\w+)\s+REFERENCES\s+(\S+)\((\w+)\)$`),
			func(m []string, t string) string {
				col, refTable, refCol := m[1], m[2], m[3]
				return fmt.Sprintf(
					`SELECT printf('REFERENCES failed: %%s has %%d orphan values not in %%s.%%s', '%s', COUNT(*), '%s', '%s')
					FROM %s WHERE %s IS NOT NULL AND %s NOT IN (SELECT %s FROM %s) HAVING COUNT(*) > 0`,
					col, refTable, refCol, t, col, col, refCol, refTable)
			},
		},
		// col MATCHES 'regex'
		{
			regexp.MustCompile(`(?i)^(\w+)\s+MATCHES\s+'([^']+)'$`),
			func(m []string, t string) string {
				col, pattern := m[1], m[2]
				return fmt.Sprintf(
					`SELECT printf('MATCHES failed: %%s has %%d values not matching regex ''%%s''', '%s', COUNT(*), '%s')
					FROM %s WHERE NOT regexp_matches(%s, '%s') HAVING COUNT(*) > 0`,
					col, pattern, t, col, pattern)
			},
		},
		// col EMAIL
		{
			regexp.MustCompile(`(?i)^(\w+)\s+EMAIL$`),
			func(m []string, t string) string {
				col := m[1]
				return fmt.Sprintf(
					`SELECT printf('EMAIL failed: %%s has %%d invalid email addresses', '%s', COUNT(*))
					FROM %s WHERE %s IS NOT NULL AND NOT regexp_matches(%s, '^[A-Za-z0-9._%%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') HAVING COUNT(*) > 0`,
					col, t, col, col)
			},
		},
		// col UUID
		{
			regexp.MustCompile(`(?i)^(\w+)\s+UUID$`),
			func(m []string, t string) string {
				col := m[1]
				return fmt.Sprintf(
					`SELECT printf('UUID failed: %%s has %%d invalid UUIDs', '%s', COUNT(*))
					FROM %s WHERE %s IS NOT NULL AND NOT regexp_matches(%s, '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$') HAVING COUNT(*) > 0`,
					col, t, col, col)
			},
		},
		// col LENGTH BETWEEN x AND y
		{
			regexp.MustCompile(`(?i)^(\w+)\s+LENGTH\s+BETWEEN\s+(\d+)\s+AND\s+(\d+)$`),
			func(m []string, t string) string {
				col, low, high := m[1], m[2], m[3]
				return fmt.Sprintf(
					`SELECT printf('LENGTH BETWEEN failed: %%s has %%d values with length outside [%%s, %%s]', '%s', COUNT(*), '%s', '%s')
					FROM %s WHERE LENGTH(%s) < %s OR LENGTH(%s) > %s HAVING COUNT(*) > 0`,
					col, low, high, t, col, low, col, high)
			},
		},
		// col LENGTH = N
		{
			regexp.MustCompile(`(?i)^(\w+)\s+LENGTH\s*=\s*(\d+)$`),
			func(m []string, t string) string {
				col, n := m[1], m[2]
				return fmt.Sprintf(
					`SELECT printf('LENGTH failed: %%s has %%d values with length != %%s', '%s', COUNT(*), '%s')
					FROM %s WHERE LENGTH(%s) != %s HAVING COUNT(*) > 0`,
					col, n, t, col, n)
			},
		},
		// col CHECK (expr)
		{
			regexp.MustCompile(`(?i)^(\w+)\s+CHECK\s+\((.+)\)$`),
			func(m []string, t string) string {
				col, expr := m[1], m[2]
				return fmt.Sprintf(
					`SELECT printf('CHECK failed: %%s has %%d rows violating (%%s)', '%s', COUNT(*), '%s')
					FROM %s WHERE NOT (%s) HAVING COUNT(*) > 0`,
					col, expr, t, expr)
			},
		},
		// col AT_LEAST_ONE - at least one non-NULL value
		{
			regexp.MustCompile(`(?i)^(\w+)\s+AT_LEAST_ONE$`),
			func(m []string, t string) string {
				col := m[1]
				return fmt.Sprintf(
					`SELECT printf('AT_LEAST_ONE failed: %%s has no non-NULL values', '%s')
					WHERE (SELECT COUNT(%s) FROM %s) = 0`,
					col, col, t)
			},
		},
		// col NOT_CONSTANT - at least 2 distinct values
		{
			regexp.MustCompile(`(?i)^(\w+)\s+NOT_CONSTANT$`),
			func(m []string, t string) string {
				col := m[1]
				return fmt.Sprintf(
					`SELECT printf('NOT_CONSTANT failed: %%s has only %%d distinct value(s)', '%s', (SELECT COUNT(DISTINCT %s) FROM %s))
					WHERE (SELECT COUNT(DISTINCT %s) FROM %s) < 2`,
					col, col, t, col, t)
			},
		},
		// col NULL_PERCENT < N
		{
			regexp.MustCompile(`(?i)^(\w+)\s+NULL_PERCENT\s*<\s*(\d+)$`),
			func(m []string, t string) string {
				col, pct := m[1], m[2]
				return fmt.Sprintf(
					`SELECT printf('NULL_PERCENT failed: %%s has %%.1f%%%% NULL values (threshold: <%%s%%%%)', '%s',
						(SELECT 100.0 * SUM(CASE WHEN %s IS NULL THEN 1 ELSE 0 END) / COUNT(*) FROM %s), '%s')
					WHERE (SELECT 100.0 * SUM(CASE WHEN %s IS NULL THEN 1 ELSE 0 END) / COUNT(*) FROM %s) >= %s`,
					col, col, t, pct, col, t, pct)
			},
		},
		// col SEQUENTIAL
		{
			regexp.MustCompile(`(?i)^(\w+)\s+SEQUENTIAL$`),
			func(m []string, t string) string {
				col := m[1]
				return fmt.Sprintf(
					`SELECT printf('SEQUENTIAL failed: %%s has gap of %%d at value %%s', '%s', gap::INTEGER, %s::VARCHAR)
					FROM (SELECT %s, %s - LAG(%s) OVER (ORDER BY %s) AS gap FROM %s) WHERE gap > 1 LIMIT 1`,
					col, col, col, col, col, col, t)
			},
		},
		// col SEQUENTIAL(N)
		{
			regexp.MustCompile(`(?i)^(\w+)\s+SEQUENTIAL\((\d+)\)$`),
			func(m []string, t string) string {
				col, step := m[1], m[2]
				return fmt.Sprintf(
					`SELECT printf('SEQUENTIAL(%%s) failed: %%s has gap of %%d at value %%s', '%s', '%s', gap::INTEGER, %s::VARCHAR)
					FROM (SELECT %s, %s - LAG(%s) OVER (ORDER BY %s) AS gap FROM %s) WHERE gap > %s LIMIT 1`,
					step, col, col, col, col, col, col, t, step)
			},
		},
		// col DISTINCT_COUNT OP N
		{
			regexp.MustCompile(`(?i)^(\w+)\s+DISTINCT_COUNT\s*(>=|<=|>|<|=)\s*(\d+)$`),
			func(m []string, t string) string {
				col, op, n := m[1], m[2], m[3]
				invOp := invertOp(op)
				return fmt.Sprintf(
					`SELECT printf('DISTINCT_COUNT failed: %%s has %%d distinct values (expected %s %s)', '%s', (SELECT COUNT(DISTINCT %s) FROM %s))
					WHERE (SELECT COUNT(DISTINCT %s) FROM %s) %s %s`,
					op, n, col, col, t, col, t, invOp, n)
			},
		},
		// col DUPLICATE_PERCENT < N
		{
			regexp.MustCompile(`(?i)^(\w+)\s+DUPLICATE_PERCENT\s*<\s*(\d+)$`),
			func(m []string, t string) string {
				col, pct := m[1], m[2]
				return fmt.Sprintf(
					`SELECT printf('DUPLICATE_PERCENT failed: %%s has %%.1f%%%% duplicates (threshold: <%%s%%%%)', '%s',
						(SELECT (COUNT(*) - COUNT(DISTINCT %s)) * 100.0 / NULLIF(COUNT(*), 0) FROM %s), '%s')
					WHERE (SELECT (COUNT(*) - COUNT(DISTINCT %s)) * 100.0 / NULLIF(COUNT(*), 0) FROM %s) >= %s`,
					col, col, t, pct, col, t, pct)
			},
		},
		// (start, end) NO_OVERLAP
		{
			regexp.MustCompile(`(?i)^\((\w+),\s*(\w+)\)\s+NO_OVERLAP$`),
			func(m []string, t string) string {
				startCol, endCol := m[1], m[2]
				return fmt.Sprintf(
					`SELECT printf('NO_OVERLAP failed: (%%s, %%s) has overlapping intervals', '%s', '%s')
					FROM %s a, %s b WHERE a.rowid < b.rowid AND a.%s < b.%s AND a.%s > b.%s LIMIT 1`,
					startCol, endCol, t, t, startCol, endCol, endCol, startCol)
			},
		},
	}

	// All patterns use (?i) for case-insensitive keyword matching.
	// Captures preserve original case (column names, literals, paths).
	for _, p := range patterns {
		if matches := p.regex.FindStringSubmatch(directive); matches != nil {
			return p.handler(matches, table), nil
		}
	}

	return "", fmt.Errorf("unknown constraint format: %s", directive)
}

// ConstraintsToBatchSQL converts multiple constraint directives to a single batched SQL query.
// Returns a query that returns all constraint violations in one round-trip.
// Each row in the result contains an error message for a failed constraint.
// If all constraints pass, the query returns no rows.
func ConstraintsToBatchSQL(directives []string, table string) (string, []error) {
	if len(directives) == 0 {
		return "", nil
	}

	var queries []string
	var parseErrors []error

	for _, directive := range directives {
		sql, err := ConstraintToSQL(directive, table)
		if err != nil {
			parseErrors = append(parseErrors, fmt.Errorf("constraint parse error: %w", err))
			continue
		}
		// Wrap each constraint query as a subquery that returns error_msg
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

// quoteINValues ensures each comma-separated value in an IN list is a valid SQL literal.
// Unquoted non-numeric values are wrapped in single quotes.
// Already-quoted values and numbers are left as-is.
func quoteINValues(vals string) string {
	// Split on commas while respecting single-quoted strings.
	parts := splitRespectingQuotes(vals)
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		// Already properly quoted (starts and ends with ')
		if len(p) >= 2 && p[0] == '\'' && p[len(p)-1] == '\'' {
			parts[i] = p
			continue
		}
		// Numeric (integer or float)
		isNumeric := true
		for j, c := range p {
			if c == '.' || c == '-' || c == '+' {
				if c == '-' || c == '+' {
					if j != 0 {
						isNumeric = false
						break
					}
				}
				continue
			}
			if c < '0' || c > '9' {
				isNumeric = false
				break
			}
		}
		if isNumeric && p != "" && p != "-" && p != "+" && p != "." {
			parts[i] = p
			continue
		}
		// Unquoted string — wrap in quotes, escape any internal quotes
		escaped := strings.ReplaceAll(p, "'", "''")
		parts[i] = "'" + escaped + "'"
	}
	return strings.Join(parts, ", ")
}

// splitRespectingQuotes splits a string on commas, but ignores commas inside
// single-quoted segments. Handles escaped quotes ('').
func splitRespectingQuotes(s string) []string {
	var parts []string
	var current strings.Builder
	inQuote := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\'' {
			if inQuote && i+1 < len(s) && s[i+1] == '\'' {
				// Escaped quote inside quoted string
				current.WriteByte('\'')
				current.WriteByte('\'')
				i++
				continue
			}
			inQuote = !inQuote
			current.WriteByte(c)
		} else if c == ',' && !inQuote {
			parts = append(parts, current.String())
			current.Reset()
		} else {
			current.WriteByte(c)
		}
	}
	parts = append(parts, current.String())
	return parts
}

// escapeSQLString doubles single quotes for use inside SQL string literals.
func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// invertOp returns the inverse of a comparison operator.
func invertOp(op string) string {
	switch op {
	case ">=":
		return "<"
	case "<=":
		return ">"
	case ">":
		return "<="
	case "<":
		return ">="
	case "=":
		return "!="
	case "!=":
		return "="
	default:
		return op
	}
}
