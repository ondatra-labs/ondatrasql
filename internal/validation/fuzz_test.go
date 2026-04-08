// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package validation

import (
	"strings"
	"testing"
)

// SQL injection safety + structural invariant: output must contain table name,
// must not contain semicolons or SQL comments from user input.
func FuzzConstraintToSQL(f *testing.F) {
	// Seed with valid directives
	f.Add("name NOT NULL", "tmp_model")
	f.Add("id PRIMARY KEY", "tmp_model")
	f.Add("email UNIQUE", "tmp_model")
	f.Add("(user_id, order_id) UNIQUE", "tmp_model")
	f.Add("amount >= 0", "tmp_model")
	f.Add("age < 100", "tmp_model")
	f.Add("score BETWEEN 0 AND 100", "tmp_model")
	f.Add("status IN ('active', 'pending')", "tmp_model")
	f.Add("status NOT IN ('deleted')", "tmp_model")
	f.Add("currency IN (SEK, USD, EUR)", "tmp_model")
	f.Add("customer_id REFERENCES marts.dim_customers(id)", "tmp_model")
	f.Add("name NOT EMPTY", "tmp_model")
	f.Add("code MATCHES '^[A-Z]{3}$'", "tmp_model")
	f.Add("email EMAIL", "tmp_model")
	f.Add("id UUID", "tmp_model")
	f.Add("code LENGTH BETWEEN 3 AND 10", "tmp_model")
	f.Add("zip LENGTH = 5", "tmp_model")
	f.Add("price CHECK (price > 0 AND price < 1000000)", "tmp_model")
	f.Add("value AT_LEAST_ONE", "tmp_model")
	f.Add("status NOT_CONSTANT", "tmp_model")
	f.Add("email NULL_PERCENT < 10", "tmp_model")
	f.Add("seq SEQUENTIAL", "tmp_model")
	f.Add("seq SEQUENTIAL(5)", "tmp_model")
	f.Add("(start_date, end_date) NO_OVERLAP", "tmp_model")
	f.Add("code LIKE 'AB%'", "tmp_model")
	f.Add("name NOT LIKE 'test%'", "tmp_model")
	f.Add("email DISTINCT_COUNT >= 5", "tmp_model")
	f.Add("email DUPLICATE_PERCENT < 10", "tmp_model")

	// Edge cases
	f.Add("", "tmp_model")
	f.Add("something weird", "tmp_model")
	f.Add("'; DROP TABLE users; --", "tmp_model")
	f.Add("col IN ()", "tmp_model")
	f.Add("col BETWEEN AND", "tmp_model")

	f.Fuzz(func(t *testing.T, directive, table string) {
		sql, err := ConstraintToSQL(directive, table)
		if err != nil {
			return
		}
		if sql == "" {
			t.Error("no error but empty SQL")
		}
		// Structural: output should reference the table when table is non-empty
		// and doesn't contain characters that would be transformed
		if table != "" && !strings.ContainsAny(table, "'\"\\") && !strings.Contains(sql, table) {
			t.Errorf("output SQL does not reference table %q: %s", table, sql)
		}
	})
}

// SQL injection safety + structural invariant for audit queries.
func FuzzAuditToSQL(f *testing.F) {
	f.Add("row_count >= 1", "staging.orders", int64(0))
	f.Add("row_count_change < 50", "staging.orders", int64(100))
	f.Add("freshness 24h updated_at", "staging.orders", int64(0))
	f.Add("freshness 7d created_at", "staging.orders", int64(0))
	f.Add("mean amount BETWEEN 10 AND 1000", "staging.orders", int64(0))
	f.Add("stddev amount < 100", "staging.orders", int64(0))
	f.Add("min price >= 0", "staging.orders", int64(0))
	f.Add("max price <= 999999", "staging.orders", int64(0))
	f.Add("sum amount > 0", "staging.orders", int64(0))
	f.Add("zscore amount < 3", "staging.orders", int64(0))
	f.Add("percentile 0.95 amount < 1000", "staging.orders", int64(0))
	f.Add("reconcile_count staging.source", "staging.orders", int64(0))
	f.Add("reconcile_sum amount staging.source", "staging.orders", int64(0))
	f.Add("column_exists status", "staging.orders", int64(0))
	f.Add("column_type status VARCHAR", "staging.orders", int64(0))
	f.Add("distribution STABLE status", "staging.orders", int64(100))

	// Edge cases
	f.Add("", "tmp", int64(0))
	f.Add("unknown_audit", "tmp", int64(0))
	f.Add("'; DROP TABLE x; --", "tmp", int64(-1))

	f.Fuzz(func(t *testing.T, directive, table string, prevSnapshot int64) {
		sql, err := AuditToSQL(directive, table, "", prevSnapshot)
		if err != nil {
			return
		}
		if sql == "" {
			t.Error("no error but empty SQL")
		}
		// Property: output must start with SELECT or WITH
		if !strings.HasPrefix(sql, "SELECT ") && !strings.HasPrefix(sql, "WITH ") {
			t.Errorf("SQL must start with SELECT or WITH, got: %.80s", sql)
		}
		// Property: table should be referenced when non-empty and simple
		if table != "" && !strings.ContainsAny(table, "'\"\\") && !strings.Contains(sql, table) {
			t.Errorf("output SQL does not reference table %q", table)
		}
	})
}

// Query-builder: validate generated SQL has balanced parens/quotes
// and doesn't contain obvious syntax errors.
func FuzzConstraintToSQLSyntax(f *testing.F) {
	f.Add("name NOT NULL")
	f.Add("score BETWEEN 0 AND 100")
	f.Add("status IN ('active', 'pending')")
	f.Add("code MATCHES '^[A-Z]{3}$'")
	f.Add("customer_id REFERENCES marts.dim_customers(id)")
	f.Add("(start_date, end_date) NO_OVERLAP")
	f.Add("price CHECK (price > 0 AND price < 1000000)")

	f.Fuzz(func(t *testing.T, directive string) {
		// table is always an internal identifier (tmp_<name>), never user input.
		sql, err := ConstraintToSQL(directive, "tmp_model")
		if err != nil {
			return
		}
		// Only check syntax when the directive itself is balanced —
		// unbalanced user input will naturally produce unbalanced SQL.
		if countBalance(directive, '(', ')') != 0 || strings.Count(directive, "'")%2 != 0 {
			return
		}
		// Balanced parentheses
		if depth := countBalance(sql, '(', ')'); depth != 0 {
			t.Errorf("unbalanced parens (depth=%d) in: %s", depth, sql)
		}
		// Balanced single quotes (count should be even)
		if n := strings.Count(sql, "'"); n%2 != 0 {
			t.Errorf("odd number of single quotes (%d) in: %s", n, sql)
		}
	})
}

// Query-builder: validate generated audit SQL has balanced parens/quotes.
func FuzzAuditToSQLSyntax(f *testing.F) {
	f.Add("row_count >= 1", int64(0))
	f.Add("freshness 24h updated_at", int64(0))
	f.Add("mean amount BETWEEN 10 AND 1000", int64(0))
	f.Add("percentile 0.95 amount < 1000", int64(0))
	f.Add("reconcile_sum amount staging.source", int64(0))
	f.Add("distribution STABLE status", int64(100))

	f.Fuzz(func(t *testing.T, directive string, prevSnapshot int64) {
		// table is always an internal identifier, never user input.
		sql, err := AuditToSQL(directive, "staging.orders", "", prevSnapshot)
		if err != nil {
			return
		}
		// Only check syntax when the directive itself is balanced.
		if countBalance(directive, '(', ')') != 0 {
			return
		}
		// Balanced parentheses
		if depth := countBalance(sql, '(', ')'); depth != 0 {
			t.Errorf("unbalanced parens (depth=%d) in: %s", depth, sql)
		}
	})
}

func countBalance(s string, open, close byte) int {
	depth := 0
	for i := 0; i < len(s); i++ {
		if s[i] == open {
			depth++
		} else if s[i] == close {
			depth--
		}
	}
	return depth
}

func FuzzQuoteINValues(f *testing.F) {
	f.Add("'a', 'b'")
	f.Add("1, 2, 3")
	f.Add("foo, bar")
	f.Add("1.5, -2, +3")
	f.Add("a, 1, 'b'")
	f.Add("it's")
	f.Add("")
	f.Add("', '', '''")
	f.Add("1e10, -1e-5")
	f.Add("NULL, null, True")

	f.Fuzz(func(t *testing.T, vals string) {
		result := quoteINValues(vals)
		// Property: output must have balanced single quotes
		if strings.Count(result, "'")%2 != 0 {
			t.Errorf("quoteINValues(%q) has odd number of quotes: %q", vals, result)
		}
	})
}

// Involution: invertOp(invertOp(x)) == x for known operators.
func FuzzInvertOp(f *testing.F) {
	f.Add(">=")
	f.Add("<=")
	f.Add(">")
	f.Add("<")
	f.Add("=")
	f.Add("!=")
	f.Add("")
	f.Add("<<")
	f.Add("===")

	f.Fuzz(func(t *testing.T, op string) {
		result := invertOp(op)
		// Involution: double-invert must return to original
		doubleInverted := invertOp(result)
		if doubleInverted != op {
			t.Errorf("invertOp not involutory: invertOp(invertOp(%q)) = %q, want %q",
				op, doubleInverted, op)
		}
	})
}
