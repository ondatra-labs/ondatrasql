// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package validation

import (
	"strings"
	"testing"
)

// FuzzDispatchAudit verifies DispatchAudit never panics on arbitrary input.
func FuzzDispatchAudit(f *testing.F) {
	f.Add("row_count(>=, 1)")
	f.Add("freshness(updated_at, 24h)")
	f.Add("mean_between(amount, 50, 500)")
	f.Add("balanced_ledger")
	f.Add("column_type(id, INTEGER)")
	f.Add("percentile(amount, 0.95, <=, 100)")
	f.Add("check(price, price > 0 AND price < 1000000)")
	f.Add("required_if(delivery_date, status = 'delivered')")
	f.Add("")
	f.Add("!!!")
	f.Add("a(b(c(d)))")
	f.Add("name(arg with 'quotes')")
	f.Add("SELECT 1; DROP TABLE x;--")
	f.Add(strings.Repeat("a", 10000))

	f.Fuzz(func(t *testing.T, directive string) {
		sql, err := DispatchAudit(directive, "staging.orders")
		if err != nil {
			return // parse errors are expected
		}
		if sql == "" {
			t.Error("non-error dispatch returned empty SQL")
		}
		if !strings.Contains(sql, "memory.ondatra_audit_") {
			t.Errorf("dispatch SQL missing audit prefix: %s", sql)
		}
	})
}

// FuzzDispatchConstraint verifies DispatchConstraint never panics on arbitrary input.
func FuzzDispatchConstraint(f *testing.F) {
	f.Add("not_null(id)")
	f.Add("unique(email)")
	f.Add("compare(amount, >=, 0)")
	f.Add("between(age, 0, 150)")
	f.Add("check(price, price > 0)")
	f.Add("")
	f.Add("!!!")
	f.Add(strings.Repeat("x(", 1000))

	f.Fuzz(func(t *testing.T, directive string) {
		sql, err := DispatchConstraint(directive, "tmp_model")
		if err != nil {
			return
		}
		if sql == "" {
			t.Error("non-error dispatch returned empty SQL")
		}
		if !strings.Contains(sql, "memory.ondatra_constraint_") {
			t.Errorf("dispatch SQL missing constraint prefix: %s", sql)
		}
	})
}

// FuzzDispatchWarning verifies DispatchWarning never panics on arbitrary input.
func FuzzDispatchWarning(f *testing.F) {
	f.Add("freshness(updated_at, 24h)")
	f.Add("row_count(>=, 100)")
	f.Add("not_null(id)")
	f.Add("")
	f.Add("!!!")

	f.Fuzz(func(t *testing.T, directive string) {
		sql, err := DispatchWarning(directive, "staging.orders")
		if err != nil {
			return
		}
		if sql == "" {
			t.Error("non-error dispatch returned empty SQL")
		}
		if !strings.Contains(sql, "memory.ondatra_warning_") {
			t.Errorf("dispatch SQL missing warning prefix: %s", sql)
		}
	})
}

// FuzzParseDirective verifies parseDirective never panics.
func FuzzParseDirective(f *testing.F) {
	f.Add("name(a, b, c)")
	f.Add("name")
	f.Add("()")
	f.Add("a(b(c))")
	f.Add("name('quoted, arg', other)")
	f.Add("")
	f.Add(strings.Repeat("(", 500))
	f.Add(strings.Repeat(")", 500))
	f.Add("a(b, c, d, e, f, g, h, i, j, k)")

	f.Fuzz(func(t *testing.T, input string) {
		name, args, _ := parseDirective(input)
		_ = name
		_ = args
		// Just verify no panic (errors are expected for malformed input)
	})
}

// FuzzFormatArg verifies formatArg never panics and always returns non-empty.
func FuzzFormatArg(f *testing.F) {
	f.Add(">=")
	f.Add("100")
	f.Add("0.95")
	f.Add("24h")
	f.Add("7d")
	f.Add("'quoted'")
	f.Add("column_name")
	f.Add("")
	f.Add("'; DROP TABLE x;--")

	f.Fuzz(func(t *testing.T, arg string) {
		result := formatArg(arg)
		if result == "" {
			t.Error("formatArg returned empty string")
		}
		// SQL injection check: result should never contain unescaped single quotes
		// (except the wrapping quotes themselves)
		inner := result
		if len(inner) >= 2 && inner[0] == '\'' && inner[len(inner)-1] == '\'' {
			inner = inner[1 : len(inner)-1]
		}
		if strings.Contains(inner, "'") && !strings.Contains(inner, "''") {
			// Has single quotes that aren't escaped
			unescaped := strings.ReplaceAll(inner, "''", "")
			if strings.Contains(unescaped, "'") {
				t.Errorf("formatArg produced unescaped quotes: %s", result)
			}
		}
	})
}
