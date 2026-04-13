// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package validation

import (
	"strings"
	"testing"
)

func TestParseDirective(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input    string
		wantName string
		wantArgs []string
	}{
		{"row_count(>=, 1)", "row_count", []string{">=", "1"}},
		{"freshness(updated_at, 24h)", "freshness", []string{"updated_at", "24h"}},
		{"mean_between(amount, 50, 500)", "mean_between", []string{"amount", "50", "500"}},
		{"reconcile_count(staging.orders)", "reconcile_count", []string{"staging.orders"}},
		{"column_exists(id)", "column_exists", []string{"id"}},
		{"balanced_ledger", "balanced_ledger", nil},
		{"not_null(customer_id)", "not_null", []string{"customer_id"}},
		{"check(price, price > 0 AND price < 1000000)", "check", []string{"price", "price > 0 AND price < 1000000"}},
		{"required_if(delivery_date, status = 'delivered')", "required_if", []string{"delivery_date", "status = 'delivered'"}},
		{"percentile(amount, 0.95, <=, 100)", "percentile", []string{"amount", "0.95", "<=", "100"}},
		// Curly braces in regex must not split on comma inside {}
		{"matches(phone, ^\\+[0-9]{10,15}$)", "matches", []string{"phone", "^\\+[0-9]{10,15}$"}},
		// Nested parens + curly braces
		{"check(col, col > 0 AND len(col) IN {1,2,3})", "check", []string{"col", "col > 0 AND len(col) IN {1,2,3}"}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			name, args, err := parseDirective(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if name != tt.wantName {
				t.Errorf("name = %q, want %q", name, tt.wantName)
			}
			if len(args) != len(tt.wantArgs) {
				t.Fatalf("args len = %d, want %d: %v", len(args), len(tt.wantArgs), args)
			}
			for i, arg := range args {
				if arg != tt.wantArgs[i] {
					t.Errorf("args[%d] = %q, want %q", i, arg, tt.wantArgs[i])
				}
			}
		})
	}

	// Error cases: malformed directives must be rejected
	errorTests := []struct {
		input   string
		wantErr string
	}{
		{"row_count(>=, 1", "unbalanced parentheses"},
		{"check(price, x > 0) trailing", "unexpected trailing text"},
		{"name(()", "unbalanced parentheses"},
		{"name(a, b)) extra", "unexpected trailing text"},
	}
	for _, tt := range errorTests {
		t.Run("error_"+tt.input, func(t *testing.T) {
			_, _, err := parseDirective(tt.input)
			if err == nil {
				t.Fatalf("expected error for %q", tt.input)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want substring %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestFormatArg(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{">=", "'>='"},
		{"1", "1"},
		{"100", "100"},
		{"0.95", "0.95"},
		{"amount", "'amount'"},
		{"updated_at", "'updated_at'"},
		{"24h", "'24 HOUR'"},
		{"7d", "'7 DAY'"},
		{"staging.orders", "'staging.orders'"},
		{"'delivered'", "'delivered'"},
		{"price > 0 AND price < 1000000", "'price > 0 AND price < 1000000'"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := formatArg(tt.input)
			if got != tt.want {
				t.Errorf("formatArg(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestDispatchAudit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		directive string
		table     string
		wantParts []string
	}{
		{
			"row_count(>=, 1)",
			"staging.orders",
			[]string{"ondatra_audit_row_count", "'staging.orders'", "'>='", "1"},
		},
		{
			"freshness(updated_at, 24h)",
			"staging.orders",
			[]string{"ondatra_audit_freshness", "'staging.orders'", "'updated_at'", "'24 HOUR'"},
		},
		{
			"mean_between(amount, 50, 500)",
			"staging.orders",
			[]string{"ondatra_audit_mean_between", "'staging.orders'", "'amount'", "50", "500"},
		},
		{
			"balanced_ledger",
			"staging.ledger",
			[]string{"ondatra_audit_balanced_ledger", "'staging.ledger'"},
		},
		{
			"column_type(id, INTEGER)",
			"raw.t",
			[]string{"ondatra_audit_column_type", "'raw.t'", "'id'", "'INTEGER'"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.directive, func(t *testing.T) {
			sql, err := DispatchAudit(tt.directive, tt.table)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for _, part := range tt.wantParts {
				if !strings.Contains(sql, part) {
					t.Errorf("SQL missing %q\ngot: %s", part, sql)
				}
			}
		})
	}
}

func TestDispatchConstraint(t *testing.T) {
	t.Parallel()
	sql, err := DispatchConstraint("not_null(customer_id)", "tmp_model")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "ondatra_constraint_not_null") {
		t.Errorf("SQL missing prefix\ngot: %s", sql)
	}
	if !strings.Contains(sql, "'tmp_model'") {
		t.Errorf("SQL missing table\ngot: %s", sql)
	}
	if !strings.Contains(sql, "'customer_id'") {
		t.Errorf("SQL missing column\ngot: %s", sql)
	}
}

func TestDispatchWarning(t *testing.T) {
	t.Parallel()
	sql, err := DispatchWarning("freshness(updated_at, 24h)", "staging.orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "ondatra_warning_freshness") {
		t.Errorf("SQL missing prefix\ngot: %s", sql)
	}
}

func TestDispatchAuditsTransactional(t *testing.T) {
	t.Parallel()

	// Empty → empty
	sql, errs := DispatchAuditsTransactional(nil, "staging.orders")
	if sql != "" {
		t.Errorf("nil directives should return empty, got: %s", sql)
	}
	if len(errs) != 0 {
		t.Errorf("nil directives should have no errors, got: %v", errs)
	}

	// Single audit
	sql, errs = DispatchAuditsTransactional([]string{"row_count(>=, 1)"}, "staging.orders")
	if len(errs) != 0 {
		t.Errorf("unexpected errors: %v", errs)
	}
	if !strings.Contains(sql, "audit_failures") {
		t.Errorf("SQL missing CTE wrapper\ngot: %s", sql)
	}
	if !strings.Contains(sql, "error(") {
		t.Errorf("SQL missing error() call\ngot: %s", sql)
	}
	if !strings.Contains(sql, "ondatra_audit_row_count") {
		t.Errorf("SQL missing macro call\ngot: %s", sql)
	}

	// Multiple audits
	sql, errs = DispatchAuditsTransactional(
		[]string{"row_count(>=, 1)", "freshness(updated_at, 24h)"},
		"staging.orders",
	)
	if len(errs) != 0 {
		t.Errorf("unexpected errors: %v", errs)
	}
	if !strings.Contains(sql, "UNION ALL") {
		t.Errorf("SQL missing UNION ALL\ngot: %s", sql)
	}
}

func TestDispatchConstraintsBatch(t *testing.T) {
	t.Parallel()
	sql, errs := DispatchConstraintsBatch(
		[]string{"not_null(id)", "unique(email)"},
		"tmp_model",
	)
	if len(errs) != 0 {
		t.Errorf("unexpected errors: %v", errs)
	}
	if !strings.Contains(sql, "UNION ALL") {
		t.Errorf("SQL missing UNION ALL\ngot: %s", sql)
	}
	if !strings.Contains(sql, "ondatra_constraint_not_null") {
		t.Errorf("SQL missing not_null\ngot: %s", sql)
	}
	if !strings.Contains(sql, "ondatra_constraint_unique") {
		t.Errorf("SQL missing unique\ngot: %s", sql)
	}
}

func TestDispatchEmpty(t *testing.T) {
	t.Parallel()
	_, err := DispatchAudit("", "t")
	if err == nil {
		t.Error("expected error for empty directive")
	}
	_, err = DispatchConstraint("", "t")
	if err == nil {
		t.Error("expected error for empty directive")
	}
	_, err = DispatchWarning("", "t")
	if err == nil {
		t.Error("expected error for empty directive")
	}
}

func TestEscapeSQL(t *testing.T) {
	t.Parallel()
	if escapeSQL("it's") != "it''s" {
		t.Errorf("escapeSQL failed: %s", escapeSQL("it's"))
	}
}
