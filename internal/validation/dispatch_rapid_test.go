// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package validation

import (
	"strings"
	"testing"

	"pgregory.net/rapid"
)

// genMacroName generates valid macro names (lowercase + underscores).
func genMacroName() *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		prefix := rapid.SampledFrom([]string{
			"row_count", "freshness", "mean_between", "mean", "stddev",
			"min", "max", "sum", "zscore", "percentile",
			"reconcile_count", "reconcile_sum", "column_exists", "column_type",
			"golden", "row_count_delta", "not_null", "unique", "primary_key",
			"compare", "between", "email", "uuid", "check", "sequential",
			"balanced_ledger", "custom_check",
		}).Draw(t, "name")
		return prefix
	})
}

// genArg generates valid macro arguments.
func genArg() *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		kind := rapid.IntRange(0, 5).Draw(t, "argKind")
		switch kind {
		case 0: // operator
			return rapid.SampledFrom([]string{">=", "<=", ">", "<", "=", "!="}).Draw(t, "op")
		case 1: // integer
			return rapid.SampledFrom([]string{"0", "1", "5", "10", "100", "999"}).Draw(t, "int")
		case 2: // float
			return rapid.SampledFrom([]string{"0.5", "0.95", "1.0", "50.0"}).Draw(t, "float")
		case 3: // column name
			return rapid.SampledFrom([]string{"id", "amount", "email", "status", "updated_at"}).Draw(t, "col")
		case 4: // duration
			return rapid.SampledFrom([]string{"24h", "7d", "1h", "30d"}).Draw(t, "dur")
		default: // table ref
			return rapid.SampledFrom([]string{"staging.orders", "raw.source", "mart.revenue"}).Draw(t, "tbl")
		}
	})
}

// Property: DispatchAudit always produces SQL starting with SELECT * FROM ondatra_audit_
func TestRapid_DispatchAudit_Prefix(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		name := genMacroName().Draw(t, "name")
		nArgs := rapid.IntRange(0, 4).Draw(t, "nArgs")

		var args []string
		for range nArgs {
			args = append(args, genArg().Draw(t, "arg"))
		}

		directive := name
		if len(args) > 0 {
			directive += "(" + strings.Join(args, ", ") + ")"
		}

		sql, err := DispatchAudit(directive, "staging.orders")
		if err != nil {
			t.Fatalf("DispatchAudit(%q) failed: %v", directive, err)
		}
		if !strings.HasPrefix(sql, "SELECT * FROM memory.ondatra_audit_"+name) {
			t.Fatalf("SQL doesn't start with expected prefix: %s", sql)
		}
		if !strings.Contains(sql, "'staging.orders'") {
			t.Fatalf("SQL missing table reference: %s", sql)
		}
	})
}

// Property: DispatchConstraint always produces SQL with ondatra_constraint_ prefix
func TestRapid_DispatchConstraint_Prefix(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		name := genMacroName().Draw(t, "name")
		arg := genArg().Draw(t, "arg")
		directive := name + "(" + arg + ")"

		sql, err := DispatchConstraint(directive, "tmp_model")
		if err != nil {
			t.Fatalf("DispatchConstraint(%q) failed: %v", directive, err)
		}
		if !strings.Contains(sql, "memory.ondatra_constraint_"+name) {
			t.Fatalf("SQL missing constraint prefix: %s", sql)
		}
	})
}

// Property: parseDirective round-trips — name is always the part before first (
func TestRapid_ParseDirective_NameExtraction(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		name := genMacroName().Draw(t, "name")
		nArgs := rapid.IntRange(0, 3).Draw(t, "nArgs")

		var args []string
		for range nArgs {
			args = append(args, genArg().Draw(t, "arg"))
		}

		var directive string
		if len(args) > 0 {
			directive = name + "(" + strings.Join(args, ", ") + ")"
		} else {
			directive = name
		}

		parsedName, parsedArgs, err := parseDirective(directive)
		if err != nil {
			t.Fatalf("parseDirective(%q): unexpected error: %v", directive, err)
		}
		if parsedName != name {
			t.Fatalf("parseDirective(%q): name = %q, want %q", directive, parsedName, name)
		}
		if len(parsedArgs) != len(args) {
			t.Fatalf("parseDirective(%q): got %d args, want %d", directive, len(parsedArgs), len(args))
		}
	})
}

// Property: DispatchAuditsBatch with N directives produces N-1 UNION ALL separators
func TestRapid_DispatchAuditsBatch_UnionCount(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(1, 5).Draw(t, "n")
		var directives []string
		for range n {
			name := genMacroName().Draw(t, "name")
			arg := genArg().Draw(t, "arg")
			directives = append(directives, name+"("+arg+")")
		}

		sql, errs := DispatchAuditsBatch(directives, "staging.orders")
		if len(errs) > 0 {
			t.Fatalf("unexpected errors: %v", errs)
		}
		unions := strings.Count(sql, "UNION ALL")
		if unions != n-1 {
			t.Fatalf("expected %d UNION ALL, got %d in: %s", n-1, unions, sql)
		}
	})
}

// Property: formatArg never produces empty strings
func TestRapid_FormatArg_NonEmpty(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		arg := genArg().Draw(t, "arg")
		result := formatArg(arg)
		if result == "" {
			t.Fatalf("formatArg(%q) returned empty", arg)
		}
	})
}

// Property: operators are always single-quoted in output
func TestRapid_FormatArg_OperatorsQuoted(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		op := rapid.SampledFrom([]string{">=", "<=", ">", "<", "=", "!="}).Draw(t, "op")
		result := formatArg(op)
		if result != "'"+op+"'" {
			t.Fatalf("formatArg(%q) = %q, want '%s'", op, result, op)
		}
	})
}

// Property: numbers pass through unquoted
func TestRapid_FormatArg_NumbersUnquoted(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.SampledFrom([]string{"0", "1", "42", "100", "0.5", "0.95", "1.0"}).Draw(t, "num")
		result := formatArg(n)
		if result != n {
			t.Fatalf("formatArg(%q) = %q, want %q (numbers should be unquoted)", n, result, n)
		}
	})
}

// Property: escapeSQL doubles all single quotes
func TestRapid_EscapeSQL(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		s := rapid.String().Draw(t, "input")
		result := escapeSQL(s)
		// After escaping, no single ' should exist without being doubled
		unescaped := strings.ReplaceAll(result, "''", "")
		if strings.Contains(unescaped, "'") {
			t.Fatalf("escapeSQL(%q) left unescaped quote: %q", s, result)
		}
	})
}
