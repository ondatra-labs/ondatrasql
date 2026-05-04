// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute

import (
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/duckast"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/libregistry"
	"github.com/ondatra-labs/ondatrasql/internal/lineage"
)

// TestDetectLibCalls_WrapperShape regression-tests the AST-based wrapper
// in execute/libcall.go. The wrapper's job is small but load-bearing:
// forward AST + registry to libcall.Detect and lift each returned
// libcall.Call into a runtime-augmentable LibCall whose runtime-only
// fields (TempTable, ScriptResult) start empty.
//
// The string-based fallback (`detectLibCallsFromSQL`) already has
// regression coverage in regression_test.go; this fills the gap for
// the AST-based path that runtime takes when json_serialize_sql works.
func TestDetectLibCalls_WrapperShape(t *testing.T) {
	sess, err := duckdb.NewSession(":memory:")
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	defer sess.Close()

	parseAST := func(sql string) *duckast.AST {
		t.Helper()
		raw, err := lineage.GetAST(sess, sql)
		if err != nil {
			t.Fatalf("GetAST(%q): %v", sql, err)
		}
		ast, err := duckast.Parse(raw)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		return ast
	}

	reg := libregistry.NewRegistryForTest(map[string]*libregistry.LibFunc{
		"example_api": {Name: "example_api", IsSink: false},
	})

	t.Run("nil_inputs_returns_nil", func(t *testing.T) {
		if got := detectLibCalls(nil, nil); got != nil {
			t.Errorf("detectLibCalls(nil,nil) = %v, want nil", got)
		}
		if got := detectLibCalls(nil, reg); got != nil {
			t.Errorf("detectLibCalls(nil, reg) = %v, want nil", got)
		}
	})

	t.Run("no_table_function_returns_nil", func(t *testing.T) {
		ast := parseAST("SELECT 1 AS id")
		if got := detectLibCalls(ast, reg); got != nil {
			t.Errorf("detectLibCalls(no-tablefn, reg) = %v, want nil", got)
		}
	})

	t.Run("matching_table_function_lifted_to_LibCall", func(t *testing.T) {
		ast := parseAST(`SELECT id::BIGINT AS id FROM example_api(api_key=>'k')`)
		got := detectLibCalls(ast, reg)
		if len(got) != 1 {
			t.Fatalf("len(calls) = %d, want 1", len(got))
		}
		c := got[0]
		// Forwarded fields from libcall.Call must survive the lift.
		if c.FuncName != "example_api" {
			t.Errorf("FuncName = %q, want example_api", c.FuncName)
		}
		if c.CallIndex != 0 {
			t.Errorf("CallIndex = %d, want 0", c.CallIndex)
		}
		if c.Lib == nil || c.Lib.Name != "example_api" {
			t.Errorf("Lib = %v, want pointer to example_api lib", c.Lib)
		}
		// Runtime-only fields must start empty — the runner fills them
		// after Badger claim + temp-table creation.
		if c.TempTable != "" {
			t.Errorf("TempTable = %q, want empty (runner fills after temp table is created)", c.TempTable)
		}
		if c.ScriptResult != nil {
			t.Errorf("ScriptResult = %v, want nil (runner fills after script execution)", c.ScriptResult)
		}
	})

	t.Run("two_calls_get_distinct_indices", func(t *testing.T) {
		ast := parseAST(`
			SELECT a.id::BIGINT AS id1, b.id::BIGINT AS id2
			FROM example_api(api_key=>'k1') a
			JOIN example_api(api_key=>'k2') b ON a.id = b.id`)
		got := detectLibCalls(ast, reg)
		if len(got) != 2 {
			t.Fatalf("len(calls) = %d, want 2", len(got))
		}
		seen := make(map[int]bool)
		for _, c := range got {
			seen[c.CallIndex] = true
		}
		if len(seen) != 2 {
			t.Errorf("CallIndex values = %v, want two distinct indices", seen)
		}
	})

	t.Run("empty_registry_returns_nil_even_with_table_functions", func(t *testing.T) {
		emptyReg := libregistry.NewRegistryForTest(nil)
		ast := parseAST(`SELECT id::BIGINT AS id FROM example_api(api_key=>'k')`)
		if got := detectLibCalls(ast, emptyReg); got != nil {
			t.Errorf("detectLibCalls(ast, empty-reg) = %v, want nil (no registered funcs to match)", got)
		}
	})
}
