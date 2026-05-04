// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package libcall

import (
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/duckast"
	"github.com/ondatra-labs/ondatrasql/internal/libregistry"
)

func TestDetect_NilInputs(t *testing.T) {
	if got := Detect(nil, nil); got != nil {
		t.Errorf("Detect(nil, nil) = %v, want nil", got)
	}
	reg := libregistry.NewRegistryForTest(map[string]*libregistry.LibFunc{
		"foo": {Name: "foo"},
	})
	if got := Detect(nil, reg); got != nil {
		t.Errorf("Detect(nil, reg) = %v, want nil", got)
	}
}

func TestDetect_EmptyRegistry(t *testing.T) {
	reg := libregistry.NewRegistryForTest(map[string]*libregistry.LibFunc{})
	ast := mustParseAST(t, sampleASTNoTableFunc)
	if got := Detect(ast, reg); got != nil {
		t.Errorf("Detect(ast, empty-reg) = %v, want nil", got)
	}
}

func TestDetectInSQL_NilSession(t *testing.T) {
	reg := libregistry.NewRegistryForTest(map[string]*libregistry.LibFunc{
		"foo": {Name: "foo"},
	})
	got, err := DetectInSQL(nil, "SELECT 1", reg)
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if got != nil {
		t.Errorf("got = %v, want nil", got)
	}
}

func TestDetectInSQL_EmptySQL(t *testing.T) {
	reg := libregistry.NewRegistryForTest(map[string]*libregistry.LibFunc{
		"foo": {Name: "foo"},
	})
	// Pass non-nil sentinel for sess to test the SQL-empty path; we won't
	// actually exercise the session because SQL is empty.
	got, err := DetectInSQL(nil, "", reg)
	if err != nil {
		t.Errorf("err = %v", err)
	}
	if got != nil {
		t.Errorf("got = %v, want nil", got)
	}
}

// mustParseAST is a tiny helper for tests that need a parsed AST without
// going through DuckDB. The JSON we feed in is a minimal valid serialization
// produced once for representative cases.
func mustParseAST(t *testing.T, j string) *duckast.AST {
	t.Helper()
	a, err := duckast.Parse(j)
	if err != nil {
		t.Fatalf("parse AST: %v", err)
	}
	return a
}

// sampleASTNoTableFunc is a minimal serialized AST for "SELECT 1" — no
// TABLE_FUNCTION nodes. Captured from json_serialize_sql for stability.
const sampleASTNoTableFunc = `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","modifiers":[],"cte_map":{"map":[]},"select_list":[{"class":"CONSTANT","type":"VALUE_CONSTANT","alias":"","query_location":7,"value":{"type":{"id":"INTEGER","type_info":null},"is_null":false,"value":1}}],"from_table":{"type":"EMPTY","alias":"","sample":null,"input_aliases":[],"column_name_alias":[],"query_location":18446744073709551615},"where_clause":null,"group_expressions":[],"group_sets":[],"aggregate_handling":"STANDARD_HANDLING","having":null,"sample":null,"qualify":null}}]}`

// TestAllTableFunctionNames_NilAST pins safe behavior for nil input.
// Critical because callers in validate may have nil AST when SQL parsing
// fails — must not panic.
func TestAllTableFunctionNames_NilAST(t *testing.T) {
	if got := AllTableFunctionNames(nil); got != nil {
		t.Errorf("AllTableFunctionNames(nil) = %v, want nil", got)
	}
}

// TestAllTableFunctionNames_NoTableFunctions pins that a SELECT-only
// AST with no FROM-table-function returns empty (not nil-vs-empty
// inconsistency).
func TestAllTableFunctionNames_NoTableFunctions(t *testing.T) {
	ast := mustParseAST(t, sampleASTNoTableFunc)
	got := AllTableFunctionNames(ast)
	if len(got) != 0 {
		t.Errorf("expected no names, got %v", got)
	}
}

// TestBuiltinTableFunctions_NilSession pins safe behavior for nil
// session input. validate may call this when session creation fails
// — must return (nil, nil), never panic.
func TestBuiltinTableFunctions_NilSession(t *testing.T) {
	got, err := BuiltinTableFunctions(nil)
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if got != nil {
		t.Errorf("got = %v, want nil", got)
	}
}
