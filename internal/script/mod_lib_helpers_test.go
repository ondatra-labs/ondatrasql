// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"strings"
	"testing"

	"go.starlark.net/starlark"
)

// runStarlark evaluates `expr` in a thread that has lib_helpers
// pre-declared and returns the result. Used to test the public
// `lib_helpers.to_json_schema` surface end-to-end through Starlark.
func runStarlark(t *testing.T, expr string) starlark.Value {
	t.Helper()
	thread := &starlark.Thread{Name: "test"}
	predeclared := starlark.StringDict{
		"lib_helpers": libHelpersModule(),
	}
	v, err := starlark.Eval(thread, "test.star", expr, predeclared)
	if err != nil {
		t.Fatalf("eval %q: %v", expr, err)
	}
	return v
}

func mustString(t *testing.T, v starlark.Value) string {
	t.Helper()
	s, ok := v.(starlark.String)
	if !ok {
		t.Fatalf("expected starlark string, got %T (%v)", v, v)
	}
	return string(s)
}

// schemaTypeOf returns the value of the "type" key from a JSON Schema
// dict produced by lib_helpers.to_json_schema. Test helper.
func schemaTypeOf(t *testing.T, v starlark.Value) string {
	t.Helper()
	d, ok := v.(*starlark.Dict)
	if !ok {
		t.Fatalf("expected dict, got %T (%v)", v, v)
	}
	tv, found, err := d.Get(starlark.String("type"))
	if err != nil || !found {
		t.Fatalf("no `type` key in dict %v", v)
	}
	return mustString(t, tv)
}

func TestToJSONSchema_Integers(t *testing.T) {
	t.Parallel()
	for _, duckdb := range []string{"BIGINT", "INTEGER", "SMALLINT", "TINYINT", "HUGEINT", "UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT", "UHUGEINT"} {
		v := runStarlark(t, `lib_helpers.to_json_schema("`+duckdb+`")`)
		if got := schemaTypeOf(t, v); got != "integer" {
			t.Errorf("%s: type = %q, want integer", duckdb, got)
		}
	}
}

func TestToJSONSchema_Numbers(t *testing.T) {
	t.Parallel()
	for _, duckdb := range []string{"DECIMAL(18,3)", "DECIMAL(10,4)", "NUMERIC(8,2)", "DOUBLE", "FLOAT", "REAL"} {
		v := runStarlark(t, `lib_helpers.to_json_schema("`+duckdb+`")`)
		if got := schemaTypeOf(t, v); got != "number" {
			t.Errorf("%s: type = %q, want number", duckdb, got)
		}
	}
}

func TestToJSONSchema_Boolean(t *testing.T) {
	t.Parallel()
	v := runStarlark(t, `lib_helpers.to_json_schema("BOOLEAN")`)
	if got := schemaTypeOf(t, v); got != "boolean" {
		t.Errorf("type = %q, want boolean", got)
	}
}

func TestToJSONSchema_Date(t *testing.T) {
	t.Parallel()
	v := runStarlark(t, `lib_helpers.to_json_schema("DATE")`)
	d := v.(*starlark.Dict)
	if got, _, _ := d.Get(starlark.String("type")); mustString(t, got) != "string" {
		t.Errorf("DATE type = %v, want string", got)
	}
	if got, _, _ := d.Get(starlark.String("format")); mustString(t, got) != "date" {
		t.Errorf("DATE format = %v, want date", got)
	}
}

func TestToJSONSchema_TimestampVariants(t *testing.T) {
	t.Parallel()
	for _, duckdb := range []string{"TIMESTAMP", "TIMESTAMPTZ", "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS"} {
		v := runStarlark(t, `lib_helpers.to_json_schema("`+duckdb+`")`)
		d := v.(*starlark.Dict)
		gotType, _, _ := d.Get(starlark.String("type"))
		if mustString(t, gotType) != "string" {
			t.Errorf("%s: type = %v, want string", duckdb, gotType)
		}
		gotFmt, _, _ := d.Get(starlark.String("format"))
		if mustString(t, gotFmt) != "date-time" {
			t.Errorf("%s: format = %v, want date-time", duckdb, gotFmt)
		}
	}
}

func TestToJSONSchema_StringFallback(t *testing.T) {
	t.Parallel()
	// Catch-all category: VARCHAR, UUID, JSON, BLOB, BIT, INTERVAL, TIME,
	// MAP(...), UNION(...), and unknown — all → {"type": "string"}.
	for _, duckdb := range []string{
		"VARCHAR", "UUID", "JSON", "BLOB", "BIT", "INTERVAL",
		"TIME", "TIMETZ", "TIME_NS",
		"MAP(VARCHAR, BIGINT)", "UNION(a VARCHAR, b INTEGER)",
		"WHATEVER_UNKNOWN_TYPE",
	} {
		v := runStarlark(t, `lib_helpers.to_json_schema("`+duckdb+`")`)
		if got := schemaTypeOf(t, v); got != "string" {
			t.Errorf("%s: type = %q, want string", duckdb, got)
		}
	}
}

func TestToJSONSchema_List(t *testing.T) {
	t.Parallel()
	// LIST of VARCHAR
	v := runStarlark(t, `lib_helpers.to_json_schema(["VARCHAR"])`)
	d := v.(*starlark.Dict)

	gotType, _, _ := d.Get(starlark.String("type"))
	if mustString(t, gotType) != "array" {
		t.Errorf("type = %v, want array", gotType)
	}
	items, _, _ := d.Get(starlark.String("items"))
	if got := schemaTypeOf(t, items); got != "string" {
		t.Errorf("items type = %q, want string (VARCHAR → string)", got)
	}

	// Nested LIST of BIGINT
	v = runStarlark(t, `lib_helpers.to_json_schema(["BIGINT"])`)
	d = v.(*starlark.Dict)
	items, _, _ = d.Get(starlark.String("items"))
	if got := schemaTypeOf(t, items); got != "integer" {
		t.Errorf("LIST<BIGINT> items type = %q, want integer", got)
	}
}

func TestToJSONSchema_Struct(t *testing.T) {
	t.Parallel()
	// STRUCT(street VARCHAR, zip INTEGER)
	v := runStarlark(t, `lib_helpers.to_json_schema({"street": "VARCHAR", "zip": "INTEGER"})`)
	d := v.(*starlark.Dict)

	gotType, _, _ := d.Get(starlark.String("type"))
	if mustString(t, gotType) != "object" {
		t.Errorf("type = %v, want object", gotType)
	}
	props, _, _ := d.Get(starlark.String("properties"))
	pd, ok := props.(*starlark.Dict)
	if !ok {
		t.Fatalf("properties is %T, want dict", props)
	}
	streetSchema, _, _ := pd.Get(starlark.String("street"))
	if got := schemaTypeOf(t, streetSchema); got != "string" {
		t.Errorf("street type = %q, want string", got)
	}
	zipSchema, _, _ := pd.Get(starlark.String("zip"))
	if got := schemaTypeOf(t, zipSchema); got != "integer" {
		t.Errorf("zip type = %q, want integer", got)
	}
}

func TestToJSONSchema_NestedListOfStruct(t *testing.T) {
	t.Parallel()
	// LIST<STRUCT(name VARCHAR, age INTEGER)> — recursive case
	v := runStarlark(t, `lib_helpers.to_json_schema([{"name": "VARCHAR", "age": "INTEGER"}])`)
	d := v.(*starlark.Dict)

	if got := schemaTypeOf(t, d); got != "array" {
		t.Fatalf("outer type = %q, want array", got)
	}
	items, _, _ := d.Get(starlark.String("items"))
	if got := schemaTypeOf(t, items); got != "object" {
		t.Fatalf("items type = %q, want object", got)
	}
	props, _, _ := items.(*starlark.Dict).Get(starlark.String("properties"))
	pd := props.(*starlark.Dict)
	nameSchema, _, _ := pd.Get(starlark.String("name"))
	if got := schemaTypeOf(t, nameSchema); got != "string" {
		t.Errorf("name type = %q, want string", got)
	}
	ageSchema, _, _ := pd.Get(starlark.String("age"))
	if got := schemaTypeOf(t, ageSchema); got != "integer" {
		t.Errorf("age type = %q, want integer", got)
	}
}

func TestToJSONSchema_RejectsBadInput(t *testing.T) {
	t.Parallel()
	thread := &starlark.Thread{Name: "test"}
	predeclared := starlark.StringDict{"lib_helpers": libHelpersModule()}
	_, err := starlark.Eval(thread, "test.star", `lib_helpers.to_json_schema(42)`, predeclared)
	if err == nil {
		t.Fatal("expected error for int input")
	}
	if !strings.Contains(err.Error(), "expected string, list, or dict") {
		t.Errorf("error should explain expected types, got: %v", err)
	}
}
