// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package duckast

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// loadDuckDB2 reads a fixture from testdata/duckdb2: DuckDB 2.0's own
// json_serialize_sql output for the queries listed in duckdb2Fixtures
// (duckdb2_integration_test.go), not hand-written JSON.
func loadDuckDB2(t *testing.T, name string) *Node {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "duckdb2", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	ast, err := Parse(string(data))
	if err != nil {
		t.Fatal(err)
	}
	stmts := ast.Statements()
	if len(stmts) != 1 {
		t.Fatalf("%s: %d statements, want 1", name, len(stmts))
	}
	return stmts[0]
}

// A chain of one set operation is a single node in 2.0, its arms under
// `children` and `left`/`right` null. Reading left/right found no arms, so the
// lineage of every UNION was empty.
func TestDuckDB2_SetOpArms(t *testing.T) {
	t.Parallel()
	root := loadDuckDB2(t, "union_chain")
	arms := root.SetOpArms()
	if len(arms) != 3 {
		t.Fatalf("got %d arms, want 3", len(arms))
	}
	for i, want := range []string{"t", "u", "v"} {
		if got := arms[i].FromTable().TableName(); got != want {
			t.Errorf("arm %d reads %q, want %q", i, got, want)
		}
	}
	if got := root.SetOpFirst().FromTable().TableName(); got != "t" {
		t.Errorf("SetOpFirst reads %q, want t", got)
	}
}

// The 1.5 shape nests binary nodes under left/right; SetOpArms still reads it.
func TestSetOpArms_LegacyBinary(t *testing.T) {
	t.Parallel()
	n := NewNode(map[string]any{
		"type":  "SET_OPERATION_NODE",
		"left":  map[string]any{"type": "SELECT_NODE", "alias": "l"},
		"right": map[string]any{"type": "SELECT_NODE", "alias": "r"},
	})
	arms := n.SetOpArms()
	if len(arms) != 2 || arms[0].Alias() != "l" || arms[1].Alias() != "r" {
		t.Fatalf("got %v, want [l r]", arms)
	}
}

// A CTE body moved from value.query.node to value.query_node.
func TestDuckDB2_CTEs(t *testing.T) {
	t.Parallel()
	ctes := loadDuckDB2(t, "cte").CTEs()
	if len(ctes) != 1 {
		t.Fatalf("got %d CTEs, want 1", len(ctes))
	}
	c := ctes[0]
	if c.Name != "c" || !reflect.DeepEqual(c.Aliases, []string{"x"}) {
		t.Errorf("got name %q aliases %v, want c [x]", c.Name, c.Aliases)
	}
	if got := c.Node.FromTable().TableName(); got != "t" {
		t.Errorf("CTE body reads %q, want t", got)
	}
}

// Function arguments moved from `children` to `arguments: [{name, expression}]`.
// Reading only children gave every lib call zero arguments, which surfaced as
// "function fetch missing 1 argument".
func TestDuckDB2_TableFunctionArgs(t *testing.T) {
	t.Parallel()
	from := loadDuckDB2(t, "table_function_args").FromTable()
	if got := from.TableFunctionName(); got != "f" {
		t.Fatalf("function name %q, want f", got)
	}
	args := from.TableFunctionArgs()
	if len(args) != 2 {
		t.Fatalf("got %d args, want 2", len(args))
	}
	var got []any
	for _, a := range args {
		v, ok := a.ConstantValue()
		if !ok {
			t.Fatalf("arg %v is not a plain constant", a.Raw())
		}
		got = append(got, v)
	}
	if !reflect.DeepEqual(got, []any{"k", int64(2)}) {
		t.Errorf("arg values %v, want [k 2]", got)
	}
	named := from.Field("function").Arguments()
	if named[0].Name != "api_key" || named[1].Name != "" {
		t.Errorf("argument names %q %q, want api_key and empty", named[0].Name, named[1].Name)
	}
}

func TestDuckDB2_ConstantValue(t *testing.T) {
	t.Parallel()
	list := loadDuckDB2(t, "constants").SelectList()
	want := []struct {
		v  any
		ok bool
	}{
		{"it's", true},
		{int64(1000), true},
		{int64(-5), true},
		{true, true},
		{nil, true},
		{nil, false},      // 1.5 is a decimal: left to DuckDB
		{nil, false},      // does not fit int64
		{nil, false},      // blob
		{int64(10), true}, // 010 is ten, not octal eight
	}
	if len(list) != len(want) {
		t.Fatalf("got %d select items, want %d", len(list), len(want))
	}
	for i, w := range want {
		v, ok := list[i].ConstantValue()
		if ok != w.ok || !reflect.DeepEqual(v, w.v) {
			t.Errorf("item %d: got (%v, %v), want (%v, %v)", i, v, ok, w.v, w.ok)
		}
	}
}

// The cast target moved from `cast_type: {id, type_info}` to a TYPE node under
// `type_expr`. Reading cast_type made every typed column VARCHAR.
func TestDuckDB2_CastType(t *testing.T) {
	t.Parallel()
	list := loadDuckDB2(t, "cast_types").SelectList()
	ty := func(name string, params []string, fields ...TypeField) *TypeRef {
		return &TypeRef{Name: name, Params: params, Fields: fields}
	}
	want := []*TypeRef{
		ty("DECIMAL", []string{"10", "2"}),
		ty("STRUCT", nil,
			TypeField{Name: "x", Type: ty("INTEGER", nil)},
			TypeField{Name: "y", Type: ty("LIST", nil, TypeField{Type: ty("VARCHAR", nil)})}),
		ty("MAP", nil, TypeField{Type: ty("VARCHAR", nil)}, TypeField{Type: ty("INTEGER", nil)}),
		ty("INT4", nil),
		ty("ARRAY", []string{"3"}, TypeField{Type: ty("INTEGER", nil)}),
	}
	if len(list) != len(want) {
		t.Fatalf("got %d select items, want %d", len(list), len(want))
	}
	for i, w := range want {
		if got := list[i].CastType(); !reflect.DeepEqual(got, w) {
			t.Errorf("item %d: got %+v, want %+v", i, got, w)
		}
	}
}

// json_deserialize_sql renders a table from qualified_name.path, so
// SetTableRef has to keep it in step with the name fields.
func TestDuckDB2_SetTableRef(t *testing.T) {
	t.Parallel()
	from := loadDuckDB2(t, "base_table").FromTable()
	from.SetTableRef("lake", from.SchemaName(), from.TableName())
	if got := from.CatalogName(); got != "lake" {
		t.Errorf("catalog_name %q, want lake", got)
	}
	path := from.Field("qualified_name").StringList("path")
	if !reflect.DeepEqual(path, []string{"lake", "s", "t"}) {
		t.Errorf("qualified_name path %v, want [lake s t]", path)
	}
}
