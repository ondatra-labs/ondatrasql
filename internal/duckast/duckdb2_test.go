// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package duckast

import (
	"fmt"
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
	if got := root.FirstSelect().FromTable().TableName(); got != "t" {
		t.Errorf("FirstSelect reads %q, want t", got)
	}
}

// A chain of different set operations nests: the first arm of the UNION ALL
// is the UNION BY NAME, whose own select list is empty. FirstSelect has to
// descend to the SELECT, or a check on the "first arm" checks nothing.
func TestDuckDB2_FirstSelect_MixedSetOps(t *testing.T) {
	t.Parallel()
	root := loadDuckDB2(t, "mixed_setop")
	if root.SetOpType() != "UNION" || !root.Bool("setop_all") {
		t.Fatalf("outer node is %s all=%v, want UNION ALL", root.SetOpType(), root.Bool("setop_all"))
	}
	first := root.SetOpArms()[0]
	if !first.IsSetOpNode() || first.SetOpType() != "UNION_BY_NAME" {
		t.Fatalf("first arm is %s %s, want a UNION_BY_NAME set operation", first.NodeType(), first.SetOpType())
	}
	sel := root.FirstSelect()
	if !sel.IsSelectNode() || sel.FromTable().TableName() != "t" {
		t.Errorf("FirstSelect = %s from %q, want the SELECT from t", sel.NodeType(), sel.FromTable().TableName())
	}
}

// A recursive CTE body is a RECURSIVE_CTE_NODE, which unlike a set operation
// still keeps its anchor and recursive part under left and right.
func TestDuckDB2_RecursiveCTE(t *testing.T) {
	t.Parallel()
	ctes := loadDuckDB2(t, "recursive_cte").CTEs()
	if len(ctes) != 1 {
		t.Fatalf("got %d CTEs, want 1", len(ctes))
	}
	body := ctes[0].Node
	if body.NodeType() != "RECURSIVE_CTE_NODE" {
		t.Fatalf("CTE body is %s, want RECURSIVE_CTE_NODE", body.NodeType())
	}
	arms := body.SetOpArms()
	if len(arms) != 2 || arms[1].FromTable().TableName() != "r" {
		t.Fatalf("recursive CTE arms = %d, want the anchor and the part reading r", len(arms))
	}
	if sel := body.FirstSelect(); sel.FromTable().NodeType() != "EMPTY" {
		t.Errorf("FirstSelect reads %s, want the anchor (SELECT 1, no FROM)", sel.FromTable().NodeType())
	}
}

// Parse rejects a tree in the DuckDB 1.5 shape instead of handing readers a
// tree they would read as empty: no function arguments, no set-operation
// arms, no CTE bodies, no constant values, no cast targets.
func TestParse_RejectsDuckDB15Shape(t *testing.T) {
	t.Parallel()
	wrap := func(node string) string {
		return `{"error":false,"statements":[{"node":` + node + `}]}`
	}
	sel := func(item string) string {
		return wrap(`{"type":"SELECT_NODE","select_list":[` + item + `],"from_table":{"type":"EMPTY"}}`)
	}
	for name, js := range map[string]string{
		"function children": sel(`{"class":"FUNCTION","type":"FUNCTION","function_name":"sum","children":[{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["x"]}]}`),
		"window children":   sel(`{"class":"WINDOW","type":"WINDOW_AGGREGATE","function_name":"sum","children":[]}`),
		"constant value":    sel(`{"class":"CONSTANT","type":"VALUE_CONSTANT","value":{"type":{"id":"INTEGER"},"is_null":false,"value":1}}`),
		"cast_type":         sel(`{"class":"CAST","type":"OPERATOR_CAST","child":{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["x"]},"cast_type":{"id":"BIGINT"}}`),
		"set op left/right": wrap(`{"type":"SET_OPERATION_NODE","setop_type":"UNION","left":{"type":"SELECT_NODE"},"right":{"type":"SELECT_NODE"}}`),
		"cte query.node":    wrap(`{"type":"SELECT_NODE","cte_map":{"map":[{"key":"c","value":{"aliases":[],"materialized":"CTE_MATERIALIZE_DEFAULT","query":{"node":{"type":"SELECT_NODE"}}}}]}}`),
		// A malformed argument would shift every later positional one.
		"argument without expression": sel(`{"class":"FUNCTION","type":"FUNCTION","function_name":"f","arguments":[{"name":"a"},{"expression":{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["x"]}}]}`),
	} {
		if _, err := Parse(js); err == nil {
			t.Errorf("%s: Parse accepted a tree that is not in the 2.0 shape", name)
		}
	}
}

// Arguments errors on a malformed entry of a hand-built node rather than
// skipping it and shifting the rest.
func TestArguments_MalformedEntryIsAnError(t *testing.T) {
	t.Parallel()
	n := NewNode(map[string]any{
		"class": "FUNCTION", "function_name": "f",
		"arguments": []any{map[string]any{"name": "a"}, map[string]any{"expression": map[string]any{"class": "CONSTANT"}}},
	})
	if args, err := n.Arguments(); err == nil {
		t.Errorf("Arguments = %v, want an error", args)
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
	args, err := from.TableFunctionArgs()
	if err != nil || len(args) != 2 {
		t.Fatalf("got %d args (%v), want 2", len(args), err)
	}
	if args[0].Name != "api_key" || args[1].Name != "" {
		t.Errorf("argument names %q %q, want api_key and empty", args[0].Name, args[1].Name)
	}
	var got []string
	for _, a := range args {
		v, ok := a.Expr.ConstantString()
		if !ok {
			t.Fatalf("arg %v is not a plain constant", a.Expr.Raw())
		}
		got = append(got, v)
	}
	if !reflect.DeepEqual(got, []string{"k", "2"}) {
		t.Errorf("arg values %q, want [k 2]", got)
	}
}

// ConstantString gives the string DuckDB renders for CAST(literal AS VARCHAR)
// where the literal is that string, and declines the rest. The integration
// test TestConstantString_MatchesDuckDBCast pins the agreement with DuckDB.
func TestDuckDB2_ConstantString(t *testing.T) {
	t.Parallel()
	list := loadDuckDB2(t, "constants").SelectList()
	want := []struct {
		v  string
		ok bool
	}{
		{"it's", true},
		{"1000", true},
		{"-5", true},
		{"true", true},
		{"", true},   // NULL
		{"", false},  // 1.5 is a decimal: left to DuckDB
		{"", false},  // does not fit BIGINT
		{"", false},  // blob
		{"10", true}, // 010 is ten, not octal eight
	}
	if len(list) != len(want) {
		t.Fatalf("got %d select items, want %d", len(list), len(want))
	}
	for i, w := range want {
		v, ok := list[i].ConstantString()
		if ok != w.ok || v != w.v {
			t.Errorf("item %d: got (%q, %v), want (%q, %v)", i, v, ok, w.v, w.ok)
		}
	}
}

func ty(name string, params []string, fields ...TypeField) *TypeRef {
	return &TypeRef{Name: name, Params: params, Fields: fields}
}

func castTypes(t *testing.T, fixture string) []*TypeRef {
	t.Helper()
	var out []*TypeRef
	for i, item := range loadDuckDB2(t, fixture).SelectList() {
		ref, err := item.CastType()
		if err != nil {
			t.Fatalf("item %d: %v", i, err)
		}
		out = append(out, ref)
	}
	return out
}

// The cast target moved from `cast_type: {id, type_info}` to a TYPE node under
// `type_expr`. Reading cast_type made every typed column VARCHAR.
func TestDuckDB2_CastType(t *testing.T) {
	t.Parallel()
	want := []*TypeRef{
		ty("DECIMAL", []string{"10", "2"}),
		ty("STRUCT", nil,
			TypeField{Name: "x", Type: ty("INTEGER", nil)},
			TypeField{Name: "y", Type: ty("list", nil, TypeField{Type: ty("VARCHAR", nil)})}),
		ty("MAP", nil, TypeField{Type: ty("VARCHAR", nil)}, TypeField{Type: ty("INTEGER", nil)}),
		ty("int4", nil),
		ty("array", []string{"3"}, TypeField{Type: ty("INTEGER", nil)}),
	}
	if got := castTypes(t, "cast_types"); !reflect.DeepEqual(got, want) {
		t.Errorf("got %s, want %s", dumpTypes(got), dumpTypes(want))
	}
}

// DuckDB's parser normalizes the multi-word spellings; the rest arrive as
// written. A qualified name keeps its schema and catalog, and a string
// modifier keeps its quotes, doubled inside.
func TestDuckDB2_CastType_Spellings(t *testing.T) {
	t.Parallel()
	want := []*TypeRef{
		ty("DOUBLE", nil),
		ty("VARCHAR", []string{"5"}),
		ty("TIMESTAMP", nil),
		ty("varint", nil),
		ty("GEOMETRY", []string{"'OGC:CRS84'"}),
		{Schema: "myschema", Name: "mytype"},
		{Catalog: "cat", Schema: "sch", Name: "typ"},
		ty("VARCHAR", []string{"'it''s'"}),
	}
	got := castTypes(t, "type_spellings")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %s, want %s", dumpTypes(got), dumpTypes(want))
	}
	if q := got[6].QualifiedName(); q != "cat.sch.typ" {
		t.Errorf("QualifiedName = %q, want cat.sch.typ", q)
	}
}

// A CAST without a target type, or with a modifier that is not a constant,
// is an error rather than some default type.
func TestCastType_Malformed(t *testing.T) {
	t.Parallel()
	for name, raw := range map[string]map[string]any{
		"no type_expr": {"class": "CAST"},
		"no type name": {"class": "CAST", "type_expr": map[string]any{"class": "TYPE"}},
		"column modifier": {"class": "CAST", "type_expr": map[string]any{
			"class": "TYPE", "type_name": "DECIMAL",
			"children": []any{map[string]any{"class": "COLUMN_REF", "column_names": []any{"x"}}},
		}},
	} {
		if ref, err := NewNode(raw).CastType(); err == nil {
			t.Errorf("%s: CastType = %+v, want an error", name, ref)
		}
	}
}

func dumpTypes(ts []*TypeRef) string {
	var b []byte
	for _, t := range ts {
		b = append(b, fmt.Sprintf("%+v ", *t)...)
	}
	return string(b)
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
