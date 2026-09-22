// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package lineage

import (
	"reflect"
	"testing"
)

// AST for: SELECT * EXCLUDE (b) FROM c.s.t — shape from json_serialize_sql.
const starExcludeAST = `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},` +
	`"select_list":[{"class":"STAR","type":"STAR","alias":"","relation_name":"","exclude_list":["b"],` +
	`"replace_list":[],"columns":false,"expr":null,"qualified_exclude_list":[],"rename_list":[]}],` +
	`"from_table":{"type":"BASE_TABLE","alias":"","schema_name":"s","table_name":"t","column_name_alias":[],"catalog_name":"c"}}}]}`

func TestExpandStar_UsesResolverWithNameParts(t *testing.T) {
	t.Parallel()
	var got [3]string
	resolver := func(catalog, schema, table string) ([]string, bool) {
		got = [3]string{catalog, schema, table}
		return []string{"a", "b", "c"}, true
	}
	cols, err := ExtractFromASTWithColumns(starExcludeAST, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if want := [3]string{"c", "s", "t"}; got != want {
		t.Errorf("resolver called with %v, want %v", got, want)
	}
	if names := []string{cols[0].Column, cols[1].Column}; len(cols) != 2 || !reflect.DeepEqual(names, []string{"a", "c"}) {
		t.Fatalf("columns = %+v, want a, c", cols)
	}
	if src := cols[1].Sources; len(src) != 1 || src[0].Table != "s.t" || src[0].Column != "c" {
		t.Errorf("c sources = %+v, want s.t.c", src)
	}
}

func TestExpandStar_ResolverMissFallsBackToPlaceholder(t *testing.T) {
	t.Parallel()
	resolver := func(string, string, string) ([]string, bool) { return nil, false }
	cols, err := ExtractFromASTWithColumns(starExcludeAST, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 1 || cols[0].Column != "?" || len(cols[0].Sources) != 0 {
		t.Errorf("cols = %+v, want a single sourceless ?", cols)
	}
}

// The classification rules are pure functions, so they are pinned without a
// DuckDB session: the integration tests above need one, and `make test` would
// otherwise cover none of this.
func TestOperatorTransform(t *testing.T) {
	t.Parallel()
	cases := []struct {
		nodeType  string
		transform TransformationType
		name      string
	}{
		{"OPERATOR_COALESCE", TransformFunction, "COALESCE"},
		{"OPERATOR_TRY", TransformFunction, "TRY"},
		{"ARRAY_EXTRACT", TransformFunction, "ARRAY EXTRACT"},
		{"STRUCT_EXTRACT", TransformFunction, "STRUCT EXTRACT"},
		{"ARRAY_SLICE", TransformFunction, "ARRAY SLICE"},
		{"OPERATOR_IS_NULL", TransformConditional, "IS NULL"},
		{"COMPARE_IN", TransformConditional, "IN"},
		{"CONJUNCTION_AND", TransformConditional, "AND"},
		{"COMPARE_BETWEEN", TransformConditional, "BETWEEN"},
	}
	for _, tc := range cases {
		transform, name := operatorTransform(tc.nodeType)
		if transform != tc.transform || name != tc.name {
			t.Errorf("operatorTransform(%q) = %q, %q; want %q, %q",
				tc.nodeType, transform, name, tc.transform, tc.name)
		}
	}
}

// An aggregate underneath a scalar wrapper has to survive: GetCDCTables reads
// it to decide that a delta is unsound. The wrapper's own classification wins
// only when it is the aggregate.
func TestKeepAggregate(t *testing.T) {
	t.Parallel()
	agg := SourceColumn{Transformation: TransformAggregation, FunctionName: "max"}
	if got, fn := keepAggregate(agg, TransformFunction, "upper"); got != TransformAggregation || fn != "max" {
		t.Errorf("upper(max(x)) = %q, %q; want AGGREGATION, max", got, fn)
	}
	plain := SourceColumn{Transformation: TransformIdentity}
	if got, fn := keepAggregate(plain, TransformAggregation, "max"); got != TransformAggregation || fn != "max" {
		t.Errorf("max(x) = %q, %q; want AGGREGATION, max", got, fn)
	}
	arith := SourceColumn{Transformation: TransformArithmetic}
	if got, _ := keepAggregate(arith, TransformAggregation, "sum"); got != TransformAggregation {
		t.Errorf("sum(a + b) = %q; want AGGREGATION", got)
	}
	cast := SourceColumn{Transformation: TransformCast}
	if got, _ := keepAggregate(cast, TransformFunction, "upper"); got != TransformCast {
		t.Errorf("upper(x::INT) = %q; want CAST", got)
	}
}
