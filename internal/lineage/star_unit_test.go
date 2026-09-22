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
