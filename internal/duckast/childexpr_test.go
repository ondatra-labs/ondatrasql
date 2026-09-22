// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package duckast

import (
	"reflect"
	"testing"
)

// ChildExpressions is what lets a walker handle an expression class it does
// not know by name. Its load-bearing invariant is that a subquery is NOT
// returned: a subquery resolves its names against its own FROM clause, so a
// caller tracing it with the enclosing scope would attribute its columns to
// the wrong tables.
func TestChildExpressions(t *testing.T) {
	t.Parallel()
	ref := func(name string) map[string]any {
		return map[string]any{"class": "COLUMN_REF", "type": "COLUMN_REF", "column_names": []any{name}}
	}
	node := NewNode(map[string]any{
		"class":    "OPERATOR",
		"type":     "OPERATOR_COALESCE",
		"children": []any{ref("a"), ref("b")},
		// A map that is not itself an expression is descended into.
		"order_bys": map[string]any{
			"type":   "ORDER_MODIFIER",
			"orders": []any{map[string]any{"expression": ref("c")}},
		},
		"subquery": map[string]any{"node": map[string]any{"select_list": []any{ref("hidden")}}},
		"alias":    "v",
	})

	var got []string
	for _, c := range node.ChildExpressions() {
		got = append(got, c.ColumnNames()[0])
	}
	if want := []string{"a", "b", "c"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ChildExpressions = %v, want %v (subquery must be excluded)", got, want)
	}
}

func TestChildExpressions_Nil(t *testing.T) {
	t.Parallel()
	var n *Node
	if got := n.ChildExpressions(); got != nil {
		t.Errorf("nil node returned %v", got)
	}
}
