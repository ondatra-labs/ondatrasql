// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package duckast

import (
	"strings"
	"testing"
)

// minimalSelect = `SELECT a, b FROM t1`
const minimalSelect = `{
  "statements":[{"node":{
    "type":"SELECT_NODE",
    "cte_map":{"map":[]},
    "select_list":[
      {"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["a"]},
      {"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["b"]}
    ],
    "from_table":{"type":"BASE_TABLE","alias":"","schema_name":"","table_name":"t1"}
  }}]
}`

func TestParse_RoundTrip(t *testing.T) {
	t.Parallel()
	a, err := Parse(minimalSelect)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	out, err := a.Serialize()
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	if !strings.Contains(out, "BASE_TABLE") || !strings.Contains(out, "t1") {
		t.Errorf("round-trip lost data: %s", out)
	}
}

func TestParse_Empty(t *testing.T) {
	t.Parallel()
	if _, err := Parse(""); err == nil {
		t.Error("expected error on empty input")
	}
	if _, err := Parse("not json"); err == nil {
		t.Error("expected error on garbage input")
	}
}

func TestStatements_Basic(t *testing.T) {
	t.Parallel()
	a, _ := Parse(minimalSelect)
	stmts := a.Statements()
	if len(stmts) != 1 {
		t.Fatalf("got %d statements, want 1", len(stmts))
	}
	if !stmts[0].IsSelectNode() {
		t.Errorf("expected SELECT_NODE, got %s", stmts[0].NodeType())
	}
}

func TestNode_NilSafety(t *testing.T) {
	t.Parallel()
	// Nil chains should not panic.
	var n *Node
	if !n.IsNil() {
		t.Error("nil Node should report IsNil")
	}
	// Calls on nil return zero values.
	if n.NodeType() != "" || n.String("anything") != "" || n.Bool("x") {
		t.Error("nil Node accessors should return zero values")
	}
	if n.Field("x") != nil {
		t.Error("nil.Field should return nil")
	}
	if n.FieldList("x") != nil {
		t.Error("nil.FieldList should return nil")
	}

	// An empty Node behaves the same.
	empty := &Node{}
	if !empty.IsNil() {
		t.Error("empty Node should report IsNil")
	}
}

func TestNode_SelectList(t *testing.T) {
	t.Parallel()
	a, _ := Parse(minimalSelect)
	root := a.Statements()[0]
	cols := root.SelectList()
	if len(cols) != 2 {
		t.Fatalf("got %d columns, want 2", len(cols))
	}
	if cols[0].Class() != "COLUMN_REF" {
		t.Errorf("col[0].Class = %q, want COLUMN_REF", cols[0].Class())
	}
	if names := cols[0].ColumnNames(); len(names) != 1 || names[0] != "a" {
		t.Errorf("col[0].ColumnNames = %v, want [a]", names)
	}
}

func TestNode_FromTable_BaseTable(t *testing.T) {
	t.Parallel()
	a, _ := Parse(minimalSelect)
	from := a.Statements()[0].FromTable()
	if !from.IsBaseTable() {
		t.Errorf("expected BASE_TABLE, got %s", from.NodeType())
	}
	if from.TableName() != "t1" {
		t.Errorf("TableName = %q, want t1", from.TableName())
	}
	if from.FullTableName() != "t1" {
		t.Errorf("FullTableName = %q, want t1", from.FullTableName())
	}
}

// Walker robustness — the walker discovers ALL nodes in the tree
// regardless of whether the typed accessors model the field path.
// This is the property that eliminates the silent-field-drop bug class.
func TestWalk_VisitsEverything(t *testing.T) {
	t.Parallel()
	// AST with set-op + sub-select FROM + CASE + COMPARISON left/right
	// + LIMIT modifier — all the shapes that previously had
	// extraction gaps. Walker should see every BASE_TABLE.
	const sql = `{
	  "statements":[{"node":{
	    "type":"SET_OPERATION_NODE",
	    "setop_type":"UNION",
	    "left":{
	      "type":"SELECT_NODE",
	      "select_list":[],
	      "from_table":{"type":"BASE_TABLE","schema_name":"raw","table_name":"left_tbl"},
	      "where_clause":{
	        "class":"COMPARISON",
	        "left":{"class":"COLUMN_REF","column_names":["x"]},
	        "right":{
	          "class":"SUBQUERY",
	          "subquery":{"node":{
	            "type":"SELECT_NODE",
	            "from_table":{"type":"BASE_TABLE","schema_name":"raw","table_name":"where_eq"}
	          }}
	        }
	      }
	    },
	    "right":{
	      "type":"SELECT_NODE",
	      "from_table":{
	        "type":"JOIN",
	        "left":{"type":"BASE_TABLE","schema_name":"raw","table_name":"join_left"},
	        "right":{"type":"BASE_TABLE","schema_name":"raw","table_name":"join_right"},
	        "condition":{
	          "class":"COMPARISON",
	          "left":{"class":"COLUMN_REF","column_names":["a"]},
	          "right":{
	            "class":"SUBQUERY",
	            "subquery":{"node":{
	              "type":"SELECT_NODE",
	              "from_table":{"type":"BASE_TABLE","schema_name":"raw","table_name":"join_on_sub"}
	            }}
	          }
	        }
	      },
	      "modifiers":[{
	        "type":"LIMIT_MODIFIER",
	        "limit":{
	          "class":"SUBQUERY",
	          "subquery":{"node":{
	            "type":"SELECT_NODE",
	            "from_table":{"type":"BASE_TABLE","schema_name":"raw","table_name":"limit_sub"}
	          }}
	        }
	      }]
	    }
	  }}]
	}`
	a, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	seen := make(map[string]bool)
	a.Walk(func(n *Node) bool {
		if n.IsBaseTable() {
			seen[n.FullTableName()] = true
		}
		return true
	})

	want := []string{
		"raw.left_tbl",
		"raw.where_eq",   // hidden in WHERE = subquery (COMPARISON.right)
		"raw.join_left",
		"raw.join_right",
		"raw.join_on_sub", // hidden in JOIN ON subquery (Condition + COMPARISON.right)
		"raw.limit_sub",   // hidden in LIMIT subquery (modifiers[].limit)
	}
	for _, w := range want {
		if !seen[w] {
			t.Errorf("walker missed %s; saw %v", w, seen)
		}
	}
}

func TestWalk_StopsOnFalse(t *testing.T) {
	t.Parallel()
	a, _ := Parse(minimalSelect)
	count := 0
	a.Walk(func(n *Node) bool {
		count++
		return false // skip children of every node
	})
	// Only the root statement node is visited (because Walk on AST
	// recurses into the raw map's children, but each child returns
	// false immediately).
	if count == 0 {
		t.Error("walker should visit at least one node")
	}
}

// Mutation: ReplaceBaseTables substitutes BASE_TABLE nodes in place.
// Used by CDC to swap raw table refs with time-travel SUBQUERY nodes.
func TestReplaceBaseTables(t *testing.T) {
	t.Parallel()
	a, _ := Parse(minimalSelect)

	a.ReplaceBaseTables(
		func(n *Node) bool { return n.TableName() == "t1" },
		func(n *Node) map[string]any {
			return map[string]any{
				"type":      "BASE_TABLE",
				"schema_name": "v2",
				"table_name":  "t1_replaced",
			}
		},
	)

	// After mutation, the FromTable should report the replacement.
	from := a.Statements()[0].FromTable()
	if from.SchemaName() != "v2" || from.TableName() != "t1_replaced" {
		t.Errorf("replacement didn't take: schema=%q table=%q",
			from.SchemaName(), from.TableName())
	}

	// Round-trip preserves the mutation.
	out, _ := a.Serialize()
	if !strings.Contains(out, "t1_replaced") {
		t.Errorf("serialized output missing replacement: %s", out)
	}
	if strings.Contains(out, `"table_name":"t1"`) {
		t.Errorf("serialized output still contains original: %s", out)
	}
}

// Test that arbitrary fields not modeled by typed accessors are still
// preserved through round-trip. This is the "robustness" guarantee.
func TestRoundTrip_PreservesUnknownFields(t *testing.T) {
	t.Parallel()
	const sql = `{
	  "statements":[{"node":{
	    "type":"SELECT_NODE",
	    "select_list":[],
	    "from_table":{"type":"BASE_TABLE","schema_name":"r","table_name":"t"},
	    "made_up_future_field":{"some":"value","nested":[1,2,3]},
	    "another_unknown":"hello"
	  }}]
	}`
	a, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	out, err := a.Serialize()
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	if !strings.Contains(out, "made_up_future_field") {
		t.Error("unknown field dropped during round-trip — robustness broken")
	}
	if !strings.Contains(out, "another_unknown") {
		t.Error("unknown field dropped during round-trip — robustness broken")
	}
}
