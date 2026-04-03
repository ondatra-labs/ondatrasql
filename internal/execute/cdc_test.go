// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"encoding/json"
	"strings"
	"testing"
)

// --- Pure Go unit tests (no DuckDB session) ---

func TestWalkAST_FindsBaseTable(t *testing.T) {
	t.Parallel()
	// Minimal AST with a BASE_TABLE node
	root := map[string]any{
		"statements": []any{
			map[string]any{
				"node": map[string]any{
					"from_table": map[string]any{
						"type":        "BASE_TABLE",
						"schema_name": "staging",
						"table_name":  "orders",
						"alias":       "",
					},
				},
			},
		},
	}

	var found bool
	walkAST(root, func(node map[string]any) map[string]any {
		if nodeType, _ := node["type"].(string); nodeType == "BASE_TABLE" {
			found = true
		}
		return nil
	})

	if !found {
		t.Error("walkAST did not find BASE_TABLE node")
	}
}

func TestWalkAST_FindsNestedBaseTable(t *testing.T) {
	t.Parallel()
	// JOIN structure: left and right are BASE_TABLE nodes
	root := map[string]any{
		"from_table": map[string]any{
			"type": "JOIN",
			"left": map[string]any{
				"type":        "BASE_TABLE",
				"schema_name": "staging",
				"table_name":  "orders",
			},
			"right": map[string]any{
				"type":        "BASE_TABLE",
				"schema_name": "staging",
				"table_name":  "products",
			},
		},
	}

	var tables []string
	walkAST(root, func(node map[string]any) map[string]any {
		if nodeType, _ := node["type"].(string); nodeType == "BASE_TABLE" {
			schema, _ := node["schema_name"].(string)
			table, _ := node["table_name"].(string)
			tables = append(tables, schema+"."+table)
		}
		return nil
	})

	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d: %v", len(tables), tables)
	}
}

func TestWalkAST_ReplacesNode(t *testing.T) {
	t.Parallel()
	root := map[string]any{
		"child": map[string]any{
			"type":  "BASE_TABLE",
			"table": "orders",
		},
	}

	walkAST(root, func(node map[string]any) map[string]any {
		if nodeType, _ := node["type"].(string); nodeType == "BASE_TABLE" {
			return map[string]any{"type": "SUBQUERY", "replaced": true}
		}
		return nil
	})

	child := root["child"].(map[string]any)
	if child["type"] != "SUBQUERY" {
		t.Errorf("node was not replaced, got type=%v", child["type"])
	}
}

func TestBuildCDCSubquery_Structure(t *testing.T) {
	t.Parallel()
	node := buildCDCSubquery("staging", "orders", "", "o", 42)

	if node["type"] != "SUBQUERY" {
		t.Fatalf("expected SUBQUERY type, got %v", node["type"])
	}
	if node["alias"] != "o" {
		t.Fatalf("expected alias 'o', got %v", node["alias"])
	}

	subquery, ok := node["subquery"].(map[string]any)
	if !ok {
		t.Fatal("missing subquery")
	}
	stmtNode, ok := subquery["node"].(map[string]any)
	if !ok {
		t.Fatal("missing node in subquery")
	}
	if stmtNode["type"] != "SET_OPERATION_NODE" {
		t.Fatalf("expected SET_OPERATION_NODE, got %v", stmtNode["type"])
	}
	if stmtNode["setop_type"] != "EXCEPT" {
		t.Fatalf("expected EXCEPT, got %v", stmtNode["setop_type"])
	}

	// Check right side has at_clause with version
	right := stmtNode["right"].(map[string]any)
	fromTable := right["from_table"].(map[string]any)
	atClause := fromTable["at_clause"].(map[string]any)
	if atClause["unit"] != "VERSION" {
		t.Fatalf("expected VERSION unit, got %v", atClause["unit"])
	}
	expr := atClause["expr"].(map[string]any)
	val := expr["value"].(map[string]any)
	// JSON numbers from Go marshal are float64, but we constructed with int64
	if v, ok := val["value"].(int64); ok && v != 42 {
		t.Fatalf("expected snapshot 42, got %v", v)
	}
}

func TestBuildCDCSubquery_WithCatalog(t *testing.T) {
	t.Parallel()
	node := buildCDCSubquery("staging", "orders", "lake", "", 10)
	subquery := node["subquery"].(map[string]any)
	stmtNode := subquery["node"].(map[string]any)

	// Both left and right should reference catalog "lake"
	left := stmtNode["left"].(map[string]any)
	leftFrom := left["from_table"].(map[string]any)
	if leftFrom["catalog_name"] != "lake" {
		t.Errorf("left catalog = %v, want 'lake'", leftFrom["catalog_name"])
	}
	right := stmtNode["right"].(map[string]any)
	rightFrom := right["from_table"].(map[string]any)
	if rightFrom["catalog_name"] != "lake" {
		t.Errorf("right catalog = %v, want 'lake'", rightFrom["catalog_name"])
	}
}

func TestBuildEmptyCDCSubquery_Structure(t *testing.T) {
	t.Parallel()
	node := buildEmptyCDCSubquery("staging", "orders", "", "")

	if node["type"] != "SUBQUERY" {
		t.Fatalf("expected SUBQUERY type, got %v", node["type"])
	}

	subquery := node["subquery"].(map[string]any)
	stmtNode := subquery["node"].(map[string]any)
	if stmtNode["type"] != "SELECT_NODE" {
		t.Fatalf("expected SELECT_NODE, got %v", stmtNode["type"])
	}
	if stmtNode["where_clause"] == nil {
		t.Fatal("expected where_clause for WHERE false")
	}

	whereClause := stmtNode["where_clause"].(map[string]any)
	if whereClause["type"] != "OPERATOR_CAST" {
		t.Fatalf("expected OPERATOR_CAST for WHERE false, got %v", whereClause["type"])
	}
}

func TestQualifyTableInAST(t *testing.T) {
	t.Parallel()
	root := map[string]any{
		"statements": []any{
			map[string]any{
				"node": map[string]any{
					"from_table": map[string]any{
						"type":         "BASE_TABLE",
						"schema_name":  "staging",
						"table_name":   "orders",
						"catalog_name": "",
					},
				},
			},
		},
	}

	tablesToQualify := map[string]bool{"staging.orders": true}
	qualifyTablesInAST(root, tablesToQualify, "lake")

	fromTable := root["statements"].([]any)[0].(map[string]any)["node"].(map[string]any)["from_table"].(map[string]any)
	if fromTable["catalog_name"] != "lake" {
		t.Errorf("catalog_name = %v, want 'lake'", fromTable["catalog_name"])
	}
}

func TestQualifyTableInAST_SkipsNonMatching(t *testing.T) {
	t.Parallel()
	root := map[string]any{
		"from_table": map[string]any{
			"type":         "BASE_TABLE",
			"schema_name":  "raw",
			"table_name":   "source",
			"catalog_name": "",
		},
	}

	tablesToQualify := map[string]bool{"staging.orders": true}
	qualifyTablesInAST(root, tablesToQualify, "lake")

	fromTable := root["from_table"].(map[string]any)
	if fromTable["catalog_name"] != "" {
		t.Errorf("catalog_name should be empty, got %v", fromTable["catalog_name"])
	}
}

func TestWalkAST_DoesNotMutateStringConstants(t *testing.T) {
	t.Parallel()
	// Simulate AST with a string constant containing "staging.orders"
	root := map[string]any{
		"from_table": map[string]any{
			"type":        "BASE_TABLE",
			"schema_name": "staging",
			"table_name":  "orders",
		},
		"where_clause": map[string]any{
			"type":  "VALUE_CONSTANT",
			"class": "CONSTANT",
			"value": map[string]any{
				"value": "staging.orders",
				"type":  map[string]any{"id": "VARCHAR"},
			},
		},
	}

	// Walk and count BASE_TABLE replacements
	var replaced int
	walkAST(root, func(node map[string]any) map[string]any {
		if nodeType, _ := node["type"].(string); nodeType == "BASE_TABLE" {
			replaced++
			return map[string]any{"type": "SUBQUERY", "replaced": true}
		}
		return nil
	})

	if replaced != 1 {
		t.Errorf("expected exactly 1 replacement, got %d", replaced)
	}

	// The string constant should be untouched
	whereClause := root["where_clause"].(map[string]any)
	val := whereClause["value"].(map[string]any)
	if val["value"] != "staging.orders" {
		t.Errorf("string constant was modified: %v", val["value"])
	}
}

func TestQuoteTableName_ThreePart(t *testing.T) {
	t.Parallel()
	got := quoteTableName("catalog.schema.table")
	want := `"catalog"."schema"."table"`
	if got != want {
		t.Errorf("quoteTableName 3-part = %q, want %q", got, want)
	}
}

func TestQuoteTableName_WithQuotesInName(t *testing.T) {
	t.Parallel()
	got := quoteTableName(`my"table`)
	if got != `"my""table"` {
		t.Errorf("got %q, want %q", got, `"my""table"`)
	}
}

func TestBuildCDCSubquery_RoundTripsToJSON(t *testing.T) {
	t.Parallel()
	node := buildCDCSubquery("staging", "orders", "", "o", 42)

	// Must be valid JSON
	bytes, err := json.Marshal(node)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(bytes, &parsed); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if parsed["type"] != "SUBQUERY" {
		t.Errorf("round-trip lost type, got %v", parsed["type"])
	}
}

func TestStarExpr_Structure(t *testing.T) {
	t.Parallel()
	star := starExpr()
	if star["class"] != "STAR" {
		t.Errorf("expected class STAR, got %v", star["class"])
	}
	if star["type"] != "STAR" {
		t.Errorf("expected type STAR, got %v", star["type"])
	}
}

func TestWhereFalseExpr_Structure(t *testing.T) {
	t.Parallel()
	wf := whereFalseExpr()
	if wf["class"] != "CAST" {
		t.Errorf("expected class CAST, got %v", wf["class"])
	}
	castType := wf["cast_type"].(map[string]any)
	if castType["id"] != "BOOLEAN" {
		t.Errorf("expected BOOLEAN cast, got %v", castType["id"])
	}
	child := wf["child"].(map[string]any)
	val := child["value"].(map[string]any)
	if val["value"] != "f" {
		t.Errorf("expected 'f' value, got %v", val["value"])
	}
}

func TestWalkAST_HandlesNilAndPrimitives(t *testing.T) {
	t.Parallel()
	// walkAST should handle nil, strings, numbers without panic
	result := walkAST(nil, func(node map[string]any) map[string]any { return nil })
	if result != nil {
		t.Errorf("nil input should return nil, got %v", result)
	}

	str := walkAST("hello", func(node map[string]any) map[string]any { return nil })
	if str != "hello" {
		t.Errorf("string input should pass through, got %v", str)
	}

	num := walkAST(42.0, func(node map[string]any) map[string]any { return nil })
	if num != 42.0 {
		t.Errorf("number input should pass through, got %v", num)
	}
}

func TestWalkAST_WalksArrays(t *testing.T) {
	t.Parallel()
	root := map[string]any{
		"items": []any{
			map[string]any{"type": "BASE_TABLE", "name": "a"},
			map[string]any{"type": "BASE_TABLE", "name": "b"},
			"not_a_map",
		},
	}

	var count int
	walkAST(root, func(node map[string]any) map[string]any {
		if nodeType, _ := node["type"].(string); nodeType == "BASE_TABLE" {
			count++
		}
		return nil
	})

	if count != 2 {
		t.Errorf("expected 2 BASE_TABLE nodes in array, got %d", count)
	}
}

func TestWalkAST_ReplacesInArray(t *testing.T) {
	t.Parallel()
	root := map[string]any{
		"items": []any{
			map[string]any{"type": "BASE_TABLE"},
			map[string]any{"type": "OTHER"},
		},
	}

	walkAST(root, func(node map[string]any) map[string]any {
		if nodeType, _ := node["type"].(string); nodeType == "BASE_TABLE" {
			return map[string]any{"type": "REPLACED"}
		}
		return nil
	})

	items := root["items"].([]any)
	first := items[0].(map[string]any)
	if first["type"] != "REPLACED" {
		t.Errorf("array element not replaced, got type=%v", first["type"])
	}
	second := items[1].(map[string]any)
	if second["type"] != "OTHER" {
		t.Errorf("non-matching element was changed, got type=%v", second["type"])
	}
}

func TestQualifyTablesInAST_JoinStructure(t *testing.T) {
	t.Parallel()
	root := map[string]any{
		"from_table": map[string]any{
			"type": "JOIN",
			"left": map[string]any{
				"type":         "BASE_TABLE",
				"schema_name":  "staging",
				"table_name":   "orders",
				"catalog_name": "",
			},
			"right": map[string]any{
				"type":         "BASE_TABLE",
				"schema_name":  "raw",
				"table_name":   "products",
				"catalog_name": "",
			},
		},
	}

	// Only qualify staging.orders, not raw.products
	tablesToQualify := map[string]bool{"staging.orders": true}
	qualifyTablesInAST(root, tablesToQualify, "prod_lake")

	fromTable := root["from_table"].(map[string]any)
	left := fromTable["left"].(map[string]any)
	right := fromTable["right"].(map[string]any)

	if left["catalog_name"] != "prod_lake" {
		t.Errorf("left catalog = %v, want 'prod_lake'", left["catalog_name"])
	}
	if right["catalog_name"] != "" {
		t.Errorf("right catalog = %v, want empty", right["catalog_name"])
	}
}

func TestBuildCDCSubquery_PreservesAlias(t *testing.T) {
	t.Parallel()
	tests := []struct {
		alias string
	}{
		{"o"},
		{""},
		{"my_alias"},
	}
	for _, tt := range tests {
		node := buildCDCSubquery("s", "t", "", tt.alias, 1)
		if node["alias"] != tt.alias {
			t.Errorf("alias = %v, want %v", node["alias"], tt.alias)
		}
	}
}

func TestBuildEmptyCDCSubquery_PreservesAlias(t *testing.T) {
	t.Parallel()
	node := buildEmptyCDCSubquery("s", "t", "", "myalias")
	if node["alias"] != "myalias" {
		t.Errorf("alias = %v, want 'myalias'", node["alias"])
	}
}

// Verify that the full AST JSON stays valid through walk+marshal.
func TestWalkAST_PreservesValidJSON(t *testing.T) {
	t.Parallel()
	// Simulate a realistic AST fragment
	astJSON := `{
		"error": false,
		"statements": [{
			"node": {
				"type": "SELECT_NODE",
				"from_table": {
					"type": "BASE_TABLE",
					"schema_name": "raw",
					"table_name": "events",
					"catalog_name": "",
					"alias": "e"
				},
				"where_clause": {
					"type": "VALUE_CONSTANT",
					"value": {"value": "raw.events"}
				}
			}
		}]
	}`

	var root map[string]any
	if err := json.Unmarshal([]byte(astJSON), &root); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Walk without modifications
	walkAST(root, func(node map[string]any) map[string]any {
		return nil
	})

	// Should still be valid JSON
	out, err := json.Marshal(root)
	if err != nil {
		t.Fatalf("marshal after walk: %v", err)
	}
	if !strings.Contains(string(out), `"raw.events"`) {
		t.Error("string constant was lost")
	}
}
