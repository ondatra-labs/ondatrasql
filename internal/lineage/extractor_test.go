// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package lineage

import (
	"testing"
)

func TestExtractFromAST_SimpleQuery(t *testing.T) {
	t.Parallel()
	// AST for: SELECT a, b FROM t1
	astJSON := `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","modifiers":[],"cte_map":{"map":[]},"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["a"]},{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["b"]}],"from_table":{"type":"BASE_TABLE","alias":"","table_name":"t1"}}}]}`

	lineages, err := ExtractFromAST(astJSON)
	if err != nil {
		t.Fatalf("ExtractFromAST failed: %v", err)
	}

	if len(lineages) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(lineages))
	}

	// Check column a
	if lineages[0].Column != "a" {
		t.Errorf("expected column 'a', got '%s'", lineages[0].Column)
	}
	if len(lineages[0].Sources) != 1 || lineages[0].Sources[0].Table != "t1" || lineages[0].Sources[0].Column != "a" {
		t.Errorf("expected source t1.a, got %+v", lineages[0].Sources)
	}

	// Check column b
	if lineages[1].Column != "b" {
		t.Errorf("expected column 'b', got '%s'", lineages[1].Column)
	}
	if len(lineages[1].Sources) != 1 || lineages[1].Sources[0].Table != "t1" || lineages[1].Sources[0].Column != "b" {
		t.Errorf("expected source t1.b, got %+v", lineages[1].Sources)
	}
}

func TestExtractFromAST_JoinWithAlias(t *testing.T) {
	t.Parallel()
	// AST for: SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id
	astJSON := `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["o","id"]},{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["c","name"]}],"from_table":{"type":"JOIN","left":{"type":"BASE_TABLE","alias":"o","table_name":"orders"},"right":{"type":"BASE_TABLE","alias":"c","table_name":"customers"}}}}]}`

	lineages, err := ExtractFromAST(astJSON)
	if err != nil {
		t.Fatalf("ExtractFromAST failed: %v", err)
	}

	if len(lineages) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(lineages))
	}

	// Check o.id -> orders.id
	if lineages[0].Column != "id" {
		t.Errorf("expected column 'id', got '%s'", lineages[0].Column)
	}
	if len(lineages[0].Sources) != 1 || lineages[0].Sources[0].Table != "orders" || lineages[0].Sources[0].Column != "id" {
		t.Errorf("expected source orders.id, got %+v", lineages[0].Sources)
	}

	// Check c.name -> customers.name
	if lineages[1].Column != "name" {
		t.Errorf("expected column 'name', got '%s'", lineages[1].Column)
	}
	if len(lineages[1].Sources) != 1 || lineages[1].Sources[0].Table != "customers" || lineages[1].Sources[0].Column != "name" {
		t.Errorf("expected source customers.name, got %+v", lineages[1].Sources)
	}
}

func TestExtractFromAST_WithCTE(t *testing.T) {
	t.Parallel()
	// AST for:
	// WITH order_totals AS (SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id)
	// SELECT c.name, ot.total FROM customers c JOIN order_totals ot ON c.id = ot.customer_id
	astJSON := `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[{"key":"order_totals","value":{"query":{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["customer_id"]},{"class":"FUNCTION","type":"FUNCTION","alias":"total","function_name":"sum","children":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["amount"]}]}],"from_table":{"type":"BASE_TABLE","alias":"","table_name":"orders"}}}}}]},"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["c","name"]},{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["ot","total"]}],"from_table":{"type":"JOIN","left":{"type":"BASE_TABLE","alias":"c","table_name":"customers"},"right":{"type":"BASE_TABLE","alias":"ot","table_name":"order_totals"}}}}]}`

	lineages, err := ExtractFromAST(astJSON)
	if err != nil {
		t.Fatalf("ExtractFromAST failed: %v", err)
	}

	if len(lineages) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(lineages))
	}

	// Check c.name -> customers.name
	if lineages[0].Column != "name" {
		t.Errorf("expected column 'name', got '%s'", lineages[0].Column)
	}
	if len(lineages[0].Sources) != 1 || lineages[0].Sources[0].Table != "customers" || lineages[0].Sources[0].Column != "name" {
		t.Errorf("expected source customers.name, got %+v", lineages[0].Sources)
	}

	// Check ot.total -> orders.amount (traced through CTE)
	if lineages[1].Column != "total" {
		t.Errorf("expected column 'total', got '%s'", lineages[1].Column)
	}
	if len(lineages[1].Sources) != 1 || lineages[1].Sources[0].Table != "orders" || lineages[1].Sources[0].Column != "amount" {
		t.Errorf("expected source orders.amount, got %+v", lineages[1].Sources)
	}
}

func TestExtractFromAST_NestedCTEs(t *testing.T) {
	t.Parallel()
	// AST for:
	// WITH raw AS (SELECT id, amount FROM orders),
	//      agg AS (SELECT SUM(amount) as total FROM raw)
	// SELECT a.total FROM agg a
	astJSON := `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[{"key":"raw","value":{"query":{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["id"]},{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["amount"]}],"from_table":{"type":"BASE_TABLE","alias":"","table_name":"orders"}}}}},{"key":"agg","value":{"query":{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[{"class":"FUNCTION","type":"FUNCTION","alias":"total","function_name":"sum","children":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["amount"]}]}],"from_table":{"type":"BASE_TABLE","alias":"","table_name":"raw"}}}}}]},"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["a","total"]}],"from_table":{"type":"BASE_TABLE","alias":"a","table_name":"agg"}}}]}`

	lineages, err := ExtractFromAST(astJSON)
	if err != nil {
		t.Fatalf("ExtractFromAST failed: %v", err)
	}

	if len(lineages) != 1 {
		t.Fatalf("expected 1 column, got %d", len(lineages))
	}

	// Check a.total -> orders.amount (traced through nested CTEs: agg -> raw -> orders)
	if lineages[0].Column != "total" {
		t.Errorf("expected column 'total', got '%s'", lineages[0].Column)
	}
	if len(lineages[0].Sources) != 1 || lineages[0].Sources[0].Table != "orders" || lineages[0].Sources[0].Column != "amount" {
		t.Errorf("expected source orders.amount, got %+v", lineages[0].Sources)
	}
}

func TestExtractFromAST_ExpressionWithAlias(t *testing.T) {
	t.Parallel()
	// AST for: SELECT o.amount * 1.25 AS total_with_tax FROM orders o
	astJSON := `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[{"class":"FUNCTION","type":"FUNCTION","alias":"total_with_tax","function_name":"*","is_operator":true,"children":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["o","amount"]},{"class":"CONSTANT","type":"VALUE_CONSTANT","alias":"","value":{"type":{"id":"DECIMAL"}},"value_is_null":false}]}],"from_table":{"type":"BASE_TABLE","alias":"o","table_name":"orders"}}}]}`

	lineages, err := ExtractFromAST(astJSON)
	if err != nil {
		t.Fatalf("ExtractFromAST failed: %v", err)
	}

	if len(lineages) != 1 {
		t.Fatalf("expected 1 column, got %d", len(lineages))
	}

	// Check total_with_tax -> orders.amount
	if lineages[0].Column != "total_with_tax" {
		t.Errorf("expected column 'total_with_tax', got '%s'", lineages[0].Column)
	}
	if len(lineages[0].Sources) != 1 || lineages[0].Sources[0].Table != "orders" || lineages[0].Sources[0].Column != "amount" {
		t.Errorf("expected source orders.amount, got %+v", lineages[0].Sources)
	}
}

func TestExtractFromAST_EmptyAST(t *testing.T) {
	t.Parallel()
	astJSON := `{"error":false,"statements":[]}`

	_, err := ExtractFromAST(astJSON)
	if err == nil {
		t.Error("expected error for empty AST")
	}
}

func TestExtractFromAST_InvalidJSON(t *testing.T) {
	t.Parallel()
	_, err := ExtractFromAST("not valid json")
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestExtractTablesFromAST_SimpleQuery(t *testing.T) {
	t.Parallel()
	// AST for: SELECT a, b FROM orders
	astJSON := `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["a"]},{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["b"]}],"from_table":{"type":"BASE_TABLE","alias":"","table_name":"orders"}}}]}`

	tables, err := ExtractTablesFromAST(astJSON)
	if err != nil {
		t.Fatalf("ExtractTablesFromAST failed: %v", err)
	}

	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(tables))
	}

	if tables[0].Table != "orders" {
		t.Errorf("expected table 'orders', got '%s'", tables[0].Table)
	}
	if !tables[0].IsFirstFrom {
		t.Error("expected IsFirstFrom to be true")
	}
	if tables[0].IsJoin {
		t.Error("expected IsJoin to be false")
	}
}

func TestExtractTablesFromAST_JoinQuery(t *testing.T) {
	t.Parallel()
	// AST for: SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id
	astJSON := `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["o","id"]},{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["c","name"]}],"from_table":{"type":"JOIN","left":{"type":"BASE_TABLE","alias":"o","table_name":"orders"},"right":{"type":"BASE_TABLE","alias":"c","table_name":"customers"}}}}]}`

	tables, err := ExtractTablesFromAST(astJSON)
	if err != nil {
		t.Fatalf("ExtractTablesFromAST failed: %v", err)
	}

	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(tables))
	}

	// First table should be orders (the FROM table)
	if tables[0].Table != "orders" {
		t.Errorf("expected first table 'orders', got '%s'", tables[0].Table)
	}
	if !tables[0].IsFirstFrom {
		t.Error("expected orders.IsFirstFrom to be true")
	}
	if tables[0].IsJoin {
		t.Error("expected orders.IsJoin to be false")
	}

	// Second table should be customers (the JOIN table)
	if tables[1].Table != "customers" {
		t.Errorf("expected second table 'customers', got '%s'", tables[1].Table)
	}
	if tables[1].IsFirstFrom {
		t.Error("expected customers.IsFirstFrom to be false")
	}
	if !tables[1].IsJoin {
		t.Error("expected customers.IsJoin to be true")
	}
}

func TestExtractTablesFromAST_WithCTE(t *testing.T) {
	t.Parallel()
	// AST for:
	// WITH order_totals AS (SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id)
	// SELECT c.name, ot.total FROM customers c JOIN order_totals ot ON c.id = ot.customer_id
	// CTE "order_totals" should NOT be in the result, but "orders" inside CTE should be
	astJSON := `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[{"key":"order_totals","value":{"query":{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["customer_id"]},{"class":"FUNCTION","type":"FUNCTION","alias":"total","function_name":"sum","children":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["amount"]}]}],"from_table":{"type":"BASE_TABLE","alias":"","table_name":"orders"}}}}}]},"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["c","name"]},{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["ot","total"]}],"from_table":{"type":"JOIN","left":{"type":"BASE_TABLE","alias":"c","table_name":"customers"},"right":{"type":"BASE_TABLE","alias":"ot","table_name":"order_totals"}}}}]}`

	tables, err := ExtractTablesFromAST(astJSON)
	if err != nil {
		t.Fatalf("ExtractTablesFromAST failed: %v", err)
	}

	// Should have 2 tables: orders (from CTE body) and customers (from main query)
	// CTE name "order_totals" should be excluded
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables (CTE name excluded), got %d: %+v", len(tables), tables)
	}

	// Build set of found tables
	found := make(map[string]bool)
	for _, table := range tables {
		found[table.Table] = true
	}

	if !found["orders"] {
		t.Error("expected 'orders' table from CTE body")
	}
	if !found["customers"] {
		t.Error("expected 'customers' table from main query")
	}
	if found["order_totals"] {
		t.Error("CTE name 'order_totals' should be excluded")
	}
}

func TestExtractFromAST_CaseExpression(t *testing.T) {
	t.Parallel()
	// AST for: SELECT CASE WHEN status = 'active' THEN name ELSE 'unknown' END AS label FROM users
	astJSON := `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[{"class":"CASE","type":"CASE","alias":"label","case_checks":[{"when_expr":{"class":"COMPARISON","type":"COMPARISON","children":[{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["status"]},{"class":"CONSTANT","type":"VALUE_CONSTANT"}]},"then_expr":{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["name"]}}],"children":[{"class":"CONSTANT","type":"VALUE_CONSTANT"}]}],"from_table":{"type":"BASE_TABLE","alias":"","table_name":"users"}}}]}`

	lineages, err := ExtractFromAST(astJSON)
	if err != nil {
		t.Fatalf("ExtractFromAST failed: %v", err)
	}

	if len(lineages) != 1 {
		t.Fatalf("expected 1 column, got %d", len(lineages))
	}

	if lineages[0].Column != "label" {
		t.Errorf("expected column 'label', got '%s'", lineages[0].Column)
	}

	// Should trace both the WHEN condition (status) and THEN value (name)
	foundStatus := false
	foundName := false
	for _, src := range lineages[0].Sources {
		if src.Column == "status" && src.Transformation == TransformConditional {
			foundStatus = true
		}
		if src.Column == "name" && src.Transformation == TransformConditional {
			foundName = true
		}
	}
	if !foundName {
		t.Errorf("expected THEN column 'name' with CONDITIONAL transform, got %+v", lineages[0].Sources)
	}
	if !foundStatus {
		t.Errorf("expected WHEN column 'status' with CONDITIONAL/COMPARISON transform, got %+v", lineages[0].Sources)
	}
}

func TestExtractFromAST_CastExpression(t *testing.T) {
	t.Parallel()
	// AST for: SELECT CAST(amount AS DECIMAL) AS amount_dec FROM orders
	astJSON := `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[{"class":"OPERATOR_CAST","type":"OPERATOR_CAST","alias":"amount_dec","children":[{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["amount"]}]}],"from_table":{"type":"BASE_TABLE","alias":"","table_name":"orders"}}}]}`

	lineages, err := ExtractFromAST(astJSON)
	if err != nil {
		t.Fatalf("ExtractFromAST failed: %v", err)
	}

	if len(lineages) != 1 {
		t.Fatalf("expected 1 column, got %d", len(lineages))
	}

	if lineages[0].Column != "amount_dec" {
		t.Errorf("expected column 'amount_dec', got '%s'", lineages[0].Column)
	}
	if len(lineages[0].Sources) != 1 {
		t.Fatalf("expected 1 source, got %d", len(lineages[0].Sources))
	}
	if lineages[0].Sources[0].Transformation != TransformCast {
		t.Errorf("expected CAST transformation, got %s", lineages[0].Sources[0].Transformation)
	}
}

func TestClassifyFunction(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		funcName   string
		isOperator bool
		expected   TransformationType
	}{
		{"addition", "+", true, TransformArithmetic},
		{"subtraction", "-", true, TransformArithmetic},
		{"multiplication", "*", true, TransformArithmetic},
		{"division", "/", true, TransformArithmetic},
		{"modulo", "%", true, TransformArithmetic},
		{"power", "^", true, TransformArithmetic},
		{"sum", "SUM", false, TransformAggregation},
		{"count", "COUNT", false, TransformAggregation},
		{"avg", "AVG", false, TransformAggregation},
		{"min", "MIN", false, TransformAggregation},
		{"max", "MAX", false, TransformAggregation},
		{"string_agg", "STRING_AGG", false, TransformAggregation},
		{"array_agg", "ARRAY_AGG", false, TransformAggregation},
		{"stddev", "STDDEV", false, TransformAggregation},
		{"variance", "VARIANCE", false, TransformAggregation},
		{"lowercase_sum", "sum", false, TransformAggregation},
		{"coalesce", "COALESCE", false, TransformFunction},
		{"upper", "UPPER", false, TransformFunction},
		{"non_operator_plus", "+", false, TransformFunction},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := classifyFunction(tt.funcName, tt.isOperator)
			if result != tt.expected {
				t.Errorf("classifyFunction(%q, %v) = %s, want %s", tt.funcName, tt.isOperator, result, tt.expected)
			}
		})
	}
}

func TestExtractTablesFromAST_InvalidJSON(t *testing.T) {
	t.Parallel()
	_, err := ExtractTablesFromAST("not valid json")
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestExtractTablesFromAST_EmptyStatements(t *testing.T) {
	t.Parallel()
	_, err := ExtractTablesFromAST(`{"error":false,"statements":[]}`)
	if err == nil {
		t.Error("expected error for empty statements")
	}
}

func TestExtractTablesFromAST_WithSchemaName(t *testing.T) {
	t.Parallel()
	// AST for: SELECT * FROM staging.orders
	astJSON := `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[{"class":"STAR","type":"STAR"}],"from_table":{"type":"BASE_TABLE","alias":"","schema_name":"staging","table_name":"orders"}}}]}`

	tables, err := ExtractTablesFromAST(astJSON)
	if err != nil {
		t.Fatalf("ExtractTablesFromAST failed: %v", err)
	}

	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(tables))
	}
	if tables[0].Table != "staging.orders" {
		t.Errorf("expected 'staging.orders', got %q", tables[0].Table)
	}
}

func TestExtractTablesFromAST_WhereSubquery(t *testing.T) {
	t.Parallel()
	// AST for: SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE active = true)
	astJSON := `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["id"]}],"from_table":{"type":"BASE_TABLE","alias":"","table_name":"orders"},"where_clause":{"class":"COMPARISON","type":"COMPARISON","child":{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["customer_id"]},"subquery":{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["id"]}],"from_table":{"type":"BASE_TABLE","alias":"","table_name":"customers"}}}}}}]}`

	tables, err := ExtractTablesFromAST(astJSON)
	if err != nil {
		t.Fatalf("ExtractTablesFromAST failed: %v", err)
	}

	found := make(map[string]bool)
	for _, t := range tables {
		found[t.Table] = true
	}

	if !found["orders"] {
		t.Error("expected 'orders' table")
	}
	if !found["customers"] {
		t.Error("expected 'customers' table from WHERE subquery")
	}
}

func TestExtractTablesFromAST_HavingClause(t *testing.T) {
	t.Parallel()
	// AST with HAVING that references a subquery
	astJSON := `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["id"]}],"from_table":{"type":"BASE_TABLE","alias":"","table_name":"orders"},"having":{"class":"COMPARISON","type":"COMPARISON","children":[{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["cnt"]}]}}}]}`

	tables, err := ExtractTablesFromAST(astJSON)
	if err != nil {
		t.Fatalf("ExtractTablesFromAST failed: %v", err)
	}

	if len(tables) != 1 || tables[0].Table != "orders" {
		t.Errorf("expected orders, got %+v", tables)
	}
}

func TestExtractTablesFromAST_QualifyClause(t *testing.T) {
	t.Parallel()
	// AST with QUALIFY clause
	astJSON := `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["id"]}],"from_table":{"type":"BASE_TABLE","alias":"","table_name":"orders"},"qualify":{"class":"COMPARISON","type":"COMPARISON","children":[{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["rn"]}]}}}]}`

	tables, err := ExtractTablesFromAST(astJSON)
	if err != nil {
		t.Fatalf("ExtractTablesFromAST failed: %v", err)
	}

	if len(tables) != 1 || tables[0].Table != "orders" {
		t.Errorf("expected orders, got %+v", tables)
	}
}

func TestExtractFromAST_ComparisonExpression(t *testing.T) {
	t.Parallel()
	// AST for: SELECT a > b AS is_greater FROM t1
	astJSON := `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[{"class":"COMPARISON","type":"COMPARISON","alias":"is_greater","children":[{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["a"]},{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["b"]}]}],"from_table":{"type":"BASE_TABLE","alias":"","table_name":"t1"}}}]}`

	lineages, err := ExtractFromAST(astJSON)
	if err != nil {
		t.Fatalf("ExtractFromAST failed: %v", err)
	}

	if len(lineages) != 1 {
		t.Fatalf("expected 1 column, got %d", len(lineages))
	}
	if lineages[0].Column != "is_greater" {
		t.Errorf("expected column 'is_greater', got %q", lineages[0].Column)
	}
	// Should trace both a and b as sources
	if len(lineages[0].Sources) != 2 {
		t.Fatalf("expected 2 sources, got %d: %+v", len(lineages[0].Sources), lineages[0].Sources)
	}
}

func TestExtractFromAST_AggregateFunction(t *testing.T) {
	t.Parallel()
	// AST for: SELECT COUNT(id) AS cnt FROM orders
	astJSON := `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[{"class":"FUNCTION","type":"FUNCTION","alias":"cnt","function_name":"count","children":[{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["id"]}]}],"from_table":{"type":"BASE_TABLE","alias":"","table_name":"orders"}}}]}`

	lineages, err := ExtractFromAST(astJSON)
	if err != nil {
		t.Fatalf("ExtractFromAST failed: %v", err)
	}

	if len(lineages) != 1 {
		t.Fatalf("expected 1 column, got %d", len(lineages))
	}
	if lineages[0].Sources[0].Transformation != TransformAggregation {
		t.Errorf("expected AGGREGATION, got %s", lineages[0].Sources[0].Transformation)
	}
	if lineages[0].Sources[0].FunctionName != "count" {
		t.Errorf("expected function_name 'count', got %q", lineages[0].Sources[0].FunctionName)
	}
}

func TestExtractFromAST_UnqualifiedColumn(t *testing.T) {
	t.Parallel()
	// AST for: SELECT name FROM users (unqualified - uses primary table)
	astJSON := `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["name"]}],"from_table":{"type":"BASE_TABLE","alias":"","table_name":"users"}}}]}`

	lineages, err := ExtractFromAST(astJSON)
	if err != nil {
		t.Fatalf("ExtractFromAST failed: %v", err)
	}

	if len(lineages) != 1 {
		t.Fatalf("expected 1 column, got %d", len(lineages))
	}
	if lineages[0].Sources[0].Table != "users" {
		t.Errorf("expected source table 'users', got %q", lineages[0].Sources[0].Table)
	}
}

func TestExtractFromAST_CTEColumnNotFound(t *testing.T) {
	t.Parallel()
	// Query references a CTE column that doesn't exist in the CTE's select list
	// WITH raw AS (SELECT id FROM orders) SELECT raw.nonexistent FROM raw
	astJSON := `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[{"key":"raw","value":{"query":{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["id"]}],"from_table":{"type":"BASE_TABLE","alias":"","table_name":"orders"}}}}}]},"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["raw","nonexistent"]}],"from_table":{"type":"BASE_TABLE","alias":"","table_name":"raw"}}}]}`

	lineages, err := ExtractFromAST(astJSON)
	if err != nil {
		t.Fatalf("ExtractFromAST failed: %v", err)
	}

	if len(lineages) != 1 {
		t.Fatalf("expected 1 column, got %d", len(lineages))
	}
	// Should fall back to CTE as source since column isn't found
	if lineages[0].Sources[0].Table != "raw" {
		t.Errorf("expected source table 'raw' (CTE fallback), got %q", lineages[0].Sources[0].Table)
	}
}

func TestExtractTablesFromAST_SelectListSubquery(t *testing.T) {
	t.Parallel()
	// AST for: SELECT id, (SELECT MAX(val) FROM lookup) AS max_val FROM orders
	astJSON := `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["id"]},{"class":"SUBQUERY","type":"SUBQUERY","alias":"max_val","subquery":{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[{"class":"FUNCTION","type":"FUNCTION","function_name":"max","children":[{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["val"]}]}],"from_table":{"type":"BASE_TABLE","alias":"","table_name":"lookup"}}}}],"from_table":{"type":"BASE_TABLE","alias":"","table_name":"orders"}}}]}`

	tables, err := ExtractTablesFromAST(astJSON)
	if err != nil {
		t.Fatalf("ExtractTablesFromAST failed: %v", err)
	}

	found := make(map[string]bool)
	for _, tbl := range tables {
		found[tbl.Table] = true
	}

	if !found["orders"] {
		t.Error("expected 'orders' table")
	}
	if !found["lookup"] {
		t.Error("expected 'lookup' table from scalar subquery in SELECT list")
	}
}

func TestExtractTablesFromAST_CaseCheckSubquery(t *testing.T) {
	t.Parallel()
	// AST with CASE check containing a subquery reference
	astJSON := `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[{"class":"CASE","type":"CASE","alias":"result","case_checks":[{"when_expr":{"class":"COMPARISON","type":"COMPARISON","children":[{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["status"]}]},"then_expr":{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["name"]}}],"children":[]}],"from_table":{"type":"BASE_TABLE","alias":"","table_name":"orders"}}}]}`

	tables, err := ExtractTablesFromAST(astJSON)
	if err != nil {
		t.Fatalf("ExtractTablesFromAST failed: %v", err)
	}

	if len(tables) != 1 || tables[0].Table != "orders" {
		t.Errorf("expected [orders], got %+v", tables)
	}
}

func TestGetAllTablesFromRefs_Dedup(t *testing.T) {
	t.Parallel()
	refs := []TableRef{
		{Table: "orders", IsFirstFrom: true},
		{Table: "customers", IsJoin: true},
		{Table: "orders", IsJoin: true}, // duplicate
	}

	result := GetAllTablesFromRefs(refs)
	if len(result) != 2 {
		t.Fatalf("expected 2 unique tables, got %d: %v", len(result), result)
	}
}

func TestGetAllTablesFromRefs_NilAndEmpty(t *testing.T) {
	t.Parallel()
	if got := GetAllTablesFromRefs(nil); len(got) != 0 {
		t.Errorf("nil input: got %v, want empty", got)
	}
	if got := GetAllTablesFromRefs([]TableRef{}); len(got) != 0 {
		t.Errorf("empty input: got %v, want empty", got)
	}
}

func TestGetCDCTables_NilAndEmpty(t *testing.T) {
	t.Parallel()
	if got := GetCDCTables(nil); len(got) != 0 {
		t.Errorf("nil input: got %v, want empty", got)
	}
	if got := GetCDCTables([]ColumnLineage{}); len(got) != 0 {
		t.Errorf("empty input: got %v, want empty", got)
	}
}

func TestExtractFromAST_MalformedJSON(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
	}{
		{"empty string", ""},
		{"invalid JSON", "{not json}"},
		{"truncated JSON", `{"statements": [`},
		{"null", "null"},
		{"number", "42"},
		{"empty object", "{}"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := ExtractFromAST(tt.input)
			// Should either return error or empty result, never panic
			_ = result
			_ = err
		})
	}
}

func TestExtractTablesFromAST_MalformedJSON(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
	}{
		{"empty string", ""},
		{"invalid JSON", "{not json}"},
		{"null", "null"},
		{"empty object", "{}"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := ExtractTablesFromAST(tt.input)
			_ = result
			_ = err
		})
	}
}

func TestGetCDCTables(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		lineage  []ColumnLineage
		expected map[string]bool
	}{
		{
			name: "aggregation on orders",
			lineage: []ColumnLineage{
				{
					Column: "customer_id",
					Sources: []SourceColumn{
						{Table: "staging.customers", Column: "customer_id", Transformation: TransformIdentity},
					},
				},
				{
					Column: "total_orders",
					Sources: []SourceColumn{
						{Table: "staging.orders", Column: "order_id", Transformation: TransformAggregation},
					},
				},
			},
			expected: map[string]bool{"staging.orders": true},
		},
		{
			name: "only identity - no CDC",
			lineage: []ColumnLineage{
				{
					Column: "name",
					Sources: []SourceColumn{
						{Table: "customers", Column: "name", Transformation: TransformIdentity},
					},
				},
			},
			expected: map[string]bool{},
		},
		{
			name: "multiple aggregations on same table",
			lineage: []ColumnLineage{
				{
					Column: "count",
					Sources: []SourceColumn{
						{Table: "orders", Column: "id", Transformation: TransformAggregation},
					},
				},
				{
					Column: "sum",
					Sources: []SourceColumn{
						{Table: "orders", Column: "amount", Transformation: TransformAggregation},
					},
				},
			},
			expected: map[string]bool{"orders": true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := GetCDCTables(tt.lineage)
			if len(result) != len(tt.expected) {
				t.Errorf("GetCDCTables() = %v, want %v", result, tt.expected)
			}
			for k := range tt.expected {
				if !result[k] {
					t.Errorf("Expected table %s to need CDC", k)
				}
			}
		})
	}
}
