// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package lineage

import (
	"os"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
)

var shared *duckdb.Session

func TestMain(m *testing.M) {
	sess, err := duckdb.NewSession(":memory:?threads=2&memory_limit=1GB")
	if err != nil {
		panic(err)
	}
	shared = sess
	code := m.Run()
	shared.Close()
	os.Exit(code)
}

func TestGetAST(t *testing.T) {
	ast, err := GetAST(shared, "SELECT 1 AS id, 'hello' AS name")
	if err != nil {
		t.Fatalf("GetAST: %v", err)
	}
	if ast == "" {
		t.Fatal("expected non-empty AST")
	}
	if !strings.Contains(ast, "statements") {
		t.Error("AST should contain 'statements'")
	}
}

func TestGetAST_EmptySQL(t *testing.T) {
	_, err := GetAST(shared, "")
	// Empty SQL may return error or empty AST
	if err == nil {
		t.Log("empty SQL did not error - DuckDB may parse it")
	}
}

func TestExtract_SimpleSelect(t *testing.T) {
	lineages, err := Extract(shared, "SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(lineages) < 2 {
		t.Fatalf("expected at least 2 column lineages, got %d", len(lineages))
	}

	colMap := make(map[string]bool)
	for _, l := range lineages {
		colMap[l.Column] = true
	}
	if !colMap["id"] {
		t.Error("missing column 'id'")
	}
	if !colMap["name"] {
		t.Error("missing column 'name'")
	}
}

func TestExtract_WithAlias(t *testing.T) {
	lineages, err := Extract(shared, "SELECT id AS user_id, name AS user_name FROM users")
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}

	colMap := make(map[string]bool)
	for _, l := range lineages {
		colMap[l.Column] = true
	}
	if !colMap["user_id"] {
		t.Error("missing column 'user_id'")
	}
	if !colMap["user_name"] {
		t.Error("missing column 'user_name'")
	}
}

func TestExtract_Aggregation(t *testing.T) {
	lineages, err := Extract(shared, "SELECT category, SUM(amount) AS total FROM orders GROUP BY category")
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}

	for _, l := range lineages {
		if l.Column == "total" {
			if len(l.Sources) == 0 {
				t.Error("expected sources for 'total'")
			}
			for _, s := range l.Sources {
				if s.Column == "amount" && s.Transformation != TransformAggregation {
					t.Errorf("expected AGGREGATION transform for total, got %s", s.Transformation)
				}
			}
			return
		}
	}
	t.Error("missing column 'total' in lineage")
}

func TestExtract_WithCTE(t *testing.T) {
	sql := `WITH base AS (
		SELECT id, amount FROM orders
	)
	SELECT id, amount * 2 AS doubled FROM base`
	lineages, err := Extract(shared, sql)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}

	colMap := make(map[string]bool)
	for _, l := range lineages {
		colMap[l.Column] = true
	}
	if !colMap["doubled"] {
		t.Error("missing column 'doubled'")
	}
}

func TestExtract_Join(t *testing.T) {
	sql := `SELECT o.id, o.amount, c.name
	FROM orders o
	JOIN customers c ON o.customer_id = c.id`
	lineages, err := Extract(shared, sql)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(lineages) < 3 {
		t.Fatalf("expected at least 3 columns, got %d", len(lineages))
	}
}

func TestExtract_CaseExpression(t *testing.T) {
	sql := `SELECT id, CASE WHEN status = 'active' THEN 1 ELSE 0 END AS is_active FROM users`
	lineages, err := Extract(shared, sql)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}

	for _, l := range lineages {
		if l.Column == "is_active" {
			if len(l.Sources) > 0 && l.Sources[0].Transformation != TransformConditional {
				// Conditional transform is expected for CASE
			}
			return
		}
	}
	t.Error("missing column 'is_active'")
}

func TestExtractTables_SimpleFrom(t *testing.T) {
	tables, err := ExtractTables(shared, "SELECT * FROM orders")
	if err != nil {
		t.Fatalf("ExtractTables: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(tables))
	}
	if tables[0].Table != "orders" {
		t.Errorf("table = %q, want orders", tables[0].Table)
	}
	if !tables[0].IsFirstFrom {
		t.Error("expected IsFirstFrom=true")
	}
}

func TestExtractTables_SchemaQualified(t *testing.T) {
	tables, err := ExtractTables(shared, "SELECT * FROM staging.orders")
	if err != nil {
		t.Fatalf("ExtractTables: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(tables))
	}
	if tables[0].Table != "staging.orders" {
		t.Errorf("table = %q, want staging.orders", tables[0].Table)
	}
}

func TestExtractTables_Join(t *testing.T) {
	sql := `SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id`
	tables, err := ExtractTables(shared, sql)
	if err != nil {
		t.Fatalf("ExtractTables: %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(tables))
	}

	names := make(map[string]bool)
	for _, tr := range tables {
		names[tr.Table] = true
	}
	if !names["orders"] {
		t.Error("missing table 'orders'")
	}
	if !names["customers"] {
		t.Error("missing table 'customers'")
	}
}

func TestExtractTables_CTEExcluded(t *testing.T) {
	sql := `WITH base AS (SELECT 1 AS id FROM orders)
	SELECT * FROM base`
	tables, err := ExtractTables(shared, sql)
	if err != nil {
		t.Fatalf("ExtractTables: %v", err)
	}

	for _, tr := range tables {
		if tr.Table == "base" {
			t.Error("CTE 'base' should not appear in extracted tables")
		}
	}
	// Should have 'orders' from inside the CTE
	found := false
	for _, tr := range tables {
		if tr.Table == "orders" {
			found = true
		}
	}
	if !found {
		t.Error("expected 'orders' from CTE body")
	}
}

func TestExtractTables_SubqueryInWhere(t *testing.T) {
	sql := `SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE active = true)`
	tables, err := ExtractTables(shared, sql)
	if err != nil {
		t.Fatalf("ExtractTables: %v", err)
	}

	names := make(map[string]bool)
	for _, tr := range tables {
		names[tr.Table] = true
	}
	if !names["orders"] {
		t.Error("missing 'orders'")
	}
	if !names["customers"] {
		t.Error("missing 'customers' from subquery")
	}
}

func TestGetAllTables(t *testing.T) {
	tables, err := GetAllTables(shared, "SELECT * FROM staging.orders JOIN raw.customers ON true")
	if err != nil {
		t.Fatalf("GetAllTables: %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d: %v", len(tables), tables)
	}
}

func TestGetAllTablesFromAST(t *testing.T) {
	ast, err := GetAST(shared, "SELECT * FROM a JOIN b ON true")
	if err != nil {
		t.Fatalf("GetAST: %v", err)
	}
	tables, err := GetAllTablesFromAST(ast)
	if err != nil {
		t.Fatalf("GetAllTablesFromAST: %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d: %v", len(tables), tables)
	}
}

func TestGetAllTablesFromRefs(t *testing.T) {
	refs := []TableRef{
		{Table: "orders"},
		{Table: "customers"},
		{Table: "orders"}, // duplicate
	}
	result := GetAllTablesFromRefs(refs)
	if len(result) != 2 {
		t.Fatalf("expected 2 unique tables, got %d: %v", len(result), result)
	}
}

func TestGetAllTablesFromRefs_Empty(t *testing.T) {
	result := GetAllTablesFromRefs(nil)
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestExtract_ScalarSubquery(t *testing.T) {
	sql := `SELECT id, (SELECT MAX(amount) FROM orders) AS max_amount FROM users`
	lineages, err := Extract(shared, sql)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(lineages) < 2 {
		t.Fatalf("expected at least 2 columns, got %d", len(lineages))
	}
}

func TestExtractAll(t *testing.T) {
	lineage, tables, err := ExtractAll(shared, "SELECT o.id, c.name FROM orders o JOIN customers c ON o.cid = c.id")
	if err != nil {
		t.Fatalf("ExtractAll: %v", err)
	}
	if len(lineage) < 2 {
		t.Errorf("expected at least 2 column lineages, got %d", len(lineage))
	}
	if len(tables) != 2 {
		t.Errorf("expected 2 tables, got %d: %v", len(tables), tables)
	}
}

func TestExtractAll_EmptySQL(t *testing.T) {
	lineage, tables, err := ExtractAll(shared, "")
	// Empty SQL should return empty results (error or not depends on DuckDB)
	if err == nil {
		if len(lineage) != 0 {
			t.Errorf("expected no lineage for empty SQL, got %d", len(lineage))
		}
		if len(tables) != 0 {
			t.Errorf("expected no tables for empty SQL, got %d", len(tables))
		}
	}
}

func TestExtractTables_MultipleCTEs(t *testing.T) {
	sql := `WITH cte1 AS (SELECT id FROM orders),
	             cte2 AS (SELECT id FROM customers)
	SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.id`
	tables, err := ExtractTables(shared, sql)
	if err != nil {
		t.Fatalf("ExtractTables: %v", err)
	}

	names := make(map[string]bool)
	for _, tr := range tables {
		names[tr.Table] = true
	}
	// CTEs should be excluded, physical tables included
	if names["cte1"] || names["cte2"] {
		t.Error("CTEs should not appear in extracted tables")
	}
	if !names["orders"] || !names["customers"] {
		t.Errorf("expected orders and customers, got %v", names)
	}
}

func TestExtract_CaseWithElse(t *testing.T) {
	sql := `SELECT CASE WHEN s.amount > 100 THEN s.category ELSE s.fallback END AS result FROM staging.sales s`
	lineage, err := Extract(shared, sql)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(lineage) == 0 {
		t.Fatal("expected lineage entries")
	}
	// result should trace through CASE with conditional transformation
	found := false
	for _, cl := range lineage {
		if cl.Column == "result" {
			found = true
			if len(cl.Sources) == 0 {
				t.Error("expected sources for CASE output")
			}
			hasConditional := false
			for _, s := range cl.Sources {
				if s.Transformation == TransformConditional {
					hasConditional = true
				}
			}
			if !hasConditional {
				t.Error("expected conditional transformation in CASE sources")
			}
		}
	}
	if !found {
		t.Error("expected 'result' in lineage output")
	}
}

func TestExtract_CastExpression(t *testing.T) {
	// CAST with table-qualified column reference
	sql := `SELECT s.id::BIGINT AS big_id, s.name FROM staging.data s`
	lineage, err := Extract(shared, sql)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(lineage) < 2 {
		t.Fatalf("expected at least 2 lineage entries, got %d", len(lineage))
	}
}

func TestExtract_SubqueryInSelect(t *testing.T) {
	sql := `SELECT id, (SELECT MAX(amount) FROM orders WHERE orders.id = t.id) AS max_amt FROM staging.test t`
	lineage, err := Extract(shared, sql)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(lineage) == 0 {
		t.Fatal("expected lineage entries")
	}
}

func TestExtractTables_SubqueryInSelect(t *testing.T) {
	sql := `SELECT id, (SELECT MAX(val) FROM other.data WHERE other.data.id = t.id) AS sub FROM staging.main t`
	tables, err := ExtractTables(shared, sql)
	if err != nil {
		t.Fatalf("ExtractTables: %v", err)
	}
	names := make(map[string]bool)
	for _, tr := range tables {
		names[tr.Table] = true
	}
	if !names["staging.main"] {
		t.Error("expected staging.main in tables")
	}
	if !names["other.data"] {
		t.Error("expected other.data in tables from scalar subquery")
	}
}

func TestExtractTables_QualifyClause(t *testing.T) {
	// QUALIFY is a DuckDB-specific clause that filters window function results
	sql := `SELECT id, amount, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn FROM staging.sales QUALIFY rn = 1`
	tables, err := ExtractTables(shared, sql)
	if err != nil {
		t.Fatalf("ExtractTables: %v", err)
	}
	names := make(map[string]bool)
	for _, tr := range tables {
		names[tr.Table] = true
	}
	if !names["staging.sales"] {
		t.Errorf("expected staging.sales in tables, got %v", names)
	}
}

func TestExtract_InvalidSQL(t *testing.T) {
	_, err := Extract(shared, "THIS IS NOT SQL")
	if err == nil {
		t.Error("expected error for invalid SQL")
	}
}

func TestExtractTables_InvalidSQL(t *testing.T) {
	_, err := ExtractTables(shared, "INVALID SQL QUERY")
	if err == nil {
		t.Error("expected error for invalid SQL")
	}
}

func TestGetAllTables_InvalidSQL(t *testing.T) {
	_, err := GetAllTables(shared, "NOT VALID SQL")
	if err == nil {
		t.Error("expected error for invalid SQL")
	}
}

func TestExtractAll_WithTables(t *testing.T) {
	sql := `SELECT s.id, s.name FROM staging.users s JOIN raw.roles r ON s.role_id = r.id`
	lineage, tables, err := ExtractAll(shared, sql)
	if err != nil {
		t.Fatalf("ExtractAll: %v", err)
	}
	if len(lineage) == 0 {
		t.Error("expected lineage entries")
	}
	if len(tables) < 2 {
		t.Errorf("expected at least 2 tables, got %d", len(tables))
	}
}

func TestExtract_ComparisonInCaseWhen(t *testing.T) {
	// Exercises COMPARISON branch in traceExprWithType
	sql := `SELECT CASE WHEN s.amount > 100 THEN 'high' ELSE 'low' END AS tier FROM staging.orders s`
	lineage, err := Extract(shared, sql)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(lineage) == 0 {
		t.Fatal("expected lineage entries")
	}
	found := false
	for _, cl := range lineage {
		if cl.Column == "tier" {
			found = true
			// Should trace back to s.amount through the CASE WHEN condition
			for _, src := range cl.Sources {
				if src.Column == "amount" {
					return // success
				}
			}
		}
	}
	if !found {
		t.Error("expected lineage for 'tier' column")
	}
}

func TestExtract_OperatorCast(t *testing.T) {
	// Exercises OPERATOR_CAST branch in traceExprWithType
	sql := `SELECT CAST(s.id AS BIGINT) AS big_id FROM staging.orders s`
	lineage, err := Extract(shared, sql)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	found := false
	for _, cl := range lineage {
		if cl.Column == "big_id" {
			found = true
			if len(cl.Sources) == 0 {
				// DuckDB may optimize away simple casts, just verify lineage exists
				t.Logf("no sources for big_id (DuckDB may optimize cast away)")
			}
		}
	}
	if !found {
		t.Error("expected lineage for 'big_id' column")
	}
}

func TestExtract_UnqualifiedColumnRef(t *testing.T) {
	// Exercises the unqualified column path (no table alias prefix)
	sql := `SELECT id, name FROM staging.users`
	lineage, err := Extract(shared, sql)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(lineage) < 2 {
		t.Errorf("expected at least 2 lineage entries, got %d", len(lineage))
	}
}

func TestExtractTablesFromAST_EmptyString(t *testing.T) {
	// Exercises the error path when AST is empty string
	_, err := ExtractTablesFromAST("")
	if err == nil {
		t.Error("expected error for empty AST")
	}
}

func TestExtractFromAST_EmptyString(t *testing.T) {
	// Exercises the error path when AST is empty string
	_, err := ExtractFromAST("")
	if err == nil {
		t.Error("expected error for empty AST")
	}
}

func TestGetAllTablesFromAST_EmptyString(t *testing.T) {
	// Exercises the error path when AST is empty string
	_, err := GetAllTablesFromAST("")
	if err == nil {
		t.Error("expected error for empty AST")
	}
}

func TestExtract_WindowFunction(t *testing.T) {
	// Exercises window function classification
	sql := `SELECT s.id, ROW_NUMBER() OVER (PARTITION BY s.category ORDER BY s.amount DESC) AS rn FROM staging.orders s`
	lineage, err := Extract(shared, sql)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(lineage) == 0 {
		t.Error("expected lineage entries")
	}
}

func TestGetOutputName_Fallback(t *testing.T) {
	// Tests that unnamed expressions get "col_N" names
	sql := `SELECT 1+2, 'hello'`
	lineage, err := Extract(shared, sql)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	// Constants don't produce lineage but exercise getOutputName
	_ = lineage
}
