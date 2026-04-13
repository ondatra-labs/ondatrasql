// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute_test

import (
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/execute"
	"github.com/ondatra-labs/ondatrasql/internal/lineage"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// helper to get AST and apply CDC via exported runner method
func cdcHelper(t *testing.T, p *testutil.Project, sql, kind string, cdcTables []string, snapshotID int64) string {
	t.Helper()
	runner := execute.NewRunner(p.Sess, execute.ModeRun, "test-cdc")
	astJSON, err := runner.GetAST(sql)
	if err != nil {
		t.Fatalf("GetAST: %v", err)
	}
	result, err := runner.ApplySmartCDC(astJSON, kind, cdcTables, snapshotID)
	if err != nil {
		t.Fatalf("ApplySmartCDC: %v", err)
	}
	return result
}

func emptyCDCHelper(t *testing.T, p *testutil.Project, sql string, cdcTables []string) string {
	t.Helper()
	runner := execute.NewRunner(p.Sess, execute.ModeRun, "test-cdc")
	astJSON, err := runner.GetAST(sql)
	if err != nil {
		t.Fatalf("GetAST: %v", err)
	}
	result, err := runner.ApplyEmptySmartCDC(astJSON, cdcTables)
	if err != nil {
		t.Fatalf("ApplyEmptySmartCDC: %v", err)
	}
	return result
}

func TestASTCDC_SimpleTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	result := cdcHelper(t, p,
		"SELECT * FROM staging.orders",
		"append", []string{"staging.orders"}, 42)

	if !strings.Contains(result, "EXCEPT") {
		t.Errorf("expected EXCEPT in CDC SQL, got: %s", result)
	}
	if !strings.Contains(result, "staging.orders") {
		t.Errorf("expected staging.orders reference, got: %s", result)
	}
	if !strings.Contains(result, "VERSION => 42") {
		t.Errorf("expected VERSION => 42, got: %s", result)
	}
}

func TestASTCDC_JoinPreservation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	sql := "SELECT o.*, p.name FROM staging.orders o JOIN staging.products p ON o.product_id = p.id"
	// Only orders gets CDC, products is a dimension lookup
	result := cdcHelper(t, p, sql, "append", []string{"staging.orders"}, 10)

	// Should contain EXCEPT for orders
	if !strings.Contains(result, "EXCEPT") {
		t.Errorf("expected EXCEPT for CDC table, got: %s", result)
	}
	// JOIN should be preserved
	if !strings.Contains(strings.ToUpper(result), "JOIN") {
		t.Errorf("expected JOIN to be preserved, got: %s", result)
	}
	// products should remain as-is (not CDC'd)
	if strings.Count(result, "EXCEPT") > 1 {
		t.Errorf("expected only 1 EXCEPT (orders only), got multiple in: %s", result)
	}
}

func TestASTCDC_AliasPreservation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	sql := "SELECT o.id FROM staging.orders o"
	result := cdcHelper(t, p, sql, "append", []string{"staging.orders"}, 5)

	// The alias "o" should be preserved on the subquery
	if !strings.Contains(result, "AS o") {
		t.Errorf("expected alias 'o' to be preserved, got: %s", result)
	}
}

func TestASTCDC_StringLiteralSafety(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// The string constant 'staging.orders' should NOT be affected by CDC rewriting
	sql := "SELECT * FROM staging.orders WHERE note = 'staging.orders'"
	result := cdcHelper(t, p, sql, "append", []string{"staging.orders"}, 42)

	// The string literal should be preserved exactly
	if !strings.Contains(result, "'staging.orders'") {
		t.Errorf("string literal was modified! got: %s", result)
	}
	// CDC should still be applied to the actual table reference
	if !strings.Contains(result, "EXCEPT") {
		t.Errorf("expected EXCEPT for CDC, got: %s", result)
	}
}

func TestASTCDC_CTEHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// CTE "cte" should not be CDC'd, only the actual table staging.orders inside it
	sql := "WITH cte AS (SELECT * FROM staging.orders) SELECT * FROM cte"
	result := cdcHelper(t, p, sql, "append", []string{"staging.orders"}, 7)

	// Should have EXCEPT for staging.orders inside the CTE
	if !strings.Contains(result, "EXCEPT") {
		t.Errorf("expected EXCEPT for staging.orders in CTE, got: %s", result)
	}
}

func TestASTCDC_EmptyCDC(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	result := emptyCDCHelper(t, p,
		"SELECT * FROM staging.orders",
		[]string{"staging.orders"})

	// DuckDB deserializes WHERE false as CAST('f' AS BOOLEAN)
	if !strings.Contains(result, "CAST('f' AS BOOLEAN)") && !strings.Contains(strings.ToLower(result), "false") {
		t.Errorf("expected WHERE false in empty CDC, got: %s", result)
	}
}

func TestASTCDC_EmptyCDC_MultipleTables(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	result := emptyCDCHelper(t, p,
		"SELECT * FROM staging.orders JOIN staging.products ON true",
		[]string{"staging.orders", "staging.products"})

	// Both tables should get WHERE false (DuckDB renders as CAST('f' AS BOOLEAN))
	castCount := strings.Count(result, "CAST('f' AS BOOLEAN)")
	if castCount < 2 {
		t.Errorf("expected 2x WHERE false for 2 tables, got %d in: %s", castCount, result)
	}
}

func TestASTCDC_SandboxQualification(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Test that qualifyTablesInAST works via round-trip
	runner := execute.NewRunner(p.Sess, execute.ModeRun, "test-qualify")
	astJSON, err := runner.GetAST("SELECT * FROM staging.orders")
	if err != nil {
		t.Fatalf("GetAST: %v", err)
	}

	result, err := runner.QualifyAndDeserialize(astJSON, map[string]bool{"staging.orders": true}, "lake")
	if err != nil {
		t.Fatalf("QualifyAndDeserialize: %v", err)
	}

	if !strings.Contains(result, "lake.staging.orders") {
		t.Errorf("expected catalog-qualified table, got: %s", result)
	}
}

func TestASTCDC_InvalidKindPassthrough(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// "table" kind should not apply CDC
	result := cdcHelper(t, p,
		"SELECT * FROM staging.orders",
		"table", []string{"staging.orders"}, 42)

	if strings.Contains(result, "EXCEPT") {
		t.Errorf("table kind should not apply CDC, got: %s", result)
	}
}

func TestASTCDC_EmptyTablesPassthrough(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Empty cdcTables should return original SQL
	result := cdcHelper(t, p,
		"SELECT * FROM staging.orders",
		"append", nil, 42)

	if strings.Contains(result, "EXCEPT") {
		t.Errorf("empty cdcTables should not apply CDC, got: %s", result)
	}
}

func TestASTCDC_MultipleCDCTables(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	sql := "SELECT o.*, p.name FROM staging.orders o JOIN staging.products p ON o.product_id = p.id"
	result := cdcHelper(t, p, sql, "append",
		[]string{"staging.orders", "staging.products"}, 10)

	// Both tables should get EXCEPT
	exceptCount := strings.Count(result, "EXCEPT")
	if exceptCount < 2 {
		t.Errorf("expected 2x EXCEPT for 2 CDC tables, got %d in: %s", exceptCount, result)
	}
}

func TestASTCDC_AllValidKinds(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	for _, kind := range []string{"append", "merge", "scd2", "partition"} {
		t.Run(kind, func(t *testing.T) {
			result := cdcHelper(t, p,
				"SELECT * FROM staging.orders",
				kind, []string{"staging.orders"}, 42)

			if !strings.Contains(result, "EXCEPT") {
				t.Errorf("kind=%s should produce CDC with EXCEPT, got: %s", kind, result)
			}
		})
	}
}

func TestASTCDC_RoundTripPreservesSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Round-trip without CDC should preserve SQL semantics
	runner := execute.NewRunner(p.Sess, execute.ModeRun, "test-roundtrip")

	original := "SELECT id, name FROM staging.orders WHERE id > 0 ORDER BY name"
	astJSON, err := runner.GetAST(original)
	if err != nil {
		t.Fatalf("GetAST: %v", err)
	}
	result, err := runner.DeserializeAST(astJSON)
	if err != nil {
		t.Fatalf("DeserializeAST: %v", err)
	}

	// DuckDB may normalize formatting but semantics should match
	// Check key elements are present
	upper := strings.ToUpper(result)
	if !strings.Contains(upper, "STAGING.ORDERS") {
		t.Errorf("lost table reference in round-trip: %s", result)
	}
	if !strings.Contains(upper, "ORDER BY") {
		t.Errorf("lost ORDER BY in round-trip: %s", result)
	}
}

// Verify that ExtractTablesFromAST still works correctly with the AST
// (important since we're relying on the same AST for both extraction and CDC)
func TestASTCDC_ConsistentTableExtraction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	runner := execute.NewRunner(p.Sess, execute.ModeRun, "test-extract")
	sql := "SELECT o.*, p.name FROM staging.orders o JOIN staging.products p ON o.id = p.id"

	astJSON, err := runner.GetAST(sql)
	if err != nil {
		t.Fatalf("GetAST: %v", err)
	}

	tables, err := lineage.ExtractTablesFromAST(astJSON)
	if err != nil {
		t.Fatalf("ExtractTablesFromAST: %v", err)
	}

	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(tables))
	}

	tableNames := map[string]bool{}
	for _, tbl := range tables {
		tableNames[tbl.Table] = true
	}
	if !tableNames["staging.orders"] {
		t.Error("missing staging.orders")
	}
	if !tableNames["staging.products"] {
		t.Error("missing staging.products")
	}
}
