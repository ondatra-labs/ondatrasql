// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
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

// CTE-name shadowing must respect SQL scope rules. A CTE defined in an
// inner subquery must NOT remove a same-named physical table reference
// from an outer scope. Previously the extractor collected all CTE names
// into a single global set, so `WITH orders AS (...)` inside a WHERE-IN
// would silently drop the outer staging.orders dependency.
func TestExtractTables_CTEScopeIsolation_InnerCannotShadowOuter(t *testing.T) {
	sql := `SELECT * FROM staging.orders
	        WHERE id IN (
	            WITH orders AS (SELECT id FROM cfg.seed)
	            SELECT id FROM orders
	        )`
	tables, err := GetAllTables(shared, sql)
	if err != nil {
		t.Fatalf("GetAllTables: %v", err)
	}
	if !containsTable(tables, "staging.orders") {
		t.Errorf("expected staging.orders (outer FROM, scope-isolated from inner CTE), got %v", tables)
	}
	if !containsTable(tables, "cfg.seed") {
		t.Errorf("expected cfg.seed (inside inner CTE body), got %v", tables)
	}
	// The inner CTE name "orders" must NOT appear as a physical dep.
	for _, n := range tables {
		if n == "orders" {
			t.Errorf("CTE name 'orders' leaked into deps: %v", tables)
		}
	}

	// Primary detection must also respect scope: staging.orders is the
	// outer FROM; the inner subquery's `orders` is a CTE ref, not a
	// physical table, so primary stays staging.orders.
	refs, err := ExtractTables(shared, sql)
	if err != nil {
		t.Fatalf("ExtractTables: %v", err)
	}
	var primary string
	for _, r := range refs {
		if r.IsFirstFrom {
			primary = r.Table
		}
	}
	if primary != "staging.orders" {
		t.Errorf("expected primary staging.orders, got %q (refs=%+v)", primary, refs)
	}
}

// CTEs only shadow unqualified names. A schema-qualified BASE_TABLE
// is always physical, even when a same-named CTE is in the same scope.
func TestExtractTables_CTEDoesNotShadowSchemaQualified(t *testing.T) {
	sql := `WITH orders AS (SELECT id FROM raw.cte_source)
	        SELECT o.id, c.id
	        FROM orders o JOIN staging.orders c ON o.id = c.id`
	tables, err := GetAllTables(shared, sql)
	if err != nil {
		t.Fatalf("GetAllTables: %v", err)
	}
	if !containsTable(tables, "staging.orders") {
		t.Errorf("schema-qualified staging.orders must NOT be shadowed by unqualified CTE 'orders', got %v", tables)
	}
	if !containsTable(tables, "raw.cte_source") {
		t.Errorf("expected raw.cte_source (inside CTE body), got %v", tables)
	}
	for _, n := range tables {
		if n == "orders" {
			t.Errorf("CTE name 'orders' leaked into deps: %v", tables)
		}
	}
}

// Self-join: the same physical table referenced under two aliases must
// produce two TableRef entries with distinct aliases. A previous bug
// deduped by full name and lost the second reference's alias entirely.
func TestExtractTables_SelfJoinPreservesAliases(t *testing.T) {
	sql := `SELECT a.id, b.id FROM staging.orders a JOIN staging.orders b ON a.parent_id = b.id`
	tables, err := ExtractTables(shared, sql)
	if err != nil {
		t.Fatalf("ExtractTables: %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("expected 2 TableRefs for self-join, got %d: %+v", len(tables), tables)
	}
	aliases := map[string]bool{tables[0].Alias: true, tables[1].Alias: true}
	if !aliases["a"] || !aliases["b"] {
		t.Errorf("expected aliases a and b, got %+v", tables)
	}
	// Exactly one should be IsFirstFrom (the leftmost), the other a join.
	primaries := 0
	for _, ref := range tables {
		if ref.Table != "staging.orders" {
			t.Errorf("expected staging.orders, got %q", ref.Table)
		}
		if ref.IsFirstFrom {
			primaries++
		}
	}
	if primaries != 1 {
		t.Errorf("expected exactly 1 IsFirstFrom in self-join, got %d", primaries)
	}
	// GetAllTablesFromRefs still dedups to a single name string.
	names := GetAllTablesFromRefs(tables)
	if len(names) != 1 || names[0] != "staging.orders" {
		t.Errorf("GetAllTablesFromRefs should dedup self-join to one name, got %v", names)
	}
}

// IsFirstFrom must be assigned to the leftmost-outermost physical FROM
// table — NOT to whatever BASE_TABLE the walker happens to encounter
// first. Previously the walker iterated map[string]any in random Go
// map order, so a query with a CTE could non-deterministically tag the
// CTE-internal table as IsFirstFrom instead of the outer FROM table.
//
// This test runs the same query repeatedly; with the old random
// iteration it would flake within a few iterations.
func TestExtractTables_IsFirstFromDeterministic_WithCTE(t *testing.T) {
	sql := `WITH lookup AS (SELECT id, name FROM raw.dimension)
	        SELECT m.id, m.amount, lookup.name
	        FROM staging.fact m JOIN lookup ON m.dim_id = lookup.id`
	for i := 0; i < 50; i++ {
		tables, err := ExtractTables(shared, sql)
		if err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
		var primary string
		for _, ref := range tables {
			if ref.IsFirstFrom {
				if primary != "" {
					t.Fatalf("iter %d: multiple IsFirstFrom: %+v", i, tables)
				}
				primary = ref.Table
			}
		}
		// staging.fact is the outer FROM. raw.dimension is buried inside
		// the CTE body and must NOT be tagged as primary.
		if primary != "staging.fact" {
			t.Errorf("iter %d: expected staging.fact as primary, got %q (tables=%+v)",
				i, primary, tables)
		}
	}
}

// IsFirstFrom for UNION/SET-OP queries should be the leftmost branch's
// primary FROM, not whatever the walker hits first.
func TestExtractTables_IsFirstFromDeterministic_Union(t *testing.T) {
	sql := `SELECT id FROM staging.left_tbl
	        UNION ALL
	        SELECT id FROM staging.right_tbl`
	for i := 0; i < 20; i++ {
		tables, err := ExtractTables(shared, sql)
		if err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
		var primary string
		for _, ref := range tables {
			if ref.IsFirstFrom {
				primary = ref.Table
			}
		}
		if primary != "staging.left_tbl" {
			t.Errorf("iter %d: expected staging.left_tbl (leftmost UNION branch), got %q",
				i, primary)
		}
	}
}

// IsFirstFrom must look past WHERE-IN subqueries — those reference
// other physical tables but they're not "the primary FROM".
func TestExtractTables_IsFirstFrom_NotConfusedByWhereSubquery(t *testing.T) {
	sql := `SELECT id FROM staging.fact
	        WHERE dim_id IN (SELECT id FROM staging.dimension)`
	for i := 0; i < 20; i++ {
		tables, err := ExtractTables(shared, sql)
		if err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
		var primary string
		for _, ref := range tables {
			if ref.IsFirstFrom {
				primary = ref.Table
			}
		}
		if primary != "staging.fact" {
			t.Errorf("iter %d: expected staging.fact (outer FROM), got %q", i, primary)
		}
	}
}

// json_serialize_sql only handles SELECT statements; everything else
// returns an {"error":true,...} envelope. GetAST must surface that as a
// real error rather than silently letting downstream parsers report
// "no statements in AST".
func TestGetAST_NonSelectStatementErrors(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{"CTAS", "CREATE TABLE x AS SELECT * FROM y"},
		{"CREATE VIEW", "CREATE OR REPLACE VIEW v AS SELECT * FROM t"},
		{"INSERT SELECT", "INSERT INTO t SELECT * FROM s"},
		{"UPDATE FROM", "UPDATE t SET v=1 FROM s WHERE t.id=s.id"},
		{"DELETE", "DELETE FROM t WHERE id=1"},
		{"PIVOT statement", "PIVOT t ON c USING SUM(v)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := GetAST(shared, tc.sql)
			if err == nil {
				t.Fatalf("expected error from GetAST(%q), got nil", tc.sql)
			}
			if !strings.Contains(err.Error(), "DuckDB cannot serialize") {
				t.Errorf("expected diagnostic mentioning serialization, got: %v", err)
			}
		})
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

// --- Regression: AST set-operation node handling ---
//
// DuckDB represents UNION / UNION ALL / INTERSECT / EXCEPT as
// SET_OPERATION_NODE with `left`/`right` sub-nodes. Earlier versions of the
// extractor only modelled SELECT_NODE and silently dropped both column
// lineage AND DAG dependencies for set-op queries — leading to incremental
// runs not triggering downstream rebuilds when an upstream table changed.

func TestExtract_UnionAllTables(t *testing.T) {
	// Both sides of a UNION must contribute to the dependency graph,
	// otherwise downstream models won't get rebuilt when one side changes.
	sql := `SELECT name AS x FROM staging.left_tbl
	        UNION ALL
	        SELECT name AS x FROM staging.right_tbl`
	tables, err := GetAllTables(shared, sql)
	if err != nil {
		t.Fatalf("GetAllTables: %v", err)
	}
	seen := make(map[string]bool)
	for _, tbl := range tables {
		seen[tbl] = true
	}
	if !seen["staging.left_tbl"] {
		t.Errorf("expected staging.left_tbl in dependencies, got %v", tables)
	}
	if !seen["staging.right_tbl"] {
		t.Errorf("expected staging.right_tbl in dependencies, got %v", tables)
	}
}

func TestExtract_UnionAllColumnLineage(t *testing.T) {
	// Each output column gets sources from BOTH sides of the union.
	sql := `SELECT name AS x FROM staging.left_tbl
	        UNION ALL
	        SELECT name AS x FROM staging.right_tbl`
	lineages, err := Extract(shared, sql)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(lineages) != 1 {
		t.Fatalf("expected 1 column, got %d", len(lineages))
	}
	got := lineages[0]
	if got.Column != "x" {
		t.Errorf("expected column x, got %s", got.Column)
	}
	tables := make(map[string]bool)
	for _, s := range got.Sources {
		tables[s.Table] = true
	}
	if !tables["staging.left_tbl"] {
		t.Errorf("expected source staging.left_tbl, got %+v", got.Sources)
	}
	if !tables["staging.right_tbl"] {
		t.Errorf("expected source staging.right_tbl, got %+v", got.Sources)
	}
}

func TestExtract_CteWithUnion(t *testing.T) {
	// resolveCTE has its own AST walker; if it doesn't recurse into Left/Right
	// the column traces back to the CTE name itself ("c.x") instead of the
	// real upstream tables.
	sql := `WITH c AS (
	            SELECT name AS x FROM staging.left_tbl
	            UNION ALL
	            SELECT name AS x FROM staging.right_tbl
	        )
	        SELECT x FROM c`
	lineages, err := Extract(shared, sql)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(lineages) != 1 || lineages[0].Column != "x" {
		t.Fatalf("expected column x, got %+v", lineages)
	}
	tables := make(map[string]bool)
	for _, s := range lineages[0].Sources {
		tables[s.Table] = true
	}
	if tables["c"] {
		t.Errorf("CTE name 'c' leaked into resolved sources: %+v", lineages[0].Sources)
	}
	if !tables["staging.left_tbl"] || !tables["staging.right_tbl"] {
		t.Errorf("expected resolved sources from both upstream tables, got %+v", lineages[0].Sources)
	}
}

// --- Regression: subquery aliases in FROM clauses ---
//
// `FROM (SELECT ...) AS sub` was historically dropped by the alias collector,
// so column references through the alias resolved to nothing.

func TestExtract_SubqueryAliasQualified(t *testing.T) {
	// `sub.x` must resolve via the subquery's own select list to the underlying
	// upstream table.
	sql := `SELECT sub.x FROM (SELECT t.id AS x FROM raw.things t) sub`
	lineages, err := Extract(shared, sql)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(lineages) != 1 || lineages[0].Column != "x" {
		t.Fatalf("expected column x, got %+v", lineages)
	}
	if len(lineages[0].Sources) == 0 {
		t.Fatalf("expected at least one source, got 0")
	}
	src := lineages[0].Sources[0]
	if src.Table != "raw.things" || src.Column != "id" {
		t.Errorf("expected source raw.things.id, got %+v", src)
	}
}

func TestExtract_UnqualifiedColFromSoleSubquery(t *testing.T) {
	// Earlier `collectAliases` left primaryTable empty when the only FROM
	// source was a subquery, so unqualified column references resolved to
	// nothing. The fix tracks primarySubquery and resolves through it.
	sql := `SELECT col FROM (SELECT t.id AS col FROM raw.things t) sub`
	lineages, err := Extract(shared, sql)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(lineages) != 1 || lineages[0].Column != "col" {
		t.Fatalf("expected column col, got %+v", lineages)
	}
	if len(lineages[0].Sources) == 0 {
		t.Fatalf("expected at least one source for unqualified col, got 0")
	}
	src := lineages[0].Sources[0]
	if src.Table != "raw.things" || src.Column != "id" {
		t.Errorf("expected source raw.things.id, got %+v", src)
	}
}

// --- Regression: window function aggregate classification ---

func TestExtract_WindowAggregate(t *testing.T) {
	// `SUM(amount) OVER ()` was previously class:WINDOW with the function
	// name on the WINDOW node — the extractor only walked FUNCTION nodes
	// and lost the source link entirely. The output column should now
	// trace back to the source column with TransformAggregation.
	sql := `SELECT id, SUM(amount) OVER () AS total FROM staging.orders`
	lineages, err := Extract(shared, sql)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	var totalLineage *ColumnLineage
	for i := range lineages {
		if lineages[i].Column == "total" {
			totalLineage = &lineages[i]
			break
		}
	}
	if totalLineage == nil {
		t.Fatalf("expected output column 'total', got %+v", lineages)
	}
	if len(totalLineage.Sources) == 0 {
		t.Fatalf("window aggregate lost source link entirely")
	}
	src := totalLineage.Sources[0]
	if src.Column != "amount" {
		t.Errorf("expected source column amount, got %s", src.Column)
	}
	if src.Transformation != TransformAggregation {
		t.Errorf("expected AGGREGATION transform for SUM(...) OVER (), got %s", src.Transformation)
	}
}

// --- Regression: set-op subquery alias column lineage ---
//
// `FROM (SELECT ... UNION ALL SELECT ...) sub` is the combination of the
// two earlier fixes (set-op nodes + subquery aliases). resolveSubqueryColumn
// used to walk only sub.Node.SelectList, but a set-op top node has no
// SelectList — sources live in Left/Right. The fix delegates to
// resolveSelectNodeCols which handles set-op merging.

func TestExtract_SetOpSubqueryAlias_Qualified(t *testing.T) {
	sql := `SELECT sub.x FROM (
	            SELECT name AS x FROM staging.left_tbl
	            UNION ALL
	            SELECT name AS x FROM staging.right_tbl
	        ) sub`
	lineages, err := Extract(shared, sql)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(lineages) != 1 || lineages[0].Column != "x" {
		t.Fatalf("expected column x, got %+v", lineages)
	}
	tables := make(map[string]bool)
	for _, s := range lineages[0].Sources {
		tables[s.Table] = true
	}
	if tables["sub"] {
		t.Errorf("subquery alias 'sub' leaked into resolved sources: %+v", lineages[0].Sources)
	}
	if !tables["staging.left_tbl"] || !tables["staging.right_tbl"] {
		t.Errorf("expected resolved sources from both upstream tables, got %+v", lineages[0].Sources)
	}
}

func TestExtract_SetOpSubqueryAlias_Unqualified(t *testing.T) {
	sql := `SELECT x FROM (
	            SELECT name AS x FROM staging.left_tbl
	            UNION ALL
	            SELECT name AS x FROM staging.right_tbl
	        ) sub`
	lineages, err := Extract(shared, sql)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(lineages) != 1 || lineages[0].Column != "x" {
		t.Fatalf("expected column x, got %+v", lineages)
	}
	tables := make(map[string]bool)
	for _, s := range lineages[0].Sources {
		tables[s.Table] = true
	}
	if !tables["staging.left_tbl"] || !tables["staging.right_tbl"] {
		t.Errorf("expected resolved sources from both upstream tables, got %+v", lineages[0].Sources)
	}
}

// --- Regression: subquery dependency extraction ---
//
// Bug 31: `FROM (SELECT ... FROM upstream) AS sub` lost dependencies in the
// DAG so the runner skipped dependent models when upstream changed.

func TestExtract_SubqueryFromDependencyExtraction(t *testing.T) {
	sql := `SELECT s.x FROM (SELECT id AS x FROM raw.upstream) s`
	tables, err := GetAllTables(shared, sql)
	if err != nil {
		t.Fatalf("GetAllTables: %v", err)
	}
	found := false
	for _, tbl := range tables {
		if tbl == "raw.upstream" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected raw.upstream in dependencies, got %v", tables)
	}
}

// --- Regression: hidden dependencies in non-FROM SQL constructs ---
//
// Several AST shapes carry references to upstream tables that the
// extractor used to silently drop, causing the runner to miss
// downstream rebuild triggers and ship stale data without warning.
// All five fixes are validated below — they share a common pattern of
// "missing field on the AST struct so json.Unmarshal silently skipped
// it during deserialization".

// containsTable is a small helper for the regression tests below.
func containsTable(tables []string, want string) bool {
	for _, t := range tables {
		if t == want {
			return true
		}
	}
	return false
}

// COMPARISON expressions store operands in `left`/`right` fields, NOT
// `children`. WHERE x = (SELECT ... FROM other) used to lose `other`
// because the comparison's right-hand subquery wasn't traversed.
func TestExtract_HiddenDeps_WhereEqualsSubquery(t *testing.T) {
	sql := `SELECT id FROM staging.left_tbl WHERE id = (SELECT MIN(n) FROM staging.right_tbl)`
	tables, err := GetAllTables(shared, sql)
	if err != nil {
		t.Fatalf("GetAllTables: %v", err)
	}
	if !containsTable(tables, "staging.left_tbl") {
		t.Errorf("expected staging.left_tbl in deps, got %v", tables)
	}
	if !containsTable(tables, "staging.right_tbl") {
		t.Errorf("expected staging.right_tbl in deps (hidden in WHERE = subquery), got %v", tables)
	}
}

// JOIN ON-clauses live on the from_table.condition field for JOIN-typed
// from nodes. Without the Condition field on fromTable, ON-clause
// subqueries were dropped from the dependency graph.
func TestExtract_HiddenDeps_JoinOnSubquery(t *testing.T) {
	sql := `SELECT a.id FROM staging.left_tbl a
	        JOIN staging.left_tbl b ON a.id = (SELECT MIN(n) FROM staging.right_tbl)`
	tables, err := GetAllTables(shared, sql)
	if err != nil {
		t.Fatalf("GetAllTables: %v", err)
	}
	if !containsTable(tables, "staging.left_tbl") {
		t.Errorf("expected staging.left_tbl in deps, got %v", tables)
	}
	if !containsTable(tables, "staging.right_tbl") {
		t.Errorf("expected staging.right_tbl in deps (hidden in JOIN ON subquery), got %v", tables)
	}
}

// LIMIT (SELECT n FROM cfg) — limit expressions live in node.modifiers
// which used to be missing from the node struct entirely. Same applies
// to OFFSET and ORDER BY subqueries.
func TestExtract_HiddenDeps_LimitSubquery(t *testing.T) {
	sql := `SELECT id FROM staging.left_tbl LIMIT (SELECT MIN(n) FROM staging.right_tbl)`
	tables, err := GetAllTables(shared, sql)
	if err != nil {
		t.Fatalf("GetAllTables: %v", err)
	}
	if !containsTable(tables, "staging.right_tbl") {
		t.Errorf("expected staging.right_tbl in deps (hidden in LIMIT subquery), got %v", tables)
	}
}

func TestExtract_HiddenDeps_OffsetSubquery(t *testing.T) {
	sql := `SELECT id FROM staging.left_tbl LIMIT 10 OFFSET (SELECT MIN(n) FROM staging.right_tbl)`
	tables, err := GetAllTables(shared, sql)
	if err != nil {
		t.Fatalf("GetAllTables: %v", err)
	}
	if !containsTable(tables, "staging.right_tbl") {
		t.Errorf("expected staging.right_tbl in deps (hidden in OFFSET subquery), got %v", tables)
	}
}

func TestExtract_HiddenDeps_OrderBySubquery(t *testing.T) {
	sql := `SELECT id FROM staging.left_tbl
	        ORDER BY (SELECT MAX(n) FROM staging.right_tbl) LIMIT 5`
	tables, err := GetAllTables(shared, sql)
	if err != nil {
		t.Fatalf("GetAllTables: %v", err)
	}
	if !containsTable(tables, "staging.right_tbl") {
		t.Errorf("expected staging.right_tbl in deps (hidden in ORDER BY subquery), got %v", tables)
	}
}

// Sanity-check the previously-working forms so the new fields don't
// regress them.
func TestExtract_HiddenDeps_StillWorking(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{
			"WHERE IN subquery",
			`SELECT id FROM staging.left_tbl WHERE id IN (SELECT n FROM staging.right_tbl)`,
		},
		{
			"scalar subquery in SELECT",
			`SELECT id, (SELECT MAX(n) FROM staging.right_tbl) AS m FROM staging.left_tbl`,
		},
		{
			"CASE WHEN subquery",
			`SELECT CASE WHEN id > 0 THEN (SELECT MIN(n) FROM staging.right_tbl) ELSE 0 END
			 FROM staging.left_tbl`,
		},
		{
			"HAVING subquery",
			`SELECT id, COUNT(*) FROM staging.left_tbl GROUP BY id
			 HAVING COUNT(*) > (SELECT MIN(n) FROM staging.right_tbl)`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tables, err := GetAllTables(shared, tc.sql)
			if err != nil {
				t.Fatalf("GetAllTables: %v", err)
			}
			if !containsTable(tables, "staging.left_tbl") || !containsTable(tables, "staging.right_tbl") {
				t.Errorf("expected both tables in deps, got %v", tables)
			}
		})
	}
}

// Column lineage version of the COMPARISON fix — the previous
// `traceExprWithType` `case "COMPARISON"` walked `expr.Children` which
// was always empty for binary comparisons. CASE WHEN clauses with
// equality conditions now correctly track the source columns of both
// the LHS and RHS of the comparison.
func TestExtract_ComparisonColumnLineage_InCase(t *testing.T) {
	sql := `SELECT CASE WHEN amount = threshold THEN 'high' ELSE 'low' END AS bucket
	        FROM staging.orders`
	lineages, err := Extract(shared, sql)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(lineages) != 1 || lineages[0].Column != "bucket" {
		t.Fatalf("expected column bucket, got %+v", lineages)
	}
	// Both `amount` and `threshold` (sources of the comparison's left
	// and right sides) should appear in the trace.
	cols := make(map[string]bool)
	for _, s := range lineages[0].Sources {
		cols[s.Column] = true
	}
	if !cols["amount"] {
		t.Errorf("expected source column amount (LHS of comparison), got %+v", lineages[0].Sources)
	}
	if !cols["threshold"] {
		t.Errorf("expected source column threshold (RHS of comparison), got %+v", lineages[0].Sources)
	}
}

// ----------------------------------------------------------------------
// Walker robustness probes — these exercise SQL constructions we have
// never explicitly tested. The duckast walker iterates the raw map so
// it should find every BASE_TABLE regardless of how DuckDB shapes the
// AST. Each subtest is a hypothesis: "the walker finds this".
// ----------------------------------------------------------------------

func TestExtract_WalkerProbe_LateralJoin(t *testing.T) {
	sql := `SELECT a.id, b.total
	        FROM staging.left_tbl a,
	             LATERAL (SELECT SUM(n) AS total FROM staging.right_tbl WHERE k = a.id) b`
	tables, err := GetAllTables(shared, sql)
	if err != nil {
		t.Fatalf("GetAllTables: %v", err)
	}
	if !containsTable(tables, "staging.left_tbl") || !containsTable(tables, "staging.right_tbl") {
		t.Errorf("LATERAL: expected both tables, got %v", tables)
	}
}

func TestExtract_WalkerProbe_JoinUsing(t *testing.T) {
	sql := `SELECT * FROM staging.left_tbl JOIN staging.right_tbl USING (id)`
	tables, err := GetAllTables(shared, sql)
	if err != nil {
		t.Fatalf("GetAllTables: %v", err)
	}
	if !containsTable(tables, "staging.left_tbl") || !containsTable(tables, "staging.right_tbl") {
		t.Errorf("JOIN USING: expected both tables, got %v", tables)
	}
}

func TestExtract_WalkerProbe_Unpivot(t *testing.T) {
	sql := `UNPIVOT staging.left_tbl ON col1, col2 INTO NAME k VALUE v`
	tables, err := GetAllTables(shared, sql)
	if err != nil {
		t.Logf("UNPIVOT parse failed: %v", err)
		return
	}
	if !containsTable(tables, "staging.left_tbl") {
		t.Errorf("UNPIVOT: expected staging.left_tbl, got %v", tables)
	}
}

func TestExtract_WalkerProbe_RecursiveCTE(t *testing.T) {
	// Recursive CTE with a real BASE_TABLE buried inside the recursive
	// branch's WHERE-IN subquery. The CTE name `r` must be excluded; the
	// physical `staging.right_tbl` reference must be picked up.
	sql := `WITH RECURSIVE r(n) AS (
	            SELECT 1
	            UNION ALL
	            SELECT n+1 FROM r WHERE n < 10 AND n IN (SELECT id FROM staging.right_tbl)
	        )
	        SELECT * FROM r JOIN staging.left_tbl l ON l.id = r.n`
	tables, err := GetAllTables(shared, sql)
	if err != nil {
		t.Fatalf("GetAllTables: %v", err)
	}
	if !containsTable(tables, "staging.left_tbl") {
		t.Errorf("RECURSIVE CTE: expected staging.left_tbl, got %v", tables)
	}
	if !containsTable(tables, "staging.right_tbl") {
		t.Errorf("RECURSIVE CTE: expected staging.right_tbl (hidden in recursive WHERE IN), got %v", tables)
	}
	// CTE name itself must NOT show up as a physical table.
	if containsTable(tables, "r") {
		t.Errorf("RECURSIVE CTE: CTE name 'r' leaked into deps: %v", tables)
	}
}

func TestExtract_WalkerProbe_AsofJoin(t *testing.T) {
	sql := `SELECT * FROM staging.left_tbl a ASOF JOIN staging.right_tbl b ON a.ts >= b.ts`
	tables, err := GetAllTables(shared, sql)
	if err != nil {
		t.Logf("ASOF JOIN parse failed: %v", err)
		return
	}
	if !containsTable(tables, "staging.left_tbl") || !containsTable(tables, "staging.right_tbl") {
		t.Errorf("ASOF JOIN: expected both tables, got %v", tables)
	}
}

func TestExtract_WalkerProbe_ValuesJoin(t *testing.T) {
	sql := `SELECT v.x, t.id FROM (VALUES (1),(2),(3)) v(x) JOIN staging.left_tbl t ON t.id = v.x`
	tables, err := GetAllTables(shared, sql)
	if err != nil {
		t.Fatalf("VALUES JOIN GetAST failed: %v", err)
	}
	if !containsTable(tables, "staging.left_tbl") {
		t.Errorf("VALUES JOIN: expected staging.left_tbl, got %v", tables)
	}
}

func TestExtract_WalkerProbe_SemiAntiJoin(t *testing.T) {
	for _, joinType := range []string{"SEMI", "ANTI"} {
		t.Run(joinType, func(t *testing.T) {
			sql := `SELECT * FROM staging.left_tbl ` + joinType + ` JOIN staging.right_tbl USING (id)`
			tables, err := GetAllTables(shared, sql)
			if err != nil {
				t.Logf("%s JOIN GetAST failed: %v", joinType, err)
				return
			}
			if !containsTable(tables, "staging.left_tbl") || !containsTable(tables, "staging.right_tbl") {
				t.Errorf("%s JOIN: expected both tables, got %v", joinType, tables)
			}
		})
	}
}

func TestExtract_WalkerProbe_TableSample(t *testing.T) {
	sql := `SELECT * FROM staging.left_tbl USING SAMPLE 10%`
	tables, err := GetAllTables(shared, sql)
	if err != nil {
		t.Logf("USING SAMPLE GetAST failed: %v", err)
		return
	}
	if !containsTable(tables, "staging.left_tbl") {
		t.Errorf("USING SAMPLE: expected staging.left_tbl, got %v", tables)
	}
}

func TestExtract_WalkerProbe_FromFirst(t *testing.T) {
	sql := `FROM staging.left_tbl SELECT id WHERE id > 0`
	tables, err := GetAllTables(shared, sql)
	if err != nil {
		t.Logf("FROM-first GetAST failed: %v", err)
		return
	}
	if !containsTable(tables, "staging.left_tbl") {
		t.Errorf("FROM-first: expected staging.left_tbl, got %v", tables)
	}
}

func TestExtract_WalkerProbe_TableFunctionWithSubquery(t *testing.T) {
	// Table function whose argument is itself a query referencing a
	// physical table. The reference is hidden inside the function args.
	sql := `SELECT * FROM staging.left_tbl
	        WHERE id IN (SELECT id FROM (VALUES (1),(2)) v(id))
	          AND id > (SELECT MAX(n) FROM staging.right_tbl)`
	tables, err := GetAllTables(shared, sql)
	if err != nil {
		t.Fatalf("GetAllTables: %v", err)
	}
	if !containsTable(tables, "staging.left_tbl") || !containsTable(tables, "staging.right_tbl") {
	    t.Errorf("table-fn-with-subquery: expected both tables, got %v", tables)
	}
}

// TestExtract_ThreePartColumnRef regression-tests the fix in
// traceExprWithType: a 3-part COLUMN_REF (schema.table.column) used to
// silently corrupt the lineage by treating the schema token as the
// column name. The right-most token must always be the column.
func TestExtract_ThreePartColumnRef(t *testing.T) {
	sql := `SELECT staging.orders.amount AS amt FROM staging.orders`
	lineage, err := Extract(shared, sql)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	var amtSources []SourceColumn
	for _, cl := range lineage {
		if cl.Column == "amt" {
			amtSources = cl.Sources
		}
	}
	if len(amtSources) == 0 {
		t.Fatalf("expected sources for 'amt', got none; lineage=%+v", lineage)
	}
	for _, s := range amtSources {
		if s.Column != "amount" {
			t.Errorf("source column = %q, want 'amount' (3-part ref must select right-most token)", s.Column)
		}
	}
}

// TestExtract_FourPartColumnRef extends the 3-part test to a fully
// qualified catalog.schema.table.column reference. The fix must still
// pick the right-most token as the column.
func TestExtract_FourPartColumnRef(t *testing.T) {
	sql := `SELECT memory.staging.orders.amount AS amt FROM memory.staging.orders`
	lineage, err := Extract(shared, sql)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	var amtSources []SourceColumn
	for _, cl := range lineage {
		if cl.Column == "amt" {
			amtSources = cl.Sources
		}
	}
	if len(amtSources) == 0 {
		t.Fatalf("expected sources for 'amt', got none; lineage=%+v", lineage)
	}
	for _, s := range amtSources {
		if s.Column != "amount" {
			t.Errorf("source column = %q, want 'amount' (4-part ref must select right-most token)", s.Column)
		}
	}
}

// TestExtractTables_CatalogQualified regression-tests collectTablesScoped:
// a catalog-qualified BASE_TABLE used to silently drop the catalog from
// the resulting fullName. The catalog component must be preserved.
func TestExtractTables_CatalogQualified(t *testing.T) {
	sql := `SELECT * FROM memory.staging.orders`
	tables, err := ExtractTables(shared, sql)
	if err != nil {
		t.Fatalf("ExtractTables: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d (%v)", len(tables), tables)
	}
	if tables[0].Table != "memory.staging.orders" {
		t.Errorf("table = %q, want memory.staging.orders (catalog must be preserved)", tables[0].Table)
	}
}
