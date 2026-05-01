// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute

import (
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/duckast"
	"github.com/ondatra-labs/ondatrasql/internal/lineage"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// astFromSQL is a small helper that parses SQL through DuckDB's
// json_serialize_sql + duckast.Parse, returning a typed AST.
func astFromSQL(t *testing.T, p *testutil.Project, sql string) *duckast.AST {
	t.Helper()
	astJSON, err := lineage.GetAST(p.Sess, sql)
	if err != nil {
		t.Fatalf("GetAST: %v", err)
	}
	ast, err := duckast.Parse(astJSON)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return ast
}

func TestValidateNoLimitOffset_NilAST(t *testing.T) {
	t.Parallel()
	if err := validateNoLimitOffset(nil); err != nil {
		t.Errorf("nil AST should be allowed (parse error surfaces elsewhere): %v", err)
	}
}

func TestValidateNoLimitOffset_PlainSelect_OK(t *testing.T) {
	p := testutil.NewProject(t)
	ast := astFromSQL(t, p, "SELECT 1::INTEGER AS id, 'a'::VARCHAR AS name")
	if err := validateNoLimitOffset(ast); err != nil {
		t.Errorf("plain SELECT should pass: %v", err)
	}
}

func TestValidateNoLimitOffset_TopLevelLimit_Rejected(t *testing.T) {
	p := testutil.NewProject(t)
	ast := astFromSQL(t, p, "SELECT 1 AS id LIMIT 10")
	err := validateNoLimitOffset(ast)
	if err == nil {
		t.Fatal("top-level LIMIT should be rejected")
	}
	if !strings.Contains(err.Error(), "LIMIT is not allowed") {
		t.Errorf("error should name LIMIT, got: %v", err)
	}
	if !strings.Contains(err.Error(), "ROW_NUMBER") {
		t.Errorf("error should mention ROW_NUMBER alternative, got: %v", err)
	}
}

func TestValidateNoLimitOffset_TopLevelOffset_Rejected(t *testing.T) {
	p := testutil.NewProject(t)
	ast := astFromSQL(t, p, "SELECT 1 AS id LIMIT 10 OFFSET 5")
	err := validateNoLimitOffset(ast)
	if err == nil {
		t.Fatal("LIMIT+OFFSET should be rejected")
	}
	// Either LIMIT or OFFSET surfaces first depending on walk order;
	// what matters is that one of them triggers the error.
	if !strings.Contains(err.Error(), "is not allowed") {
		t.Errorf("error should report disallowed clause, got: %v", err)
	}
}

func TestValidateNoLimitOffset_LimitInCTE_Rejected(t *testing.T) {
	p := testutil.NewProject(t)
	ast := astFromSQL(t, p, `
		WITH capped AS (SELECT id FROM t LIMIT 100)
		SELECT id FROM capped
	`)
	err := validateNoLimitOffset(ast)
	if err == nil {
		t.Fatal("LIMIT inside CTE should be rejected")
	}
	if !strings.Contains(err.Error(), "LIMIT is not allowed") {
		t.Errorf("error should name LIMIT, got: %v", err)
	}
}

func TestValidateNoLimitOffset_LimitInSubquery_Rejected(t *testing.T) {
	p := testutil.NewProject(t)
	ast := astFromSQL(t, p, `
		SELECT id FROM (SELECT id FROM t LIMIT 50) AS s
	`)
	err := validateNoLimitOffset(ast)
	if err == nil {
		t.Fatal("LIMIT inside subquery should be rejected")
	}
	if !strings.Contains(err.Error(), "LIMIT is not allowed") {
		t.Errorf("error should name LIMIT, got: %v", err)
	}
}

func TestValidateNoLimitOffset_LimitInSetOpBranch_Rejected(t *testing.T) {
	p := testutil.NewProject(t)
	// LIMIT inside a UNION branch — the validator must walk into both sides.
	ast := astFromSQL(t, p, `
		SELECT id FROM a
		UNION ALL
		SELECT id FROM b LIMIT 10
	`)
	err := validateNoLimitOffset(ast)
	if err == nil {
		t.Fatal("LIMIT inside set-op branch should be rejected")
	}
	if !strings.Contains(err.Error(), "LIMIT is not allowed") {
		t.Errorf("error should name LIMIT, got: %v", err)
	}
}

func TestValidateNoLimitOffset_RowNumberWithWhere_OK(t *testing.T) {
	p := testutil.NewProject(t)
	// Top-N-per-group via ROW_NUMBER + WHERE — the documented alternative,
	// must keep working.
	ast := astFromSQL(t, p, `
		SELECT customer_id, order_date, total
		FROM (
			SELECT customer_id, order_date, total,
				ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn
			FROM orders
		) ranked
		WHERE rn <= 10
	`)
	if err := validateNoLimitOffset(ast); err != nil {
		t.Errorf("ROW_NUMBER + WHERE should pass (it is the documented alternative to LIMIT): %v", err)
	}
}

func TestValidateNoLimitOffset_OffsetInCTE_Rejected(t *testing.T) {
	p := testutil.NewProject(t)
	ast := astFromSQL(t, p, `
		WITH skipped AS (SELECT id FROM t LIMIT 100 OFFSET 50)
		SELECT id FROM skipped
	`)
	err := validateNoLimitOffset(ast)
	if err == nil {
		t.Fatal("OFFSET inside CTE should be rejected")
	}
	if !strings.Contains(err.Error(), "is not allowed") {
		t.Errorf("error should report disallowed clause, got: %v", err)
	}
}

// OFFSET appears with no LIMIT — separate from the LIMIT+OFFSET case.
// DuckDB accepts OFFSET-without-LIMIT and emits an OFFSET_MODIFIER node.
// The validator must reject it on its own.
func TestValidateNoLimitOffset_OffsetWithoutLimit_Rejected(t *testing.T) {
	p := testutil.NewProject(t)
	ast := astFromSQL(t, p, "SELECT 1 AS id OFFSET 5")
	err := validateNoLimitOffset(ast)
	if err == nil {
		t.Fatal("OFFSET without LIMIT should be rejected")
	}
	if !strings.Contains(err.Error(), "OFFSET is not allowed") {
		t.Errorf("error should name OFFSET, got: %v", err)
	}
}
