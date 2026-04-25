// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute

import (
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/duckast"
	"github.com/ondatra-labs/ondatrasql/internal/lineage"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

func TestExtractTypedSelectColumns_CastTypes(t *testing.T) {
	t.Parallel()
	p := testutil.NewProject(t)

	astJSON, err := lineage.GetAST(p.Sess, "SELECT total::DECIMAL AS total, qty::INTEGER AS qty, name FROM t")
	if err != nil {
		t.Fatalf("get AST: %v", err)
	}
	ast, err := duckast.Parse(astJSON)
	if err != nil {
		t.Fatalf("parse AST: %v", err)
	}
	cols := extractTypedSelectColumns(ast)
	if len(cols) != 3 {
		t.Fatalf("got %d columns, want 3", len(cols))
	}

	col0 := cols[0].(map[string]any)
	if col0["name"] != "total" || col0["type"] != "decimal" {
		t.Errorf("col 0: name=%v type=%v, want total/decimal", col0["name"], col0["type"])
	}

	col1 := cols[1].(map[string]any)
	if col1["name"] != "qty" || col1["type"] != "integer" {
		t.Errorf("col 1: name=%v type=%v, want qty/integer", col1["name"], col1["type"])
	}

	col2 := cols[2].(map[string]any)
	if col2["name"] != "name" || col2["type"] != "string" {
		t.Errorf("col 2: name=%v type=%v, want name/string", col2["name"], col2["type"])
	}
}

func TestExtractTypedSelectColumns_AliasOverrides(t *testing.T) {
	t.Parallel()
	p := testutil.NewProject(t)

	astJSON, err := lineage.GetAST(p.Sess, "SELECT price::DOUBLE AS unit_price FROM t")
	if err != nil {
		t.Fatalf("get AST: %v", err)
	}
	ast, _ := duckast.Parse(astJSON)
	cols := extractTypedSelectColumns(ast)
	if len(cols) != 1 {
		t.Fatalf("got %d columns, want 1", len(cols))
	}
	col := cols[0].(map[string]any)
	if col["name"] != "unit_price" {
		t.Errorf("name = %v, want unit_price (alias should override)", col["name"])
	}
	if col["type"] != "float" {
		t.Errorf("type = %v, want float", col["type"])
	}
}

func TestExtractTypedSelectColumns_NoCast(t *testing.T) {
	t.Parallel()
	p := testutil.NewProject(t)

	astJSON, _ := lineage.GetAST(p.Sess, "SELECT name, email FROM t")
	ast, _ := duckast.Parse(astJSON)
	cols := extractTypedSelectColumns(ast)
	if len(cols) != 2 {
		t.Fatalf("got %d columns, want 2", len(cols))
	}
	for _, c := range cols {
		col := c.(map[string]any)
		if col["type"] != "string" {
			t.Errorf("col %v: type = %v, want string (no cast)", col["name"], col["type"])
		}
	}
}
