// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package script

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestQuerySelect(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(sharedSess, nil)

	code := `
rows = query("SELECT 42 AS id, 'hello' AS msg")
if len(rows) != 1:
    fail("expected 1 row, got %d" % len(rows))
if rows[0]["id"] != "42":
    fail("expected id=42, got %s" % rows[0]["id"])
if rows[0]["msg"] != "hello":
    fail("expected msg=hello, got %s" % rows[0]["msg"])
`
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := rt.Run(ctx, "test.query_select", code)
	if err != nil {
		t.Fatalf("query select: %v", err)
	}
}

func TestQueryMultiRow(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(sharedSess, nil)

	code := `
rows = query("SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)")
if len(rows) != 3:
    fail("expected 3 rows, got %d" % len(rows))
if rows[0]["id"] != "1":
    fail("expected first id=1, got %s" % rows[0]["id"])
if rows[2]["name"] != "c":
    fail("expected third name=c, got %s" % rows[2]["name"])
`
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := rt.Run(ctx, "test.query_multi", code)
	if err != nil {
		t.Fatalf("query multi row: %v", err)
	}
}

func TestQueryEmpty(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(sharedSess, nil)

	code := `
rows = query("SELECT 1 WHERE false")
if len(rows) != 0:
    fail("expected 0 rows, got %d" % len(rows))
`
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := rt.Run(ctx, "test.query_empty", code)
	if err != nil {
		t.Fatalf("query empty: %v", err)
	}
}

func TestQueryWithCTE(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(sharedSess, nil)

	code := `
rows = query("WITH cte AS (SELECT 1 AS x) SELECT x FROM cte")
if len(rows) != 1:
    fail("expected 1 row, got %d" % len(rows))
if rows[0]["x"] != "1":
    fail("expected x=1, got %s" % rows[0]["x"])
`
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := rt.Run(ctx, "test.query_cte", code)
	if err != nil {
		t.Fatalf("query with CTE: %v", err)
	}
}

func TestQueryRejectsInsert(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(sharedSess, nil)

	code := `query("INSERT INTO foo VALUES (1)")`
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := rt.Run(ctx, "test.query_insert", code)
	if err == nil {
		t.Fatal("expected error for INSERT")
	}
	if !strings.Contains(err.Error(), "INSERT") {
		t.Errorf("error should mention INSERT: %v", err)
	}
}

func TestQueryRejectsDelete(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(sharedSess, nil)

	code := `query("DELETE FROM foo WHERE id = 1")`
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := rt.Run(ctx, "test.query_delete", code)
	if err == nil {
		t.Fatal("expected error for DELETE")
	}
	if !strings.Contains(err.Error(), "DELETE") {
		t.Errorf("error should mention DELETE: %v", err)
	}
}

func TestQueryRejectsDrop(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(sharedSess, nil)

	code := `query("DROP TABLE foo")`
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := rt.Run(ctx, "test.query_drop", code)
	if err == nil {
		t.Fatal("expected error for DROP")
	}
	if !strings.Contains(err.Error(), "DROP") {
		t.Errorf("error should mention DROP: %v", err)
	}
}

func TestValidateReadOnly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sql     string
		wantErr bool
	}{
		{"SELECT 1", false},
		{"  SELECT 1", false},
		{"WITH x AS (SELECT 1) SELECT * FROM x", false},
		{"-- comment\nSELECT 1", false},
		{"/* block */ SELECT 1", false},
		{"INSERT INTO foo VALUES (1)", true},
		{"DELETE FROM foo", true},
		{"DROP TABLE foo", true},
		{"UPDATE foo SET x = 1", true},
		{"CREATE TABLE foo (id INT)", true},
		{"", true},
	}

	for _, tt := range tests {
		err := validateReadOnly(tt.sql)
		if (err != nil) != tt.wantErr {
			t.Errorf("validateReadOnly(%q) err=%v, wantErr=%v", tt.sql, err, tt.wantErr)
		}
	}
}
