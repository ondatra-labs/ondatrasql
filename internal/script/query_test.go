// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"testing"

	"go.starlark.net/starlark"
)

func TestQueryBuiltin_NilSession_ReturnsEmptyList(t *testing.T) {
	t.Parallel()
	b := queryBuiltin(nil)
	thread := &starlark.Thread{Name: "test"}
	result, err := starlark.Call(thread, b, starlark.Tuple{starlark.String("SELECT 1")}, nil)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	list, ok := result.(*starlark.List)
	if !ok {
		t.Fatalf("expected list, got %T", result)
	}
	if list.Len() != 0 {
		t.Errorf("expected empty list for nil session, got %d items", list.Len())
	}
}

func TestQueryBuiltin_RejectsWriteStatements(t *testing.T) {
	t.Parallel()
	b := queryBuiltin(nil)
	thread := &starlark.Thread{Name: "test"}

	writes := []string{
		"DELETE FROM t",
		"INSERT INTO t VALUES (1)",
		"UPDATE t SET x = 1",
		"DROP TABLE t",
		"CREATE TABLE t (x INT)",
		"TRUNCATE t",
	}
	for _, sql := range writes {
		_, err := starlark.Call(thread, b, starlark.Tuple{starlark.String(sql)}, nil)
		if err == nil {
			t.Errorf("expected error for %q, got nil", sql)
		}
	}
}

func TestQueryBuiltin_AllowsSelect(t *testing.T) {
	t.Parallel()
	b := queryBuiltin(nil)
	thread := &starlark.Thread{Name: "test"}
	// nil session returns empty list, but no error
	_, err := starlark.Call(thread, b, starlark.Tuple{starlark.String("SELECT 1 AS x")}, nil)
	if err != nil {
		t.Errorf("SELECT should be allowed: %v", err)
	}
}

func TestQueryBuiltin_AllowsWithCTE(t *testing.T) {
	t.Parallel()
	b := queryBuiltin(nil)
	thread := &starlark.Thread{Name: "test"}
	_, err := starlark.Call(thread, b, starlark.Tuple{starlark.String("WITH t AS (SELECT 1) SELECT * FROM t")}, nil)
	if err != nil {
		t.Errorf("WITH CTE should be allowed: %v", err)
	}
}

func TestQueryBuiltin_RejectsWritableCTE(t *testing.T) {
	t.Parallel()
	b := queryBuiltin(nil)
	thread := &starlark.Thread{Name: "test"}
	_, err := starlark.Call(thread, b, starlark.Tuple{starlark.String("WITH t AS (DELETE FROM x RETURNING *) SELECT * FROM t")}, nil)
	if err == nil {
		t.Error("expected error for writable CTE, got nil")
	}
}

func TestQueryBuiltin_RejectsEmptySQL(t *testing.T) {
	t.Parallel()
	b := queryBuiltin(nil)
	thread := &starlark.Thread{Name: "test"}
	_, err := starlark.Call(thread, b, starlark.Tuple{starlark.String("")}, nil)
	if err == nil {
		t.Error("expected error for empty SQL, got nil")
	}
}
