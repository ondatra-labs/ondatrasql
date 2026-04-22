// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package dag

import "testing"

// --- extractCalls: guarded type assertion for LoadStmt.Module.Value ---

func TestExtractCalls_LoadStatement(t *testing.T) {
	t.Parallel()
	// Before fix: bare type assertion loadStmt.Module.Value.(string) could panic.
	// After fix: guarded with ok check.
	code := `
load("lib/helpers.star", "make_rows")

def fetch(save):
    for row in make_rows(3):
        save.row(row)
`
	calls := extractCalls(code)

	if len(calls.loads) != 1 {
		t.Fatalf("expected 1 load, got %d: %v", len(calls.loads), calls.loads)
	}
	if calls.loads[0] != "lib/helpers.star" {
		t.Errorf("load[0] = %q, want %q", calls.loads[0], "lib/helpers.star")
	}
}

func TestExtractCalls_MultipleLoads(t *testing.T) {
	t.Parallel()
	code := `
load("lib/a.star", "func_a")
load("lib/b.star", "func_b")

def fetch(save):
    pass
`
	calls := extractCalls(code)

	if len(calls.loads) != 2 {
		t.Fatalf("expected 2 loads, got %d: %v", len(calls.loads), calls.loads)
	}
}

func TestExtractCalls_EmptyCode(t *testing.T) {
	t.Parallel()
	calls := extractCalls("")
	if len(calls.loads) != 0 {
		t.Errorf("expected 0 loads for empty code, got %d", len(calls.loads))
	}
	if len(calls.querySQLs) != 0 {
		t.Errorf("expected 0 queries for empty code, got %d", len(calls.querySQLs))
	}
}

func TestExtractCalls_InvalidSyntax(t *testing.T) {
	t.Parallel()
	// Should not panic on invalid Starlark code
	calls := extractCalls("def broken(")
	if len(calls.loads) != 0 {
		t.Errorf("expected 0 loads for invalid code, got %d", len(calls.loads))
	}
}
