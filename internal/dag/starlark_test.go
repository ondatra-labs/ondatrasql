// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package dag

import (
	"os"
	"path/filepath"
	"testing"
)

// Unit tests — no DuckDB session required.

func TestExtractQueryCalls_Simple(t *testing.T) {
	t.Parallel()
	code := `
rows = query("SELECT * FROM staging.orders")
for r in rows:
    save.row(r)
`
	sqls := extractQueryCalls(code)
	if len(sqls) != 1 {
		t.Fatalf("got %d calls, want 1", len(sqls))
	}
	if sqls[0] != "SELECT * FROM staging.orders" {
		t.Errorf("got %q", sqls[0])
	}
}

func TestExtractQueryCalls_Multiple(t *testing.T) {
	t.Parallel()
	code := `
a = query("SELECT * FROM raw.users")
b = query("SELECT * FROM staging.orders WHERE id > 0")
`
	sqls := extractQueryCalls(code)
	if len(sqls) != 2 {
		t.Fatalf("got %d calls, want 2", len(sqls))
	}
}

func TestExtractQueryCalls_NoQuery(t *testing.T) {
	t.Parallel()
	code := `
x = http.get("https://api.example.com/data")
save.row({"id": 1})
`
	sqls := extractQueryCalls(code)
	if len(sqls) != 0 {
		t.Errorf("got %d calls, want 0", len(sqls))
	}
}

func TestExtractQueryCalls_DynamicSQL(t *testing.T) {
	t.Parallel()
	code := `
table = "staging.orders"
rows = query("SELECT * FROM " + table)
`
	// Dynamic SQL (string concatenation) should be silently ignored
	sqls := extractQueryCalls(code)
	if len(sqls) != 0 {
		t.Errorf("dynamic SQL should be ignored, got %d calls", len(sqls))
	}
}

func TestExtractQueryCalls_InvalidSyntax(t *testing.T) {
	t.Parallel()
	code := `this is not valid starlark {{{`
	sqls := extractQueryCalls(code)
	if len(sqls) != 0 {
		t.Errorf("invalid syntax should return 0 calls, got %d", len(sqls))
	}
}

func TestExtractQueryCalls_VariableArg(t *testing.T) {
	t.Parallel()
	code := `
sql = "SELECT 1"
rows = query(sql)
`
	sqls := extractQueryCalls(code)
	if len(sqls) != 0 {
		t.Errorf("variable arg should be ignored, got %d calls", len(sqls))
	}
}

func TestExtractQueryCalls_NestedInFunction(t *testing.T) {
	t.Parallel()
	code := `
def fetch_data():
    return query("SELECT id, name FROM staging.customers")

data = fetch_data()
`
	sqls := extractQueryCalls(code)
	if len(sqls) != 1 {
		t.Fatalf("got %d calls, want 1", len(sqls))
	}
	if sqls[0] != "SELECT id, name FROM staging.customers" {
		t.Errorf("got %q", sqls[0])
	}
}

func TestExtractStarlarkDeps_NilSession(t *testing.T) {
	t.Parallel()
	code := `rows = query("SELECT * FROM staging.orders")`
	deps := ExtractStarlarkDeps(nil, code)
	if len(deps) != 0 {
		t.Errorf("nil session should return 0 deps, got %d", len(deps))
	}
}

func TestExtractCalls_LoadStatements(t *testing.T) {
	t.Parallel()
	code := `
load("lib/helpers.star", "fetch")
rows = query("SELECT * FROM staging.orders")
`
	calls := extractCalls(code)
	if len(calls.querySQLs) != 1 {
		t.Errorf("querySQLs = %d, want 1", len(calls.querySQLs))
	}
	if len(calls.loads) != 1 {
		t.Fatalf("loads = %d, want 1", len(calls.loads))
	}
	if calls.loads[0] != "lib/helpers.star" {
		t.Errorf("load path = %q, want lib/helpers.star", calls.loads[0])
	}
}

func TestExtractAllQueryCalls_RecursiveLoad(t *testing.T) {
	t.Parallel()

	// Create a temp project dir with lib/ modules
	dir := t.TempDir()
	libDir := filepath.Join(dir, "lib")
	os.MkdirAll(libDir, 0o755)

	// lib/helpers.star contains a query() call
	os.WriteFile(filepath.Join(libDir, "helpers.star"), []byte(`
def fetch():
    return query("SELECT * FROM staging.customers")
`), 0o644)

	// Root code loads helpers and has its own query
	code := `
load("lib/helpers.star", "fetch")
rows = query("SELECT * FROM raw.orders")
data = fetch()
`
	visited := make(map[string]bool)
	sqls := extractAllQueryCalls(dir, code, visited)

	if len(sqls) != 2 {
		t.Fatalf("got %d SQLs, want 2: %v", len(sqls), sqls)
	}

	// Should contain both the root query and the helper's query
	found := map[string]bool{}
	for _, s := range sqls {
		found[s] = true
	}
	if !found["SELECT * FROM raw.orders"] {
		t.Error("missing root query")
	}
	if !found["SELECT * FROM staging.customers"] {
		t.Error("missing helper query")
	}
}

func TestExtractAllQueryCalls_CycleDetection(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	libDir := filepath.Join(dir, "lib")
	os.MkdirAll(libDir, 0o755)

	// a.star loads b.star, b.star loads a.star (cycle)
	os.WriteFile(filepath.Join(libDir, "a.star"), []byte(`
load("lib/b.star", "b_func")
rows = query("SELECT 1 AS x")
`), 0o644)
	os.WriteFile(filepath.Join(libDir, "b.star"), []byte(`
load("lib/a.star", "a_func")
def b_func():
    return query("SELECT 2 AS y")
`), 0o644)

	code := `load("lib/a.star", "stuff")`
	visited := make(map[string]bool)
	sqls := extractAllQueryCalls(dir, code, visited)

	// Should not hang or panic, and should extract what it can
	if len(sqls) < 1 {
		t.Errorf("expected at least 1 SQL from cyclic modules, got %d", len(sqls))
	}
}

func TestExtractAllQueryCalls_PathTraversal(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	code := `load("../../etc/passwd", "x")`
	visited := make(map[string]bool)
	sqls := extractAllQueryCalls(dir, code, visited)

	if len(sqls) != 0 {
		t.Errorf("path traversal should return 0 SQLs, got %d", len(sqls))
	}
}

func TestExtractAllQueryCalls_DeepNesting(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	libDir := filepath.Join(dir, "lib")
	os.MkdirAll(libDir, 0o755)

	// Chain: root -> a.star -> b.star (query lives in b.star)
	os.WriteFile(filepath.Join(libDir, "a.star"), []byte(`
load("lib/b.star", "deep_query")
def wrapper():
    return deep_query()
`), 0o644)
	os.WriteFile(filepath.Join(libDir, "b.star"), []byte(`
def deep_query():
    return query("SELECT * FROM mart.revenue")
`), 0o644)

	code := `
load("lib/a.star", "wrapper")
result = wrapper()
`
	visited := make(map[string]bool)
	sqls := extractAllQueryCalls(dir, code, visited)

	found := false
	for _, s := range sqls {
		if s == "SELECT * FROM mart.revenue" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected deep nested query, got %v", sqls)
	}
}
