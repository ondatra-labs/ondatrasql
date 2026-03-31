// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package dag

import (
	"sort"
	"testing"
)

func TestExtractStarlarkDeps_SingleTable(t *testing.T) {
	code := `
rows = query("SELECT * FROM staging.orders")
for r in rows:
    save.row(r)
`
	deps := ExtractStarlarkDeps(shared, code)
	if len(deps) != 1 {
		t.Fatalf("got %d deps, want 1: %v", len(deps), deps)
	}
	if deps[0] != "staging.orders" {
		t.Errorf("got %q, want staging.orders", deps[0])
	}
}

func TestExtractStarlarkDeps_JoinTables(t *testing.T) {
	code := `
rows = query("SELECT o.id, c.name FROM staging.orders o JOIN staging.customers c ON o.customer_id = c.id")
`
	deps := ExtractStarlarkDeps(shared, code)
	sort.Strings(deps)
	if len(deps) != 2 {
		t.Fatalf("got %d deps, want 2: %v", len(deps), deps)
	}
	if deps[0] != "staging.customers" || deps[1] != "staging.orders" {
		t.Errorf("got %v", deps)
	}
}
