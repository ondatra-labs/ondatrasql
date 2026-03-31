// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package dag

import (
	"os"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
)

var shared *duckdb.Session

func TestMain(m *testing.M) {
	sess, err := duckdb.NewSession(":memory:?threads=4&memory_limit=2GB")
	if err != nil {
		panic("create shared session: " + err.Error())
	}
	shared = sess
	code := m.Run()
	shared.Close()
	os.Exit(code)
}

func TestGraph_Sort_Simple(t *testing.T) {
	g := NewGraph(shared)

	// staging.orders has no deps (reads from raw.orders which is not a model)
	g.Add(&parser.Model{
		Target: "staging.orders",
		SQL:    "SELECT * FROM raw.orders",
	})

	// marts.fact_orders depends on staging.orders (auto-detected from SQL)
	g.Add(&parser.Model{
		Target: "marts.fact_orders",
		SQL:    "SELECT * FROM staging.orders",
	})

	sorted, err := g.Sort()
	if err != nil {
		t.Fatalf("Sort: %v", err)
	}

	if len(sorted) != 2 {
		t.Fatalf("got %d models, want 2", len(sorted))
	}

	// staging.orders should come before marts.fact_orders
	if sorted[0].Target != "staging.orders" {
		t.Errorf("first model = %q, want %q", sorted[0].Target, "staging.orders")
	}
	if sorted[1].Target != "marts.fact_orders" {
		t.Errorf("second model = %q, want %q", sorted[1].Target, "marts.fact_orders")
	}
}

func TestGraph_Sort_MultiLevel(t *testing.T) {
	g := NewGraph(shared)

	// Dependencies auto-detected from SQL
	g.Add(&parser.Model{Target: "staging.raw", SQL: "SELECT 1"})
	g.Add(&parser.Model{Target: "staging.clean", SQL: "SELECT * FROM staging.raw"})
	g.Add(&parser.Model{Target: "marts.final", SQL: "SELECT * FROM staging.clean"})

	sorted, err := g.Sort()
	if err != nil {
		t.Fatalf("Sort: %v", err)
	}

	// Check order: raw -> clean -> final
	order := make(map[string]int)
	for i, m := range sorted {
		order[m.Target] = i
	}

	if order["staging.raw"] >= order["staging.clean"] {
		t.Error("staging.raw should come before staging.clean")
	}
	if order["staging.clean"] >= order["marts.final"] {
		t.Error("staging.clean should come before marts.final")
	}
}

func TestGraph_Sort_CircularDependency(t *testing.T) {
	g := NewGraph(shared)

	// Circular dependency auto-detected from SQL
	g.Add(&parser.Model{Target: "schema.a", SQL: "SELECT * FROM schema.b"})
	g.Add(&parser.Model{Target: "schema.b", SQL: "SELECT * FROM schema.a"})

	_, err := g.Sort()
	if err == nil {
		t.Error("expected error for circular dependency")
	}
}

func TestGraph_Sort_ImplicitDeps(t *testing.T) {
	g := NewGraph(shared)

	g.Add(&parser.Model{
		Target: "staging.orders",
		SQL:    "SELECT * FROM raw.data",
	})

	g.Add(&parser.Model{
		Target: "marts.fact",
		SQL:    "SELECT * FROM staging.orders JOIN staging.customers ON 1=1",
	})

	g.Add(&parser.Model{
		Target: "staging.customers",
		SQL:    "SELECT * FROM raw.customers",
	})

	sorted, err := g.Sort()
	if err != nil {
		t.Fatalf("Sort: %v", err)
	}

	// Check that staging models come before marts
	order := make(map[string]int)
	for i, m := range sorted {
		order[m.Target] = i
	}

	if order["staging.orders"] >= order["marts.fact"] {
		t.Error("staging.orders should come before marts.fact")
	}
	if order["staging.customers"] >= order["marts.fact"] {
		t.Error("staging.customers should come before marts.fact")
	}
}

func TestGraph_Sort_DeterministicOrder(t *testing.T) {
	// Run multiple times to verify deterministic ordering
	for i := 0; i < 10; i++ {
		g := NewGraph(shared)

		// Add models in random-ish order (map iteration is non-deterministic)
		g.Add(&parser.Model{Target: "staging.d", SQL: "SELECT 1"})
		g.Add(&parser.Model{Target: "staging.a", SQL: "SELECT 1"})
		g.Add(&parser.Model{Target: "staging.c", SQL: "SELECT 1"})
		g.Add(&parser.Model{Target: "staging.b", SQL: "SELECT 1"})

		sorted, err := g.Sort()
		if err != nil {
			t.Fatalf("Sort: %v", err)
		}

		// Since there are no dependencies, order should be alphabetical
		expected := []string{"staging.a", "staging.b", "staging.c", "staging.d"}
		for j, m := range sorted {
			if m.Target != expected[j] {
				t.Errorf("run %d: position %d = %q, want %q", i, j, m.Target, expected[j])
			}
		}
	}
}

func TestGraph_Sort_DiamondDependency(t *testing.T) {
	g := NewGraph(shared)

	// Diamond: A depends on B and C, both B and C depend on D
	//     D
	//    / \
	//   B   C
	//    \ /
	//     A
	g.Add(&parser.Model{Target: "schema.a", SQL: "SELECT * FROM schema.b JOIN schema.c ON 1=1"})
	g.Add(&parser.Model{Target: "schema.b", SQL: "SELECT * FROM schema.d"})
	g.Add(&parser.Model{Target: "schema.c", SQL: "SELECT * FROM schema.d"})
	g.Add(&parser.Model{Target: "schema.d", SQL: "SELECT 1"})

	sorted, err := g.Sort()
	if err != nil {
		t.Fatalf("Sort: %v", err)
	}

	if len(sorted) != 4 {
		t.Fatalf("got %d models, want 4", len(sorted))
	}

	// Check order constraints
	order := make(map[string]int)
	for i, m := range sorted {
		order[m.Target] = i
	}

	// D must come before B and C
	if order["schema.d"] >= order["schema.b"] {
		t.Error("schema.d should come before schema.b")
	}
	if order["schema.d"] >= order["schema.c"] {
		t.Error("schema.d should come before schema.c")
	}
	// B and C must come before A
	if order["schema.b"] >= order["schema.a"] {
		t.Error("schema.b should come before schema.a")
	}
	if order["schema.c"] >= order["schema.a"] {
		t.Error("schema.c should come before schema.a")
	}
}

func TestGraph_Sort_IndependentSubgraphs(t *testing.T) {
	g := NewGraph(shared)

	// Two independent chains:
	// Chain 1: x -> y
	// Chain 2: a -> b
	g.Add(&parser.Model{Target: "chain1.x", SQL: "SELECT 1"})
	g.Add(&parser.Model{Target: "chain1.y", SQL: "SELECT * FROM chain1.x"})
	g.Add(&parser.Model{Target: "chain2.a", SQL: "SELECT 1"})
	g.Add(&parser.Model{Target: "chain2.b", SQL: "SELECT * FROM chain2.a"})

	sorted, err := g.Sort()
	if err != nil {
		t.Fatalf("Sort: %v", err)
	}

	if len(sorted) != 4 {
		t.Fatalf("got %d models, want 4", len(sorted))
	}

	order := make(map[string]int)
	for i, m := range sorted {
		order[m.Target] = i
	}

	// Within each chain, parent must come before child
	if order["chain1.x"] >= order["chain1.y"] {
		t.Error("chain1.x should come before chain1.y")
	}
	if order["chain2.a"] >= order["chain2.b"] {
		t.Error("chain2.a should come before chain2.b")
	}

	// Verify deterministic ordering of independent nodes
	// chain1.x and chain2.a have no dependencies - should be sorted alphabetically
	if order["chain1.x"] >= order["chain2.a"] {
		t.Error("chain1.x should come before chain2.a (alphabetical)")
	}
}

func TestExtractDeps(t *testing.T) {
	g := NewGraph(shared)

	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "simple FROM",
			sql:  "SELECT * FROM staging.orders",
			want: []string{"staging.orders"},
		},
		{
			name: "JOIN",
			sql:  "SELECT * FROM staging.orders JOIN staging.customers ON 1=1",
			want: []string{"staging.orders", "staging.customers"},
		},
		{
			name: "multiple FROM (subquery)",
			sql:  "SELECT * FROM staging.orders WHERE id IN (SELECT order_id FROM staging.items)",
			want: []string{"staging.orders", "staging.items"},
		},
		{
			name: "skip temp tables",
			sql:  "SELECT * FROM tmp_model",
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := g.extractDeps(tt.sql)
			if len(got) != len(tt.want) {
				t.Errorf("extractDeps() = %v, want %v", got, tt.want)
				return
			}
			// Check that all expected deps are present (order may vary)
			gotMap := make(map[string]bool)
			for _, dep := range got {
				gotMap[dep] = true
			}
			for _, dep := range tt.want {
				if !gotMap[dep] {
					t.Errorf("missing dep %q in %v", dep, got)
				}
			}
		})
	}
}

func TestExtractDeps_NilSession(t *testing.T) {
	g := NewGraph(nil) // nil session
	deps := g.extractDeps("SELECT * FROM staging.orders")
	if deps != nil {
		t.Errorf("expected nil deps with nil session, got %v", deps)
	}
}

func TestExtractDeps_InvalidSQL(t *testing.T) {
	g := NewGraph(shared)
	// Invalid SQL that can't be parsed → returns nil deps
	deps := g.extractDeps("THIS IS NOT VALID SQL AT ALL !!!")
	if deps != nil {
		t.Errorf("expected nil deps for invalid SQL, got %v", deps)
	}
}

func TestExtractDeps_SystemTables(t *testing.T) {
	g := NewGraph(shared)
	// System tables should be filtered out
	deps := g.extractDeps("SELECT * FROM information_schema.columns")
	if len(deps) != 0 {
		t.Errorf("system tables should be filtered, got %v", deps)
	}
}

func TestExtractDeps_CTE(t *testing.T) {
	g := NewGraph(shared)
	deps := g.extractDeps("WITH cte AS (SELECT * FROM staging.orders) SELECT * FROM cte")
	found := false
	for _, d := range deps {
		if d == "staging.orders" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected staging.orders in deps from CTE, got %v", deps)
	}
}

func TestGraph_Dependents(t *testing.T) {
	g := NewGraph(shared)

	// A has no deps, B depends on A, C depends on A and B
	g.Add(&parser.Model{Target: "staging.a", SQL: "SELECT 1"})
	g.Add(&parser.Model{Target: "staging.b", SQL: "SELECT * FROM staging.a"})
	g.Add(&parser.Model{Target: "staging.c", SQL: "SELECT * FROM staging.a JOIN staging.b ON 1=1"})

	// Sort filters deps to known models
	_, err := g.Sort()
	if err != nil {
		t.Fatalf("Sort: %v", err)
	}

	deps := g.Dependents()

	// staging.a should have downstream: staging.b, staging.c
	aDownstream := deps["staging.a"]
	if len(aDownstream) != 2 {
		t.Errorf("staging.a dependents = %v, want 2 entries", aDownstream)
	}

	// staging.b should have downstream: staging.c
	bDownstream := deps["staging.b"]
	if len(bDownstream) != 1 || bDownstream[0] != "staging.c" {
		t.Errorf("staging.b dependents = %v, want [staging.c]", bDownstream)
	}

	// staging.c has no downstream
	cDownstream := deps["staging.c"]
	if len(cDownstream) != 0 {
		t.Errorf("staging.c dependents = %v, want empty", cDownstream)
	}
}

func TestGraph_Add_UnqualifiedDepsFiltered(t *testing.T) {
	g := NewGraph(shared)

	// Model referencing unqualified table (no dot) → should be filtered
	g.Add(&parser.Model{
		Target: "staging.test",
		SQL:    "SELECT * FROM unqualified_table",
	})

	node := g.nodes["staging.test"]
	if len(node.Deps) != 0 {
		t.Errorf("unqualified deps should be filtered, got %v", node.Deps)
	}
}

func TestIsSystemTable(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{"tmp_model", true},
		{"temp_data", true},
		{"information_schema.columns", true},
		{"pg_catalog.tables", true},
		{"dual", true},
		{"staging.orders", false},
		{"raw.data", false},
	}
	for _, tt := range tests {
		got := isSystemTable(tt.name)
		if got != tt.want {
			t.Errorf("isSystemTable(%q) = %v, want %v", tt.name, got, tt.want)
		}
	}
}
