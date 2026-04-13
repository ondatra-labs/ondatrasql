// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package dag

import (
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"pgregory.net/rapid"
)

// genModelTarget generates a valid schema.table target name.
func genModelTarget() *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		schema := rapid.SampledFrom([]string{"raw", "staging", "mart"}).Draw(t, "schema")
		table := rapid.StringMatching(`^[a-z][a-z0-9_]{1,10}$`).Draw(t, "table")
		return schema + "." + table
	})
}

// Property: Sort output contains all input nodes (completeness).
func TestRapid_Sort_Completeness(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(1, 15).Draw(t, "n")

		// Generate unique targets
		targets := make([]string, 0, n)
		seen := make(map[string]bool)
		for len(targets) < n {
			target := genModelTarget().Draw(t, "target")
			if !seen[target] {
				seen[target] = true
				targets = append(targets, target)
			}
		}

		// Build graph with no deps (no session for AST extraction)
		g := NewGraph(nil)
		for _, target := range targets {
			g.Add(&parser.Model{
				Target: target,
				Kind:   "table",
				SQL:    "SELECT 1",
			})
		}

		sorted, err := g.Sort()
		if err != nil {
			t.Fatalf("Sort: %v", err)
		}

		if len(sorted) != len(targets) {
			t.Fatalf("Sort returned %d models, want %d", len(sorted), len(targets))
		}

		// Check all targets present in output
		outputTargets := make(map[string]bool)
		for _, m := range sorted {
			outputTargets[m.Target] = true
		}
		for _, target := range targets {
			if !outputTargets[target] {
				t.Fatalf("missing target in output: %s", target)
			}
		}
	})
}

// Property: Sort is deterministic (same input → same output).
func TestRapid_Sort_Deterministic(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(1, 10).Draw(t, "n")
		targets := make([]string, 0, n)
		seen := make(map[string]bool)
		for len(targets) < n {
			target := genModelTarget().Draw(t, "target")
			if !seen[target] {
				seen[target] = true
				targets = append(targets, target)
			}
		}

		// Build same graph twice
		build := func() []*parser.Model {
			g := NewGraph(nil)
			for _, target := range targets {
				g.Add(&parser.Model{Target: target, Kind: "table", SQL: "SELECT 1"})
			}
			sorted, err := g.Sort()
			if err != nil {
				t.Fatalf("Sort: %v", err)
			}
			return sorted
		}

		r1 := build()
		r2 := build()

		if len(r1) != len(r2) {
			t.Fatal("different lengths")
		}
		for i := range r1 {
			if r1[i].Target != r2[i].Target {
				t.Fatalf("position %d: %s != %s", i, r1[i].Target, r2[i].Target)
			}
		}
	})
}

// --- DAG State Machine ---
// Tests that building a graph incrementally (adding nodes, adding deps)
// maintains topological sort invariants and detects cycles.

type dagMachine struct {
	graph   *Graph
	targets []string
	deps    map[string][]string // target → deps
}

func (sm *dagMachine) AddNode(t *rapid.T) {
	target := genModelTarget().Draw(t, "target")
	// Skip if already exists
	if _, exists := sm.graph.nodes[target]; exists {
		t.Skip("target already exists")
	}

	sm.graph.Add(&parser.Model{Target: target, Kind: "table", SQL: "SELECT 1"})
	sm.targets = append(sm.targets, target)
	sm.deps[target] = nil
}

func (sm *dagMachine) AddDep(t *rapid.T) {
	if len(sm.targets) < 2 {
		t.Skip("need at least 2 nodes")
	}

	// Pick a dependent and a dependency
	depIdx := rapid.IntRange(0, len(sm.targets)-1).Draw(t, "dependent")
	srcIdx := rapid.IntRange(0, len(sm.targets)-1).Draw(t, "dependency")
	if depIdx == srcIdx {
		t.Skip("can't depend on self")
	}

	dependent := sm.targets[depIdx]
	dependency := sm.targets[srcIdx]

	// Add the dependency
	node := sm.graph.nodes[dependent]
	// Check if already has this dep
	for _, d := range node.Deps {
		if d == dependency {
			t.Skip("dep already exists")
		}
	}
	node.Deps = append(node.Deps, dependency)
	sm.deps[dependent] = append(sm.deps[dependent], dependency)
}

func (sm *dagMachine) Check(t *rapid.T) {
	if len(sm.targets) == 0 {
		return
	}

	sorted, err := sm.sortFresh()
	if err != nil {
		// Cycle detected — verify it's actually a cycle
		if len(sm.targets) < 2 {
			t.Fatalf("cycle with < 2 nodes: %v", err)
		}
		return // cycles are valid outcomes
	}

	// Invariant 1: completeness
	if len(sorted) != len(sm.targets) {
		t.Fatalf("sort returned %d, want %d", len(sorted), len(sm.targets))
	}

	// Invariant 2: all targets present
	outputSet := make(map[string]bool)
	for _, m := range sorted {
		outputSet[m.Target] = true
	}
	for _, tgt := range sm.targets {
		if !outputSet[tgt] {
			t.Fatalf("missing target: %s", tgt)
		}
	}

	// Invariant 3: dependency order
	pos := make(map[string]int)
	for i, m := range sorted {
		pos[m.Target] = i
	}
	for _, node := range sm.graph.nodes {
		for _, dep := range node.Deps {
			if _, exists := sm.graph.nodes[dep]; !exists {
				continue // external dep, filtered out by Sort
			}
			if pos[dep] >= pos[node.Model.Target] {
				t.Fatalf("dep %s (pos %d) after %s (pos %d)", dep, pos[dep], node.Model.Target, pos[node.Model.Target])
			}
		}
	}
}

// sortFresh rebuilds the graph to avoid Sort's internal mutation affecting state.
func (sm *dagMachine) sortFresh() ([]*parser.Model, error) {
	g := NewGraph(nil)
	for _, tgt := range sm.targets {
		g.Add(&parser.Model{Target: tgt, Kind: "table", SQL: "SELECT 1"})
	}
	for tgt, deps := range sm.deps {
		if node, ok := g.nodes[tgt]; ok {
			node.Deps = append(node.Deps, deps...)
		}
	}
	return g.Sort()
}

func TestRapid_DAG_StateMachine(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		sm := &dagMachine{
			graph: NewGraph(nil),
			deps:  make(map[string][]string),
		}
		rt.Repeat(rapid.StateMachineActions(sm))
	})
}

// Property: self-cycle is detected.
func TestRapid_DAG_SelfCycleDetected(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		target := genModelTarget().Draw(t, "target")
		g := NewGraph(nil)
		g.Add(&parser.Model{Target: target, Kind: "table", SQL: "SELECT 1"})
		g.nodes[target].Deps = []string{target}

		_, err := g.Sort()
		if err == nil {
			t.Fatal("expected cycle error for self-dependency")
		}
	})
}

// Property: with manually injected deps, dependencies appear before dependents.
func TestRapid_Sort_DependencyOrder(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		// Generate a chain of 2-5 models where each depends on the previous
		chainLen := rapid.IntRange(2, 5).Draw(t, "chainLen")
		targets := make([]string, 0, chainLen)
		seen := make(map[string]bool)
		for len(targets) < chainLen {
			target := genModelTarget().Draw(t, "target")
			if !seen[target] {
				seen[target] = true
				targets = append(targets, target)
			}
		}

		g := NewGraph(nil)
		for i, target := range targets {
			node := &parser.Model{Target: target, Kind: "table", SQL: "SELECT 1"}
			g.Add(node)
			if i > 0 {
				// Manually inject dependency: targets[i] depends on targets[i-1]
				g.nodes[target].Deps = []string{targets[i-1]}
			}
		}

		sorted, err := g.Sort()
		if err != nil {
			t.Fatalf("Sort: %v", err)
		}

		// Build position map
		pos := make(map[string]int)
		for i, m := range sorted {
			pos[m.Target] = i
		}

		// Verify: for each edge i→i-1, dep appears before dependent
		for i := 1; i < len(targets); i++ {
			depPos, ok1 := pos[targets[i-1]]
			nodePos, ok2 := pos[targets[i]]
			if !ok1 || !ok2 {
				t.Fatalf("missing target in output")
			}
			if depPos >= nodePos {
				t.Fatalf("dependency order violated: %s (pos %d) should appear before %s (pos %d)",
					targets[i-1], depPos, targets[i], nodePos)
			}
		}
	})
}
