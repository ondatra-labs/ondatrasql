// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package dag

import (
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"pgregory.net/rapid"
)

// Property: script models with no query() calls have no deps (appear as roots).
func TestRapid_ScriptModel_NoQueryNoDeps(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		target := genModelTarget().Draw(t, "target")
		// Script code with no query() calls
		code := rapid.SampledFrom([]string{
			`save.row({"id": 1})`,
			`resp = http.get("https://api.example.com")\nfor r in resp.json:\n    save.row(r)`,
			`x = 1 + 2`,
			``,
		}).Draw(t, "code")

		g := NewGraph(nil) // nil session = no SQL dep extraction
		g.Add(&parser.Model{
			Target:   target,
			Kind:     "table",
			IsScript: true,
			SQL:      code,
		})

		node := g.nodes[target]
		if len(node.Deps) != 0 {
			t.Fatalf("script with no query() should have 0 deps, got %v", node.Deps)
		}
	})
}

// Property: script + SQL models maintain topological sort invariants.
func TestRapid_MixedDAG_SortInvariants(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		nSQL := rapid.IntRange(1, 5).Draw(t, "nSQL")
		nScript := rapid.IntRange(1, 3).Draw(t, "nScript")

		g := NewGraph(nil)
		var allTargets []string
		seen := make(map[string]bool)

		// Add SQL models
		for range nSQL {
			target := genModelTarget().Draw(t, "sqlTarget")
			if seen[target] {
				continue
			}
			seen[target] = true
			allTargets = append(allTargets, target)
			g.Add(&parser.Model{
				Target: target,
				Kind:   "table",
				SQL:    "SELECT 1",
			})
		}

		// Add script models (no deps since nil session)
		for range nScript {
			target := genModelTarget().Draw(t, "scriptTarget")
			if seen[target] {
				continue
			}
			seen[target] = true
			allTargets = append(allTargets, target)
			g.Add(&parser.Model{
				Target:   target,
				Kind:     "append",
				IsScript: true,
				SQL:      `save.row({"id": 1})`,
			})
		}

		if len(allTargets) == 0 {
			return
		}

		// Inject some manual deps between known nodes
		if len(allTargets) >= 2 {
			nDeps := rapid.IntRange(0, len(allTargets)-1).Draw(t, "nDeps")
			for range nDeps {
				from := rapid.IntRange(0, len(allTargets)-1).Draw(t, "from")
				to := rapid.IntRange(0, len(allTargets)-1).Draw(t, "to")
				if from != to {
					node := g.nodes[allTargets[from]]
					// Avoid duplicates
					hasDep := false
					for _, d := range node.Deps {
						if d == allTargets[to] {
							hasDep = true
							break
						}
					}
					if !hasDep {
						node.Deps = append(node.Deps, allTargets[to])
					}
				}
			}
		}

		sorted, err := g.Sort()
		if err != nil {
			return // cycles are valid outcomes
		}

		// Invariant: completeness
		if len(sorted) != len(allTargets) {
			t.Fatalf("sort returned %d, want %d", len(sorted), len(allTargets))
		}

		// Invariant: all targets present
		outputSet := make(map[string]bool)
		for _, m := range sorted {
			outputSet[m.Target] = true
		}
		for _, tgt := range allTargets {
			if !outputSet[tgt] {
				t.Fatalf("missing target: %s", tgt)
			}
		}

		// Invariant: dependency order
		pos := make(map[string]int)
		for i, m := range sorted {
			pos[m.Target] = i
		}
		for _, node := range g.nodes {
			for _, dep := range node.Deps {
				if _, exists := g.nodes[dep]; !exists {
					continue
				}
				if pos[dep] >= pos[node.Model.Target] {
					t.Fatalf("dep %s (pos %d) after %s (pos %d)",
						dep, pos[dep], node.Model.Target, pos[node.Model.Target])
				}
			}
		}
	})
}
