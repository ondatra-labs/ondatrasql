// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package dag_test

import (
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/dag"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// TestAnalyzeImpact_ClosedSession tests the error path when GetDownstreamModels
// fails (e.g. due to a closed session).
func TestAnalyzeImpact_ClosedSession(t *testing.T) {
	sess, err := duckdb.NewSession(":memory:")
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	sess.Close()

	_, err = dag.AnalyzeImpact(sess, "raw.orders")
	if err == nil {
		t.Error("expected error from AnalyzeImpact with closed session")
	}
}

// TestAnalyzeTransitiveImpact_ClosedSession tests the error path when
// GetDownstreamModels fails in AnalyzeTransitiveImpact.
func TestAnalyzeTransitiveImpact_ClosedSession(t *testing.T) {
	sess, err := duckdb.NewSession(":memory:")
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	sess.Close()

	_, err = dag.AnalyzeTransitiveImpact(sess, "raw.base", "staging.derived")
	if err == nil {
		t.Error("expected error from AnalyzeTransitiveImpact with closed session")
	}
}

// TestGetFullImpactTree_ClosedSession tests the error path when
// GetDownstreamModels fails during tree traversal.
func TestGetFullImpactTree_ClosedSession(t *testing.T) {
	sess, err := duckdb.NewSession(":memory:")
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	sess.Close()

	_, err = dag.GetFullImpactTree(sess, "raw.events")
	if err == nil {
		t.Error("expected error from GetFullImpactTree with closed session")
	}
}

// TestGetFullImpactTree_DiamondDependency tests the visited-node dedup branch
// in GetFullImpactTree. When two models depend on the same downstream model,
// the traversal should visit it only once (visited[model] returns true on second visit).
func TestGetFullImpactTree_DiamondDependency(t *testing.T) {
	p := testutil.NewProject(t)

	// Build a diamond:
	//     raw.root
	//      /   \
	//  stg.a   stg.b
	//      \   /
	//     mart.c
	p.AddModel("raw/root.sql", `-- @kind: table
SELECT 1 AS id, 'x' AS val`)
	runModel(t, p, "raw/root.sql")

	p.AddModel("staging/a.sql", `-- @kind: table
SELECT id FROM raw.root`)
	runModel(t, p, "staging/a.sql")

	p.AddModel("staging/b.sql", `-- @kind: table
SELECT val FROM raw.root`)
	runModel(t, p, "staging/b.sql")

	p.AddModel("mart/c.sql", `-- @kind: table
SELECT a.id, b.val FROM staging.a AS a JOIN staging.b AS b ON a.id = 1`)
	runModel(t, p, "mart/c.sql")

	tree, err := dag.GetFullImpactTree(p.Sess, "raw.root")
	if err != nil {
		t.Fatalf("GetFullImpactTree: %v", err)
	}

	// Should contain staging.a, staging.b, and mart.c
	treeSet := make(map[string]bool)
	for _, m := range tree {
		treeSet[m] = true
	}
	if !treeSet["staging.a"] {
		t.Errorf("expected staging.a in tree, got %v", tree)
	}
	if !treeSet["staging.b"] {
		t.Errorf("expected staging.b in tree, got %v", tree)
	}
	if !treeSet["mart.c"] {
		t.Errorf("expected mart.c in tree, got %v", tree)
	}

	// Each downstream model must appear exactly once. mart.c is reached
	// through both staging.a and staging.b in this diamond, but the dedup
	// in GetFullImpactTree should prevent the duplicate append.
	if len(tree) != 3 {
		t.Errorf("expected exactly 3 entries (staging.a, staging.b, mart.c), got %d: %v", len(tree), tree)
	}
	martCCount := 0
	for _, m := range tree {
		if m == "mart.c" {
			martCCount++
		}
	}
	if martCCount != 1 {
		t.Errorf("mart.c should appear exactly once (diamond dedup), got %d in %v", martCCount, tree)
	}
}

// TestGetFullImpactTree_NonexistentModel tests traversal for a model that
// does not exist - should return empty tree with no error.
func TestGetFullImpactTree_NonexistentModel(t *testing.T) {
	p := testutil.NewProject(t)

	tree, err := dag.GetFullImpactTree(p.Sess, "nonexistent.model")
	if err != nil {
		t.Fatalf("GetFullImpactTree for nonexistent model: %v", err)
	}
	if len(tree) != 0 {
		t.Errorf("expected empty tree for nonexistent model, got %v", tree)
	}
}

// TestAnalyzeImpact_NonexistentModel tests AnalyzeImpact for a model that
// doesn't exist - should return empty impacts, no error.
func TestAnalyzeImpact_NonexistentModel(t *testing.T) {
	p := testutil.NewProject(t)

	analysis, err := dag.AnalyzeImpact(p.Sess, "nonexistent.model")
	if err != nil {
		t.Fatalf("AnalyzeImpact: %v", err)
	}
	if analysis == nil {
		t.Fatal("expected non-nil analysis")
	}
	if len(analysis.Impacts) != 0 {
		t.Errorf("expected 0 impacts for nonexistent model, got %d", len(analysis.Impacts))
	}
}

// TestAnalyzeTransitiveImpact_WithColumnLineage tests AnalyzeTransitiveImpact
// where the transitive model has detailed column lineage with transformations.
func TestAnalyzeTransitiveImpact_WithColumnLineage(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/src.sql", `-- @kind: table
SELECT 1 AS id, 100 AS amount, 'US' AS region`)
	runModel(t, p, "raw/src.sql")

	p.AddModel("staging/agg.sql", `-- @kind: table
SELECT region, SUM(amount) AS total FROM raw.src GROUP BY region`)
	runModel(t, p, "staging/agg.sql")

	impact, err := dag.AnalyzeTransitiveImpact(p.Sess, "raw.src", "staging.agg")
	if err != nil {
		t.Fatalf("AnalyzeTransitiveImpact: %v", err)
	}
	if impact == nil {
		t.Fatal("expected non-nil impact")
	}
	if !impact.RebuildNeeded {
		t.Error("expected RebuildNeeded=true")
	}
	if len(impact.AffectedColumns) == 0 && impact.Reason == "" {
		t.Error("expected either affected columns or a reason")
	}
}

// TestAnalyzeTransitiveImpact_CaseInsensitive verifies that the entire
// case-insensitive lookup story works end-to-end. This used to be broken
// at three layers and any case variant would silently lose impact:
//
//   - The `ondatra_get_downstream` macro used `LIKE`, case-sensitive
//   - The `ondatra_get_commit_info` macro used `=`, case-sensitive
//   - `AnalyzeTransitiveImpact` itself compared the downstream list
//     against `transitiveModel` using `==`, also case-sensitive
//
// Now the macros use `LOWER()`/`ILIKE` and the Go function uses
// `strings.EqualFold`, so any combination of cases must resolve.
func TestAnalyzeTransitiveImpact_CaseInsensitive(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/src.sql", `-- @kind: table
SELECT 1 AS id, 100 AS amount`)
	runModel(t, p, "raw/src.sql")

	p.AddModel("staging/agg.sql", `-- @kind: table
SELECT id, amount FROM raw.src`)
	runModel(t, p, "staging/agg.sql")

	cases := []struct {
		changed     string
		transitive  string
		description string
	}{
		{"raw.src", "staging.agg", "both lowercase (canonical)"},
		{"RAW.SRC", "staging.agg", "uppercase changed model"},
		{"raw.src", "STAGING.AGG", "uppercase transitive model"},
		{"Raw.Src", "Staging.Agg", "mixed case both"},
	}
	for _, tc := range cases {
		impact, err := dag.AnalyzeTransitiveImpact(p.Sess, tc.changed, tc.transitive)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", tc.description, err)
			continue
		}
		if impact == nil {
			t.Errorf("%s: expected non-nil impact (case mismatch lost it)", tc.description)
		}
	}
}
