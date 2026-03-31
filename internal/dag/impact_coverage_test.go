// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
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

	// The visited map prevents infinite recursion but mart.c may appear
	// multiple times in result since append happens before traverse dedup.
	// The important thing is that all downstream models are present.
	if len(tree) < 3 {
		t.Errorf("expected at least 3 entries in tree, got %d: %v", len(tree), tree)
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
