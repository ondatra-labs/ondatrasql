// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package dag_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/dag"
	"github.com/ondatra-labs/ondatrasql/internal/execute"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

func runModel(t *testing.T, p *testutil.Project, relPath string) {
	t.Helper()
	modelPath := filepath.Join(p.Dir, "models", relPath)
	model, err := parser.ParseModel(modelPath, p.Dir)
	if err != nil {
		t.Fatalf("parse %s: %v", relPath, err)
	}
	runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
	_, err = runner.Run(context.Background(), model)
	if err != nil {
		t.Fatalf("run %s: %v", relPath, err)
	}
}

func TestGenerateRunID(t *testing.T) {
	id1 := dag.GenerateRunID()
	id2 := dag.GenerateRunID()

	if id1 == "" {
		t.Fatal("GenerateRunID returned empty string")
	}
	if id1 == id2 {
		t.Error("two consecutive GenerateRunID should differ")
	}
}

func TestGenerateRunID_Format(t *testing.T) {
	id := dag.GenerateRunID()
	parts := strings.Split(id, "-")
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts (YYYYMMDD-HHMMSS-RANDOM), got %d: %q", len(parts), id)
	}
	if len(parts[0]) != 8 {
		t.Errorf("date part = %q, expected 8 chars", parts[0])
	}
	if len(parts[1]) != 6 {
		t.Errorf("time part = %q, expected 6 chars", parts[1])
	}
	if len(parts[2]) != 6 {
		t.Errorf("random part = %q, expected 6 hex chars", parts[2])
	}
}

func TestAnalyzeImpact_NoDownstream(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("raw/source.sql", `-- @kind: table
SELECT 1 AS id, 'test' AS value`)
	runModel(t, p, "raw/source.sql")

	analysis, err := dag.AnalyzeImpact(p.Sess, "raw.source")
	if err != nil {
		t.Fatalf("AnalyzeImpact: %v", err)
	}
	if analysis == nil {
		t.Fatal("expected non-nil analysis")
	}
	if analysis.ChangedModel != "raw.source" {
		t.Errorf("ChangedModel = %q, want raw.source", analysis.ChangedModel)
	}
	if len(analysis.Impacts) != 0 {
		t.Errorf("expected 0 impacts, got %d", len(analysis.Impacts))
	}
}

func TestAnalyzeImpact_WithDownstream(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/orders.sql", `-- @kind: table
SELECT 1 AS id, 100 AS amount`)
	runModel(t, p, "raw/orders.sql")

	p.AddModel("staging/totals.sql", `-- @kind: table
SELECT id, amount * 2 AS doubled FROM raw.orders`)
	runModel(t, p, "staging/totals.sql")

	analysis, err := dag.AnalyzeImpact(p.Sess, "raw.orders")
	if err != nil {
		t.Fatalf("AnalyzeImpact: %v", err)
	}
	if len(analysis.Impacts) == 0 {
		t.Fatal("expected at least 1 impact")
	}

	found := false
	for _, imp := range analysis.Impacts {
		if imp.Target == "staging.totals" {
			found = true
			if !imp.RebuildNeeded {
				t.Error("expected RebuildNeeded=true")
			}
		}
	}
	if !found {
		t.Error("expected impact on staging.totals")
	}
}

func TestGetFullImpactTree(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/events.sql", `-- @kind: table
SELECT 1 AS id, 'click' AS event_type`)
	runModel(t, p, "raw/events.sql")

	p.AddModel("staging/events.sql", `-- @kind: table
SELECT id, event_type FROM raw.events`)
	runModel(t, p, "staging/events.sql")

	p.AddModel("mart/event_counts.sql", `-- @kind: table
SELECT event_type, COUNT(*) AS cnt FROM staging.events GROUP BY event_type`)
	runModel(t, p, "mart/event_counts.sql")

	tree, err := dag.GetFullImpactTree(p.Sess, "raw.events")
	if err != nil {
		t.Fatalf("GetFullImpactTree: %v", err)
	}

	// Should include staging.events and mart.event_counts
	treeSet := make(map[string]bool)
	for _, m := range tree {
		treeSet[m] = true
	}
	if !treeSet["staging.events"] {
		t.Errorf("expected staging.events in tree, got %v", tree)
	}
	if !treeSet["mart.event_counts"] {
		t.Errorf("expected mart.event_counts in tree, got %v", tree)
	}
}

func TestGetFullImpactTree_NoDownstream(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/isolated.sql", `-- @kind: table
SELECT 1 AS id`)
	runModel(t, p, "raw/isolated.sql")

	tree, err := dag.GetFullImpactTree(p.Sess, "raw.isolated")
	if err != nil {
		t.Fatalf("GetFullImpactTree: %v", err)
	}
	if len(tree) != 0 {
		t.Errorf("expected empty tree, got %v", tree)
	}
}

func TestAnalyzeTransitiveImpact(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/base.sql", `-- @kind: table
SELECT 1 AS id, 'data' AS val`)
	runModel(t, p, "raw/base.sql")

	p.AddModel("staging/derived.sql", `-- @kind: table
SELECT id, val FROM raw.base`)
	runModel(t, p, "staging/derived.sql")

	impact, err := dag.AnalyzeTransitiveImpact(p.Sess, "raw.base", "staging.derived")
	if err != nil {
		t.Fatalf("AnalyzeTransitiveImpact: %v", err)
	}
	if impact == nil {
		t.Fatal("expected non-nil impact")
	}
	if !impact.RebuildNeeded {
		t.Error("expected RebuildNeeded=true")
	}
}

func TestAnalyzeTransitiveImpact_NotDirect(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/a.sql", `-- @kind: table
SELECT 1 AS id`)
	runModel(t, p, "raw/a.sql")

	p.AddModel("raw/b.sql", `-- @kind: table
SELECT 2 AS id`)
	runModel(t, p, "raw/b.sql")

	// b does not depend on a
	impact, err := dag.AnalyzeTransitiveImpact(p.Sess, "raw.a", "raw.b")
	if err != nil {
		t.Fatalf("AnalyzeTransitiveImpact: %v", err)
	}
	if impact != nil {
		t.Errorf("expected nil impact for non-dependency, got %+v", impact)
	}
}

func TestAnalyzeImpact_WithTransformations(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/sales.sql", `-- @kind: table
SELECT 1 AS id, 100 AS amount, 'US' AS region`)
	runModel(t, p, "raw/sales.sql")

	// Downstream with aggregation (function transformation)
	p.AddModel("staging/agg_sales.sql", `-- @kind: table
SELECT region, SUM(amount) AS total FROM raw.sales GROUP BY region`)
	runModel(t, p, "staging/agg_sales.sql")

	analysis, err := dag.AnalyzeImpact(p.Sess, "raw.sales")
	if err != nil {
		t.Fatalf("AnalyzeImpact: %v", err)
	}
	if len(analysis.Impacts) == 0 {
		t.Fatal("expected at least 1 impact")
	}

	for _, imp := range analysis.Impacts {
		if imp.Target == "staging.agg_sales" {
			if !imp.RebuildNeeded {
				t.Error("expected RebuildNeeded=true")
			}
			if imp.Reason == "" {
				t.Error("expected non-empty reason")
			}
		}
	}
}

func TestAnalyzeImpact_MultipleDownstream(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/data.sql", `-- @kind: table
SELECT 1 AS id, 'hello' AS val`)
	runModel(t, p, "raw/data.sql")

	p.AddModel("staging/copy1.sql", `-- @kind: table
SELECT id, val FROM raw.data`)
	runModel(t, p, "staging/copy1.sql")

	p.AddModel("staging/copy2.sql", `-- @kind: table
SELECT id FROM raw.data`)
	runModel(t, p, "staging/copy2.sql")

	analysis, err := dag.AnalyzeImpact(p.Sess, "raw.data")
	if err != nil {
		t.Fatalf("AnalyzeImpact: %v", err)
	}
	if len(analysis.Impacts) < 2 {
		t.Errorf("expected at least 2 impacts, got %d", len(analysis.Impacts))
	}

	targets := make(map[string]bool)
	for _, imp := range analysis.Impacts {
		targets[imp.Target] = true
	}
	if !targets["staging.copy1"] {
		t.Error("expected impact on staging.copy1")
	}
	if !targets["staging.copy2"] {
		t.Error("expected impact on staging.copy2")
	}
}

func TestGetFullImpactTree_TransitiveChain(t *testing.T) {
	p := testutil.NewProject(t)

	// 3-level chain: raw → staging → mart
	p.AddModel("raw/chain_src.sql", `-- @kind: table
SELECT 1 AS id, 10 AS val`)
	runModel(t, p, "raw/chain_src.sql")

	p.AddModel("staging/chain_mid.sql", `-- @kind: table
SELECT id, val * 2 AS doubled FROM raw.chain_src`)
	runModel(t, p, "staging/chain_mid.sql")

	p.AddModel("mart/chain_end.sql", `-- @kind: table
SELECT SUM(doubled) AS total FROM staging.chain_mid`)
	runModel(t, p, "mart/chain_end.sql")

	tree, err := dag.GetFullImpactTree(p.Sess, "raw.chain_src")
	if err != nil {
		t.Fatalf("GetFullImpactTree: %v", err)
	}

	treeSet := make(map[string]bool)
	for _, m := range tree {
		treeSet[m] = true
	}
	if !treeSet["staging.chain_mid"] {
		t.Errorf("expected staging.chain_mid in tree, got %v", tree)
	}
	if !treeSet["mart.chain_end"] {
		t.Errorf("expected mart.chain_end in tree, got %v", tree)
	}
}

func TestAnalyzeImpact_ModelWithNoLineage(t *testing.T) {
	p := testutil.NewProject(t)

	// Source model
	p.AddModel("raw/nol_src.sql", `-- @kind: table
SELECT 1 AS id`)
	runModel(t, p, "raw/nol_src.sql")

	// Downstream that uses SELECT * (might not have detailed column lineage)
	p.AddModel("staging/nol_dst.sql", `-- @kind: table
SELECT * FROM raw.nol_src`)
	runModel(t, p, "staging/nol_dst.sql")

	analysis, err := dag.AnalyzeImpact(p.Sess, "raw.nol_src")
	if err != nil {
		t.Fatalf("AnalyzeImpact: %v", err)
	}
	if len(analysis.Impacts) == 0 {
		t.Fatal("expected at least 1 impact")
	}
	// Even without detailed column lineage, should still mark as rebuild needed
	for _, imp := range analysis.Impacts {
		if imp.Target == "staging.nol_dst" && !imp.RebuildNeeded {
			t.Error("expected RebuildNeeded=true even without column lineage")
		}
	}
}
