// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build e2e

package e2e

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/dag"
	"github.com/ondatra-labs/ondatrasql/internal/execute"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// TestE2E_StarlarkQuery verifies the query() built-in reads DuckDB tables.
func TestE2E_StarlarkQuery(t *testing.T) {
	p := testutil.NewProject(t)

	// Create a SQL source table
	p.AddModel("staging/users.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name
UNION ALL
SELECT 2, 'Bob'
`)
	runModel(t, p, "staging/users.sql")

	// Starlark model reads from it via query()
	p.AddModel("mart/user_export.star", `# @kind: table
rows = query("SELECT id, name FROM staging.users")
for row in rows:
    save.row({"user_id": row["id"], "user_name": row["name"], "exported": "true"})
`)
	result := runModel(t, p, "mart/user_export.star")

	if result.RowsAffected != 2 {
		t.Errorf("rows = %d, want 2", result.RowsAffected)
	}

	val, err := p.Sess.QueryValue("SELECT COUNT(*) FROM mart.user_export")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "2" {
		t.Errorf("count = %s, want 2", val)
	}

	// Verify content
	val, _ = p.Sess.QueryValue("SELECT user_name FROM mart.user_export WHERE user_id = '1'")
	if val != "Alice" {
		t.Errorf("user_name = %s, want Alice", val)
	}
}

// TestE2E_StarlarkQueryDAGOrdering verifies that query() calls create DAG dependencies
// and models execute in the correct order.
func TestE2E_StarlarkQueryDAGOrdering(t *testing.T) {
	p := testutil.NewProject(t)

	// SQL source
	p.AddModel("staging/products.sql", `-- @kind: table
SELECT 1 AS id, 'Widget' AS name, 100 AS price
UNION ALL
SELECT 2, 'Gadget', 200
`)

	// Starlark model that query()s the SQL model
	p.AddModel("mart/product_report.star", `# @kind: table
rows = query("SELECT id, name, price FROM staging.products WHERE price > 50")
for row in rows:
    save.row({"product_id": row["id"], "product_name": row["name"], "expensive": "true"})
`)

	// Parse both models and build DAG
	paths := []string{"staging/products.sql", "mart/product_report.star"}
	var models []*parser.Model
	for _, relPath := range paths {
		modelPath := filepath.Join(p.Dir, "models", relPath)
		m, err := parser.ParseModel(modelPath, p.Dir)
		if err != nil {
			t.Fatalf("parse %s: %v", relPath, err)
		}
		models = append(models, m)
	}

	// Build DAG with projectDir for Starlark dep extraction
	g := dag.NewGraph(p.Sess, p.Dir)
	for _, m := range models {
		g.Add(m)
	}
	sorted, err := g.Sort()
	if err != nil {
		t.Fatalf("sort: %v", err)
	}

	// Verify ordering: staging.products must come before mart.product_report
	order := map[string]int{}
	for i, m := range sorted {
		order[m.Target] = i
	}

	if order["staging.products"] >= order["mart.product_report"] {
		t.Errorf("staging.products (pos %d) should come before mart.product_report (pos %d)",
			order["staging.products"], order["mart.product_report"])
	}

	// Execute in DAG order
	runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
	runner.SetProjectDir(p.Dir)
	for _, m := range sorted {
		if _, err := runner.Run(context.Background(), m); err != nil {
			t.Fatalf("run %s: %v", m.Target, err)
		}
	}

	// Verify final table
	val, err := p.Sess.QueryValue("SELECT COUNT(*) FROM mart.product_report")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "2" {
		t.Errorf("count = %s, want 2", val)
	}

	snap := newSnapshot()
	for _, m := range sorted {
		snap.addLine("order: " + m.Target)
	}
	snap.addQuery(p.Sess, "count", "SELECT COUNT(*) FROM mart.product_report")
	assertGolden(t, "starlark_query_dag_ordering", snap)
}

// TestE2E_StarlarkQueryInLoadedModule verifies that query() in a load()-ed library
// module is detected for DAG ordering.
func TestE2E_StarlarkQueryInLoadedModule(t *testing.T) {
	p := testutil.NewProject(t)

	// SQL source
	p.AddModel("staging/customers.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name
`)

	// Library module with query()
	testutil.WriteFile(t, p.Dir, "lib/enricher.star", `
def enrich(save):
    rows = query("SELECT id, name FROM staging.customers")
    for row in rows:
        save.row({"customer_id": row["id"], "enriched_name": row["name"] + " (enriched)"})
`)

	// Starlark model that loads the library
	p.AddModel("mart/enriched.star", `# @kind: table
load("lib/enricher.star", "enrich")
enrich(save)
`)

	// Parse and build DAG
	paths := []string{"staging/customers.sql", "mart/enriched.star"}
	var models []*parser.Model
	for _, relPath := range paths {
		modelPath := filepath.Join(p.Dir, "models", relPath)
		m, err := parser.ParseModel(modelPath, p.Dir)
		if err != nil {
			t.Fatalf("parse %s: %v", relPath, err)
		}
		models = append(models, m)
	}

	g := dag.NewGraph(p.Sess, p.Dir)
	for _, m := range models {
		g.Add(m)
	}
	sorted, err := g.Sort()
	if err != nil {
		t.Fatalf("sort: %v", err)
	}

	// Verify ordering: staging.customers must come before mart.enriched
	order := map[string]int{}
	for i, m := range sorted {
		order[m.Target] = i
	}

	if order["staging.customers"] >= order["mart.enriched"] {
		t.Errorf("staging.customers (pos %d) should come before mart.enriched (pos %d)",
			order["staging.customers"], order["mart.enriched"])
	}

	// Execute in DAG order
	runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
	runner.SetProjectDir(p.Dir)
	for _, m := range sorted {
		if _, err := runner.Run(context.Background(), m); err != nil {
			t.Fatalf("run %s: %v", m.Target, err)
		}
	}

	val, _ := p.Sess.QueryValue("SELECT enriched_name FROM mart.enriched WHERE customer_id = '1'")
	if val != "Alice (enriched)" {
		t.Errorf("enriched_name = %s, want 'Alice (enriched)'", val)
	}

	snap := newSnapshot()
	for _, m := range sorted {
		snap.addLine("order: " + m.Target)
	}
	snap.addQuery(p.Sess, "enriched_name", "SELECT enriched_name FROM mart.enriched WHERE customer_id = '1'")
	assertGolden(t, "starlark_query_in_loaded_module", snap)
}
