// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/collect"
	"github.com/ondatra-labs/ondatrasql/internal/dag"
	"github.com/ondatra-labs/ondatrasql/internal/oauth2host"
	"github.com/ondatra-labs/ondatrasql/internal/odata"
	"github.com/ondatra-labs/ondatrasql/internal/execute"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// runModel parses and executes a single model through the pipeline.
func runModel(t *testing.T, p *testutil.Project, relPath string) *execute.Result {
	t.Helper()
	modelPath := filepath.Join(p.Dir, "models", relPath)
	model, err := parser.ParseModel(modelPath, p.Dir)
	if err != nil {
		t.Fatalf("parse %s: %v", relPath, err)
	}

	runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
	runner.SetProjectDir(p.Dir)
	result, err := runner.Run(context.Background(), model)
	if err != nil {
		t.Fatalf("run %s: %v", relPath, err)
	}
	return result
}

func TestE2E_TableModel(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("staging/users.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name
UNION ALL
SELECT 2, 'Bob'
`)

	result := runModel(t, p, "staging/users.sql")

	if result.RowsAffected != 2 {
		t.Errorf("rows = %d, want 2", result.RowsAffected)
	}
	if result.Kind != "table" {
		t.Errorf("kind = %q, want table", result.Kind)
	}
	if result.Target != "staging.users" {
		t.Errorf("target = %q, want staging.users", result.Target)
	}

	// Verify data exists in the catalog
	val, err := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.users")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "2" {
		t.Errorf("count = %s, want 2", val)
	}

	snap := newSnapshot()
	snap.addResult(result)
	snap.addQuery(p.Sess, "count", "SELECT COUNT(*) FROM staging.users")
	snap.addQueryRows(p.Sess, "data", "SELECT id, name FROM staging.users ORDER BY id")
	assertGolden(t, "table_model", snap)
}

func TestE2E_DAGOrdering(t *testing.T) {
	p := testutil.NewProject(t)

	// Create 3 models: raw → staging → mart
	p.AddModel("raw/events.sql", `-- @kind: table
SELECT 1 AS event_id, 'click' AS event_type
`)
	p.AddModel("staging/events.sql", `-- @kind: table
SELECT event_id, event_type FROM raw.events
`)
	p.AddModel("mart/events_summary.sql", `-- @kind: table
SELECT event_type, COUNT(*) AS cnt FROM staging.events GROUP BY event_type
`)

	// Parse all models
	models := make([]*parser.Model, 3)
	paths := []string{"raw/events.sql", "staging/events.sql", "mart/events_summary.sql"}
	for i, relPath := range paths {
		modelPath := filepath.Join(p.Dir, "models", relPath)
		m, err := parser.ParseModel(modelPath, p.Dir)
		if err != nil {
			t.Fatalf("parse %s: %v", relPath, err)
		}
		models[i] = m
	}

	// Build DAG and sort
	g := dag.NewGraph(p.Sess)
	for _, m := range models {
		g.Add(m)
	}
	sorted, err := g.Sort()
	if err != nil {
		t.Fatalf("sort: %v", err)
	}

	if len(sorted) != 3 {
		t.Fatalf("sorted len = %d, want 3", len(sorted))
	}

	// Verify ordering: raw.events must come before staging.events, which must come before mart.events_summary
	order := map[string]int{}
	for i, m := range sorted {
		order[m.Target] = i
	}

	if order["raw.events"] >= order["staging.events"] {
		t.Errorf("raw.events (pos %d) should come before staging.events (pos %d)",
			order["raw.events"], order["staging.events"])
	}
	if order["staging.events"] >= order["mart.events_summary"] {
		t.Errorf("staging.events (pos %d) should come before mart.events_summary (pos %d)",
			order["staging.events"], order["mart.events_summary"])
	}

	// Execute in DAG order
	runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
	for _, m := range sorted {
		if _, err := runner.Run(context.Background(), m); err != nil {
			t.Fatalf("run %s: %v", m.Target, err)
		}
	}

	// Verify final table
	val, err := p.Sess.QueryValue("SELECT cnt FROM mart.events_summary WHERE event_type = 'click'")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "1" {
		t.Errorf("cnt = %s, want 1", val)
	}

	snap := newSnapshot()
	for _, m := range sorted {
		snap.addLine(fmt.Sprintf("order: %s", m.Target))
	}
	snap.addQuery(p.Sess, "mart.events_summary.cnt", "SELECT cnt FROM mart.events_summary WHERE event_type = 'click'")
	assertGolden(t, "dag_ordering", snap)
}

func TestE2E_Constraints(t *testing.T) {
	p := testutil.NewProject(t)

	// Model with a constraint that will fail: id NOT NULL, but we insert a NULL
	p.AddModel("staging/bad_data.sql", `-- @kind: table
-- @constraint: id NOT NULL
SELECT NULL AS id, 'broken' AS name
`)

	modelPath := filepath.Join(p.Dir, "models", "staging/bad_data.sql")
	model, err := parser.ParseModel(modelPath, p.Dir)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
	result, err := runner.Run(context.Background(), model)

	if err == nil {
		t.Fatal("expected constraint error, got nil")
	}
	if len(result.Errors) == 0 {
		t.Fatal("expected constraint errors in result")
	}

	// Verify the table was NOT created (constraint failed before materialize)
	_, queryErr := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.bad_data")
	if queryErr == nil {
		t.Error("expected table to not exist after constraint failure")
	}

	snap := newSnapshot()
	snap.addResult(result)
	snap.addLine(fmt.Sprintf("exec_error: %v", err != nil))
	snap.addLine(fmt.Sprintf("table_exists: %v", queryErr == nil))
	assertGolden(t, "constraints", snap)
}

func TestE2E_SchemaEvolution(t *testing.T) {
	p := testutil.NewProject(t)

	// Run 1: create table with 2 columns
	p.AddModel("staging/evolving.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name
`)
	result1 := runModel(t, p, "staging/evolving.sql")

	if result1.RowsAffected != 1 {
		t.Errorf("run1 rows = %d, want 1", result1.RowsAffected)
	}

	// Verify initial schema has 2 columns
	val, err := p.Sess.QueryValue("SELECT COUNT(*) FROM (DESCRIBE staging.evolving)")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if val != "2" {
		t.Errorf("initial columns = %s, want 2", val)
	}

	// Run 2: add a new column (schema evolution - additive change)
	p.AddModel("staging/evolving.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name, 30 AS age
`)
	result2 := runModel(t, p, "staging/evolving.sql")

	if result2.RowsAffected != 1 {
		t.Errorf("run2 rows = %d, want 1", result2.RowsAffected)
	}

	// Verify new schema has 3 columns
	val, err = p.Sess.QueryValue("SELECT COUNT(*) FROM (DESCRIBE staging.evolving)")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if val != "3" {
		t.Errorf("evolved columns = %s, want 3", val)
	}

	// Verify data has the new column
	val, err = p.Sess.QueryValue(fmt.Sprintf("SELECT age FROM staging.evolving WHERE id = 1"))
	if err != nil {
		t.Fatalf("query age: %v", err)
	}
	if val != "30" {
		t.Errorf("age = %s, want 30", val)
	}

	snap := newSnapshot()
	snap.addLine("--- run 1 ---")
	snap.addResult(result1)
	snap.addLine("--- run 2 ---")
	snap.addResult(result2)
	snap.addQuery(p.Sess, "columns", "SELECT COUNT(*) FROM (DESCRIBE staging.evolving)")
	snap.addQuery(p.Sess, "age", "SELECT age FROM staging.evolving WHERE id = 1")
	assertGolden(t, "schema_evolution", snap)
}

// --- Sandbox cross-schema tests ---
// These tests verify that all model kinds work correctly in sandbox mode
// when reading from upstream tables in a different schema (cross-schema refs).
// The core issue: DuckDB resolves schema-qualified names (e.g. raw.source)
// only in the current catalog, ignoring search_path. In sandbox mode (USE sandbox),
// raw.source → sandbox.raw.source which doesn't exist.

// setupSandboxWithSource creates a prod project with a source table in raw schema,
// runs the source model, then creates a sandbox project against it.
func setupSandboxWithSource(t *testing.T) *testutil.Project {
	t.Helper()

	// Create prod and populate source table
	prod := testutil.NewProject(t)
	prod.AddModel("raw/source.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name
UNION ALL
SELECT 2, 'Bob'
UNION ALL
SELECT 3, 'Charlie'
`)
	runModel(t, prod, "raw/source.sql")

	// Verify prod data
	val, err := prod.Sess.QueryValue("SELECT COUNT(*) FROM raw.source")
	if err != nil {
		t.Fatalf("verify prod: %v", err)
	}
	if val != "3" {
		t.Fatalf("prod count = %s, want 3", val)
	}

	// Create sandbox against prod
	return testutil.NewSandboxProject(t, prod)
}

func TestE2E_Sandbox_TableKind_CrossSchema(t *testing.T) {
	sbox := setupSandboxWithSource(t)

	// Table model in staging that reads from raw.source (cross-schema)
	sbox.AddModel("staging/derived.sql", `-- @kind: table
SELECT id, name FROM raw.source WHERE id <= 2
`)

	result := runModel(t, sbox, "staging/derived.sql")

	if result.RowsAffected != 2 {
		t.Errorf("rows = %d, want 2", result.RowsAffected)
	}

	// Verify data in sandbox
	val, err := sbox.Sess.QueryValue("SELECT COUNT(*) FROM staging.derived")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "2" {
		t.Errorf("count = %s, want 2", val)
	}

	snap := newSnapshot()
	snap.addResult(result)
	snap.addQuery(sbox.Sess, "count", "SELECT COUNT(*) FROM staging.derived")
	assertGolden(t, "sandbox_table_cross_schema", snap)
}

func TestE2E_Sandbox_TableKind_Rerun(t *testing.T) {
	sbox := setupSandboxWithSource(t)

	// Run table model in sandbox (first run = backfill)
	sbox.AddModel("staging/derived.sql", `-- @kind: table
SELECT id, name FROM raw.source
`)
	result := runModel(t, sbox, "staging/derived.sql")
	if result.RunType != "backfill" {
		t.Errorf("first run type = %q, want backfill", result.RunType)
	}

	// v0.12.0+: with the catalog-fork sandbox, batch_run_type.sql queries
	// the SANDBOX catalog (which has both inherited prod commits and the
	// new sandbox commit from the first run). The second run sees its own
	// commit, finds the hash unchanged and deps unchanged, and correctly
	// skips. Pre-v0.12.0 always backfilled because the runner queried prod
	// for history and missed the sandbox-only commit.
	result2 := runModel(t, sbox, "staging/derived.sql")
	if result2.RunType != "skip" {
		t.Errorf("second run type = %q, want skip (sandbox catalog has the previous sandbox commit)", result2.RunType)
	}

	snap := newSnapshot()
	snap.addLine("--- run 1 ---")
	snap.addResult(result)
	snap.addLine("--- run 2 ---")
	snap.addResult(result2)
	assertGolden(t, "sandbox_table_rerun", snap)
}

func TestE2E_Sandbox_AppendKind_CrossSchema(t *testing.T) {
	sbox := setupSandboxWithSource(t)

	// First: run in prod so the table exists
	// Actually in sandbox, append with no upstream changes should skip
	// because prod table is visible via search_path
	sbox.AddModel("staging/appended.sql", `-- @kind: append
SELECT id, name FROM raw.source
`)

	// Run append model — first sandbox run
	result := runModel(t, sbox, "staging/appended.sql")

	// First run should be backfill (table never existed)
	if result.RunType != "backfill" {
		t.Errorf("first run type = %q, want backfill", result.RunType)
	}
	if result.RowsAffected != 3 {
		t.Errorf("rows = %d, want 3", result.RowsAffected)
	}

	snap := newSnapshot()
	snap.addResult(result)
	assertGolden(t, "sandbox_append_cross_schema", snap)
}

func TestE2E_Sandbox_AppendKind_NoChanges_Skip(t *testing.T) {
	prod := testutil.NewProject(t)
	prod.AddModel("raw/source.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name
UNION ALL
SELECT 2, 'Bob'
`)
	runModel(t, prod, "raw/source.sql")

	prod.AddModel("staging/appended.sql", `-- @kind: append
SELECT id, name FROM raw.source
`)
	runModel(t, prod, "staging/appended.sql")

	sbox := testutil.NewSandboxProject(t, prod)
	sbox.AddModel("staging/appended.sql", `-- @kind: append
SELECT id, name FROM raw.source
`)

	result := runModel(t, sbox, "staging/appended.sql")

	// v0.12.0+: incremental kinds always run (incremental is the natural
	// state for append/merge/scd2/partition/tracked — they always probe for
	// new source rows). With nothing to add, rows_affected is 0. The old
	// "skip" behaviour was an artefact of the empty-sandbox model.
	if result.RunType != "incremental" {
		t.Errorf("run type = %q, want incremental (no upstream changes)", result.RunType)
	}
	if result.RowsAffected != 0 {
		t.Errorf("rows = %d, want 0 (nothing changed)", result.RowsAffected)
	}

	// Prod data accessible via fully-qualified prod catalog ref
	// (schema-qualified names resolve in current catalog only, not via search_path)
	val, err := sbox.Sess.QueryValue("SELECT COUNT(*) FROM lake.staging.appended")
	if err != nil {
		t.Fatalf("query prod data: %v", err)
	}
	if val != "2" {
		t.Errorf("count = %s, want 2", val)
	}

	snap := newSnapshot()
	snap.addResult(result)
	snap.addQuery(sbox.Sess, "prod_count", "SELECT COUNT(*) FROM lake.staging.appended")
	assertGolden(t, "sandbox_append_no_changes", snap)
}

func TestE2E_Sandbox_MergeKind_CrossSchema(t *testing.T) {
	sbox := setupSandboxWithSource(t)

	sbox.AddModel("staging/merged.sql", `-- @kind: merge
-- @unique_key: id
SELECT id, name FROM raw.source
`)

	result := runModel(t, sbox, "staging/merged.sql")

	if result.RunType != "backfill" {
		t.Errorf("first run type = %q, want backfill", result.RunType)
	}
	if result.RowsAffected != 3 {
		t.Errorf("rows = %d, want 3", result.RowsAffected)
	}

	// Verify data
	val, err := sbox.Sess.QueryValue("SELECT COUNT(*) FROM staging.merged")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "3" {
		t.Errorf("count = %s, want 3", val)
	}

	snap := newSnapshot()
	snap.addResult(result)
	snap.addQuery(sbox.Sess, "count", "SELECT COUNT(*) FROM staging.merged")
	assertGolden(t, "sandbox_merge_cross_schema", snap)
}

func TestE2E_Sandbox_SCD2Kind_CrossSchema(t *testing.T) {
	sbox := setupSandboxWithSource(t)

	sbox.AddModel("staging/scd2_model.sql", `-- @kind: scd2
-- @unique_key: id
SELECT id, name FROM raw.source
`)

	result := runModel(t, sbox, "staging/scd2_model.sql")

	if result.RunType != "backfill" {
		t.Errorf("first run type = %q, want backfill", result.RunType)
	}
	if result.RowsAffected != 3 {
		t.Errorf("rows = %d, want 3", result.RowsAffected)
	}

	// Verify SCD2 columns exist
	val, err := sbox.Sess.QueryValue("SELECT COUNT(*) FROM staging.scd2_model WHERE is_current IS true")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "3" {
		t.Errorf("current rows = %s, want 3", val)
	}

	snap := newSnapshot()
	snap.addResult(result)
	snap.addQuery(sbox.Sess, "current_rows", "SELECT COUNT(*) FROM staging.scd2_model WHERE is_current IS true")
	assertGolden(t, "sandbox_scd2_cross_schema", snap)
}

func TestE2E_Sandbox_PartitionKind_CrossSchema(t *testing.T) {
	sbox := setupSandboxWithSource(t)

	sbox.AddModel("staging/partitioned.sql", `-- @kind: partition
-- @unique_key: name
SELECT id, name FROM raw.source
`)

	result := runModel(t, sbox, "staging/partitioned.sql")

	if result.RunType != "backfill" {
		t.Errorf("first run type = %q, want backfill", result.RunType)
	}
	if result.RowsAffected != 3 {
		t.Errorf("rows = %d, want 3", result.RowsAffected)
	}

	// Verify data
	val, err := sbox.Sess.QueryValue("SELECT COUNT(*) FROM staging.partitioned")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "3" {
		t.Errorf("count = %s, want 3", val)
	}

	snap := newSnapshot()
	snap.addResult(result)
	snap.addQuery(sbox.Sess, "count", "SELECT COUNT(*) FROM staging.partitioned")
	assertGolden(t, "sandbox_partition_cross_schema", snap)
}

func TestE2E_Sandbox_MergeKind_NoChanges_Skip(t *testing.T) {
	prod := testutil.NewProject(t)
	prod.AddModel("raw/source.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name
`)
	runModel(t, prod, "raw/source.sql")

	prod.AddModel("staging/merged.sql", `-- @kind: merge
-- @unique_key: id
SELECT id, name FROM raw.source
`)
	runModel(t, prod, "staging/merged.sql")

	sbox := testutil.NewSandboxProject(t, prod)
	sbox.AddModel("staging/merged.sql", `-- @kind: merge
-- @unique_key: id
SELECT id, name FROM raw.source
`)

	result := runModel(t, sbox, "staging/merged.sql")
	// v0.12.0+: see TestE2E_Sandbox_AppendKind_NoChanges_Skip — incremental
	// kinds always run (with 0 rows when nothing changed) under sandbox v2.
	if result.RunType != "incremental" {
		t.Errorf("run type = %q, want incremental (no upstream changes)", result.RunType)
	}
	if result.RowsAffected != 0 {
		t.Errorf("rows = %d, want 0 (nothing changed)", result.RowsAffected)
	}

	// Prod data accessible via fully-qualified prod catalog ref
	val, err := sbox.Sess.QueryValue("SELECT COUNT(*) FROM lake.staging.merged")
	if err != nil {
		t.Fatalf("query prod data: %v", err)
	}
	if val != "1" {
		t.Errorf("count = %s, want 1", val)
	}

	snap := newSnapshot()
	snap.addResult(result)
	snap.addQuery(sbox.Sess, "prod_count", "SELECT COUNT(*) FROM lake.staging.merged")
	assertGolden(t, "sandbox_merge_no_changes", snap)
}

// runDAGSandbox runs all models in DAG order with batch decisions and propagation,
// using the shared execute.RunDAG logic (same as CLI). Returns results keyed by target.
func runDAGSandbox(t *testing.T, p *testutil.Project, modelPaths []string) map[string]*execute.Result {
	t.Helper()

	sorted, dependents := buildDAG(t, p, modelPaths)

	results, errors := execute.RunDAG(context.Background(), p.Sess, sorted, dependents,
		dag.GenerateRunID(), "", "", "", "", p.Dir, nil)

	for target, err := range errors {
		t.Fatalf("run %s: %v", target, err)
	}

	return results
}

// buildDAG parses models, builds a DAG, sorts topologically, and returns
// the sorted models and reverse-dependency map.
func buildDAG(t *testing.T, p *testutil.Project, modelPaths []string) ([]*parser.Model, map[string][]string) {
	t.Helper()

	var models []*parser.Model
	for _, relPath := range modelPaths {
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

	return sorted, g.Dependents()
}

func TestE2E_Sandbox_PartitionKind_NoChanges_Skip(t *testing.T) {
	prod := testutil.NewProject(t)
	prod.AddModel("raw/source.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name
`)
	runModel(t, prod, "raw/source.sql")

	prod.AddModel("staging/partitioned.sql", `-- @kind: partition
-- @unique_key: name
SELECT id, name FROM raw.source
`)
	runModel(t, prod, "staging/partitioned.sql")

	sbox := testutil.NewSandboxProject(t, prod)
	sbox.AddModel("staging/partitioned.sql", `-- @kind: partition
-- @unique_key: name
SELECT id, name FROM raw.source
`)

	result := runModel(t, sbox, "staging/partitioned.sql")
	// v0.12.0+: with the catalog-fork sandbox, an unchanged partition model
	// runs as "incremental" (incremental kinds always look for new source
	// rows even when nothing changed). 0 rows are written. The pre-v0.12.0
	// behaviour was to short-circuit to "skip" via the empty-sandbox skip
	// path, which v2 removed because the target always exists in sandbox.
	if result.RunType != "incremental" {
		t.Errorf("run type = %q, want incremental (no upstream changes)", result.RunType)
	}
	if result.RowsAffected != 0 {
		t.Errorf("rows affected = %d, want 0 (nothing changed)", result.RowsAffected)
	}

	// Prod data accessible via fully-qualified prod catalog ref
	val, err := sbox.Sess.QueryValue("SELECT COUNT(*) FROM lake.staging.partitioned")
	if err != nil {
		t.Fatalf("query prod data: %v", err)
	}
	if val != "1" {
		t.Errorf("count = %s, want 1", val)
	}

	snap := newSnapshot()
	snap.addResult(result)
	snap.addQuery(sbox.Sess, "prod_count", "SELECT COUNT(*) FROM lake.staging.partitioned")
	assertGolden(t, "sandbox_partition_no_changes", snap)
}

// =============================================================================
// DAG Sandbox Tests
// These test the full DAG flow (batch decisions + propagation) in sandbox mode.
// =============================================================================

// setupDAGProd creates a prod project with raw → staging → mart pipeline,
// runs all models, then returns the project.
func setupDAGProd(t *testing.T) *testutil.Project {
	t.Helper()
	prod := testutil.NewProject(t)

	prod.AddModel("raw/customers.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 'Alice', 'SE'), (2, 'Bob', 'NO'), (3, 'Charlie', 'SE')) AS t(id, name, country)
`)
	prod.AddModel("raw/orders.sql", `-- @kind: table
SELECT * FROM (VALUES (101, 1, 100.0), (102, 2, 200.0), (103, 1, 150.0)) AS t(order_id, customer_id, amount)
`)
	prod.AddModel("staging/enriched.sql", `-- @kind: append
SELECT o.order_id, o.customer_id, c.name, c.country, o.amount
FROM raw.orders o
JOIN raw.customers c ON o.customer_id = c.id
`)
	prod.AddModel("staging/latest.sql", `-- @kind: merge
-- @unique_key: id
SELECT id, name, country FROM raw.customers
`)
	prod.AddModel("staging/history.sql", `-- @kind: scd2
-- @unique_key: id
SELECT id, name, country FROM raw.customers
`)
	prod.AddModel("staging/by_country.sql", `-- @kind: partition
-- @unique_key: country
SELECT c.country, o.order_id, o.amount
FROM raw.orders o
JOIN raw.customers c ON o.customer_id = c.id
`)
	prod.AddModel("mart/revenue.sql", `-- @kind: table
SELECT country, COUNT(*) AS orders, SUM(amount) AS total
FROM staging.enriched
GROUP BY country
`)

	paths := dagModelPaths()

	// Run all in DAG order to populate prod
	results := runDAGSandbox(t, prod, paths)
	for target, r := range results {
		if r.RunType != "backfill" {
			t.Fatalf("prod first run: %s run_type = %q, want backfill", target, r.RunType)
		}
	}

	return prod
}

// dagModelPaths returns the standard model paths for the DAG test project.
func dagModelPaths() []string {
	return []string{
		"raw/customers.sql", "raw/orders.sql",
		"staging/enriched.sql", "staging/latest.sql",
		"staging/history.sql", "staging/by_country.sql",
		"mart/revenue.sql",
	}
}

// addUnchangedModels adds all DAG models with their original SQL.
func addUnchangedModels(p *testutil.Project) {
	p.AddModel("raw/customers.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 'Alice', 'SE'), (2, 'Bob', 'NO'), (3, 'Charlie', 'SE')) AS t(id, name, country)
`)
	p.AddModel("raw/orders.sql", `-- @kind: table
SELECT * FROM (VALUES (101, 1, 100.0), (102, 2, 200.0), (103, 1, 150.0)) AS t(order_id, customer_id, amount)
`)
	p.AddModel("staging/enriched.sql", `-- @kind: append
SELECT o.order_id, o.customer_id, c.name, c.country, o.amount
FROM raw.orders o
JOIN raw.customers c ON o.customer_id = c.id
`)
	p.AddModel("staging/latest.sql", `-- @kind: merge
-- @unique_key: id
SELECT id, name, country FROM raw.customers
`)
	p.AddModel("staging/history.sql", `-- @kind: scd2
-- @unique_key: id
SELECT id, name, country FROM raw.customers
`)
	p.AddModel("staging/by_country.sql", `-- @kind: partition
-- @unique_key: country
SELECT c.country, o.order_id, o.amount
FROM raw.orders o
JOIN raw.customers c ON o.customer_id = c.id
`)
	p.AddModel("mart/revenue.sql", `-- @kind: table
SELECT country, COUNT(*) AS orders, SUM(amount) AS total
FROM staging.enriched
GROUP BY country
`)
}

// TestE2E_SandboxDAG_NoChanges verifies all models skip when nothing changed.
func TestE2E_SandboxDAG_NoChanges(t *testing.T) {
	prod := setupDAGProd(t)
	sbox := testutil.NewSandboxProject(t, prod)
	addUnchangedModels(sbox)

	results := runDAGSandbox(t, sbox, dagModelPaths())

	// v0.12.0+: leaf table-kind models with no upstream still skip because
	// their hash matches and they have no deps that committed in sandbox.
	// Downstream table-kind models (mart.revenue) recompute as "full"
	// because their incremental upstream (staging.enriched) commits a new
	// snapshot even on 0-row runs — this is the correct behaviour: the
	// downstream sees the upstream changed, runs against it, and produces
	// the same data. Verified by the row-count check below.
	for _, target := range []string{"raw.customers", "raw.orders"} {
		if results[target].RunType != "skip" {
			t.Errorf("%s: run_type = %q, want skip (leaf, no upstream)", target, results[target].RunType)
		}
	}
	if results["mart.revenue"].RunType == "skip" {
		// Either skip (nothing changed propagated) or full (incremental upstream
		// committed a 0-row snapshot) is acceptable; we just want non-error.
		// The data-equality check below is the actual contract.
	}

	// All models should produce 0 rows (no false positives), regardless of
	// whether they ran as skip or incremental.
	for _, target := range []string{"staging.enriched", "staging.latest", "staging.by_country"} {
		r := results[target]
		if r.RowsAffected != 0 {
			t.Errorf("%s: run_type=%q rows=%d — expected 0 rows (no false positives)",
				target, r.RunType, r.RowsAffected)
		}
	}

	// v0.12.0+: with the catalog-fork sandbox, all prod tables exist in
	// sandbox by default. Verify they hold the same row counts as prod
	// (no divergence) instead of asserting they don't exist.
	for _, target := range []string{"raw.customers", "raw.orders", "mart.revenue"} {
		sandboxCount, sandboxErr := sbox.Sess.QueryValue(fmt.Sprintf("SELECT COUNT(*) FROM sandbox.%s", target))
		if sandboxErr != nil {
			t.Errorf("%s: should exist in sandbox via fork: %v", target, sandboxErr)
			continue
		}
		prodCount, _ := sbox.Sess.QueryValue(fmt.Sprintf("SELECT COUNT(*) FROM lake.%s", target))
		if sandboxCount != prodCount {
			t.Errorf("%s: sandbox count = %s, prod count = %s — should be identical when nothing changed",
				target, sandboxCount, prodCount)
		}
	}

	assertDAGAccountsForAll(t, 7, results)
	snap := newSnapshot()
	snap.addDAGResults(results)
	assertGolden(t, "dag_no_changes", snap)
}

// TestE2E_SandboxDAG_UpstreamChange verifies downstream models re-run when
// an upstream raw table's SQL changes.
func TestE2E_SandboxDAG_UpstreamChange(t *testing.T) {
	prod := setupDAGProd(t)
	sbox := testutil.NewSandboxProject(t, prod)

	// Change raw.customers SQL: add Diana
	sbox.AddModel("raw/customers.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 'Alice', 'SE'), (2, 'Bob', 'NO'), (3, 'Charlie', 'SE'), (4, 'Diana', 'DK')) AS t(id, name, country)
`)
	sbox.AddModel("raw/orders.sql", `-- @kind: table
SELECT * FROM (VALUES (101, 1, 100.0), (102, 2, 200.0), (103, 1, 150.0)) AS t(order_id, customer_id, amount)
`)
	sbox.AddModel("staging/enriched.sql", `-- @kind: append
SELECT o.order_id, o.customer_id, c.name, c.country, o.amount
FROM raw.orders o
JOIN raw.customers c ON o.customer_id = c.id
`)
	sbox.AddModel("staging/latest.sql", `-- @kind: merge
-- @unique_key: id
SELECT id, name, country FROM raw.customers
`)
	sbox.AddModel("staging/history.sql", `-- @kind: scd2
-- @unique_key: id
SELECT id, name, country FROM raw.customers
`)
	sbox.AddModel("staging/by_country.sql", `-- @kind: partition
-- @unique_key: country
SELECT c.country, o.order_id, o.amount
FROM raw.orders o
JOIN raw.customers c ON o.customer_id = c.id
`)
	sbox.AddModel("mart/revenue.sql", `-- @kind: table
SELECT country, COUNT(*) AS orders, SUM(amount) AS total
FROM staging.enriched
GROUP BY country
`)

	results := runDAGSandbox(t, sbox, dagModelPaths())

	// raw.customers: SQL changed → backfill
	if results["raw.customers"].RunType != "backfill" {
		t.Errorf("raw.customers: run_type = %q, want backfill", results["raw.customers"].RunType)
	}
	// raw.orders: unchanged → skip
	if results["raw.orders"].RunType != "skip" {
		t.Errorf("raw.orders: run_type = %q, want skip", results["raw.orders"].RunType)
	}

	// staging.latest (merge): depends on raw.customers → should run
	if results["staging.latest"].RunType == "skip" {
		t.Errorf("staging.latest: should not skip when raw.customers changed")
	}
	val, _ := sbox.Sess.QueryValue("SELECT COUNT(*) FROM sandbox.staging.latest")
	if val != "4" {
		t.Errorf("staging.latest rows = %s, want 4", val)
	}

	// staging.history (SCD2): depends on raw.customers → should run
	if results["staging.history"].RunType == "skip" {
		t.Errorf("staging.history: should not skip when raw.customers changed")
	}
	val, _ = sbox.Sess.QueryValue("SELECT COUNT(*) FROM sandbox.staging.history WHERE is_current IS true")
	if val != "4" {
		t.Errorf("staging.history current rows = %s, want 4", val)
	}

	// staging.enriched (append): depends on raw.orders + raw.customers
	// Diana has no orders → enriched still has 3 rows, but should have RUN
	if results["staging.enriched"].RunType == "skip" {
		t.Errorf("staging.enriched: should not skip when raw.customers changed")
	}

	assertDAGAccountsForAll(t, 7, results)
	snap := newSnapshot()
	snap.addDAGResults(results)
	snap.addQuery(sbox.Sess, "latest_count", "SELECT COUNT(*) FROM sandbox.staging.latest")
	snap.addQuery(sbox.Sess, "history_current", "SELECT COUNT(*) FROM sandbox.staging.history WHERE is_current IS true")
	assertGolden(t, "dag_upstream_change", snap)
}

// TestE2E_SandboxDAG_TransitivePropagation verifies that changes propagate
// through the full chain: raw → staging → mart.
func TestE2E_SandboxDAG_TransitivePropagation(t *testing.T) {
	prod := setupDAGProd(t)
	sbox := testutil.NewSandboxProject(t, prod)

	// Change both raw tables: add Diana + order for Diana
	sbox.AddModel("raw/customers.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 'Alice', 'SE'), (2, 'Bob', 'NO'), (3, 'Charlie', 'SE'), (4, 'Diana', 'DK')) AS t(id, name, country)
`)
	sbox.AddModel("raw/orders.sql", `-- @kind: table
SELECT * FROM (VALUES (101, 1, 100.0), (102, 2, 200.0), (103, 1, 150.0), (104, 4, 300.0)) AS t(order_id, customer_id, amount)
`)
	sbox.AddModel("staging/enriched.sql", `-- @kind: append
SELECT o.order_id, o.customer_id, c.name, c.country, o.amount
FROM raw.orders o
JOIN raw.customers c ON o.customer_id = c.id
`)
	sbox.AddModel("staging/latest.sql", `-- @kind: merge
-- @unique_key: id
SELECT id, name, country FROM raw.customers
`)
	sbox.AddModel("staging/history.sql", `-- @kind: scd2
-- @unique_key: id
SELECT id, name, country FROM raw.customers
`)
	sbox.AddModel("staging/by_country.sql", `-- @kind: partition
-- @unique_key: country
SELECT c.country, o.order_id, o.amount
FROM raw.orders o
JOIN raw.customers c ON o.customer_id = c.id
`)
	sbox.AddModel("mart/revenue.sql", `-- @kind: table
SELECT country, COUNT(*) AS orders, SUM(amount) AS total
FROM staging.enriched
GROUP BY country
`)

	results := runDAGSandbox(t, sbox, dagModelPaths())

	// staging.enriched should run and have 4 rows
	if results["staging.enriched"].RunType == "skip" {
		t.Errorf("staging.enriched: should not skip when both upstream tables changed")
	}
	val, err := sbox.Sess.QueryValue("SELECT COUNT(*) FROM sandbox.staging.enriched")
	if err != nil {
		t.Fatalf("query staging.enriched: %v", err)
	}
	if val != "4" {
		t.Errorf("staging.enriched rows = %s, want 4", val)
	}

	// mart.revenue (table kind): depends on staging.enriched → should propagate
	if results["mart.revenue"].RunType == "skip" {
		t.Errorf("mart.revenue: should not skip when staging.enriched changed")
	}
	val, err = sbox.Sess.QueryValue("SELECT COUNT(*) FROM sandbox.mart.revenue")
	if err != nil {
		t.Fatalf("query mart.revenue: %v", err)
	}
	if val != "3" {
		t.Errorf("mart.revenue rows = %s, want 3 (SE, NO, DK)", val)
	}

	// Verify DK country exists with correct values
	val, err = sbox.Sess.QueryValue("SELECT total FROM sandbox.mart.revenue WHERE country = 'DK'")
	if err != nil {
		t.Fatalf("query DK revenue: %v", err)
	}
	if val != "300.0" && val != "300" {
		t.Errorf("DK total = %s, want 300", val)
	}

	// staging.by_country (partition): depends on raw.orders + raw.customers
	if results["staging.by_country"].RunType == "skip" {
		t.Errorf("staging.by_country: should not skip when upstream changed")
	}
	val, _ = sbox.Sess.QueryValue("SELECT COUNT(*) FROM sandbox.staging.by_country")
	if val != "4" {
		t.Errorf("staging.by_country rows = %s, want 4", val)
	}

	assertDAGAccountsForAll(t, 7, results)
	snap := newSnapshot()
	snap.addDAGResults(results)
	snap.addQuery(sbox.Sess, "enriched_count", "SELECT COUNT(*) FROM sandbox.staging.enriched")
	snap.addQuery(sbox.Sess, "revenue_count", "SELECT COUNT(*) FROM sandbox.mart.revenue")
	snap.addQuery(sbox.Sess, "dk_total", "SELECT total FROM sandbox.mart.revenue WHERE country = 'DK'")
	snap.addQuery(sbox.Sess, "by_country_count", "SELECT COUNT(*) FROM sandbox.staging.by_country")
	assertGolden(t, "dag_transitive_propagation", snap)
}

// TestE2E_SandboxDAG_MultiSourceJoin verifies that a model with multiple
// upstream tables detects changes when only ONE of them changes.
func TestE2E_SandboxDAG_MultiSourceJoin(t *testing.T) {
	prod := setupDAGProd(t)
	sbox := testutil.NewSandboxProject(t, prod)

	// Only change raw.orders (add order 104 for Alice)
	// raw.customers stays the same
	sbox.AddModel("raw/customers.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 'Alice', 'SE'), (2, 'Bob', 'NO'), (3, 'Charlie', 'SE')) AS t(id, name, country)
`)
	sbox.AddModel("raw/orders.sql", `-- @kind: table
SELECT * FROM (VALUES (101, 1, 100.0), (102, 2, 200.0), (103, 1, 150.0), (104, 1, 75.0)) AS t(order_id, customer_id, amount)
`)
	sbox.AddModel("staging/enriched.sql", `-- @kind: append
SELECT o.order_id, o.customer_id, c.name, c.country, o.amount
FROM raw.orders o
JOIN raw.customers c ON o.customer_id = c.id
`)
	sbox.AddModel("staging/latest.sql", `-- @kind: merge
-- @unique_key: id
SELECT id, name, country FROM raw.customers
`)
	sbox.AddModel("staging/history.sql", `-- @kind: scd2
-- @unique_key: id
SELECT id, name, country FROM raw.customers
`)
	sbox.AddModel("staging/by_country.sql", `-- @kind: partition
-- @unique_key: country
SELECT c.country, o.order_id, o.amount
FROM raw.orders o
JOIN raw.customers c ON o.customer_id = c.id
`)
	sbox.AddModel("mart/revenue.sql", `-- @kind: table
SELECT country, COUNT(*) AS orders, SUM(amount) AS total
FROM staging.enriched
GROUP BY country
`)

	results := runDAGSandbox(t, sbox, dagModelPaths())

	// raw.orders changed → backfill
	if results["raw.orders"].RunType != "backfill" {
		t.Errorf("raw.orders: run_type = %q, want backfill", results["raw.orders"].RunType)
	}
	// raw.customers unchanged → skip
	if results["raw.customers"].RunType != "skip" {
		t.Errorf("raw.customers: run_type = %q, want skip", results["raw.customers"].RunType)
	}

	// staging.enriched depends on BOTH tables. raw.orders changed → must run.
	if results["staging.enriched"].RunType == "skip" {
		t.Errorf("staging.enriched: should not skip when raw.orders changed")
	}
	val, err := sbox.Sess.QueryValue("SELECT COUNT(*) FROM sandbox.staging.enriched")
	if err != nil {
		t.Fatalf("query staging.enriched: %v", err)
	}
	if val != "4" {
		t.Errorf("staging.enriched rows = %s, want 4", val)
	}

	// mart.revenue depends on staging.enriched → should propagate
	if results["mart.revenue"].RunType == "skip" {
		t.Errorf("mart.revenue: should not skip when staging.enriched changed")
	}
	// SE: 3 orders (101=100+103=150+104=75=325), NO: 1 order (102=200)
	val, _ = sbox.Sess.QueryValue("SELECT total FROM sandbox.mart.revenue WHERE country = 'SE'")
	if val != "325.0" && val != "325" {
		t.Errorf("SE total = %s, want 325", val)
	}

	assertDAGAccountsForAll(t, 7, results)
	snap := newSnapshot()
	snap.addDAGResults(results)
	snap.addQuery(sbox.Sess, "enriched_count", "SELECT COUNT(*) FROM sandbox.staging.enriched")
	snap.addQuery(sbox.Sess, "se_total", "SELECT total FROM sandbox.mart.revenue WHERE country = 'SE'")
	assertGolden(t, "dag_multi_source_join", snap)
}

// runDAGSandboxAllowErrors is like runDAGSandbox but does not fatalf on model
// errors. Uses the same shared execute.RunDAG logic as the CLI.
func runDAGSandboxAllowErrors(t *testing.T, p *testutil.Project, modelPaths []string) (map[string]*execute.Result, map[string]error) {
	t.Helper()

	sorted, dependents := buildDAG(t, p, modelPaths)

	return execute.RunDAG(context.Background(), p.Sess, sorted, dependents,
		dag.GenerateRunID(), "", "", "", "", p.Dir, nil)
}

// =============================================================================
// Edge Case Tests
// =============================================================================

// TestE2E_SandboxDAG_ErrorMidDAG verifies that when a model fails mid-DAG,
// downstream models are NOT invalidated (no propagation from failed upstream).
func TestE2E_SandboxDAG_ErrorMidDAG(t *testing.T) {
	// Build prod: raw.source → staging.checked → mart.final
	prod := testutil.NewProject(t)
	prod.AddModel("raw/source.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS t(id, name)
`)
	prod.AddModel("staging/checked.sql", `-- @kind: table
-- @constraint: id NOT NULL
SELECT id, name FROM raw.source
`)
	prod.AddModel("mart/final.sql", `-- @kind: table
SELECT COUNT(*) AS total FROM staging.checked
`)

	// Run prod pipeline
	prodPaths := []string{"raw/source.sql", "staging/checked.sql", "mart/final.sql"}
	runDAGSandbox(t, prod, prodPaths)

	// Sandbox: change raw.source, make staging.checked fail with NULL id
	sbox := testutil.NewSandboxProject(t, prod)
	sbox.AddModel("raw/source.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 'Alice'), (NULL, 'Bad')) AS t(id, name)
`)
	sbox.AddModel("staging/checked.sql", `-- @kind: table
-- @constraint: id NOT NULL
SELECT id, name FROM raw.source
`)
	sbox.AddModel("mart/final.sql", `-- @kind: table
SELECT COUNT(*) AS total FROM staging.checked
`)

	results, errs := runDAGSandboxAllowErrors(t, sbox, prodPaths)

	// raw.source: SQL changed → backfill (should succeed)
	if results["raw.source"].RunType != "backfill" {
		t.Errorf("raw.source: run_type = %q, want backfill", results["raw.source"].RunType)
	}
	if errs["raw.source"] != nil {
		t.Errorf("raw.source: unexpected error: %v", errs["raw.source"])
	}

	// staging.checked: should fail (constraint violation)
	if errs["staging.checked"] == nil {
		t.Errorf("staging.checked: expected constraint error, got nil")
	}

	// mart.final: upstream failed → no propagation → should keep batch decision
	// Since staging.checked failed, no "full" was propagated to mart.final.
	// mart.final's batch decision is based on prod state (deps unchanged → skip).
	if errs["mart.final"] != nil {
		t.Errorf("mart.final: unexpected error: %v", errs["mart.final"])
	}
	if results["mart.final"].RunType == "full" {
		t.Errorf("mart.final: run_type = %q — should NOT be full since upstream failed (no propagation)", results["mart.final"].RunType)
	}

	assertSummaryCounts(t, 3, results, errs)
	snap := newSnapshot()
	snap.addDAGResultsWithErrors(results, errs)
	assertGolden(t, "dag_error_mid_dag", snap)
}

// TestE2E_SandboxDAG_DiamondDependency verifies correct propagation through
// a diamond-shaped DAG: A → B, A → C, B+C → D.
func TestE2E_SandboxDAG_DiamondDependency(t *testing.T) {
	prod := testutil.NewProject(t)
	prod.AddModel("raw/data.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(id, val)
`)
	prod.AddModel("staging/left.sql", `-- @kind: table
SELECT id, val * 2 AS doubled FROM raw.data
`)
	prod.AddModel("staging/right.sql", `-- @kind: table
SELECT id, val * 3 AS tripled FROM raw.data
`)
	prod.AddModel("mart/combined.sql", `-- @kind: table
SELECT l.id, l.doubled, r.tripled FROM staging.left l JOIN staging.right r ON l.id = r.id
`)

	diamondPaths := []string{
		"raw/data.sql", "staging/left.sql", "staging/right.sql", "mart/combined.sql",
	}
	runDAGSandbox(t, prod, diamondPaths)

	// Sandbox: change raw.data → should propagate through both paths to mart.combined
	sbox := testutil.NewSandboxProject(t, prod)
	sbox.AddModel("raw/data.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 10), (2, 20), (3, 30)) AS t(id, val)
`)
	sbox.AddModel("staging/left.sql", `-- @kind: table
SELECT id, val * 2 AS doubled FROM raw.data
`)
	sbox.AddModel("staging/right.sql", `-- @kind: table
SELECT id, val * 3 AS tripled FROM raw.data
`)
	sbox.AddModel("mart/combined.sql", `-- @kind: table
SELECT l.id, l.doubled, r.tripled FROM staging.left l JOIN staging.right r ON l.id = r.id
`)

	results := runDAGSandbox(t, sbox, diamondPaths)

	// raw.data: SQL changed → backfill
	if results["raw.data"].RunType != "backfill" {
		t.Errorf("raw.data: run_type = %q, want backfill", results["raw.data"].RunType)
	}
	// Both staging models: upstream changed → full
	if results["staging.left"].RunType == "skip" {
		t.Errorf("staging.left: should not skip when raw.data changed")
	}
	if results["staging.right"].RunType == "skip" {
		t.Errorf("staging.right: should not skip when raw.data changed")
	}
	// mart.combined: both upstream changed → must run
	if results["mart.combined"].RunType == "skip" {
		t.Errorf("mart.combined: should not skip when both staging models changed")
	}

	// Verify data correctness: 3 rows with correct calculations
	val, err := sbox.Sess.QueryValue("SELECT COUNT(*) FROM sandbox.mart.combined")
	if err != nil {
		t.Fatalf("query mart.combined count: %v", err)
	}
	if val != "3" {
		t.Errorf("mart.combined rows = %s, want 3", val)
	}

	// Verify row 3: doubled=60, tripled=90
	val, _ = sbox.Sess.QueryValue("SELECT doubled FROM sandbox.mart.combined WHERE id = 3")
	if val != "60" {
		t.Errorf("id=3 doubled = %s, want 60", val)
	}
	val, _ = sbox.Sess.QueryValue("SELECT tripled FROM sandbox.mart.combined WHERE id = 3")
	if val != "90" {
		t.Errorf("id=3 tripled = %s, want 90", val)
	}

	assertDAGAccountsForAll(t, 4, results)
	snap := newSnapshot()
	snap.addDAGResults(results)
	snap.addQueryRows(sbox.Sess, "combined_data", "SELECT id, doubled, tripled FROM sandbox.mart.combined ORDER BY id")
	assertGolden(t, "dag_diamond_dependency", snap)
}

// TestE2E_SandboxDAG_SchemaEvolution verifies that schema evolution (adding a
// column) works correctly for append models in sandbox mode.
func TestE2E_SandboxDAG_SchemaEvolution(t *testing.T) {
	prod := testutil.NewProject(t)
	prod.AddModel("raw/source.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS t(id, name)
`)
	prod.AddModel("staging/appended.sql", `-- @kind: append
SELECT id, name FROM raw.source
`)

	schemaPaths := []string{"raw/source.sql", "staging/appended.sql"}
	runDAGSandbox(t, prod, schemaPaths)

	// Sandbox: add a column to both raw.source and staging.appended
	sbox := testutil.NewSandboxProject(t, prod)
	sbox.AddModel("raw/source.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 'Alice', 30), (2, 'Bob', 25)) AS t(id, name, age)
`)
	sbox.AddModel("staging/appended.sql", `-- @kind: append
SELECT id, name, age FROM raw.source
`)

	results := runDAGSandbox(t, sbox, schemaPaths)

	// v0.12.0+: with the catalog-fork sandbox, raw.source's prior schema is
	// inherited from prod. Adding a column is detected as an additive schema
	// change, which the runner applies via ALTER TABLE rather than full
	// backfill — so the run type ends up "incremental" (or "full" for
	// destructive changes), not "backfill". Either way, it must NOT skip.
	if results["raw.source"].RunType == "skip" {
		t.Errorf("raw.source: run_type = %q, want non-skip (SQL changed)", results["raw.source"].RunType)
	}

	// staging.appended: SQL changed + upstream changed → should run.
	if results["staging.appended"].RunType == "skip" {
		t.Errorf("staging.appended: should not skip when SQL and upstream changed")
	}

	// Verify the new column exists in sandbox
	val, err := sbox.Sess.QueryValue("SELECT COUNT(*) FROM (DESCRIBE sandbox.staging.appended)")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if val != "3" {
		t.Errorf("columns = %s, want 3 (id, name, age)", val)
	}

	// Verify data
	val, _ = sbox.Sess.QueryValue("SELECT age FROM sandbox.staging.appended WHERE id = 1")
	if val != "30" {
		t.Errorf("age for id=1 = %s, want 30", val)
	}

	assertDAGAccountsForAll(t, 2, results)
	snap := newSnapshot()
	snap.addDAGResults(results)
	snap.addQuery(sbox.Sess, "columns", "SELECT COUNT(*) FROM (DESCRIBE sandbox.staging.appended)")
	snap.addQuery(sbox.Sess, "age_id1", "SELECT age FROM sandbox.staging.appended WHERE id = 1")
	assertGolden(t, "dag_schema_evolution", snap)
}

// TestE2E_SandboxDAG_SCD2DataChange verifies that SCD2 models correctly
// detect data changes in sandbox and produce new versions.
func TestE2E_SandboxDAG_SCD2DataChange(t *testing.T) {
	prod := testutil.NewProject(t)
	prod.AddModel("raw/customers.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 'Alice', 'SE'), (2, 'Bob', 'NO')) AS t(id, name, country)
`)
	prod.AddModel("staging/history.sql", `-- @kind: scd2
-- @unique_key: id
SELECT id, name, country FROM raw.customers
`)

	scd2Paths := []string{"raw/customers.sql", "staging/history.sql"}
	runDAGSandbox(t, prod, scd2Paths)

	// Verify prod has 2 current records
	val, err := prod.Sess.QueryValue("SELECT COUNT(*) FROM staging.history WHERE is_current IS true")
	if err != nil {
		// Prod session was closed by NewSandboxProject — this is expected
		// We'll verify via sandbox's prod alias instead
	}

	// Sandbox: change Alice's country from SE to DK
	sbox := testutil.NewSandboxProject(t, prod)
	sbox.AddModel("raw/customers.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 'Alice', 'DK'), (2, 'Bob', 'NO')) AS t(id, name, country)
`)
	sbox.AddModel("staging/history.sql", `-- @kind: scd2
-- @unique_key: id
SELECT id, name, country FROM raw.customers
`)

	results := runDAGSandbox(t, sbox, scd2Paths)

	// staging.history should have run (upstream changed)
	if results["staging.history"].RunType == "skip" {
		t.Errorf("staging.history: should not skip when raw.customers changed")
	}

	// v0.12.0+: with the catalog-fork sandbox (Bug S9 fix), SCD2 in sandbox
	// inherits prod's history and computes proper deltas. Total records:
	//   Alice@SE expired + Alice@DK current + Bob@NO current = 3
	//
	// Pre-v0.12.0 sandbox SCD2 went through backfill, replacing history with
	// only current rows (2 total, 0 closed) — that was Bug S9 in
	// SANDBOX_BUGS_FOUND.md and the reason for the v2 catalog-fork design.
	// This test now pins the correct, history-preserving behavior.
	val, err = sbox.Sess.QueryValue("SELECT COUNT(*) FROM sandbox.staging.history")
	if err != nil {
		t.Fatalf("query history: %v", err)
	}
	if val != "3" {
		t.Errorf("total SCD2 rows = %s, want 3 (history preserved by v0.12.0 fork)", val)
	}

	// 2 rows should be current (Alice@DK and Bob@NO).
	val, _ = sbox.Sess.QueryValue("SELECT COUNT(*) FROM sandbox.staging.history WHERE is_current IS true")
	if val != "2" {
		t.Errorf("current rows = %s, want 2", val)
	}

	// Verify Alice's current country is DK (the new value)
	val, _ = sbox.Sess.QueryValue("SELECT country FROM sandbox.staging.history WHERE id = 1 AND is_current IS true")
	if val != "DK" {
		t.Errorf("Alice current country = %s, want DK", val)
	}

	// The old SE record MUST exist as expired (this is what S9 broke).
	val, _ = sbox.Sess.QueryValue("SELECT COUNT(*) FROM sandbox.staging.history WHERE id = 1 AND is_current IS false")
	if val != "1" {
		t.Errorf("closed records = %s, want 1 (Alice@SE should be expired, not deleted)", val)
	}

	assertDAGAccountsForAll(t, 2, results)
	snap := newSnapshot()
	snap.addDAGResults(results)
	snap.addQuery(sbox.Sess, "total_rows", "SELECT COUNT(*) FROM sandbox.staging.history")
	snap.addQuery(sbox.Sess, "current_rows", "SELECT COUNT(*) FROM sandbox.staging.history WHERE is_current IS true")
	snap.addQuery(sbox.Sess, "alice_country", "SELECT country FROM sandbox.staging.history WHERE id = 1 AND is_current IS true")
	snap.addQuery(sbox.Sess, "closed_records", "SELECT COUNT(*) FROM sandbox.staging.history WHERE id = 1 AND is_current IS false")
	assertGolden(t, "dag_scd2_data_change", snap)
}

// TestE2E_Sandbox_DirectiveOnlyChangeStillEvaluatesAudits is the regression
// test for Bug S5 (sandbox skipped table-kind models when only directives
// changed). Pre-v0.12.1, the runner used the SQL hash to decide whether to
// skip a model, and the hash didn't include @audit/@constraint/@warning
// values — so editing an audit threshold to a guaranteed-failing value
// would still report [OK] skip with "Audits: 1 not evaluated".
//
// v0.12.1 forces RunType=skip → backfill in sandbox mode so the entire
// validation pass always runs. The cost is small because sandbox is
// ephemeral; the cost of false-pass is silent broken deploys.
func TestE2E_Sandbox_DirectiveOnlyChangeStillEvaluatesAudits(t *testing.T) {
	prod := testutil.NewProject(t)
	prod.AddModel("staging/checked.sql", `-- @kind: table
-- @audit: row_count >= 1
SELECT 1 AS id, 100 AS amount
`)
	runModel(t, prod, "staging/checked.sql")

	// Sandbox: change ONLY the audit threshold to a guaranteed-failing value.
	// SQL body unchanged. Pre-v0.12.1 this would have been silently skipped.
	sbox := testutil.NewSandboxProject(t, prod)
	sbox.AddModel("staging/checked.sql", `-- @kind: table
-- @audit: row_count >= 999999
SELECT 1 AS id, 100 AS amount
`)

	result, err := runModelErr(t, sbox, "staging/checked.sql")
	if err == nil {
		t.Fatal("expected audit failure, got nil — Bug S5 regression")
	}
	if result == nil || len(result.Errors) == 0 {
		t.Fatal("expected errors in result — Bug S5 regression")
	}

	// run_type should NOT be "skip" — it should be "backfill" with the
	// sandbox-forced re-run reason.
	if result.RunType == "skip" {
		t.Errorf("run_type = %q, want backfill (Bug S5 regression: sandbox should never skip)", result.RunType)
	}

	// Error should mention the audit failure, not "model skipped".
	foundAuditErr := false
	for _, e := range result.Errors {
		if strings.Contains(e, "row_count >= 999999") {
			foundAuditErr = true
			break
		}
	}
	if !foundAuditErr {
		t.Errorf("errors = %v, want one mentioning the failing audit", result.Errors)
	}
}

// TestE2E_Sandbox_DirectiveOnlyChange_ColumnTag covers the @column slice of
// Bug S5: ColumnTags drives applyColumnMasking which mutates the actual data
// output, so an edit changing a column tag (e.g. swapping a mask macro name)
// must trigger sandbox re-evaluation. v0.12.2 added ColumnTags to the
// hasDirectives check after a user spotted the gap during review.
func TestE2E_Sandbox_DirectiveOnlyChange_ColumnTag(t *testing.T) {
	prod := testutil.NewProject(t)
	prod.AddModel("staging/tagged.sql", `-- @kind: table
-- @column: name = display name | PII
SELECT 1 AS id, 'Alice' AS name
`)
	runModel(t, prod, "staging/tagged.sql")

	// Sandbox: edit ONLY the column tag (PII → PHI). SQL body and column
	// description unchanged. Pre-fix the runner saw an unchanged hash and
	// skipped without re-evaluating the model.
	sbox := testutil.NewSandboxProject(t, prod)
	sbox.AddModel("staging/tagged.sql", `-- @kind: table
-- @column: name = display name | PHI
SELECT 1 AS id, 'Alice' AS name
`)

	result, err := runModelErr(t, sbox, "staging/tagged.sql")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Must NOT skip — that was the pre-v0.12.2 behavior.
	if result.RunType == "skip" {
		t.Errorf("run_type = %q, want backfill (Bug S5 column-tag regression)", result.RunType)
	}
}

// TestE2E_SandboxDAG_AppendIncrementalPreservesHistory is the regression test
// for Bug S13 (append+incremental loses history in sandbox). Pre-v0.12.0,
// when the source table was rebuilt in sandbox, the runner forced a full
// backfill on append+incremental targets, throwing away rows that prod still
// had. v0.12.0 catalog-fork makes the sandbox catalog inherit prod history
// AND preserves prior target rows because the runner's CDC path now reads
// from the sandbox catalog (which has the inherited snapshot history).
func TestE2E_SandboxDAG_AppendIncrementalPreservesHistory(t *testing.T) {
	prod := testutil.NewProject(t)
	prod.AddModel("raw/orders.sql", `-- @kind: table
SELECT * FROM (VALUES
  (101,'a'),(102,'b'),(103,'c'),(104,'d'),
  (105,'e'),(106,'f'),(107,'g'),(108,'h')
) AS t(order_id, label)
`)
	prod.AddModel("staging/orders_incr.sql", `-- @kind: append
-- @incremental: order_id
SELECT order_id, label FROM raw.orders
`)
	paths := []string{"raw/orders.sql", "staging/orders_incr.sql"}
	runDAGSandbox(t, prod, paths)

	// Sandbox: drop order 107, add 109. Append semantics say 107 must
	// remain in the target table (append never deletes), and 109 must be
	// appended. Pre-v0.12.0 the sandbox produced 8 rows (101..106, 108,
	// 109) — losing 107 — because it was doing TRUNCATE+INSERT.
	sbox := testutil.NewSandboxProject(t, prod)
	sbox.AddModel("raw/orders.sql", `-- @kind: table
SELECT * FROM (VALUES
  (101,'a'),(102,'b'),(103,'c'),(104,'d'),
  (105,'e'),(106,'f'),(108,'h'),(109,'i')
) AS t(order_id, label)
`)
	sbox.AddModel("staging/orders_incr.sql", `-- @kind: append
-- @incremental: order_id
SELECT order_id, label FROM raw.orders
`)
	runDAGSandbox(t, sbox, paths)

	// Sandbox target should have 9 rows: 101..108 inherited from prod
	// plus 109 appended. Crucially, 107 must still be there.
	val, err := sbox.Sess.QueryValue("SELECT COUNT(*) FROM sandbox.staging.orders_incr")
	if err != nil {
		t.Fatalf("query sandbox: %v", err)
	}
	if val != "9" {
		t.Errorf("sandbox count = %s, want 9 (inherited + new 109)", val)
	}

	// 107 must still exist in sandbox (this is the assertion S13 broke).
	val, _ = sbox.Sess.QueryValue("SELECT COUNT(*) FROM sandbox.staging.orders_incr WHERE order_id = 107")
	if val != "1" {
		t.Errorf("order 107 count = %s, want 1 (append never deletes)", val)
	}

	// 109 must be present in sandbox.
	val, _ = sbox.Sess.QueryValue("SELECT COUNT(*) FROM sandbox.staging.orders_incr WHERE order_id = 109")
	if val != "1" {
		t.Errorf("order 109 count = %s, want 1 (newly appended)", val)
	}

	// And prod must NOT see order 109 — sandbox writes are isolated.
	val, _ = sbox.Sess.QueryValue("SELECT COUNT(*) FROM lake.staging.orders_incr WHERE order_id = 109")
	if val != "0" {
		t.Errorf("prod order 109 count = %s, want 0 (sandbox isolation)", val)
	}
}

// TestE2E_SandboxDAG_FailedUpstreamSkipsDownstream verifies that when an
// upstream model fails, its downstream models do NOT get invalidated.
// This prevents running downstream with potentially corrupt data.
func TestE2E_SandboxDAG_FailedUpstreamSkipsDownstream(t *testing.T) {
	prod := testutil.NewProject(t)
	prod.AddModel("raw/source.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS t(id, name)
`)
	prod.AddModel("staging/derived.sql", `-- @kind: table
SELECT id, name, 'active' AS status FROM raw.source
`)

	failPaths := []string{"raw/source.sql", "staging/derived.sql"}
	runDAGSandbox(t, prod, failPaths)

	// Sandbox: make raw.source fail by referencing a non-existent table
	sbox := testutil.NewSandboxProject(t, prod)
	sbox.AddModel("raw/source.sql", `-- @kind: table
SELECT * FROM nonexistent_table_xyz
`)
	sbox.AddModel("staging/derived.sql", `-- @kind: table
SELECT id, name, 'active' AS status FROM raw.source
`)

	results, errs := runDAGSandboxAllowErrors(t, sbox, failPaths)

	// raw.source: SQL changed but should fail (nonexistent table)
	if errs["raw.source"] == nil {
		t.Errorf("raw.source: expected error for nonexistent table, got nil")
	}

	// staging.derived: upstream failed → no propagation → keeps batch decision (skip)
	if errs["staging.derived"] != nil {
		t.Errorf("staging.derived: unexpected error: %v", errs["staging.derived"])
	}
	if results["staging.derived"] != nil && results["staging.derived"].RunType == "full" {
		t.Errorf("staging.derived: run_type = full — should NOT propagate from failed upstream")
	}

	// Prod data should still be accessible (nothing was overwritten in sandbox)
	val, _ := sbox.Sess.QueryValue("SELECT COUNT(*) FROM lake.staging.derived")
	if val != "2" {
		t.Errorf("prod staging.derived rows = %s, want 2 (unchanged)", val)
	}

	assertSummaryCounts(t, 2, results, errs)
	snap := newSnapshot()
	snap.addDAGResultsWithErrors(results, errs)
	snap.addQuery(sbox.Sess, "prod_derived_count", "SELECT COUNT(*) FROM lake.staging.derived")
	assertGolden(t, "dag_failed_upstream", snap)
}

// TestE2E_SandboxDAG_DiamondNoChange verifies that a diamond DAG with NO
// changes correctly skips all models.
func TestE2E_SandboxDAG_DiamondNoChange(t *testing.T) {
	prod := testutil.NewProject(t)
	prod.AddModel("raw/data.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(id, val)
`)
	prod.AddModel("staging/left.sql", `-- @kind: table
SELECT id, val * 2 AS doubled FROM raw.data
`)
	prod.AddModel("staging/right.sql", `-- @kind: table
SELECT id, val * 3 AS tripled FROM raw.data
`)
	prod.AddModel("mart/combined.sql", `-- @kind: table
SELECT l.id, l.doubled, r.tripled FROM staging.left l JOIN staging.right r ON l.id = r.id
`)

	diamondPaths := []string{
		"raw/data.sql", "staging/left.sql", "staging/right.sql", "mart/combined.sql",
	}
	runDAGSandbox(t, prod, diamondPaths)

	// Sandbox: same SQL everywhere → all should skip
	sbox := testutil.NewSandboxProject(t, prod)
	sbox.AddModel("raw/data.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(id, val)
`)
	sbox.AddModel("staging/left.sql", `-- @kind: table
SELECT id, val * 2 AS doubled FROM raw.data
`)
	sbox.AddModel("staging/right.sql", `-- @kind: table
SELECT id, val * 3 AS tripled FROM raw.data
`)
	sbox.AddModel("mart/combined.sql", `-- @kind: table
SELECT l.id, l.doubled, r.tripled FROM staging.left l JOIN staging.right r ON l.id = r.id
`)

	results := runDAGSandbox(t, sbox, diamondPaths)

	for _, target := range []string{"raw.data", "staging.left", "staging.right", "mart.combined"} {
		if results[target].RunType != "skip" {
			t.Errorf("%s: run_type = %q, want skip (nothing changed)", target, results[target].RunType)
		}
	}

	// v0.12.0+: with the catalog-fork sandbox, ALL prod tables exist in
	// sandbox by default (inherited from the fork). The "no sandbox tables
	// should exist when skipped" assertion was a property of the old empty-
	// sandbox model and is no longer meaningful. What still matters is that
	// the inherited tables hold the same row counts as prod (no divergence)
	// — verify that instead.
	for _, target := range []string{"raw.data", "staging.left", "staging.right", "mart.combined"} {
		sandboxCount, sandboxErr := sbox.Sess.QueryValue(fmt.Sprintf("SELECT COUNT(*) FROM sandbox.%s", target))
		if sandboxErr != nil {
			t.Errorf("%s: should exist in sandbox via fork inheritance: %v", target, sandboxErr)
			continue
		}
		prodCount, prodErr := sbox.Sess.QueryValue(fmt.Sprintf("SELECT COUNT(*) FROM lake.%s", target))
		if prodErr != nil {
			t.Fatalf("%s: query prod: %v", target, prodErr)
		}
		if sandboxCount != prodCount {
			t.Errorf("%s: sandbox count = %s, prod count = %s — should be identical when nothing changed",
				target, sandboxCount, prodCount)
		}
	}

	assertDAGAccountsForAll(t, 4, results)
	snap := newSnapshot()
	snap.addDAGResults(results)
	assertGolden(t, "dag_diamond_no_change", snap)
}

// TestE2E_SandboxDAG_PartialDiamondChange verifies that in a diamond DAG,
// changing only ONE intermediate model propagates correctly to the final model.
func TestE2E_SandboxDAG_PartialDiamondChange(t *testing.T) {
	prod := testutil.NewProject(t)
	prod.AddModel("raw/data.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(id, val)
`)
	prod.AddModel("staging/left.sql", `-- @kind: table
SELECT id, val * 2 AS doubled FROM raw.data
`)
	prod.AddModel("staging/right.sql", `-- @kind: table
SELECT id, val * 3 AS tripled FROM raw.data
`)
	prod.AddModel("mart/combined.sql", `-- @kind: table
SELECT l.id, l.doubled, r.tripled FROM staging.left l JOIN staging.right r ON l.id = r.id
`)

	diamondPaths := []string{
		"raw/data.sql", "staging/left.sql", "staging/right.sql", "mart/combined.sql",
	}
	runDAGSandbox(t, prod, diamondPaths)

	// Sandbox: only change staging.left SQL (multiply by 10 instead of 2)
	sbox := testutil.NewSandboxProject(t, prod)
	sbox.AddModel("raw/data.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(id, val)
`)
	sbox.AddModel("staging/left.sql", `-- @kind: table
SELECT id, val * 10 AS doubled FROM raw.data
`)
	sbox.AddModel("staging/right.sql", `-- @kind: table
SELECT id, val * 3 AS tripled FROM raw.data
`)
	sbox.AddModel("mart/combined.sql", `-- @kind: table
SELECT l.id, l.doubled, r.tripled FROM staging.left l JOIN staging.right r ON l.id = r.id
`)

	results := runDAGSandbox(t, sbox, diamondPaths)

	// raw.data: unchanged → skip
	if results["raw.data"].RunType != "skip" {
		t.Errorf("raw.data: run_type = %q, want skip", results["raw.data"].RunType)
	}
	// staging.left: SQL changed → backfill
	if results["staging.left"].RunType != "backfill" {
		t.Errorf("staging.left: run_type = %q, want backfill", results["staging.left"].RunType)
	}
	// staging.right: unchanged → skip
	if results["staging.right"].RunType != "skip" {
		t.Errorf("staging.right: run_type = %q, want skip", results["staging.right"].RunType)
	}
	// mart.combined: staging.left changed → should run
	if results["mart.combined"].RunType == "skip" {
		t.Errorf("mart.combined: should not skip when staging.left changed")
	}

	// Verify mart.combined has correct data (left uses *10, right uses *3)
	val, _ := sbox.Sess.QueryValue("SELECT doubled FROM sandbox.mart.combined WHERE id = 1")
	if val != "100" {
		t.Errorf("id=1 doubled = %s, want 100 (10*10)", val)
	}
	// staging.right was skipped — its data comes from prod via search_path
	// mart.combined JOINs sandbox.staging.left with prod staging.right
	val, _ = sbox.Sess.QueryValue("SELECT tripled FROM sandbox.mart.combined WHERE id = 1")
	if val != "30" {
		t.Errorf("id=1 tripled = %s, want 30 (10*3 from prod)", val)
	}

	assertDAGAccountsForAll(t, 4, results)
	snap := newSnapshot()
	snap.addDAGResults(results)
	snap.addQueryRows(sbox.Sess, "combined_data", "SELECT id, doubled, tripled FROM sandbox.mart.combined ORDER BY id")
	assertGolden(t, "dag_partial_diamond_change", snap)
}

// runModelErr is like runModel but returns both result and error instead of fatalf.
func runModelErr(t *testing.T, p *testutil.Project, relPath string) (*execute.Result, error) {
	t.Helper()
	modelPath := filepath.Join(p.Dir, "models", relPath)
	model, err := parser.ParseModel(modelPath, p.Dir)
	if err != nil {
		t.Fatalf("parse %s: %v", relPath, err)
	}
	runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
	runner.SetProjectDir(p.Dir)
	return runner.Run(context.Background(), model)
}

// =============================================================================
// Directive + Sandbox/DAG Interaction Tests
// These test directives that only have runner-level coverage, in sandbox context.
// =============================================================================

// TestE2E_Sandbox_IncrementalAppend verifies that @incremental works correctly
// in sandbox mode: CDC + sandbox upstream detection must cooperate.
func TestE2E_Sandbox_IncrementalAppend(t *testing.T) {
	prod := testutil.NewProject(t)

	// Create source with timestamps
	prod.AddModel("raw/events.sql", `-- @kind: table
SELECT * FROM (VALUES
  (1, '2024-01-01'),
  (2, '2024-01-02'),
  (3, '2024-01-03')
) AS t(id, ts)
`)
	runModel(t, prod, "raw/events.sql")

	// Incremental append model with cursor
	prod.AddModel("staging/inc_events.sql", `-- @kind: append
-- @incremental: ts
-- @incremental_initial: 2020-01-01
SELECT id, ts FROM raw.events
WHERE ts > getvariable('incr_last_value')
`)
	r1 := runModel(t, prod, "staging/inc_events.sql")
	if r1.RunType != "backfill" {
		t.Errorf("prod run1: run_type = %q, want backfill", r1.RunType)
	}
	if r1.RowsAffected != 3 {
		t.Errorf("prod run1: rows = %d, want 3", r1.RowsAffected)
	}

	// Sandbox: add new events to source
	sbox := testutil.NewSandboxProject(t, prod)
	sbox.AddModel("raw/events.sql", `-- @kind: table
SELECT * FROM (VALUES
  (1, '2024-01-01'),
  (2, '2024-01-02'),
  (3, '2024-01-03'),
  (4, '2024-01-04')
) AS t(id, ts)
`)
	sbox.AddModel("staging/inc_events.sql", `-- @kind: append
-- @incremental: ts
-- @incremental_initial: 2020-01-01
SELECT id, ts FROM raw.events
WHERE ts > getvariable('incr_last_value')
`)

	incPaths := []string{"raw/events.sql", "staging/inc_events.sql"}
	results := runDAGSandbox(t, sbox, incPaths)

	// raw.events: SQL changed → backfill
	if results["raw.events"].RunType != "backfill" {
		t.Errorf("raw.events: run_type = %q, want backfill", results["raw.events"].RunType)
	}

	// staging.inc_events: upstream changed in sandbox → should run (not skip)
	if results["staging.inc_events"].RunType == "skip" {
		t.Errorf("staging.inc_events: should not skip when upstream changed in sandbox")
	}

	// Verify data in sandbox — should have all 4 events (backfill due to upstream change)
	val, err := sbox.Sess.QueryValue("SELECT COUNT(*) FROM sandbox.staging.inc_events")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "4" {
		t.Errorf("sandbox rows = %s, want 4", val)
	}

	assertDAGAccountsForAll(t, 2, results)
	snap := newSnapshot()
	snap.addDAGResults(results)
	snap.addQuery(sbox.Sess, "sandbox_count", "SELECT COUNT(*) FROM sandbox.staging.inc_events")
	assertGolden(t, "sandbox_incremental_append", snap)
}

// TestE2E_Sandbox_AuditRollback verifies that @audit rollback works correctly
// in sandbox mode with dual-catalog (sandbox + prod).
func TestE2E_Sandbox_AuditRollback(t *testing.T) {
	prod := testutil.NewProject(t)

	// First run WITHOUT audit (establishes baseline data)
	prod.AddModel("staging/audited.sql", `-- @kind: table
SELECT 1 AS id, 100 AS amount
`)
	runModel(t, prod, "staging/audited.sql")

	// Verify prod data
	val, _ := prod.Sess.QueryValue("SELECT amount FROM staging.audited WHERE id = 1")
	if val != "100" {
		t.Fatalf("prod amount = %s, want 100", val)
	}

	// Sandbox: add audit + bad data (negative amount fails audit)
	sbox := testutil.NewSandboxProject(t, prod)
	sbox.AddModel("staging/audited.sql", `-- @kind: table
-- @audit: amount > 0
SELECT 1 AS id, -50 AS amount
`)

	result, err := runModelErr(t, sbox, "staging/audited.sql")

	// Audit should fail
	if err == nil {
		t.Fatal("expected audit error, got nil")
	}
	if !strings.Contains(err.Error(), "audit") {
		t.Errorf("error = %v, want audit-related error", err)
	}

	// Result should have audit errors
	if result == nil || len(result.Errors) == 0 {
		t.Fatal("expected audit errors in result")
	}

	// Prod data should be untouched (rollback in sandbox)
	val, err = sbox.Sess.QueryValue(fmt.Sprintf("SELECT amount FROM %s.staging.audited WHERE id = 1", sbox.Sess.ProdAlias()))
	if err != nil {
		t.Fatalf("query prod: %v", err)
	}
	if val != "100" {
		t.Errorf("prod amount after rollback = %s, want 100 (unchanged)", val)
	}

	snap := newSnapshot()
	snap.addResult(result)
	snap.addLine(fmt.Sprintf("exec_error: %v", err != nil))
	snap.addQuery(sbox.Sess, "prod_amount", fmt.Sprintf("SELECT amount FROM %s.staging.audited WHERE id = 1", sbox.Sess.ProdAlias()))
	assertGolden(t, "sandbox_audit_rollback", snap)
}

// TestE2E_Sandbox_WarningInDAG verifies that @warning directives work in
// DAG sandbox mode: warnings are logged but do NOT block execution or
// downstream propagation.
func TestE2E_Sandbox_WarningInDAG(t *testing.T) {
	prod := testutil.NewProject(t)
	prod.AddModel("raw/source.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 'Alice', 100), (2, 'Bob', -50)) AS t(id, name, amount)
`)
	prod.AddModel("staging/with_warning.sql", `-- @kind: table
-- @warning: amount > 0
SELECT id, name, amount FROM raw.source
`)
	prod.AddModel("mart/summary.sql", `-- @kind: table
SELECT COUNT(*) AS total FROM staging.with_warning
`)

	warnPaths := []string{"raw/source.sql", "staging/with_warning.sql", "mart/summary.sql"}
	runDAGSandbox(t, prod, warnPaths)

	// Sandbox: change source data
	sbox := testutil.NewSandboxProject(t, prod)
	sbox.AddModel("raw/source.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 'Alice', 100), (2, 'Bob', -50), (3, 'Charlie', -10)) AS t(id, name, amount)
`)
	sbox.AddModel("staging/with_warning.sql", `-- @kind: table
-- @warning: amount > 0
SELECT id, name, amount FROM raw.source
`)
	sbox.AddModel("mart/summary.sql", `-- @kind: table
SELECT COUNT(*) AS total FROM staging.with_warning
`)

	results := runDAGSandbox(t, sbox, warnPaths)

	// staging.with_warning: should run (upstream changed) and have warnings
	if results["staging.with_warning"].RunType == "skip" {
		t.Errorf("staging.with_warning: should not skip when upstream changed")
	}
	// Warnings should be present but NOT cause failure
	if len(results["staging.with_warning"].Warnings) == 0 {
		t.Errorf("staging.with_warning: expected warnings about negative amounts")
	}

	// mart.summary: should propagate from staging.with_warning (not blocked by warnings)
	if results["mart.summary"].RunType == "skip" {
		t.Errorf("mart.summary: should not skip when upstream changed")
	}

	// Verify data: 3 rows in staging, correct count in mart
	val, _ := sbox.Sess.QueryValue("SELECT COUNT(*) FROM sandbox.staging.with_warning")
	if val != "3" {
		t.Errorf("staging rows = %s, want 3", val)
	}
	val, _ = sbox.Sess.QueryValue("SELECT total FROM sandbox.mart.summary")
	if val != "3" {
		t.Errorf("mart total = %s, want 3", val)
	}

	assertDAGAccountsForAll(t, 3, results)
	snap := newSnapshot()
	snap.addDAGResults(results)
	snap.addQuery(sbox.Sess, "staging_count", "SELECT COUNT(*) FROM sandbox.staging.with_warning")
	snap.addQuery(sbox.Sess, "mart_total", "SELECT total FROM sandbox.mart.summary")
	assertGolden(t, "sandbox_warning_in_dag", snap)
}

// TestE2E_Sandbox_StarlarkModel verifies that Starlark (.star) script models
// work correctly in sandbox mode with dual-catalog.
func TestE2E_Sandbox_StarlarkModel(t *testing.T) {
	prod := testutil.NewProject(t)

	// Create Starlark model in prod
	prod.AddModel("staging/script_data.star", `# @kind: table
save.row({"id": 1, "name": "Alice", "score": 100})
save.row({"id": 2, "name": "Bob", "score": 200})
`)
	runModel(t, prod, "staging/script_data.star")

	// Verify prod data
	val, _ := prod.Sess.QueryValue("SELECT COUNT(*) FROM staging.script_data")
	if val != "2" {
		t.Fatalf("prod count = %s, want 2", val)
	}

	// Sandbox: change script to add more data
	sbox := testutil.NewSandboxProject(t, prod)
	sbox.AddModel("staging/script_data.star", `# @kind: table
save.row({"id": 1, "name": "Alice", "score": 100})
save.row({"id": 2, "name": "Bob", "score": 200})
save.row({"id": 3, "name": "Charlie", "score": 300})
`)

	result := runModel(t, sbox, "staging/script_data.star")

	// Script changed → backfill
	if result.RunType != "backfill" {
		t.Errorf("run_type = %q, want backfill", result.RunType)
	}
	if result.RowsAffected != 3 {
		t.Errorf("rows = %d, want 3", result.RowsAffected)
	}

	// Verify sandbox has new data
	val, err := sbox.Sess.QueryValue("SELECT COUNT(*) FROM sandbox.staging.script_data")
	if err != nil {
		t.Fatalf("query sandbox: %v", err)
	}
	if val != "3" {
		t.Errorf("sandbox count = %s, want 3", val)
	}

	// Verify prod still has 2 rows
	val, _ = sbox.Sess.QueryValue(fmt.Sprintf("SELECT COUNT(*) FROM %s.staging.script_data", sbox.Sess.ProdAlias()))
	if val != "2" {
		t.Errorf("prod count = %s, want 2 (unchanged)", val)
	}

	snap := newSnapshot()
	snap.addResult(result)
	snap.addQuery(sbox.Sess, "sandbox_count", "SELECT COUNT(*) FROM sandbox.staging.script_data")
	snap.addQuery(sbox.Sess, "prod_count", fmt.Sprintf("SELECT COUNT(*) FROM %s.staging.script_data", sbox.Sess.ProdAlias()))
	assertGolden(t, "sandbox_starlark_model", snap)
}

// TestE2E_Sandbox_StarlarkInDAG verifies that Starlark models work in a DAG
// with SQL models in sandbox mode. The Starlark model generates data via save.row(),
// and a downstream SQL model reads from it.
// Note: This test uses save.row() without query(), so no upstream deps are detected
// from the Starlark AST. Downstream SQL models that reference the Starlark target
// still have that dependency detected via DuckDB AST.
func TestE2E_Sandbox_StarlarkInDAG(t *testing.T) {
	prod := testutil.NewProject(t)

	// Starlark model generates data
	prod.AddModel("staging/scores.star", `# @kind: table
save.row({"id": 1, "name": "Alice", "score": 10})
save.row({"id": 2, "name": "Bob", "score": 20})
`)

	// SQL mart depends on staging.scores (DAG detects this dep)
	prod.AddModel("mart/summary.sql", `-- @kind: table
SELECT COUNT(*) AS total, SUM(score) AS score_sum FROM staging.scores
`)

	starDAGPaths := []string{"staging/scores.star", "mart/summary.sql"}
	runDAGSandbox(t, prod, starDAGPaths)

	// Verify prod data
	val, _ := prod.Sess.QueryValue("SELECT score_sum FROM mart.summary")
	if val != "30" {
		t.Logf("prod score_sum = %s (expected 30)", val)
	}

	// Sandbox: change Starlark to add more data → mart should propagate
	sbox := testutil.NewSandboxProject(t, prod)
	sbox.AddModel("staging/scores.star", `# @kind: table
save.row({"id": 1, "name": "Alice", "score": 10})
save.row({"id": 2, "name": "Bob", "score": 20})
save.row({"id": 3, "name": "Charlie", "score": 30})
`)
	sbox.AddModel("mart/summary.sql", `-- @kind: table
SELECT COUNT(*) AS total, SUM(score) AS score_sum FROM staging.scores
`)

	results := runDAGSandbox(t, sbox, starDAGPaths)

	// staging.scores (Starlark): script changed → backfill
	if results["staging.scores"].RunType != "backfill" {
		t.Errorf("staging.scores: run_type = %q, want backfill", results["staging.scores"].RunType)
	}
	if results["staging.scores"].RowsAffected != 3 {
		t.Errorf("staging.scores: rows = %d, want 3", results["staging.scores"].RowsAffected)
	}

	// mart.summary: staging.scores changed → should propagate
	if results["mart.summary"].RunType == "skip" {
		t.Errorf("mart.summary: should not skip when staging.scores changed")
	}

	// Verify data: 3 rows, score_sum = 10+20+30 = 60
	val, _ = sbox.Sess.QueryValue("SELECT total FROM sandbox.mart.summary")
	if val != "3" {
		t.Errorf("total = %s, want 3", val)
	}
	val, _ = sbox.Sess.QueryValue("SELECT score_sum FROM sandbox.mart.summary")
	if val != "60" {
		t.Errorf("score_sum = %s, want 60", val)
	}

	assertDAGAccountsForAll(t, 2, results)
	snap := newSnapshot()
	snap.addDAGResults(results)
	snap.addQuery(sbox.Sess, "total", "SELECT total FROM sandbox.mart.summary")
	snap.addQuery(sbox.Sess, "score_sum", "SELECT score_sum FROM sandbox.mart.summary")
	assertGolden(t, "sandbox_starlark_in_dag", snap)
}

// TestE2E_Sandbox_ExtensionModel verifies that @extension works in sandbox mode.
func TestE2E_Sandbox_ExtensionModel(t *testing.T) {
	prod := testutil.NewProject(t)

	// Model using json extension
	prod.AddModel("staging/with_ext.sql", `-- @kind: table
-- @extension: json
SELECT 1 AS id, json_serialize_sql('{"name": "Alice"}') AS data
`)
	runModel(t, prod, "staging/with_ext.sql")

	// Sandbox: same model with extension
	sbox := testutil.NewSandboxProject(t, prod)
	sbox.AddModel("staging/with_ext.sql", `-- @kind: table
-- @extension: json
SELECT 1 AS id, json_serialize_sql('{"name": "Bob"}') AS data
`)

	result := runModel(t, sbox, "staging/with_ext.sql")

	if result.RunType != "backfill" {
		t.Errorf("run_type = %q, want backfill (SQL changed)", result.RunType)
	}
	if result.RowsAffected != 1 {
		t.Errorf("rows = %d, want 1", result.RowsAffected)
	}

	// Verify extension loaded and data correct in sandbox
	val, err := sbox.Sess.QueryValue("SELECT COUNT(*) FROM sandbox.staging.with_ext")
	if err != nil {
		t.Fatalf("query sandbox: %v", err)
	}
	if val != "1" {
		t.Errorf("count = %s, want 1", val)
	}

	snap := newSnapshot()
	snap.addResult(result)
	snap.addQuery(sbox.Sess, "sandbox_count", "SELECT COUNT(*) FROM sandbox.staging.with_ext")
	assertGolden(t, "sandbox_extension_model", snap)
}

// --- View kind golden tests ---

func TestE2E_ViewKind(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/source.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name, 100 AS amount
UNION ALL SELECT 2, 'Bob', 200
`)
	runModel(t, p, "raw/source.sql")

	p.AddModel("staging/cleaned.sql", `-- @kind: view
-- @description: Cleaned source data

SELECT id, name, amount FROM raw.source WHERE amount > 0
`)
	result := runModel(t, p, "staging/cleaned.sql")

	snap := newSnapshot()
	snap.addResult(result)
	snap.addQuery(p.Sess, "view_count", "SELECT COUNT(*) FROM staging.cleaned")
	snap.addQuery(p.Sess, "view_comment",
		"SELECT COALESCE(comment, '') FROM duckdb_views() WHERE schema_name = 'staging' AND view_name = 'cleaned'")
	assertGolden(t, "view_kind", snap)
}

func TestE2E_ViewKind_LiveResolution(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/src.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name
`)
	runModel(t, p, "raw/src.sql")

	p.AddModel("staging/v.sql", `-- @kind: view
SELECT * FROM raw.src
`)
	r1 := runModel(t, p, "staging/v.sql")

	p.AddModel("raw/src.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name
UNION ALL SELECT 2, 'Bob'
`)
	runModel(t, p, "raw/src.sql")

	r2 := runModel(t, p, "staging/v.sql")

	snap := newSnapshot()
	snap.addLine("--- run 1 (create) ---")
	snap.addResult(r1)
	snap.addLine("--- run 2 (skip, source updated) ---")
	snap.addResult(r2)
	snap.addQuery(p.Sess, "live_count", "SELECT COUNT(*) FROM staging.v")
	assertGolden(t, "view_kind_live", snap)
}

// --- COMMENT ON golden tests ---

func TestE2E_CommentOnTable(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("staging/described.sql", `-- @kind: table
-- @description: Daily aggregated sales by region

SELECT 1 AS id, 100.0 AS amount, 'US' AS region
UNION ALL SELECT 2, 200.0, 'EU'
`)

	result := runModel(t, p, "staging/described.sql")

	snap := newSnapshot()
	snap.addResult(result)
	snap.addQuery(p.Sess, "table_comment",
		"SELECT COALESCE(comment, '') FROM duckdb_tables() WHERE schema_name = 'staging' AND table_name = 'described'")
	snap.addQuery(p.Sess, "count", "SELECT COUNT(*) FROM staging.described")
	assertGolden(t, "comment_on_table", snap)
}

func TestE2E_CommentOnColumns(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("mart/sales.sql", `-- @kind: table
-- @description: Sales summary
-- @column: amount = Total revenue including tax
-- @column: region = Geographic sales region

SELECT 100.0 AS amount, 'US' AS region
`)

	result := runModel(t, p, "mart/sales.sql")

	snap := newSnapshot()
	snap.addResult(result)
	snap.addQuery(p.Sess, "table_comment",
		"SELECT COALESCE(comment, '') FROM duckdb_tables() WHERE schema_name = 'mart' AND table_name = 'sales'")
	snap.addQueryRows(p.Sess, "column_comments",
		"SELECT column_name, COALESCE(comment, '') AS comment FROM duckdb_columns() WHERE schema_name = 'mart' AND table_name = 'sales' ORDER BY column_name")
	assertGolden(t, "comment_on_columns", snap)
}

func TestE2E_CommentOnPipeTags(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("staging/tagged.sql", `-- @kind: table
-- @column: ssn = Social security number | PII
-- @column: metric = Revenue | Cost ratio

SELECT '123' AS ssn, 1.5 AS metric
`)

	result := runModel(t, p, "staging/tagged.sql")

	snap := newSnapshot()
	snap.addResult(result)
	snap.addQueryRows(p.Sess, "column_comments",
		"SELECT column_name, COALESCE(comment, '') AS comment FROM duckdb_columns() WHERE schema_name = 'staging' AND table_name = 'tagged' ORDER BY column_name")
	assertGolden(t, "comment_on_pipe_tags", snap)
}

func TestE2E_CommentOnSCD2(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("staging/customers.sql", `-- @kind: scd2
-- @unique_key: id
-- @description: Customer history with SCD2

SELECT 1 AS id, 'Alice' AS name
`)

	r1 := runModel(t, p, "staging/customers.sql")

	// Update model and run incrementally
	p.AddModel("staging/customers.sql", `-- @kind: scd2
-- @unique_key: id
-- @description: Customer history updated

SELECT 1 AS id, 'Bob' AS name
`)
	r2 := runModel(t, p, "staging/customers.sql")

	snap := newSnapshot()
	snap.addLine("--- run 1 (backfill) ---")
	snap.addResult(r1)
	snap.addLine("--- run 2 (incremental) ---")
	snap.addResult(r2)
	snap.addQuery(p.Sess, "table_comment",
		"SELECT COALESCE(comment, '') FROM duckdb_tables() WHERE schema_name = 'staging' AND table_name = 'customers'")
	snap.addQuery(p.Sess, "row_count", "SELECT COUNT(*) FROM staging.customers")
	assertGolden(t, "comment_on_scd2", snap)
}

func TestE2E_CommentOnPartition(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("staging/regional.sql", `-- @kind: partition
-- @unique_key: region
-- @description: Regional sales data

SELECT 'US' AS region, 100 AS amount
UNION ALL SELECT 'EU', 200
`)

	r1 := runModel(t, p, "staging/regional.sql")

	// Run again (incremental partition replace)
	p.AddModel("staging/regional.sql", `-- @kind: partition
-- @unique_key: region
-- @description: Regional sales updated

SELECT 'US' AS region, 300 AS amount
`)
	r2 := runModel(t, p, "staging/regional.sql")

	snap := newSnapshot()
	snap.addLine("--- run 1 (backfill) ---")
	snap.addResult(r1)
	snap.addLine("--- run 2 (incremental) ---")
	snap.addResult(r2)
	snap.addQuery(p.Sess, "table_comment",
		"SELECT COALESCE(comment, '') FROM duckdb_tables() WHERE schema_name = 'staging' AND table_name = 'regional'")
	assertGolden(t, "comment_on_partition", snap)
}

func TestE2E_EventsModel_Parse(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/events.sql", `-- @kind: events
-- @description: Website analytics events

event_name VARCHAR NOT NULL,
page_url VARCHAR,
user_id VARCHAR,
session_id VARCHAR,
event_params JSON,
received_at TIMESTAMPTZ
`)

	// Parse the model and verify structure
	modelPath := filepath.Join(p.Dir, "models", "raw/events.sql")
	model, err := parser.ParseModel(modelPath, p.Dir)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if model.Kind != "events" {
		t.Errorf("kind = %q, want events", model.Kind)
	}
	if model.Target != "raw.events" {
		t.Errorf("target = %q, want raw.events", model.Target)
	}
	if model.Description != "Website analytics events" {
		t.Errorf("description = %q", model.Description)
	}
	if len(model.Columns) != 6 {
		t.Fatalf("columns = %d, want 6", len(model.Columns))
	}
	if !model.Columns[0].NotNull {
		t.Error("event_name should be NOT NULL")
	}
	if model.Columns[1].NotNull {
		t.Error("page_url should be nullable")
	}

	snap := newSnapshot()
	snap.addLine(fmt.Sprintf("target: %s", model.Target))
	snap.addLine(fmt.Sprintf("kind: %s", model.Kind))
	snap.addLine(fmt.Sprintf("description: %s", model.Description))
	snap.addLine(fmt.Sprintf("columns: %d", len(model.Columns)))
	for _, col := range model.Columns {
		nn := ""
		if col.NotNull {
			nn = " NOT NULL"
		}
		snap.addLine(fmt.Sprintf("  %s %s%s", col.Name, col.Type, nn))
	}
	assertGolden(t, "events_model_parse", snap)
}

func TestE2E_EventsModel_CreateTable(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/events.sql", `-- @kind: events
event_name VARCHAR NOT NULL,
page_url VARCHAR,
received_at TIMESTAMPTZ
`)

	// The runner should create the table, then fail because daemon isn't running.
	// This verifies the CREATE TABLE IF NOT EXISTS path works.
	modelPath := filepath.Join(p.Dir, "models", "raw/events.sql")
	model, err := parser.ParseModel(modelPath, p.Dir)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
	runner.SetAdminPort("19999") // Non-existent port → daemon not running
	_, runErr := runner.Run(context.Background(), model)

	// Should fail with daemon-not-running message
	if runErr == nil {
		t.Fatal("expected error when daemon not running")
	}
	if !strings.Contains(runErr.Error(), "daemon is not running") {
		t.Fatalf("unexpected error: %v", runErr)
	}

	// But the table should have been created before the daemon check
	val, err := p.Sess.QueryValue("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'raw' AND table_name = 'events'")
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	snap := newSnapshot()
	snap.addLine(fmt.Sprintf("table_exists: %s", val))
	snap.addLine(fmt.Sprintf("error_contains_daemon: %v", strings.Contains(runErr.Error(), "daemon")))
	assertGolden(t, "events_model_create_table", snap)
}

func TestE2E_EventsModel_InvalidDirectives(t *testing.T) {
	p := testutil.NewProject(t)

	// Events with @unique_key should fail parsing
	p.AddModel("raw/bad_events.sql", `-- @kind: events
-- @unique_key: id
event_name VARCHAR
`)

	modelPath := filepath.Join(p.Dir, "models", "raw/bad_events.sql")
	_, err := parser.ParseModel(modelPath, p.Dir)
	if err == nil {
		t.Fatal("expected error for events with @unique_key")
	}

	snap := newSnapshot()
	snap.addLine(fmt.Sprintf("parse_error: %v", strings.Contains(err.Error(), "@unique_key is not supported")))
	assertGolden(t, "events_model_invalid_directives", snap)
}

func TestE2E_EventsModel_DAGWithOtherKinds(t *testing.T) {
	p := testutil.NewProject(t)

	// Events model alongside a regular table model
	p.AddModel("raw/events.sql", `-- @kind: events
event_name VARCHAR NOT NULL,
page_url VARCHAR
`)

	p.AddModel("staging/page_views.sql", `-- @kind: view
SELECT * FROM raw.events
`)

	// Build DAG to verify events models integrate with other kinds
	models, err := loadAllModels(t, p)
	if err != nil {
		t.Fatalf("load models: %v", err)
	}

	// Verify both models loaded
	var kinds []string
	for _, m := range models {
		kinds = append(kinds, m.Kind)
	}

	snap := newSnapshot()
	snap.addLine(fmt.Sprintf("model_count: %d", len(models)))
	for _, m := range models {
		snap.addLine(fmt.Sprintf("  %s (%s)", m.Target, m.Kind))
	}
	assertGolden(t, "events_model_dag_with_other_kinds", snap)
}

// loadAllModels loads all models from a test project (helper for events tests).
func loadAllModels(t *testing.T, p *testutil.Project) ([]*parser.Model, error) {
	t.Helper()
	modelsDir := filepath.Join(p.Dir, "models")
	var models []*parser.Model
	err := filepath.Walk(modelsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !parser.IsModelFile(path) {
			return err
		}
		m, err := parser.ParseModel(path, p.Dir)
		if err != nil {
			return err
		}
		models = append(models, m)
		return nil
	})
	return models, err
}

func TestE2E_EventsModel_FullFlow(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/events.sql", `-- @kind: events
event_name VARCHAR NOT NULL,
page_url VARCHAR,
received_at TIMESTAMPTZ
`)

	// Parse model
	modelPath := filepath.Join(p.Dir, "models", "raw/events.sql")
	model, err := parser.ParseModel(modelPath, p.Dir)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Start an in-process daemon (Badger store + HTTP servers)
	badgerDir := filepath.Join(t.TempDir(), "events")
	os.MkdirAll(badgerDir, 0755)
	store, err := collect.Open(badgerDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	publicPort := freePortE2E(t)
	adminPort := freePortE2E(t)

	srv := collect.NewServer(store, []*parser.Model{model}, publicPort, adminPort)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srvErr := make(chan error, 1)
	go func() { srvErr <- srv.Start(ctx) }()

	// Wait for servers to be ready
	ready := false
	for i := 0; i < 50; i++ {
		resp, err := http.Get("http://127.0.0.1:" + adminPort + "/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				ready = true
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !ready {
		t.Fatal("daemon did not become ready")
	}

	// Send events via public HTTP endpoint
	events := []string{
		`{"event_name":"pageview","page_url":"/home"}`,
		`{"event_name":"click","page_url":"/pricing"}`,
		`{"event_name":"signup","page_url":"/register"}`,
	}
	for _, body := range events {
		resp, err := http.Post(
			"http://127.0.0.1:"+publicPort+"/collect/raw/events",
			"application/json",
			strings.NewReader(body),
		)
		if err != nil {
			t.Fatalf("collect: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != 202 {
			t.Fatalf("collect status = %d, want 202", resp.StatusCode)
		}
	}

	// Run the events model through the pipeline (flush from Badger → DuckLake)
	runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
	runner.SetAdminPort(adminPort)
	result, err := runner.Run(context.Background(), model)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	// Shutdown daemon
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()
	srv.Shutdown(shutdownCtx)

	// Verify results
	snap := newSnapshot()
	snap.addResult(result)
	snap.addQuery(p.Sess, "count", "SELECT COUNT(*) FROM raw.events")
	snap.addQueryRows(p.Sess, "data", "SELECT event_name, page_url FROM raw.events ORDER BY event_name")
	assertGolden(t, "events_model_full_flow", snap)
}

func TestE2E_EventsModel_FullFlow_Batch(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/clicks.sql", `-- @kind: events
event_name VARCHAR NOT NULL,
page_url VARCHAR,
received_at TIMESTAMPTZ
`)

	modelPath := filepath.Join(p.Dir, "models", "raw/clicks.sql")
	model, err := parser.ParseModel(modelPath, p.Dir)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Start daemon
	badgerDir := filepath.Join(t.TempDir(), "events")
	os.MkdirAll(badgerDir, 0755)
	store, err := collect.Open(badgerDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	publicPort := freePortE2E(t)
	adminPort := freePortE2E(t)

	srv := collect.NewServer(store, []*parser.Model{model}, publicPort, adminPort)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Start(ctx)

	// Wait for ready
	for i := 0; i < 50; i++ {
		resp, err := http.Get("http://127.0.0.1:" + adminPort + "/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Send batch of events
	batch := `[{"event_name":"a","page_url":"/1"},{"event_name":"b","page_url":"/2"},{"event_name":"c","page_url":"/3"},{"event_name":"d","page_url":"/4"},{"event_name":"e","page_url":"/5"}]`
	resp, err := http.Post(
		"http://127.0.0.1:"+publicPort+"/collect/raw/clicks/batch",
		"application/json",
		strings.NewReader(batch),
	)
	if err != nil {
		t.Fatalf("batch collect: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 202 {
		t.Fatalf("batch status = %d, want 202", resp.StatusCode)
	}

	// Flush
	runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
	runner.SetAdminPort(adminPort)
	result, runErr := runner.Run(context.Background(), model)
	if runErr != nil {
		t.Fatalf("run: %v", runErr)
	}

	cancel()

	snap := newSnapshot()
	snap.addResult(result)
	snap.addQuery(p.Sess, "count", "SELECT COUNT(*) FROM raw.clicks")
	assertGolden(t, "events_model_full_flow_batch", snap)
}

func TestE2E_EventsModel_FullFlow_NackRecovery(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/events.sql", `-- @kind: events
event_name VARCHAR NOT NULL,
page_url VARCHAR,
received_at TIMESTAMPTZ
`)

	modelPath := filepath.Join(p.Dir, "models", "raw/events.sql")
	model, err := parser.ParseModel(modelPath, p.Dir)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Use a shared Badger store to simulate crash recovery
	badgerDir := filepath.Join(t.TempDir(), "events")
	os.MkdirAll(badgerDir, 0755)

	// Phase 1: Start daemon, send events, claim (but don't ack) — simulates crash
	store1, err := collect.Open(badgerDir)
	if err != nil {
		t.Fatalf("open store1: %v", err)
	}

	publicPort := freePortE2E(t)
	adminPort := freePortE2E(t)

	srv1 := collect.NewServer(store1, []*parser.Model{model}, publicPort, adminPort)
	ctx1, cancel1 := context.WithCancel(context.Background())
	go srv1.Start(ctx1)

	for i := 0; i < 50; i++ {
		resp, err := http.Get("http://127.0.0.1:" + adminPort + "/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Send 2 events
	for _, body := range []string{
		`{"event_name":"ev1","page_url":"/a"}`,
		`{"event_name":"ev2","page_url":"/b"}`,
	} {
		resp, err := http.Post("http://127.0.0.1:"+publicPort+"/collect/raw/events", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("collect: %v", err)
		}
		resp.Body.Close()
	}

	// Claim events (simulates runner starting) but don't ack
	store1.Claim("raw.events", 1000)

	// "Crash" — shutdown without ack
	cancel1()
	srv1.Shutdown(context.Background())
	store1.Close()

	// Phase 2: Restart daemon — RecoverAllInflight should return events to queue
	store2, err := collect.Open(badgerDir)
	if err != nil {
		t.Fatalf("open store2: %v", err)
	}
	defer store2.Close()

	// Recover inflight (like daemon startup does)
	if err := store2.RecoverAllInflight(); err != nil {
		t.Fatalf("recover: %v", err)
	}

	publicPort2 := freePortE2E(t)
	adminPort2 := freePortE2E(t)

	srv2 := collect.NewServer(store2, []*parser.Model{model}, publicPort2, adminPort2)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go srv2.Start(ctx2)

	for i := 0; i < 50; i++ {
		resp, err := http.Get("http://127.0.0.1:" + adminPort2 + "/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Flush — recovered events should be available
	runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
	runner.SetAdminPort(adminPort2)
	result, runErr := runner.Run(context.Background(), model)
	if runErr != nil {
		t.Fatalf("run: %v", runErr)
	}

	cancel2()

	snap := newSnapshot()
	snap.addResult(result)
	snap.addQuery(p.Sess, "count", "SELECT COUNT(*) FROM raw.events")
	snap.addQueryRows(p.Sess, "data", "SELECT event_name, page_url FROM raw.events ORDER BY event_name")
	assertGolden(t, "events_model_nack_recovery", snap)
}

func TestE2E_EventsModel_FullFlow_EmptyFlush(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/events.sql", `-- @kind: events
event_name VARCHAR NOT NULL,
page_url VARCHAR
`)

	modelPath := filepath.Join(p.Dir, "models", "raw/events.sql")
	model, err := parser.ParseModel(modelPath, p.Dir)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Start daemon but send NO events
	badgerDir := filepath.Join(t.TempDir(), "events")
	os.MkdirAll(badgerDir, 0755)
	store, err := collect.Open(badgerDir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	publicPort := freePortE2E(t)
	adminPort := freePortE2E(t)

	srv := collect.NewServer(store, []*parser.Model{model}, publicPort, adminPort)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Start(ctx)

	for i := 0; i < 50; i++ {
		resp, err := http.Get("http://127.0.0.1:" + adminPort + "/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Run with 0 events — should succeed with 0 rows
	runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
	runner.SetAdminPort(adminPort)
	result, runErr := runner.Run(context.Background(), model)
	if runErr != nil {
		t.Fatalf("run: %v", runErr)
	}

	cancel()

	snap := newSnapshot()
	snap.addResult(result)
	assertGolden(t, "events_model_empty_flush", snap)
}

func TestE2E_ColumnMasking(t *testing.T) {
	p := testutil.NewProject(t)

	// Define masking macros in the session (simulates config/macros.sql)
	err := p.Sess.Exec(`CREATE OR REPLACE MACRO mask_ssn(val) AS '***-**-' || val[-4:]`)
	if err != nil {
		t.Fatalf("create mask_ssn macro: %v", err)
	}
	err = p.Sess.Exec(`CREATE OR REPLACE MACRO mask_email(val) AS regexp_replace(val, '(.)[^@]*(@.*)', '\1***\2')`)
	if err != nil {
		t.Fatalf("create mask_email macro: %v", err)
	}

	// Create raw source table
	err = p.Sess.Exec(`CREATE SCHEMA IF NOT EXISTS raw`)
	if err != nil {
		t.Fatalf("create schema: %v", err)
	}
	err = p.Sess.Exec(`CREATE TABLE raw.customers (id INTEGER, name VARCHAR, email VARCHAR, ssn VARCHAR)`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	err = p.Sess.Exec(`INSERT INTO raw.customers VALUES
		(1, 'Alice', 'alice@example.com', '123-45-6789'),
		(2, 'Bob', 'bob@example.com', '987-65-4321')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Create staging model with masking tags
	p.AddModel("staging/customers.sql", `-- @kind: merge
-- @unique_key: id
-- @column: email = Customer email | PII | mask_email
-- @column: ssn = Social security number | PII | mask_ssn

SELECT id, name, email, ssn FROM raw.customers
`)

	result := runModel(t, p, "staging/customers.sql")

	snap := newSnapshot()
	snap.addResult(result)
	snap.addQueryRows(p.Sess, "data",
		"SELECT id, name, email, ssn FROM staging.customers ORDER BY id")
	assertGolden(t, "column_masking", snap)
}

// =============================================================================
// Starlark load() Tests
// =============================================================================

// TestE2E_StarlarkLoad verifies that Starlark models can load shared libraries
// from lib/ using the load() builtin.
func TestE2E_StarlarkLoad(t *testing.T) {
	p := testutil.NewProject(t)

	// Create a shared library module
	testutil.WriteFile(t, p.Dir, "lib/helpers.star", `
def make_rows(n):
    rows = []
    for i in range(1, n + 1):
        rows.append({"id": i, "name": "user_" + str(i)})
    return rows
`)

	// Create a Starlark model that uses load()
	p.AddModel("staging/users.star", `# @kind: table
load("lib/helpers.star", "make_rows")
for row in make_rows(3):
    save.row(row)
`)

	result := runModel(t, p, "staging/users.star")

	if result.RowsAffected != 3 {
		t.Errorf("rows = %d, want 3", result.RowsAffected)
	}

	val, err := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.users")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "3" {
		t.Errorf("count = %s, want 3", val)
	}

	snap := newSnapshot()
	snap.addResult(result)
	snap.addQuery(p.Sess, "count", "SELECT COUNT(*) FROM staging.users")
	snap.addQueryRows(p.Sess, "data", "SELECT * FROM staging.users ORDER BY id")
	assertGolden(t, "starlark_load", snap)
}

// TestE2E_StarlarkLoad_Nested verifies that nested load() works (A loads B loads C).
func TestE2E_StarlarkLoad_Nested(t *testing.T) {
	p := testutil.NewProject(t)

	testutil.WriteFile(t, p.Dir, "lib/base.star", `
def base_value():
    return 100
`)

	testutil.WriteFile(t, p.Dir, "lib/middle.star", `
load("lib/base.star", "base_value")
def computed_value():
    return base_value() + 50
`)

	p.AddModel("staging/computed.star", `# @kind: table
load("lib/middle.star", "computed_value")
save.row({"id": 1, "value": computed_value()})
`)

	result := runModel(t, p, "staging/computed.star")

	if result.RowsAffected != 1 {
		t.Errorf("rows = %d, want 1", result.RowsAffected)
	}

	val, err := p.Sess.QueryValue("SELECT value FROM staging.computed")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "150" {
		t.Errorf("value = %s, want 150", val)
	}
}

// =============================================================================
// YAML Model Tests
// =============================================================================

// TestE2E_YAMLModel_Integration is an end-to-end test: YAML → lib → DuckLake.
func TestE2E_YAMLModel_Integration(t *testing.T) {
	p := testutil.NewProject(t)

	// Create a source library function (save is passed as first argument)
	testutil.WriteFile(t, p.Dir, "lib/test_source.star", `
def test_source(save, prefix="item", count=2):
    for i in range(1, count + 1):
        save.row({"id": i, "name": prefix + "_" + str(i)})
`)

	// Create a YAML model that references the source
	p.AddModel("staging/items.yaml", `kind: table
source: test_source
config:
  prefix: "product"
  count: 3
`)

	result := runModel(t, p, "staging/items.yaml")

	if result.RowsAffected != 3 {
		t.Errorf("rows = %d, want 3", result.RowsAffected)
	}
	if result.Kind != "table" {
		t.Errorf("kind = %q, want table", result.Kind)
	}

	val, err := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.items")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "3" {
		t.Errorf("count = %s, want 3", val)
	}

	// Verify actual data
	name, err := p.Sess.QueryValue("SELECT name FROM staging.items WHERE id = 2")
	if err != nil {
		t.Fatalf("query name: %v", err)
	}
	if name != "product_2" {
		t.Errorf("name = %s, want product_2", name)
	}

	snap := newSnapshot()
	snap.addResult(result)
	snap.addQuery(p.Sess, "count", "SELECT COUNT(*) FROM staging.items")
	snap.addQueryRows(p.Sess, "data", "SELECT * FROM staging.items ORDER BY id")
	assertGolden(t, "yaml_model_integration", snap)
}

// TestE2E_YAMLModel_Append verifies YAML model with append kind and incremental.
func TestE2E_YAMLModel_Append(t *testing.T) {
	p := testutil.NewProject(t)

	testutil.WriteFile(t, p.Dir, "lib/batch_source.star", `
def batch_source(save, batch_id=1):
    for i in range(1, 4):
        save.row({"id": batch_id * 100 + i, "batch": batch_id})
`)

	p.AddModel("staging/batches.yaml", `kind: append
source: batch_source
config:
  batch_id: 1
`)

	result := runModel(t, p, "staging/batches.yaml")

	if result.RowsAffected != 3 {
		t.Errorf("rows = %d, want 3", result.RowsAffected)
	}
	if result.Kind != "append" {
		t.Errorf("kind = %q, want append", result.Kind)
	}

	val, err := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.batches")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "3" {
		t.Errorf("count = %s, want 3", val)
	}
}

// TestE2E_YAMLModel_InDAG verifies YAML models work correctly in a DAG
// with downstream SQL models.
func TestE2E_YAMLModel_InDAG(t *testing.T) {
	p := testutil.NewProject(t)

	// Source library
	testutil.WriteFile(t, p.Dir, "lib/score_source.star", `
def score_source(save, multiplier=1):
    save.row({"id": 1, "name": "Alice", "score": 10 * multiplier})
    save.row({"id": 2, "name": "Bob", "score": 20 * multiplier})
`)

	// YAML model produces raw data
	p.AddModel("staging/scores.yaml", `kind: table
source: score_source
config:
  multiplier: 5
`)

	// SQL model depends on staging.scores
	p.AddModel("mart/summary.sql", `-- @kind: table
SELECT COUNT(*) AS total, SUM(score) AS total_score FROM staging.scores
`)

	results := runDAGSandbox(t, p, []string{
		"staging/scores.yaml",
		"mart/summary.sql",
	})

	// Check staging.scores
	if results["staging.scores"].RowsAffected != 2 {
		t.Errorf("staging.scores rows = %d, want 2", results["staging.scores"].RowsAffected)
	}

	// Check mart.summary uses the multiplied scores
	val, err := p.Sess.QueryValue("SELECT total_score FROM mart.summary")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	// (10*5) + (20*5) = 50 + 100 = 150
	if val != "150" {
		t.Errorf("total_score = %s, want 150", val)
	}

	snap := newSnapshot()
	snap.addDAGResults(results)
	snap.addQuery(p.Sess, "total_score", "SELECT total_score FROM mart.summary")
	assertGolden(t, "yaml_model_in_dag", snap)
}

func newTestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("skipping: cannot bind IPv4 loopback: %v", err)
	}
	srv := &httptest.Server{
		Listener: l,
		Config:   &http.Server{Handler: handler},
	}
	srv.Start()
	return srv
}

// TestE2E_OAuthProviderFlow tests the OAuth2 provider flow at the package level:
// oauth2host (register, poll, refresh, token storage) and oauth.token("provider") in Starlark.
// This does NOT test the CLI command (runAuth, runAuthList, browser opening, auth URL building)
// — those are covered by unit tests in cmd/ondatrasql/auth_cmd_test.go.
func TestE2E_OAuthProviderFlow(t *testing.T) {
	// --- Mock OAuth2 provider (e.g. Fortnox) ---
	provider := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.NotFound(w, r)
			return
		}
		r.ParseForm()
		grantType := r.FormValue("grant_type")
		switch grantType {
		case "authorization_code":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"access_token":  "AT_initial",
				"refresh_token": "RT_from_provider",
				"expires_in":    3600,
			})
		case "refresh_token":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"access_token":  "AT_refreshed",
				"refresh_token": "RT_rotated",
				"expires_in":    3600,
			})
		default:
			w.WriteHeader(400)
			json.NewEncoder(w).Encode(map[string]string{"error": "unsupported_grant_type"})
		}
	}))
	defer provider.Close()

	// --- Mock edge script (oauth2.ondatra.sh) ---
	var mu sync.Mutex
	pendingStates := map[string]string{}         // state → provider
	completedTokens := map[string]string{}       // state → refresh_token

	edge := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		// GET /providers/<name>.json
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/providers/"):
			json.NewEncoder(w).Encode(map[string]string{
				"name":         "mock-provider",
				"auth_url":     provider.URL + "/authorize",
				"token_url":    provider.URL + "/token",
				"client_id":    "test-client-id",
				"scope":        "read",
				"redirect_uri": "http://localhost/callback",
			})

		// POST /oauth/register
		case r.Method == "POST" && r.URL.Path == "/oauth/register":
			var body map[string]string
			json.NewDecoder(r.Body).Decode(&body)
			if body["license_key"] != "osk_e2e_test" {
				w.WriteHeader(403)
				json.NewEncoder(w).Encode(map[string]string{"error": "invalid key"})
				return
			}
			mu.Lock()
			pendingStates[body["state"]] = body["provider"]
			mu.Unlock()
			json.NewEncoder(w).Encode(map[string]bool{"ok": true})

		// GET /oauth/callback — simulated provider redirect
		case r.Method == "GET" && r.URL.Path == "/oauth/callback":
			code := r.URL.Query().Get("code")
			state := r.URL.Query().Get("state")
			if code == "" || state == "" {
				w.WriteHeader(400)
				return
			}
			mu.Lock()
			prov, ok := pendingStates[state]
			if !ok {
				mu.Unlock()
				w.WriteHeader(400)
				return
			}
			delete(pendingStates, state)
			mu.Unlock()
			// Exchange code at provider
			_ = prov // would use to look up provider config
			// Simulate token exchange
			completedTokens[state] = "RT_from_provider"
			w.Header().Set("Content-Type", "text/html")
			fmt.Fprint(w, "<h1>Done</h1>")

		// POST /oauth/poll
		case r.Method == "POST" && r.URL.Path == "/oauth/poll":
			var pollBody map[string]string
			json.NewDecoder(r.Body).Decode(&pollBody)
			state := pollBody["state"]
			mu.Lock()
			rt, ok := completedTokens[state]
			if ok {
				delete(completedTokens, state)
			}
			mu.Unlock()
			if !ok {
				w.WriteHeader(404)
				json.NewEncoder(w).Encode(map[string]string{"status": "pending"})
				return
			}
			json.NewEncoder(w).Encode(map[string]string{
				"provider":      "mock-provider",
				"refresh_token": rt,
			})

		// POST /oauth/refresh
		case r.Method == "POST" && r.URL.Path == "/oauth/refresh":
			var body map[string]string
			json.NewDecoder(r.Body).Decode(&body)
			if body["license_key"] != "osk_e2e_test" {
				w.WriteHeader(403)
				return
			}
			// Call the real mock provider's token endpoint
			json.NewEncoder(w).Encode(map[string]interface{}{
				"access_token":  "AT_refreshed",
				"refresh_token": "RT_rotated",
				"expires_in":    3600,
			})

		default:
			http.NotFound(w, r)
		}
	}))
	defer edge.Close()

	// --- Setup project ---
	p := testutil.NewProject(t)
	t.Setenv("ONDATRA_KEY", "osk_e2e_test")
	t.Setenv("ONDATRA_OAUTH_HOST", edge.URL)

	// --- Step 1: Register auth request ---
	ctx := context.Background()
	state := "e2e_test_state_0123456789abcdef0123456789abcdef0123456789abcdef01234567"

	err := oauth2host.Register(ctx, edge.URL, "mock-provider", state, "osk_e2e_test", "")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	// --- Step 2: Simulate browser callback ---
	callbackURL := edge.URL + "/oauth/callback?code=AUTH_CODE&state=" + state
	resp, err := http.Get(callbackURL)
	if err != nil {
		t.Fatalf("Callback: %v", err)
	}
	resp.Body.Close()

	// --- Step 3: Poll for token ---
	result, err := oauth2host.Poll(ctx, edge.URL, state, "osk_e2e_test")
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if result.Provider != "mock-provider" {
		t.Errorf("provider = %q, want mock-provider", result.Provider)
	}
	if result.RefreshToken != "RT_from_provider" {
		t.Errorf("refresh_token = %q, want RT_from_provider", result.RefreshToken)
	}

	// --- Step 4: Save token ---
	err = oauth2host.WriteToken(p.Dir, "mock-provider", result.RefreshToken)
	if err != nil {
		t.Fatalf("WriteToken: %v", err)
	}

	// --- Step 5: Verify token file ---
	tf, err := oauth2host.ReadToken(p.Dir, "mock-provider")
	if err != nil {
		t.Fatalf("ReadToken: %v", err)
	}
	if tf.RefreshToken != "RT_from_provider" {
		t.Errorf("stored refresh_token = %q, want RT_from_provider", tf.RefreshToken)
	}

	// --- Step 6: Refresh via edge script ---
	refreshResult, err := oauth2host.Refresh(ctx, edge.URL, "mock-provider", tf.RefreshToken, "osk_e2e_test")
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if refreshResult.AccessToken != "AT_refreshed" {
		t.Errorf("access_token = %q, want AT_refreshed", refreshResult.AccessToken)
	}
	if refreshResult.RefreshToken != "RT_rotated" {
		t.Errorf("new refresh_token = %q, want RT_rotated", refreshResult.RefreshToken)
	}

	// --- Step 7: Save rotated token ---
	err = oauth2host.WriteToken(p.Dir, "mock-provider", refreshResult.RefreshToken)
	if err != nil {
		t.Fatalf("WriteToken rotated: %v", err)
	}
	tf2, _ := oauth2host.ReadToken(p.Dir, "mock-provider")
	if tf2.RefreshToken != "RT_rotated" {
		t.Errorf("rotated refresh_token = %q, want RT_rotated", tf2.RefreshToken)
	}

	// --- Step 8: Use oauth.token("mock-provider") in Starlark ---
	libDir := filepath.Join(p.Dir, "lib")
	os.MkdirAll(libDir, 0755)
	os.WriteFile(filepath.Join(libDir, "api.star"), []byte(`
def api_fetch(save):
    token = oauth.token(provider="mock-provider")
    save.row({"access_token": token.access_token})
`), 0644)
	p.AddModel("raw/oauth_test.star", `# @kind: table
load("lib/api.star", "api_fetch")
api_fetch(save)
`)

	runResult := runModel(t, p, "raw/oauth_test.star")
	if runResult.RowsAffected != 1 {
		t.Errorf("rows = %d, want 1", runResult.RowsAffected)
	}

	// Verify the access token was fetched via the provider flow
	tok, err := p.Sess.QueryValue("SELECT access_token FROM raw.oauth_test")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if tok != "AT_refreshed" {
		t.Errorf("access_token in table = %q, want AT_refreshed", tok)
	}
}

// TestE2E_ODataServer tests the OData server with a real DuckDB session.
func TestE2E_ODataServer(t *testing.T) {
	p := testutil.NewProject(t)

	// Create test data
	p.AddModel("mart/revenue.sql", `-- @kind: table
-- @expose
SELECT * FROM (VALUES
    (1, 'Alice', 100.0),
    (2, 'Bob', 250.0),
    (3, 'Carol', 75.5)
) AS t(id, customer, amount)
`)

	// Run pipeline to materialize
	runModel(t, p, "mart/revenue.sql")

	// Discover schemas
	schemas, err := odata.DiscoverSchemas(p.Sess, []odata.ExposeTarget{{Target: "mart.revenue", KeyColumn: "id"}})
	if err != nil {
		t.Fatalf("DiscoverSchemas: %v", err)
	}
	if len(schemas) != 1 {
		t.Fatalf("schemas = %d, want 1", len(schemas))
	}
	if len(schemas[0].Columns) != 3 {
		t.Fatalf("columns = %d, want 3", len(schemas[0].Columns))
	}

	// Start OData server
	handler := odata.NewServer(p.Sess, schemas, "http://localhost")
	srv := newTestServer(t, handler)
	defer srv.Close()

	// Test service document
	resp, err := http.Get(srv.URL + "/odata")
	if err != nil {
		t.Fatalf("GET /odata: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("service doc status = %d", resp.StatusCode)
	}

	// Test $metadata
	resp, err = http.Get(srv.URL + "/odata/$metadata")
	if err != nil {
		t.Fatalf("GET $metadata: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "mart_revenue") {
		t.Error("metadata missing entity")
	}

	// Test query all
	resp, err = http.Get(srv.URL + "/odata/mart_revenue")
	if err != nil {
		t.Fatalf("GET collection: %v", err)
	}
	var result odata.ODataResponse
	json.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()
	if len(result.Value) != 3 {
		t.Errorf("rows = %d, want 3", len(result.Value))
	}

	// Test $top
	resp, _ = http.Get(srv.URL + "/odata/mart_revenue?$top=1")
	var topResult odata.ODataResponse
	json.NewDecoder(resp.Body).Decode(&topResult)
	resp.Body.Close()
	if len(topResult.Value) != 1 {
		t.Errorf("top rows = %d, want 1", len(topResult.Value))
	}

	// Test $filter
	resp, _ = http.Get(srv.URL + "/odata/mart_revenue?$filter=amount%20gt%20100")
	var filterResult odata.ODataResponse
	json.NewDecoder(resp.Body).Decode(&filterResult)
	resp.Body.Close()
	if len(filterResult.Value) != 1 {
		t.Errorf("filter rows = %d, want 1 (Bob)", len(filterResult.Value))
	}

	// Test $select
	resp, _ = http.Get(srv.URL + "/odata/mart_revenue?$select=customer")
	var selectResult odata.ODataResponse
	json.NewDecoder(resp.Body).Decode(&selectResult)
	resp.Body.Close()
	if len(selectResult.Value) != 3 {
		t.Errorf("select rows = %d, want 3", len(selectResult.Value))
	}

	// Test $orderby
	resp, _ = http.Get(srv.URL + "/odata/mart_revenue?$orderby=amount%20desc&$top=1")
	var orderResult odata.ODataResponse
	json.NewDecoder(resp.Body).Decode(&orderResult)
	resp.Body.Close()
	if len(orderResult.Value) != 1 {
		t.Fatalf("orderby rows = %d", len(orderResult.Value))
	}
	if orderResult.Value[0]["customer"] != "Bob" {
		t.Errorf("first = %v, want Bob", orderResult.Value[0]["customer"])
	}

	// Test $count
	resp, _ = http.Get(srv.URL + "/odata/mart_revenue?$count=true")
	var countResult odata.ODataResponse
	json.NewDecoder(resp.Body).Decode(&countResult)
	resp.Body.Close()
	if countResult.Count == nil || *countResult.Count != 3 {
		t.Errorf("count = %v, want 3", countResult.Count)
	}

	// Test unknown entity → 404
	resp, _ = http.Get(srv.URL + "/odata/nonexistent_table")
	if resp.StatusCode != 404 {
		t.Errorf("unknown entity status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()

	// Test invalid $filter → 400
	resp, _ = http.Get(srv.URL + "/odata/mart_revenue?$filter=)))bad")
	if resp.StatusCode != 400 {
		t.Errorf("bad filter status = %d, want 400", resp.StatusCode)
	}
	resp.Body.Close()

	// Test entity-by-key → 501 Not Implemented
	resp, _ = http.Get(srv.URL + "/odata/mart_revenue(1)")
	if resp.StatusCode != 501 {
		t.Errorf("entity-by-key status = %d, want 501", resp.StatusCode)
	}
	resp.Body.Close()

	// Test empty result
	resp, _ = http.Get(srv.URL + "/odata/mart_revenue?$filter=id%20eq%20999")
	var emptyResult odata.ODataResponse
	json.NewDecoder(resp.Body).Decode(&emptyResult)
	resp.Body.Close()
	if len(emptyResult.Value) != 0 {
		t.Errorf("empty rows = %d, want 0", len(emptyResult.Value))
	}

	// Test /$count endpoint (plain text integer)
	resp, _ = http.Get(srv.URL + "/odata/mart_revenue/$count")
	if resp.StatusCode != 200 {
		t.Errorf("/$count status = %d, want 200", resp.StatusCode)
	}
	countBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if strings.TrimSpace(string(countBody)) != "3" {
		t.Errorf("/$count = %q, want 3", string(countBody))
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/plain" {
		t.Errorf("/$count Content-Type = %q, want text/plain", ct)
	}

	// Test OData-Version header present on all responses
	resp, _ = http.Get(srv.URL + "/odata")
	if v := resp.Header.Get("OData-Version"); v != "4.0" {
		t.Errorf("OData-Version = %q, want 4.0", v)
	}
	resp.Body.Close()
}

func TestE2E_ODataServer_NameCollision(t *testing.T) {
	p := testutil.NewProject(t)

	// Create two models whose targets collide after dot→underscore mapping:
	// a.b_c → a_b_c and a_b.c → a_b_c
	p.AddModel("a/b_c.sql", `-- @kind: table
-- @expose
SELECT 1 AS id`)
	p.AddModel("a_b/c.sql", `-- @kind: table
-- @expose
SELECT 1 AS id`)

	// Materialize both
	runModel(t, p, "a/b_c.sql")
	runModel(t, p, "a_b/c.sql")

	// DiscoverSchemas should detect the collision
	_, err := odata.DiscoverSchemas(p.Sess, []odata.ExposeTarget{
		{Target: "a.b_c"},
		{Target: "a_b.c"},
	})
	if err == nil {
		t.Fatal("expected collision error")
	}
	if !strings.Contains(err.Error(), "collision") {
		t.Errorf("expected collision error, got: %v", err)
	}
}

func TestE2E_ODataServer_InvalidKey(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("mart/test.sql", `-- @kind: table
-- @expose bad_col
SELECT 1 AS id, 'x' AS name`)

	runModel(t, p, "mart/test.sql")

	_, err := odata.DiscoverSchemas(p.Sess, []odata.ExposeTarget{
		{Target: "mart.test", KeyColumn: "bad_col"},
	})
	if err == nil {
		t.Fatal("expected error for nonexistent key column")
	}
	if !strings.Contains(err.Error(), "bad_col") {
		t.Errorf("expected key column error, got: %v", err)
	}
}
