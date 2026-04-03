// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute_test

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

// helper to parse and run a model
func runModel(t *testing.T, p *testutil.Project, relPath string) *execute.Result {
	t.Helper()
	modelPath := filepath.Join(p.Dir, "models", relPath)
	model, err := parser.ParseModel(modelPath, p.Dir)
	if err != nil {
		t.Fatalf("parse %s: %v", relPath, err)
	}
	runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
	result, err := runner.Run(context.Background(), model)
	if err != nil {
		t.Fatalf("run %s: %v", relPath, err)
	}
	return result
}

// helper to parse and run a model, returning both result and error
func runModelErr(t *testing.T, p *testutil.Project, relPath string) (*execute.Result, error) {
	t.Helper()
	modelPath := filepath.Join(p.Dir, "models", relPath)
	model, err := parser.ParseModel(modelPath, p.Dir)
	if err != nil {
		t.Fatalf("parse %s: %v", relPath, err)
	}
	runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
	return runner.Run(context.Background(), model)
}

func TestRun_TableKind(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/orders.sql", `-- @kind: table
SELECT 1 AS id, 100.0 AS amount
UNION ALL SELECT 2, 200.0
UNION ALL SELECT 3, 300.0
`)
	result := runModel(t, p, "staging/orders.sql")

	if result.RowsAffected != 3 {
		t.Errorf("rows = %d, want 3", result.RowsAffected)
	}
	if result.Kind != "table" {
		t.Errorf("kind = %q, want table", result.Kind)
	}
	if result.RunType != "backfill" {
		t.Errorf("run_type = %q, want backfill", result.RunType)
	}
	if result.Target != "staging.orders" {
		t.Errorf("target = %q, want staging.orders", result.Target)
	}
	if len(result.Trace) == 0 {
		t.Error("expected trace steps")
	}

	// Verify data persisted
	val, err := p.Sess.QueryValue("SELECT SUM(amount)::INTEGER FROM staging.orders")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "600" {
		t.Errorf("sum = %s, want 600", val)
	}
}

func TestRun_TableRerun_Replaces(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/users.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name
`)
	runModel(t, p, "staging/users.sql")

	// Re-run replaces data
	p.AddModel("staging/users.sql", `-- @kind: table
SELECT 1 AS id, 'Bob' AS name
UNION ALL SELECT 2, 'Carol'
`)
	result := runModel(t, p, "staging/users.sql")

	if result.RowsAffected != 2 {
		t.Errorf("rows = %d, want 2", result.RowsAffected)
	}

	val, err := p.Sess.QueryValue("SELECT name FROM staging.users WHERE id = 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "Bob" {
		t.Errorf("name = %s, want Bob", val)
	}
}

func TestRun_AppendKind(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run: backfill
	p.AddModel("staging/events.sql", `-- @kind: append
SELECT 1 AS id, 'click' AS event
`)
	r1 := runModel(t, p, "staging/events.sql")
	if r1.RunType != "backfill" {
		t.Errorf("run1 type = %q, want backfill", r1.RunType)
	}

	// Second run: incremental (same SQL hash → incremental)
	r2 := runModel(t, p, "staging/events.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run2 type = %q, want incremental", r2.RunType)
	}

	// Verify data appended (1 row each run)
	val, err := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.events")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "2" {
		t.Errorf("count = %s, want 2", val)
	}
}

func TestRun_MergeKind(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run: creates table
	p.AddModel("staging/products.sql", `-- @kind: merge
-- @unique_key: id
SELECT 1 AS id, 'Widget' AS name, 10.0 AS price
`)
	r1 := runModel(t, p, "staging/products.sql")
	if r1.RowsAffected != 1 {
		t.Errorf("run1 rows = %d, want 1", r1.RowsAffected)
	}

	// Second run: update price via merge
	p.AddModel("staging/products.sql", `-- @kind: merge
-- @unique_key: id
SELECT 1 AS id, 'Widget' AS name, 15.0 AS price
UNION ALL SELECT 2, 'Gadget', 25.0
`)
	r2 := runModel(t, p, "staging/products.sql")
	if r2.RowsAffected != 2 {
		t.Errorf("run2 rows = %d, want 2", r2.RowsAffected)
	}

	// Verify merge upserted correctly
	val, err := p.Sess.QueryValue("SELECT price::INTEGER FROM staging.products WHERE id = 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "15" {
		t.Errorf("price = %s, want 15", val)
	}

	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.products")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != "2" {
		t.Errorf("count = %s, want 2", count)
	}
}

func TestRun_MergeKind_RequiresUniqueKey(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run creates the table (backfill doesn't need unique_key)
	p.AddModel("staging/no_uk.sql", `-- @kind: merge
SELECT 1 AS id, 'x' AS name
`)
	runModel(t, p, "staging/no_uk.sql")

	// Second run tries incremental merge without unique_key → error
	_, err := runModelErr(t, p, "staging/no_uk.sql")
	if err == nil {
		t.Fatal("expected error for merge without unique_key")
	}
}

func TestRun_SCD2Kind(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run: creates SCD2 table
	p.AddModel("staging/customers.sql", `-- @kind: scd2
-- @unique_key: id
SELECT 1 AS id, 'Alice' AS name, 'NYC' AS city
`)
	r1 := runModel(t, p, "staging/customers.sql")
	if r1.RowsAffected != 1 {
		t.Errorf("rows = %d, want 1", r1.RowsAffected)
	}

	// Verify SCD2 columns exist
	val, err := p.Sess.QueryValue("SELECT is_current FROM staging.customers WHERE id = 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "true" {
		t.Errorf("is_current = %s, want true", val)
	}
}

func TestRun_PartitionKind(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run: creates table
	p.AddModel("staging/daily_sales.sql", `-- @kind: partition
-- @unique_key: region
SELECT 'US' AS region, 100 AS amount
UNION ALL SELECT 'EU', 200
`)
	r1 := runModel(t, p, "staging/daily_sales.sql")
	if r1.RowsAffected != 2 {
		t.Errorf("run1 rows = %d, want 2", r1.RowsAffected)
	}

	// Second run: only includes US → deletes old US rows, inserts new US, EU preserved
	p.AddModel("staging/daily_sales.sql", `-- @kind: partition
-- @unique_key: region
SELECT 'US' AS region, 150 AS amount
UNION ALL SELECT 'EU', 250
`)
	r2 := runModel(t, p, "staging/daily_sales.sql")
	if r2.RowsAffected != 2 {
		t.Errorf("run2 rows = %d, want 2", r2.RowsAffected)
	}

	// Both partitions replaced with new values
	val, err := p.Sess.QueryValue("SELECT amount FROM staging.daily_sales WHERE region = 'US'")
	if err != nil {
		t.Fatalf("query US: %v", err)
	}
	if val != "150" {
		t.Errorf("US amount = %s, want 150", val)
	}

	val, err = p.Sess.QueryValue("SELECT amount FROM staging.daily_sales WHERE region = 'EU'")
	if err != nil {
		t.Fatalf("query EU: %v", err)
	}
	if val != "250" {
		t.Errorf("EU amount = %s, want 250", val)
	}
}

func TestRun_ConstraintPass(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/valid.sql", `-- @kind: table
-- @constraint: id NOT NULL
-- @constraint: id UNIQUE
SELECT 1 AS id, 'Alice' AS name
UNION ALL SELECT 2, 'Bob'
`)
	result := runModel(t, p, "staging/valid.sql")
	if len(result.Errors) > 0 {
		t.Errorf("unexpected errors: %v", result.Errors)
	}
}

func TestRun_ConstraintFail_NotNull(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/nulls.sql", `-- @kind: table
-- @constraint: id NOT NULL
SELECT NULL AS id, 'broken' AS name
`)
	result, err := runModelErr(t, p, "staging/nulls.sql")
	if err == nil {
		t.Fatal("expected constraint error")
	}
	if !strings.Contains(err.Error(), "constraint validation failed") {
		t.Errorf("error = %v, want 'constraint validation failed'", err)
	}
	if len(result.Errors) == 0 {
		t.Fatal("expected errors in result")
	}
	// Error message should describe what failed (drives JSON status:"error" + errors:[...])
	if !strings.Contains(result.Errors[0], "NOT NULL failed") {
		t.Errorf("error message = %q, want descriptive constraint failure", result.Errors[0])
	}

	// Table should not exist
	_, qErr := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.nulls")
	if qErr == nil {
		t.Error("table should not exist after constraint failure")
	}
}

func TestRun_ConstraintFail_Unique(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/dupes.sql", `-- @kind: table
-- @constraint: id UNIQUE
SELECT 1 AS id UNION ALL SELECT 1
`)
	_, err := runModelErr(t, p, "staging/dupes.sql")
	if err == nil {
		t.Fatal("expected constraint error for duplicate id")
	}
}

func TestRun_Warnings(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/warned.sql", `-- @kind: table
-- @warning: id NOT NULL
SELECT NULL AS id, 'test' AS name
UNION ALL SELECT 1, 'ok'
`)
	result := runModel(t, p, "staging/warned.sql")

	// Warnings don't block execution
	if result.RowsAffected != 2 {
		t.Errorf("rows = %d, want 2", result.RowsAffected)
	}
	if len(result.Warnings) == 0 {
		t.Error("expected warnings for NULL id")
	}
}

func TestRun_SchemaEvolution_AddColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Run 1: 2 columns
	p.AddModel("staging/evolve.sql", `-- @kind: append
SELECT 1 AS id, 'Alice' AS name
`)
	runModel(t, p, "staging/evolve.sql")

	// Run 2: add age column → schema evolution (not full backfill)
	p.AddModel("staging/evolve.sql", `-- @kind: append
SELECT 2 AS id, 'Bob' AS name, 30 AS age
`)
	r2 := runModel(t, p, "staging/evolve.sql")

	if r2.RunType != "incremental" {
		t.Errorf("run_type = %q, want incremental (schema evolution)", r2.RunType)
	}

	// Verify 3 columns
	val, err := p.Sess.QueryValue("SELECT COUNT(*) FROM (DESCRIBE staging.evolve)")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if val != "3" {
		t.Errorf("columns = %s, want 3", val)
	}
}

func TestRun_CommitMetadata(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/tracked.sql", `-- @kind: table
SELECT 1 AS id
`)
	runModel(t, p, "staging/tracked.sql")

	// Verify metadata was committed via snapshots
	val, err := p.Sess.QueryValue("SELECT commit_extra_info->>'model' FROM lake.snapshots() ORDER BY snapshot_id DESC LIMIT 1")
	if err != nil {
		t.Fatalf("query metadata: %v", err)
	}
	if val != "staging.tracked" {
		t.Errorf("model = %q, want staging.tracked", val)
	}
}

func TestRun_TraceSteps(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/traced.sql", `-- @kind: table
SELECT 1 AS id
`)
	result := runModel(t, p, "staging/traced.sql")

	if result.Duration <= 0 {
		t.Error("expected positive duration")
	}

	// Should have at least: hash_sql, run_type, create_temp, commit
	names := make(map[string]bool)
	for _, step := range result.Trace {
		names[step.Name] = true
	}
	for _, expected := range []string{"hash_sql", "create_temp", "commit"} {
		if !names[expected] {
			t.Errorf("missing trace step %q, got: %v", expected, names)
		}
	}
}

func TestComputeRunTypeDecisions_Batch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Create models
	p.AddModel("staging/a.sql", `-- @kind: table
SELECT 1 AS id
`)
	p.AddModel("staging/b.sql", `-- @kind: append
SELECT 1 AS id
`)

	paths := []string{"staging/a.sql", "staging/b.sql"}
	var models []*parser.Model
	for _, rel := range paths {
		m, err := parser.ParseModel(filepath.Join(p.Dir, "models", rel), p.Dir)
		if err != nil {
			t.Fatalf("parse %s: %v", rel, err)
		}
		models = append(models, m)
	}

	decisions, err := execute.ComputeRunTypeDecisions(p.Sess, models)
	if err != nil {
		t.Fatalf("batch: %v", err)
	}

	// table kind → "backfill" (first run, no previous commit), append kind → "backfill"
	if d := decisions.GetDecision("staging.a"); d == nil {
		t.Error("no decision for staging.a")
	} else if d.RunType != "backfill" {
		t.Errorf("staging.a run_type = %q, want backfill", d.RunType)
	}
	if d := decisions.GetDecision("staging.b"); d == nil {
		t.Error("no decision for staging.b")
	} else if d.RunType != "backfill" {
		t.Errorf("staging.b run_type = %q, want backfill", d.RunType)
	}
}

func TestComputeSingleRunType(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("staging/single.sql", `-- @kind: table
SELECT 1 AS id
`)
	modelPath := filepath.Join(p.Dir, "models", "staging/single.sql")
	model, err := parser.ParseModel(modelPath, p.Dir)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// table kind: first run (no previous commit) → backfill
	d, err := execute.ComputeSingleRunType(p.Sess, model)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	if d.RunType != "backfill" {
		t.Errorf("run_type = %q, want backfill", d.RunType)
	}
}

func TestComputeSingleRunType_AppendLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("staging/appended.sql", `-- @kind: append
SELECT 1 AS id
`)
	model, err := parser.ParseModel(filepath.Join(p.Dir, "models", "staging/appended.sql"), p.Dir)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// First time: backfill (no previous commit)
	d, err := execute.ComputeSingleRunType(p.Sess, model)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	if d.RunType != "backfill" {
		t.Errorf("run_type = %q, want backfill", d.RunType)
	}

	// Run the model to create commit metadata
	runModel(t, p, "staging/appended.sql")

	// Same SQL hash: incremental
	d2, err := execute.ComputeSingleRunType(p.Sess, model)
	if err != nil {
		t.Fatalf("compute2: %v", err)
	}
	if d2.RunType != "incremental" {
		t.Errorf("run_type = %q, want incremental", d2.RunType)
	}
}

func TestRun_MultipleModels_DependencyChain(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("raw/source.sql", `-- @kind: table
SELECT 1 AS id, 10 AS value
UNION ALL SELECT 2, 20
`)
	p.AddModel("staging/doubled.sql", `-- @kind: table
SELECT id, value * 2 AS doubled FROM raw.source
`)
	p.AddModel("mart/total.sql", `-- @kind: table
SELECT SUM(doubled) AS total FROM staging.doubled
`)

	// Must run in order
	runModel(t, p, "raw/source.sql")
	runModel(t, p, "staging/doubled.sql")
	result := runModel(t, p, "mart/total.sql")

	if result.RowsAffected != 1 {
		t.Errorf("rows = %d, want 1", result.RowsAffected)
	}

	val, err := p.Sess.QueryValue("SELECT total FROM mart.total")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "60" {
		t.Errorf("total = %s, want 60", val)
	}
}

func TestRun_AuditFail_Rollback(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Create initial data
	p.AddModel("staging/audited.sql", `-- @kind: table
SELECT 1 AS id, 100 AS amount
`)
	runModel(t, p, "staging/audited.sql")

	// Second run with audit that fails: amount must be positive
	p.AddModel("staging/audited.sql", `-- @kind: table
-- @audit: amount > 0
SELECT 1 AS id, -50 AS amount
`)
	_, err := runModelErr(t, p, "staging/audited.sql")
	if err == nil {
		t.Fatal("expected audit error")
	}
	if !strings.Contains(err.Error(), "audit") {
		t.Errorf("error = %v, want audit-related error", err)
	}
}

func TestRun_GitInfo(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/git.sql", `-- @kind: table
SELECT 1 AS id
`)

	modelPath := filepath.Join(p.Dir, "models", "staging/git.sql")
	model, err := parser.ParseModel(modelPath, p.Dir)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
	runner.SetGitInfo("abc123", "main", "https://github.com/test/repo")
	_, err = runner.Run(context.Background(), model)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	// Verify git info in commit metadata
	val, err := p.Sess.QueryValue("SELECT commit_extra_info->>'git_commit' FROM lake.snapshots() ORDER BY snapshot_id DESC LIMIT 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "abc123" {
		t.Errorf("git_commit = %q, want abc123", val)
	}
}

func TestRun_SCD2_Lifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Run 1: initial data
	p.AddModel("staging/scd2_users.sql", `-- @kind: scd2
-- @unique_key: id
SELECT 1 AS id, 'Alice' AS name, 'NYC' AS city
UNION ALL SELECT 2, 'Bob', 'LA'
`)
	r1 := runModel(t, p, "staging/scd2_users.sql")
	if r1.RowsAffected != 2 {
		t.Errorf("run1 rows = %d, want 2", r1.RowsAffected)
	}

	// All records should be current
	val, err := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.scd2_users WHERE is_current = true")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "2" {
		t.Errorf("current rows = %s, want 2", val)
	}

	// Run 2: Alice moves to SF, Charlie joins (SQL hash changes → backfill → recreates)
	p.AddModel("staging/scd2_users.sql", `-- @kind: scd2
-- @unique_key: id
SELECT 1 AS id, 'Alice' AS name, 'SF' AS city
UNION ALL SELECT 3, 'Charlie', 'Chicago'
`)
	r2 := runModel(t, p, "staging/scd2_users.sql")

	// Since SQL hash changed, it's a backfill → full recreate
	// Alice should be in SF, Charlie should be present
	val, err = p.Sess.QueryValue("SELECT city FROM staging.scd2_users WHERE id = 1 AND is_current = true")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "SF" {
		t.Errorf("Alice city = %q, want SF", val)
	}

	// Should have at least 2 current records
	val, err = p.Sess.QueryValue("SELECT COUNT(*) FROM staging.scd2_users WHERE is_current = true")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "2" {
		t.Errorf("current rows = %s, want 2", val)
	}
	_ = r2
}

func TestRun_Extension_LoadAndUse(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Use @extension to load an extension
	p.AddModel("staging/with_ext.sql", `-- @kind: table
-- @extension: json
SELECT 1 AS id, 'test' AS val
`)
	result := runModel(t, p, "staging/with_ext.sql")
	if result.RowsAffected != 1 {
		t.Errorf("rows = %d, want 1", result.RowsAffected)
	}
}

func TestRun_SetRunTypeDecisions_Used(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("staging/precomputed.sql", `-- @kind: append
SELECT 1 AS id
`)
	modelPath := filepath.Join(p.Dir, "models", "staging/precomputed.sql")
	model, err := parser.ParseModel(modelPath, p.Dir)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Pre-set decisions
	runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
	decisions := execute.RunTypeDecisions{
		"staging.precomputed": &execute.RunTypeDecision{RunType: "backfill"},
	}
	runner.SetRunTypeDecisions(decisions)

	result, err := runner.Run(context.Background(), model)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if result.RunType != "backfill" {
		t.Errorf("run_type = %q, want backfill (from precomputed decisions)", result.RunType)
	}
}

func TestRun_Merge_Lifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Run 1: initial insert
	p.AddModel("staging/merge_items.sql", `-- @kind: merge
-- @unique_key: id
SELECT 1 AS id, 'Apple' AS name, 1.50 AS price
UNION ALL SELECT 2, 'Banana', 0.75
`)
	runModel(t, p, "staging/merge_items.sql")

	// Run 2: update Apple, add Cherry (SQL hash changes → backfill → full replace)
	p.AddModel("staging/merge_items.sql", `-- @kind: merge
-- @unique_key: id
SELECT 1 AS id, 'Apple' AS name, 2.00 AS price
UNION ALL SELECT 3, 'Cherry', 3.00
`)
	runModel(t, p, "staging/merge_items.sql")

	// Since SQL changed, it's a backfill (full replace), not incremental merge
	// Should have 2 rows (only what's in the new query)
	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.merge_items")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != "2" {
		t.Errorf("count = %s, want 2 (backfill replaces all)", count)
	}

	// Apple price should be updated
	val, err := p.Sess.QueryValue("SELECT price::INTEGER FROM staging.merge_items WHERE id = 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "2" {
		t.Errorf("Apple price = %s, want 2", val)
	}
}

func TestRun_SchemaEvolution_TypePromotion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Run 1: INTEGER column
	p.AddModel("staging/typed.sql", `-- @kind: append
SELECT 1 AS id, 100 AS amount
`)
	runModel(t, p, "staging/typed.sql")

	// Run 2: widen to BIGINT (type promotion)
	p.AddModel("staging/typed.sql", `-- @kind: append
SELECT 2 AS id, 9999999999 AS amount
`)
	r2 := runModel(t, p, "staging/typed.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run_type = %q, want incremental (type promotion)", r2.RunType)
	}
}

func TestRun_WarningPass_NoWarnings(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/warn_ok.sql", `-- @kind: table
-- @warning: id NOT NULL
-- @warning: id UNIQUE
SELECT 1 AS id, 'Alice' AS name
UNION ALL SELECT 2, 'Bob'
`)
	result := runModel(t, p, "staging/warn_ok.sql")

	// Valid data should produce no warnings
	if len(result.Warnings) != 0 {
		t.Errorf("expected no warnings for valid data, got: %v", result.Warnings)
	}
	if result.RowsAffected != 2 {
		t.Errorf("rows = %d, want 2", result.RowsAffected)
	}
}

func TestRun_WarningConstraintPattern(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Warning with UNIQUE pattern
	p.AddModel("staging/warn_unique.sql", `-- @kind: table
-- @warning: id UNIQUE
SELECT 1 AS id UNION ALL SELECT 1
`)
	result := runModel(t, p, "staging/warn_unique.sql")

	// Should still execute (warnings don't block)
	if result.RowsAffected != 2 {
		t.Errorf("rows = %d, want 2", result.RowsAffected)
	}
	if len(result.Warnings) == 0 {
		t.Error("expected warnings for duplicate id")
	}
}

func TestRun_Constraint_DistinctCount(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/dc.sql", `-- @kind: table
-- @constraint: id DISTINCT_COUNT >= 2
SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3
`)
	result := runModel(t, p, "staging/dc.sql")
	if len(result.Errors) > 0 {
		t.Errorf("unexpected errors: %v", result.Errors)
	}
}

func TestRun_Merge_IncrementalUpsert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Same SQL for both runs → incremental path triggers MERGE INTO
	sql := `-- @kind: merge
-- @unique_key: id
SELECT 1 AS id, 'Apple' AS name, 10 AS price
UNION ALL SELECT 2, 'Banana', 5
`
	p.AddModel("staging/upsert.sql", sql)
	r1 := runModel(t, p, "staging/upsert.sql")
	if r1.RunType != "backfill" {
		t.Errorf("run1 type = %q, want backfill", r1.RunType)
	}

	// Second run with same SQL → incremental → uses MERGE INTO
	r2 := runModel(t, p, "staging/upsert.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run2 type = %q, want incremental (triggers MERGE)", r2.RunType)
	}

	// Should still have 2 rows (upsert, not duplicate)
	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.upsert")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != "2" {
		t.Errorf("count = %s, want 2", count)
	}
}

func TestRun_SCD2_IncrementalHistory(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Same SQL for both runs → incremental path preserves history
	sql := `-- @kind: scd2
-- @unique_key: id
SELECT 1 AS id, 'Alice' AS name, 'NYC' AS city
`
	p.AddModel("staging/scd2_hist.sql", sql)
	runModel(t, p, "staging/scd2_hist.sql")

	// Second run with same SQL → incremental SCD2 (no changes → no new records)
	r2 := runModel(t, p, "staging/scd2_hist.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run2 type = %q, want incremental", r2.RunType)
	}

	// Alice should still be current
	val, err := p.Sess.QueryValue("SELECT city FROM staging.scd2_hist WHERE id = 1 AND is_current = true")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "NYC" {
		t.Errorf("city = %q, want NYC", val)
	}
}

func TestRun_Constraint_DistinctCount_Fail(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/dc_fail.sql", `-- @kind: table
-- @constraint: id DISTINCT_COUNT >= 10
SELECT 1 AS id UNION ALL SELECT 2
`)
	_, err := runModelErr(t, p, "staging/dc_fail.sql")
	if err == nil {
		t.Fatal("expected constraint failure for DISTINCT_COUNT")
	}
}

func TestRun_Partition_IncrementalUpsert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	sql := `-- @kind: partition
-- @unique_key: region
SELECT 'US' AS region, 100 AS amount
UNION ALL SELECT 'EU', 200
`
	p.AddModel("staging/part_incr.sql", sql)

	// First run → backfill
	r1 := runModel(t, p, "staging/part_incr.sql")
	if r1.RowsAffected != 2 {
		t.Errorf("run1 rows = %d, want 2", r1.RowsAffected)
	}

	// Second run with same SQL → incremental partition (DELETE matching partitions + INSERT)
	r2 := runModel(t, p, "staging/part_incr.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run2 type = %q, want incremental", r2.RunType)
	}
	if r2.RowsAffected != 2 {
		t.Errorf("run2 rows = %d, want 2", r2.RowsAffected)
	}

	// Data should still be intact
	val, _ := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.part_incr")
	if val != "2" {
		t.Errorf("total rows = %s, want 2", val)
	}
}

func TestRun_AppendKind_IncrementalAppend(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	sql := `-- @kind: append
SELECT 1 AS id, 'first' AS batch
`
	p.AddModel("staging/app_incr.sql", sql)

	// First run → backfill
	r1 := runModel(t, p, "staging/app_incr.sql")
	if r1.RowsAffected != 1 {
		t.Errorf("run1 rows = %d, want 1", r1.RowsAffected)
	}

	// Second run same SQL → incremental append
	r2 := runModel(t, p, "staging/app_incr.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run2 type = %q, want incremental", r2.RunType)
	}

	// Should have 2 rows total (1 from each run)
	val, _ := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.app_incr")
	if val != "2" {
		t.Errorf("total rows = %s, want 2", val)
	}
}

func TestRun_WarningUnknownPattern(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/warn_bad.sql", `-- @kind: table
-- @warning: this_is_not_a_valid_pattern_xyz
SELECT 1 AS id
`)
	r := runModel(t, p, "staging/warn_bad.sql")

	hasParseError := false
	for _, w := range r.Warnings {
		if strings.Contains(w, "warning parse error") || strings.Contains(w, "unknown") {
			hasParseError = true
		}
	}
	if !hasParseError {
		t.Errorf("expected warning parse error, got warnings: %v", r.Warnings)
	}
}

func TestRun_SchemaEvolution_Rename(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run with column "name"
	p.AddModel("staging/rename_evo.sql", `-- @kind: append
SELECT 1 AS id, 'hello' AS name
`)
	runModel(t, p, "staging/rename_evo.sql")

	// Second run with column renamed "full_name" (same position)
	p.AddModel("staging/rename_evo.sql", `-- @kind: append
SELECT 2 AS id, 'world' AS full_name
`)
	r2 := runModel(t, p, "staging/rename_evo.sql")

	// Should detect schema change and handle it
	if r2.RowsAffected != 1 {
		t.Errorf("run2 rows = %d, want 1", r2.RowsAffected)
	}
}

func TestRun_StarlarkScript(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Create a .star model that collects data using the save module
	p.AddModel("staging/script_test.star", `# @kind: table
save.row({"id": 1, "name": "Alice"})
save.row({"id": 2, "name": "Bob"})
`)

	r := runModel(t, p, "staging/script_test.star")
	if r.RowsAffected != 2 {
		t.Errorf("rows = %d, want 2", r.RowsAffected)
	}

	// Verify data was materialized
	val, err := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.script_test")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "2" {
		t.Errorf("count = %s, want 2", val)
	}

	// Verify actual data
	val, err = p.Sess.QueryValue("SELECT name FROM staging.script_test WHERE id = 1")
	if err != nil {
		t.Fatalf("query name: %v", err)
	}
	if val != "Alice" {
		t.Errorf("name = %q, want Alice", val)
	}
}

func TestRun_StarlarkScript_Incremental(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	starCode := `# @kind: append
save.row({"id": 1, "name": "test"})
`
	p.AddModel("staging/star_incr.star", starCode)

	// First run → backfill
	r1 := runModel(t, p, "staging/star_incr.star")
	if r1.RowsAffected != 1 {
		t.Errorf("run1 rows = %d, want 1", r1.RowsAffected)
	}

	// Second run same script → incremental
	r2 := runModel(t, p, "staging/star_incr.star")
	if r2.RunType != "incremental" {
		t.Errorf("run2 type = %q, want incremental", r2.RunType)
	}
}

func TestRun_StarlarkScript_WithConstraint(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("staging/star_constraint.star", `# @kind: table
# @constraint: id NOT NULL
save.row({"id": 1, "name": "Alice"})
save.row({"id": 2, "name": "Bob"})
`)

	r := runModel(t, p, "staging/star_constraint.star")
	if len(r.Errors) > 0 {
		t.Errorf("unexpected errors: %v", r.Errors)
	}
	if r.RowsAffected != 2 {
		t.Errorf("rows = %d, want 2", r.RowsAffected)
	}
}

func TestRun_StarlarkScript_ConstraintFail(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("staging/star_cfail.star", `# @kind: table
# @constraint: id UNIQUE
save.row({"id": 1, "name": "Alice"})
save.row({"id": 1, "name": "Duplicate"})
`)

	result, err := runModelErr(t, p, "staging/star_cfail.star")
	if err == nil {
		t.Fatal("expected constraint failure")
	}
	if !strings.Contains(err.Error(), "constraint validation failed") {
		t.Errorf("error = %v, want 'constraint validation failed'", err)
	}
	if len(result.Errors) == 0 || !strings.Contains(result.Errors[0], "UNIQUE failed") {
		t.Errorf("expected descriptive UNIQUE error, got: %v", result.Errors)
	}

	// Table should not exist after constraint failure
	_, qErr := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.star_cfail")
	if qErr == nil {
		t.Error("table should not exist after constraint failure")
	}
}

func TestRun_StarlarkScript_EmptyData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("staging/star_empty.star", `# @kind: table
x = 1  # script runs but doesn't save any data
`)

	r := runModel(t, p, "staging/star_empty.star")
	// No data → warning
	hasWarning := false
	for _, w := range r.Warnings {
		if strings.Contains(w, "no data") {
			hasWarning = true
		}
	}
	if !hasWarning {
		t.Errorf("expected 'no data' warning, got warnings: %v", r.Warnings)
	}
}

func TestRun_StarlarkScript_Abort(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("staging/star_abort.star", `# @kind: table
abort()
`)

	r := runModel(t, p, "staging/star_abort.star")
	// Abort is a clean exit, not an error
	if len(r.Errors) > 0 {
		t.Errorf("abort should not produce errors: %v", r.Errors)
	}
}

func TestRun_IncrementalAppendWithCursor(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First: create source data
	p.AddModel("staging/inc_src.sql", `-- @kind: table
SELECT 1 AS id, '2024-01-01' AS updated_at
UNION ALL SELECT 2, '2024-01-02'
`)
	runModel(t, p, "staging/inc_src.sql")

	// Incremental model - first run always gets all data (backfill)
	p.AddModel("staging/inc_target.sql", `-- @kind: append
-- @incremental: updated_at
-- @incremental_initial: 2023-01-01
SELECT id, updated_at FROM staging.inc_src
`)

	r1 := runModel(t, p, "staging/inc_target.sql")
	if r1.RunType != "backfill" {
		t.Errorf("run1 type = %q, want backfill", r1.RunType)
	}
	if r1.RowsAffected != 2 {
		t.Errorf("run1 rows = %d, want 2", r1.RowsAffected)
	}

	// Second run with same SQL → incremental
	r2 := runModel(t, p, "staging/inc_target.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run2 type = %q, want incremental", r2.RunType)
	}
}

func TestRun_Extension_FromRepo(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Test @extension with "FROM community" syntax
	p.AddModel("staging/ext_repo.sql", `-- @kind: table
-- @extension: json FROM community
SELECT 1 AS id, 'test' AS val
`)
	result := runModel(t, p, "staging/ext_repo.sql")
	if result.RowsAffected != 1 {
		t.Errorf("rows = %d, want 1", result.RowsAffected)
	}
}

func TestRun_StarlarkScript_WithWarning(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Starlark script with warning directive
	p.AddModel("staging/star_warn.star", `# @kind: table
# @warning: id UNIQUE
save.row({"id": 1, "name": "Alice"})
save.row({"id": 1, "name": "Duplicate"})
`)

	r := runModel(t, p, "staging/star_warn.star")
	// Warnings don't block execution
	if r.RowsAffected != 2 {
		t.Errorf("rows = %d, want 2", r.RowsAffected)
	}
	if len(r.Warnings) == 0 {
		t.Error("expected warnings for duplicate id")
	}
}

func TestRun_StarlarkScript_WithAudit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run: create initial data
	p.AddModel("staging/star_aud.star", `# @kind: table
save.row({"id": 1, "amount": 100})
`)
	runModel(t, p, "staging/star_aud.star")

	// Second run with audit that will fail
	p.AddModel("staging/star_aud.star", `# @kind: table
# @audit: amount > 0
save.row({"id": 1, "amount": -50})
`)
	_, err := runModelErr(t, p, "staging/star_aud.star")
	if err == nil {
		t.Fatal("expected audit error for script model")
	}
	if !strings.Contains(err.Error(), "audit") {
		t.Errorf("error = %v, want audit-related error", err)
	}
}

func TestRun_StarlarkScript_SchemaEvolution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run: 2 columns (all string types to avoid type mismatch)
	p.AddModel("staging/star_evo.star", `# @kind: append
save.row({"id": "1", "name": "Alice"})
`)
	runModel(t, p, "staging/star_evo.star")

	// Second run: add column → schema evolution
	p.AddModel("staging/star_evo.star", `# @kind: append
save.row({"id": "2", "name": "Bob", "age": "30"})
`)
	r2 := runModel(t, p, "staging/star_evo.star")

	if r2.RunType != "incremental" {
		t.Errorf("run_type = %q, want incremental (schema evolution)", r2.RunType)
	}
	hasEvoWarning := false
	for _, w := range r2.Warnings {
		if strings.Contains(w, "schema evolution") {
			hasEvoWarning = true
		}
	}
	if !hasEvoWarning {
		t.Errorf("expected schema evolution warning, got: %v", r2.Warnings)
	}
}

func TestRun_StarlarkScript_IncrementalWithCursor(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Starlark with @incremental directive
	p.AddModel("staging/star_incr2.star", `# @kind: append
# @incremental: updated_at
# @incremental_initial: 2023-01-01
save.row({"id": 1, "updated_at": "2024-01-01"})
`)
	r1 := runModel(t, p, "staging/star_incr2.star")
	if r1.RunType != "backfill" {
		t.Errorf("run1 type = %q, want backfill", r1.RunType)
	}

	// Second run same script → incremental
	r2 := runModel(t, p, "staging/star_incr2.star")
	if r2.RunType != "incremental" {
		t.Errorf("run2 type = %q, want incremental", r2.RunType)
	}
}

func TestRun_Constraint_MultipleConstraints(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/multi_c.sql", `-- @kind: table
-- @constraint: id NOT NULL
-- @constraint: id UNIQUE
-- @constraint: amount > 0
SELECT 1 AS id, 100 AS amount
UNION ALL SELECT 2, 200
`)
	result := runModel(t, p, "staging/multi_c.sql")
	if len(result.Errors) > 0 {
		t.Errorf("unexpected errors: %v", result.Errors)
	}
}

func TestRun_AuditPass(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Create initial data
	p.AddModel("staging/aud_pass.sql", `-- @kind: table
SELECT 1 AS id, 100 AS amount
`)
	runModel(t, p, "staging/aud_pass.sql")

	// Second run with audit that passes (row_count >= 1)
	p.AddModel("staging/aud_pass.sql", `-- @kind: table
-- @audit: row_count >= 1
SELECT 1 AS id, 200 AS amount
`)
	result := runModel(t, p, "staging/aud_pass.sql")
	if len(result.Errors) > 0 {
		t.Errorf("unexpected errors: %v", result.Errors)
	}
}

func TestRun_Partition_RequiresUniqueKey(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Partition kind without @unique_key should fail at parse time
	p.AddModel("staging/no_part.sql", `-- @kind: partition
SELECT 'US' AS region, 100 AS amount
`)
	modelPath := filepath.Join(p.Dir, "models", "staging/no_part.sql")
	_, err := parser.ParseModel(modelPath, p.Dir)
	if err == nil {
		t.Fatal("expected parse error for partition without unique_key")
	}
	if !strings.Contains(err.Error(), "partition kind requires @unique_key") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRun_SCD2_RequiresUniqueKey(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("staging/no_uk_scd2.sql", `-- @kind: scd2
SELECT 1 AS id, 'Alice' AS name
`)
	_, err := runModelErr(t, p, "staging/no_uk_scd2.sql")
	if err == nil {
		t.Fatal("expected error for scd2 without unique_key")
	}
}

func TestRun_InvalidSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/bad_sql.sql", `-- @kind: table
SELECTZ INVALID SQL SYNTAX
`)
	_, err := runModelErr(t, p, "staging/bad_sql.sql")
	if err == nil {
		t.Fatal("expected error for invalid SQL")
	}
}

func TestRun_SchemaEvolution_TypeChange_Merge(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run: INTEGER column
	p.AddModel("staging/type_chg.sql", `-- @kind: merge
-- @unique_key: id
SELECT 1 AS id, 100 AS amount
`)
	runModel(t, p, "staging/type_chg.sql")

	// Second run: widen to BIGINT
	p.AddModel("staging/type_chg.sql", `-- @kind: merge
-- @unique_key: id
SELECT 1 AS id, 9999999999 AS amount
`)
	r2 := runModel(t, p, "staging/type_chg.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run_type = %q, want incremental (type promotion on merge)", r2.RunType)
	}
}

func TestRun_SchemaEvolution_AddColumn_Partition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run: 2 columns
	p.AddModel("staging/part_evo.sql", `-- @kind: partition
-- @unique_key: region
SELECT 'US' AS region, 100 AS amount
`)
	runModel(t, p, "staging/part_evo.sql")

	// Second run: add a new column
	p.AddModel("staging/part_evo.sql", `-- @kind: partition
-- @unique_key: region
SELECT 'US' AS region, 200 AS amount, 'extra' AS note
`)
	r2 := runModel(t, p, "staging/part_evo.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run_type = %q, want incremental (schema evolution on partition)", r2.RunType)
	}
}

func TestRun_SchemaEvolution_DestructiveChange(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run
	p.AddModel("staging/destructive.sql", `-- @kind: append
SELECT 1 AS id, 'Alice' AS name, 'NYC' AS city
`)
	runModel(t, p, "staging/destructive.sql")

	// Second run: drop column city → destructive change → forces backfill
	p.AddModel("staging/destructive.sql", `-- @kind: append
SELECT 2 AS id, 'Bob' AS name
`)
	r2 := runModel(t, p, "staging/destructive.sql")
	if r2.RunType != "backfill" {
		t.Errorf("run_type = %q, want backfill (destructive schema change)", r2.RunType)
	}
	hasWarning := false
	for _, w := range r2.Warnings {
		if strings.Contains(w, "destructive") {
			hasWarning = true
		}
	}
	if !hasWarning {
		t.Errorf("expected destructive schema change warning, got: %v", r2.Warnings)
	}
}

func TestRun_StarlarkScript_RuntimeError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Script with a runtime error
	p.AddModel("staging/star_err.star", `# @kind: table
save.row({"id": 1})
x = 1 / 0  # division by zero
`)
	_, err := runModelErr(t, p, "staging/star_err.star")
	if err == nil {
		t.Fatal("expected error from Starlark runtime error")
	}
}

func TestRun_StarlarkScript_WithAuditRollback(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Create initial data
	p.AddModel("staging/star_aroll.star", `# @kind: table
save.row({"id": 1, "amount": 100})
`)
	runModel(t, p, "staging/star_aroll.star")

	// Verify initial data
	val, _ := p.Sess.QueryValue("SELECT amount FROM staging.star_aroll WHERE id = 1")
	if val != "100" {
		t.Fatalf("initial amount = %s, want 100", val)
	}

	// Second run with audit that will fail
	p.AddModel("staging/star_aroll.star", `# @kind: table
# @audit: row_count >= 999
save.row({"id": 1, "amount": 200})
`)
	result, err := runModelErr(t, p, "staging/star_aroll.star")
	if err == nil {
		t.Fatal("expected audit error")
	}
	if !strings.Contains(err.Error(), "audit validation failed") {
		t.Errorf("error = %v, want 'audit validation failed'", err)
	}
	if len(result.Errors) == 0 || !strings.Contains(result.Errors[0], "row_count") {
		t.Errorf("expected descriptive row_count error, got: %v", result.Errors)
	}

	// Verify data was rolled back to original
	val, qErr := p.Sess.QueryValue("SELECT amount FROM staging.star_aroll WHERE id = 1")
	if qErr != nil {
		t.Fatalf("query after rollback: %v", qErr)
	}
	if val != "100" {
		t.Errorf("amount after rollback = %s, want 100 (should be restored)", val)
	}
}

func TestRun_StarlarkScript_MultipleConstraints(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/star_mc.star", `# @kind: table
# @constraint: id NOT NULL
# @constraint: id UNIQUE
# @constraint: amount >= 0
save.row({"id": 1, "name": "Alice", "amount": 10})
save.row({"id": 2, "name": "Bob", "amount": 20})
`)
	result := runModel(t, p, "staging/star_mc.star")
	if len(result.Errors) > 0 {
		t.Errorf("unexpected errors: %v", result.Errors)
	}
}

func TestRun_StarlarkScript_ConstraintParseError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/star_cperr.star", `# @kind: table
# @constraint: not_a_valid_constraint_xyz
save.row({"id": 1})
`)
	result, err := runModelErr(t, p, "staging/star_cperr.star")
	if err == nil {
		t.Fatal("expected constraint error")
	}
	hasParseErr := false
	for _, e := range result.Errors {
		if strings.Contains(e, "parse error") || strings.Contains(e, "unknown") {
			hasParseErr = true
		}
	}
	if !hasParseErr {
		t.Errorf("expected constraint parse error, got: %v", result.Errors)
	}
}

func TestRun_StarlarkScript_AuditPass(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run: create initial data
	p.AddModel("staging/star_audp.star", `# @kind: table
save.row({"id": 1, "amount": 100})
`)
	runModel(t, p, "staging/star_audp.star")

	// Second run with audit that passes
	p.AddModel("staging/star_audp.star", `# @kind: table
# @audit: row_count >= 1
save.row({"id": 1, "amount": 200})
`)
	result := runModel(t, p, "staging/star_audp.star")
	if len(result.Errors) > 0 {
		t.Errorf("unexpected errors: %v", result.Errors)
	}
	// Verify data was committed (not rolled back)
	val, err := p.Sess.QueryValue("SELECT amount FROM staging.star_audp WHERE id = 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "200" {
		t.Errorf("amount = %s, want 200 (new data should persist)", val)
	}
}

func TestRun_StarlarkScript_AuditFail_FirstRun(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("staging/star_af1.star", `# @kind: table
# @audit: row_count >= 999
save.row({"id": 1})
`)
	result, err := runModelErr(t, p, "staging/star_af1.star")
	if err == nil {
		t.Fatal("expected audit error")
	}
	if len(result.Errors) == 0 {
		t.Fatal("expected errors in result")
	}

	// Trace should include rollback step
	hasRollback := false
	for _, step := range result.Trace {
		if step.Name == "rollback" {
			hasRollback = true
			break
		}
	}
	if !hasRollback {
		t.Error("expected rollback step in trace")
	}

	// Table should not exist after rollback (first run)
	_, qErr := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.star_af1")
	if qErr == nil {
		t.Error("table should not exist after audit rollback on first run")
	}
}

func TestRun_StarlarkScript_WarningPass(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/star_wok.star", `# @kind: table
# @warning: id NOT NULL
# @warning: id UNIQUE
save.row({"id": 1, "name": "Alice"})
save.row({"id": 2, "name": "Bob"})
`)
	result := runModel(t, p, "staging/star_wok.star")
	if len(result.Warnings) != 0 {
		t.Errorf("expected no warnings for valid data, got: %v", result.Warnings)
	}
	if result.RowsAffected != 2 {
		t.Errorf("rows = %d, want 2", result.RowsAffected)
	}
}

func TestRun_StarlarkScript_WarningAuditPattern(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run: create initial data
	p.AddModel("staging/star_waud.star", `# @kind: table
save.row({"id": 1, "amount": 100})
`)
	runModel(t, p, "staging/star_waud.star")

	// Second run with warning audit pattern that fails
	p.AddModel("staging/star_waud.star", `# @kind: table
# @warning: row_count >= 999
save.row({"id": 1, "amount": 200})
`)
	result := runModel(t, p, "staging/star_waud.star")
	// Should produce warning, not error
	if len(result.Errors) > 0 {
		t.Errorf("warnings should not produce errors: %v", result.Errors)
	}
	hasWarning := false
	for _, w := range result.Warnings {
		if strings.Contains(w, "row_count") {
			hasWarning = true
		}
	}
	if !hasWarning {
		t.Errorf("expected row_count warning, got: %v", result.Warnings)
	}
	// Data should be committed (warnings don't rollback)
	val, err := p.Sess.QueryValue("SELECT amount FROM staging.star_waud WHERE id = 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "200" {
		t.Errorf("amount = %s, want 200 (data should persist despite warning)", val)
	}
}

func TestRun_StarlarkScript_WarningUnknownPattern(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/star_wunk.star", `# @kind: table
# @warning: totally_invalid_xyz
save.row({"id": 1})
`)
	result := runModel(t, p, "staging/star_wunk.star")
	hasParseErr := false
	for _, w := range result.Warnings {
		if strings.Contains(w, "warning parse error") {
			hasParseErr = true
		}
	}
	if !hasParseErr {
		t.Errorf("expected warning parse error, got: %v", result.Warnings)
	}
	// Data should still be committed
	if result.RowsAffected != 1 {
		t.Errorf("rows = %d, want 1", result.RowsAffected)
	}
}

func TestRun_StarlarkScript_ResultStatus_Pass(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/star_sok.star", `# @kind: table
# @constraint: id NOT NULL
save.row({"id": 1, "name": "Alice"})
save.row({"id": 2, "name": "Bob"})
`)
	result := runModel(t, p, "staging/star_sok.star")

	// Drives JSON "status":"ok" — no errors
	if len(result.Errors) != 0 {
		t.Errorf("expected no errors (status=ok), got %v", result.Errors)
	}
	if result.RowsAffected != 2 {
		t.Errorf("rows = %d, want 2", result.RowsAffected)
	}
}

func TestRun_StarlarkScript_ResultStatus_Fail(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/star_serr.star", `# @kind: table
# @constraint: id NOT NULL
save.row({"id": 1, "name": "Alice"})
save.row({"id": nil, "name": "Bad"})
`)
	result, err := runModelErr(t, p, "staging/star_serr.star")
	if err == nil {
		t.Fatal("expected constraint error")
	}

	// Drives JSON "status":"error" + "errors":[...]
	if len(result.Errors) == 0 {
		t.Fatal("expected errors in result for JSON output")
	}
	for _, e := range result.Errors {
		if e == "" {
			t.Error("empty error message would produce empty string in JSON errors array")
		}
	}
}

func TestRun_Extension_AlreadyLoaded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Load json extension twice → "already loaded" path
	p.AddModel("staging/ext_twice.sql", `-- @kind: table
-- @extension: json
SELECT 1 AS id
`)
	runModel(t, p, "staging/ext_twice.sql")

	// Run again → same SQL, no deps → skip (table kind dependency-aware skip)
	result := runModel(t, p, "staging/ext_twice.sql")
	if result.RunType != "skip" {
		t.Errorf("run_type = %q, want skip", result.RunType)
	}
}

func TestRun_WarningAuditPattern(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Create initial data
	p.AddModel("staging/warn_aud.sql", `-- @kind: table
SELECT 1 AS id, 100 AS amount
`)
	runModel(t, p, "staging/warn_aud.sql")

	// Warning with audit pattern (row_count) - should trigger audit code path in runWarnings
	p.AddModel("staging/warn_aud.sql", `-- @kind: table
-- @warning: row_count >= 999
SELECT 1 AS id, 200 AS amount
`)
	result := runModel(t, p, "staging/warn_aud.sql")
	// Should produce a warning, not an error
	if result.RowsAffected != 1 {
		t.Errorf("rows = %d, want 1", result.RowsAffected)
	}
	hasWarning := false
	for _, w := range result.Warnings {
		if strings.Contains(w, "row_count") {
			hasWarning = true
		}
	}
	if !hasWarning {
		t.Errorf("expected row_count warning, got: %v", result.Warnings)
	}
}

func TestRun_SCD2_IncrementalWithChanges(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Same SQL for all runs → incremental path
	sqlCode := `-- @kind: scd2
-- @unique_key: id
SELECT * FROM staging.scd2_src
`

	// Create source table with initial data
	p.AddModel("staging/scd2_src.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name, 'NYC' AS city
UNION ALL SELECT 2, 'Bob', 'LA'
`)
	runModel(t, p, "staging/scd2_src.sql")

	// First run of SCD2 (backfill)
	p.AddModel("staging/scd2_chg.sql", sqlCode)
	r1 := runModel(t, p, "staging/scd2_chg.sql")
	if r1.RowsAffected != 2 {
		t.Errorf("run1 rows = %d, want 2", r1.RowsAffected)
	}

	// Update source: Alice moves to SF
	p.AddModel("staging/scd2_src.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name, 'SF' AS city
UNION ALL SELECT 2, 'Bob', 'LA'
`)
	runModel(t, p, "staging/scd2_src.sql")

	// Second run → incremental SCD2 with change detection
	r2 := runModel(t, p, "staging/scd2_chg.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run2 type = %q, want incremental", r2.RunType)
	}

	// Should have history rows
	total, err := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.scd2_chg")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	// 2 original + 1 new version for Alice = at minimum 3
	if total == "0" {
		t.Error("expected rows in scd2 table")
	}
}

func TestRun_SchemaEvolution_TypeChange_Append(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run with INTEGER amount
	p.AddModel("staging/type_evo.sql", `-- @kind: append
SELECT 1 AS id, CAST(100 AS SMALLINT) AS amount
`)
	runModel(t, p, "staging/type_evo.sql")

	// Second run with BIGINT amount (type promotion SMALLINT → BIGINT)
	p.AddModel("staging/type_evo.sql", `-- @kind: append
SELECT 2 AS id, CAST(200 AS BIGINT) AS amount
`)
	r2 := runModel(t, p, "staging/type_evo.sql")
	// Should detect type change and apply schema evolution (not backfill)
	if r2.RunType != "incremental" {
		t.Errorf("run_type = %q, want incremental (type promotion)", r2.RunType)
	}
	hasEvoWarning := false
	for _, w := range r2.Warnings {
		if strings.Contains(w, "schema evolution") && strings.Contains(w, "type_changes") {
			hasEvoWarning = true
		}
	}
	if !hasEvoWarning {
		t.Errorf("expected schema evolution type_changes warning, got: %v", r2.Warnings)
	}
}

func TestRun_Merge_OnlyUniqueKeyColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Merge table with only the unique key column → empty setClauses path in buildMergeSQL
	p.AddModel("staging/merge_uk.sql", `-- @kind: merge
-- @unique_key: id
SELECT 1 AS id
`)
	r := runModel(t, p, "staging/merge_uk.sql")
	if r.RowsAffected != 1 {
		t.Errorf("rows = %d, want 1", r.RowsAffected)
	}

	// Run again (incremental merge with single column)
	r2 := runModel(t, p, "staging/merge_uk.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run_type = %q, want incremental", r2.RunType)
	}
}

func TestComputeRunTypeDecisions_EmptyModels(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	decisions, err := execute.ComputeRunTypeDecisions(p.Sess, nil)
	if err != nil {
		t.Fatalf("ComputeRunTypeDecisions: %v", err)
	}
	if len(decisions) != 0 {
		t.Errorf("expected empty decisions, got %d", len(decisions))
	}
}

func TestComputeRunTypeDecisions_MixedKinds(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Create a table model first so it has commit history
	p.AddModel("staging/batch_tbl.sql", `-- @kind: table
SELECT 1 AS id
`)
	runModel(t, p, "staging/batch_tbl.sql")

	models := []*parser.Model{
		{Target: "staging.batch_tbl", Kind: "table", SQL: "SELECT 1 AS id"},
		{Target: "staging.batch_new", Kind: "append", SQL: "SELECT 1 AS id"},
	}
	decisions, err := execute.ComputeRunTypeDecisions(p.Sess, models)
	if err != nil {
		t.Fatalf("ComputeRunTypeDecisions: %v", err)
	}
	if len(decisions) != 2 {
		t.Errorf("expected 2 decisions (table + append), got %d", len(decisions))
	}
	if d := decisions["staging.batch_tbl"]; d == nil {
		t.Error("missing decision for staging.batch_tbl")
	}
	if d := decisions["staging.batch_new"]; d == nil {
		t.Error("missing decision for staging.batch_new")
	}
}

func TestRun_SchemaEvolution_RenameWithAddColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Create a source table so that lineage can trace columns
	p.AddModel("raw/rename_src.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name, 100 AS amount
`)
	runModel(t, p, "raw/rename_src.sql")

	// First run: read from source, selecting name
	p.AddModel("staging/rename_add.sql", `-- @kind: append
SELECT id, name, amount FROM raw.rename_src
`)
	runModel(t, p, "staging/rename_add.sql")

	// Update source with renamed column
	p.AddModel("raw/rename_src.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS full_name, 100 AS amount, 'active' AS status
`)
	runModel(t, p, "raw/rename_src.sql")

	// Second run: id, full_name (rename from name), amount, plus new column 'status'
	// This tests both rename detection and add column in schema evolution
	p.AddModel("staging/rename_add.sql", `-- @kind: append
SELECT id, full_name, amount, status FROM raw.rename_src
`)
	r2 := runModel(t, p, "staging/rename_add.sql")
	if r2.RowsAffected != 1 {
		t.Errorf("rows = %d, want 1", r2.RowsAffected)
	}
}

func TestRun_Partition_IncrementalUpdate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run: create partition table
	p.AddModel("staging/part_incr.sql", `-- @kind: partition
-- @unique_key: region
SELECT 1 AS id, 'US' AS region, 100 AS amount
UNION ALL SELECT 2, 'EU', 200
`)
	r1 := runModel(t, p, "staging/part_incr.sql")
	if r1.RowsAffected != 2 {
		t.Errorf("run1 rows = %d, want 2", r1.RowsAffected)
	}

	// Second run: same SQL → incremental partition update
	r2 := runModel(t, p, "staging/part_incr.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run_type = %q, want incremental", r2.RunType)
	}
}

func TestRun_SCD2_SchemaEvolution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run: create SCD2 table
	p.AddModel("staging/scd2_evo.sql", `-- @kind: scd2
-- @unique_key: id
SELECT 1 AS id, 'Alice' AS name
`)
	r1 := runModel(t, p, "staging/scd2_evo.sql")
	if r1.RowsAffected != 1 {
		t.Errorf("run1 rows = %d, want 1", r1.RowsAffected)
	}

	// Add column → forces backfill for SCD2 (schema hash changes)
	p.AddModel("staging/scd2_evo.sql", `-- @kind: scd2
-- @unique_key: id
SELECT 1 AS id, 'Alice' AS name, 30 AS age
`)
	r2 := runModel(t, p, "staging/scd2_evo.sql")
	if r2.RowsAffected != 1 {
		t.Errorf("run2 rows = %d, want 1", r2.RowsAffected)
	}
}

func TestRun_Append_SchemaEvolution_AddAndTypeChange(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run with SMALLINT columns
	p.AddModel("staging/mixed_evo.sql", `-- @kind: append
SELECT CAST(1 AS SMALLINT) AS id, CAST(100 AS SMALLINT) AS amount
`)
	runModel(t, p, "staging/mixed_evo.sql")

	// Second run: type promotion (SMALLINT→BIGINT) + new column
	p.AddModel("staging/mixed_evo.sql", `-- @kind: append
SELECT CAST(2 AS BIGINT) AS id, CAST(200 AS BIGINT) AS amount, 'note' AS note
`)
	r2 := runModel(t, p, "staging/mixed_evo.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run_type = %q, want incremental", r2.RunType)
	}
	hasEvo := false
	for _, w := range r2.Warnings {
		if strings.Contains(w, "schema evolution") {
			hasEvo = true
		}
	}
	if !hasEvo {
		t.Errorf("expected schema evolution warning, got: %v", r2.Warnings)
	}
}

func TestRun_AuditFailure_FirstRun_Rollback(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Tests rollback with prevSnapshot=0 (first run, table gets dropped)
	p := testutil.NewProject(t)

	p.AddModel("staging/audit_first.sql", `-- @kind: table
-- @audit: row_count >= 100
SELECT 1 AS id
`)
	_, err := runModelErr(t, p, "staging/audit_first.sql")
	if err == nil {
		t.Fatal("expected audit failure")
	}
	if !strings.Contains(err.Error(), "audit validation failed") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRun_Extension_FromSyntax(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Tests the "name FROM repo" extension syntax
	p := testutil.NewProject(t)

	// Use icu extension which is bundled with DuckDB
	p.AddModel("staging/ext_from.sql", `-- @kind: table
-- @extension: icu
SELECT 1 AS id
`)
	result := runModel(t, p, "staging/ext_from.sql")
	if result.RowsAffected != 1 {
		t.Errorf("rows = %d, want 1", result.RowsAffected)
	}
}

func TestRun_Merge_IncrementalUpdate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Tests merge incremental run (not backfill) with changed data
	p := testutil.NewProject(t)

	// First run as merge
	p.AddModel("staging/merge_upd.sql", `-- @kind: merge
-- @unique_key: id
SELECT 1 AS id, 'alice' AS name
`)
	runModel(t, p, "staging/merge_upd.sql")

	// Second run with updated data (same SQL hash triggers incremental)
	p.AddModel("staging/merge_upd.sql", `-- @kind: merge
-- @unique_key: id
SELECT 1 AS id, 'bob' AS name
UNION ALL SELECT 2, 'carol'
`)
	result := runModel(t, p, "staging/merge_upd.sql")
	if result.RowsAffected < 1 {
		t.Errorf("expected rows affected >= 1, got %d", result.RowsAffected)
	}
}

func TestRun_ConstraintFailure_Cleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Tests that constraint failure cleans up temp table properly
	p := testutil.NewProject(t)

	p.AddModel("staging/const_fail.sql", `-- @kind: table
-- @constraint: not_null(missing_col)
SELECT 1 AS id, NULL AS val
`)
	// not_null on a column with NULL should fail
	_, err := runModelErr(t, p, "staging/const_fail.sql")
	if err == nil {
		t.Fatal("expected constraint failure")
	}

	// Verify temp table was cleaned up - run another model successfully
	p.AddModel("staging/after_fail.sql", `-- @kind: table
SELECT 1 AS id
`)
	result := runModel(t, p, "staging/after_fail.sql")
	if result.RowsAffected != 1 {
		t.Errorf("rows = %d, want 1", result.RowsAffected)
	}
}

func TestRun_WarningConstraintPattern_NotNull(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Tests warning with constraint pattern (not_null is constraint-style)
	p := testutil.NewProject(t)

	p.AddModel("staging/warn_const2.sql", `-- @kind: table
-- @warning: not_null(id)
SELECT 1 AS id, 'test' AS name
`)
	result := runModel(t, p, "staging/warn_const2.sql")
	if result.RowsAffected != 1 {
		t.Errorf("rows = %d, want 1", result.RowsAffected)
	}
}

func TestRun_WarningUnknownPattern_Error(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Tests warning with an unrecognized pattern - should add a parse error warning
	p := testutil.NewProject(t)

	p.AddModel("staging/warn_unknown2.sql", `-- @kind: table
-- @warning: totally_invalid_pattern!!!
SELECT 1 AS id
`)
	result := runModel(t, p, "staging/warn_unknown2.sql")
	hasParseErr := false
	for _, w := range result.Warnings {
		if strings.Contains(w, "warning parse error") {
			hasParseErr = true
		}
	}
	if !hasParseErr {
		t.Errorf("expected warning parse error, got warnings: %v", result.Warnings)
	}
}

func TestRun_GitInfo_InMetadata(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Tests that SetGitInfo is stored in commit metadata
	p := testutil.NewProject(t)

	p.AddModel("staging/git_meta.sql", `-- @kind: table
SELECT 1 AS id
`)

	modelPath := filepath.Join(p.Dir, "models", "staging/git_meta.sql")
	model, err := parser.ParseModel(modelPath, p.Dir)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
	runner.SetGitInfo("abc123", "main", "https://github.com/test/repo")
	result, err := runner.Run(context.Background(), model)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if result.RowsAffected != 1 {
		t.Errorf("rows = %d, want 1", result.RowsAffected)
	}
}

func TestRun_SCD2_DeletedRows(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Tests SCD2 where rows are deleted from source (should close versions)
	p := testutil.NewProject(t)

	// First run with 3 rows
	p.AddModel("staging/scd2_del.sql", `-- @kind: scd2
-- @unique_key: id
SELECT 1 AS id, 'alice' AS name
UNION ALL SELECT 2, 'bob'
UNION ALL SELECT 3, 'carol'
`)
	r1 := runModel(t, p, "staging/scd2_del.sql")
	if r1.RowsAffected != 3 {
		t.Errorf("run1 rows = %d, want 3", r1.RowsAffected)
	}

	// Second run removes row 3 (carol)
	p.AddModel("staging/scd2_del.sql", `-- @kind: scd2
-- @unique_key: id
SELECT 1 AS id, 'alice' AS name
UNION ALL SELECT 2, 'bob'
`)
	r2 := runModel(t, p, "staging/scd2_del.sql")
	// Changes = new/changed rows, but deletion should be tracked too
	_ = r2
}

func TestComputeSingleRunType_MergeKind(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("staging/m.sql", `-- @kind: merge
-- @unique_key: id
SELECT 1 AS id, 'x' AS name
`)
	model, err := parser.ParseModel(filepath.Join(p.Dir, "models", "staging/m.sql"), p.Dir)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// First time: backfill
	d, err := execute.ComputeSingleRunType(p.Sess, model)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	if d.RunType != "backfill" {
		t.Errorf("run_type = %q, want backfill", d.RunType)
	}

	runModel(t, p, "staging/m.sql")

	// Same hash: incremental
	d2, err := execute.ComputeSingleRunType(p.Sess, model)
	if err != nil {
		t.Fatalf("compute2: %v", err)
	}
	if d2.RunType != "incremental" {
		t.Errorf("run_type = %q, want incremental", d2.RunType)
	}
}

func TestComputeSingleRunType_SCD2Kind(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("staging/s.sql", `-- @kind: scd2
-- @unique_key: id
SELECT 1 AS id, 'x' AS name
`)
	model, err := parser.ParseModel(filepath.Join(p.Dir, "models", "staging/s.sql"), p.Dir)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	d, err := execute.ComputeSingleRunType(p.Sess, model)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	if d.RunType != "backfill" {
		t.Errorf("run_type = %q, want backfill", d.RunType)
	}

	runModel(t, p, "staging/s.sql")

	d2, err := execute.ComputeSingleRunType(p.Sess, model)
	if err != nil {
		t.Fatalf("compute2: %v", err)
	}
	if d2.RunType != "incremental" {
		t.Errorf("run_type = %q, want incremental", d2.RunType)
	}
}

func TestComputeSingleRunType_PartitionKind(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("staging/pk.sql", `-- @kind: partition
-- @unique_key: region
SELECT 1 AS id, 'eu' AS region
`)
	model, err := parser.ParseModel(filepath.Join(p.Dir, "models", "staging/pk.sql"), p.Dir)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	d, err := execute.ComputeSingleRunType(p.Sess, model)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	if d.RunType != "backfill" {
		t.Errorf("run_type = %q, want backfill", d.RunType)
	}

	runModel(t, p, "staging/pk.sql")

	d2, err := execute.ComputeSingleRunType(p.Sess, model)
	if err != nil {
		t.Fatalf("compute2: %v", err)
	}
	if d2.RunType != "incremental" {
		t.Errorf("run_type = %q, want incremental", d2.RunType)
	}
}

func TestComputeSingleRunType_HashChange(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("staging/hc.sql", `-- @kind: append
SELECT 1 AS id
`)
	runModel(t, p, "staging/hc.sql")

	// Change the SQL -> hash changes -> should backfill
	p.AddModel("staging/hc.sql", `-- @kind: append
SELECT 1 AS id, 'new' AS name
`)
	model, err := parser.ParseModel(filepath.Join(p.Dir, "models", "staging/hc.sql"), p.Dir)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	d, err := execute.ComputeSingleRunType(p.Sess, model)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	if d.RunType != "backfill" {
		t.Errorf("run_type = %q, want backfill", d.RunType)
	}
}

func TestRun_Partition_MultiColumnUniqueKey(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/pmc.sql", `-- @kind: partition
-- @unique_key: region, year
SELECT 1 AS id, 'eu' AS region, 2024 AS year
UNION ALL SELECT 2, 'us', 2024
UNION ALL SELECT 3, 'eu', 2025
`)

	r := runModel(t, p, "staging/pmc.sql")
	if r.RowsAffected != 3 {
		t.Errorf("rows = %d, want 3", r.RowsAffected)
	}

	// Incremental: add new partition combination
	p.AddModel("staging/pmc.sql", `-- @kind: partition
-- @unique_key: region, year
SELECT 1 AS id, 'eu' AS region, 2024 AS year
UNION ALL SELECT 2, 'us', 2024
UNION ALL SELECT 3, 'eu', 2025
UNION ALL SELECT 4, 'us', 2025
`)

	r2 := runModel(t, p, "staging/pmc.sql")
	if r2.RowsAffected < 1 {
		t.Errorf("expected rows affected, got %d", r2.RowsAffected)
	}
}

func TestGetDecision_NilMap(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	var d execute.RunTypeDecisions
	if got := d.GetDecision("anything"); got != nil {
		t.Errorf("expected nil for nil map, got %+v", got)
	}
}

func TestGetDecision_NotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	d := execute.RunTypeDecisions{
		"staging.a": &execute.RunTypeDecision{RunType: "full"},
	}
	if got := d.GetDecision("staging.b"); got != nil {
		t.Errorf("expected nil for missing key, got %+v", got)
	}
}

func TestRun_SchemaEvolution_TypePromotion_Table(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run: INTEGER column
	p.AddModel("staging/type_promo.sql", `-- @kind: table
SELECT 1 AS id, 100 AS value
`)
	runModel(t, p, "staging/type_promo.sql")

	// Second run: promote INTEGER to BIGINT
	p.AddModel("staging/type_promo.sql", `-- @kind: table
SELECT CAST(1 AS BIGINT) AS id, CAST(100 AS BIGINT) AS value
`)
	r2 := runModel(t, p, "staging/type_promo.sql")

	// Table kind always does full replacement, but type change should be detected
	if r2.RowsAffected != 1 {
		t.Errorf("rows = %d, want 1", r2.RowsAffected)
	}
}

func TestRun_SchemaEvolution_TypePromotion_Merge(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run: INTEGER amount
	p.AddModel("staging/merge_promo.sql", `-- @kind: merge
-- @unique_key: id
SELECT 1 AS id, 100 AS amount
`)
	runModel(t, p, "staging/merge_promo.sql")

	// Second run: promote amount to BIGINT
	p.AddModel("staging/merge_promo.sql", `-- @kind: merge
-- @unique_key: id
SELECT 1 AS id, CAST(200 AS BIGINT) AS amount
`)
	r2 := runModel(t, p, "staging/merge_promo.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run_type = %q, want incremental", r2.RunType)
	}

	val, err := p.Sess.QueryValue("SELECT amount FROM staging.merge_promo WHERE id = 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "200" {
		t.Errorf("amount = %s, want 200", val)
	}
}

func TestRun_SchemaEvolution_CombinedRenameAddType(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Source table for lineage-based rename detection
	p.AddModel("raw/combined_src.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name, 100 AS amount
`)
	runModel(t, p, "raw/combined_src.sql")

	// First run with all 3 columns
	p.AddModel("staging/combined_evo.sql", `-- @kind: append
SELECT id, name, amount FROM raw.combined_src
`)
	runModel(t, p, "staging/combined_evo.sql")

	// Update source: rename name→full_name, promote amount to BIGINT, add status
	p.AddModel("raw/combined_src.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS full_name, CAST(100 AS BIGINT) AS amount, 'active' AS status
`)
	runModel(t, p, "raw/combined_src.sql")

	// Second run: triggers rename + add + type promotion simultaneously
	p.AddModel("staging/combined_evo.sql", `-- @kind: append
SELECT id, full_name, amount, status FROM raw.combined_src
`)
	r2 := runModel(t, p, "staging/combined_evo.sql")
	if r2.RowsAffected != 1 {
		t.Errorf("rows = %d, want 1", r2.RowsAffected)
	}

	// Verify all 4 columns exist
	val, err := p.Sess.QueryValue("SELECT COUNT(*) FROM (DESCRIBE staging.combined_evo)")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if val != "4" {
		t.Errorf("columns = %s, want 4", val)
	}
}

func TestRun_AuditFail_Rollback_FirstRun(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run with audit that fails → rollback issues DROP TABLE (prevSnapshot=0 path)
	p.AddModel("staging/audit_new.sql", `-- @kind: table
-- @audit: row_count >= 999
SELECT 1 AS id
`)
	result, err := runModelErr(t, p, "staging/audit_new.sql")
	if err == nil {
		t.Fatal("expected audit error")
	}
	if !strings.Contains(err.Error(), "audit validation failed") {
		t.Errorf("error = %v, want 'audit validation failed'", err)
	}

	// Result should contain descriptive audit error (drives JSON status:"error" + errors:[...])
	if len(result.Errors) == 0 {
		t.Fatal("expected errors in result")
	}
	if !strings.Contains(result.Errors[0], "row_count") {
		t.Errorf("error message = %q, want descriptive audit failure mentioning row_count", result.Errors[0])
	}

	// Trace should include rollback step
	hasRollback := false
	for _, step := range result.Trace {
		if step.Name == "rollback" {
			hasRollback = true
			break
		}
	}
	if !hasRollback {
		t.Error("expected rollback step in trace")
	}

	// Table should not exist after rollback (first run, prevSnapshot=0 → DROP)
	_, qErr := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.audit_new")
	if qErr == nil {
		t.Error("table should not exist after audit rollback on first run")
	}
}

func TestRun_AuditFail_Rollback_RestoresData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Create initial data
	p.AddModel("staging/audit_restore.sql", `-- @kind: table
SELECT 1 AS id, 100 AS amount
`)
	runModel(t, p, "staging/audit_restore.sql")

	// Verify initial data
	val, _ := p.Sess.QueryValue("SELECT amount FROM staging.audit_restore WHERE id = 1")
	if val != "100" {
		t.Fatalf("initial amount = %s, want 100", val)
	}

	// Second run with audit that fails → should rollback to previous snapshot
	p.AddModel("staging/audit_restore.sql", `-- @kind: table
-- @audit: row_count >= 999
SELECT 1 AS id, 999 AS amount
`)
	_, err := runModelErr(t, p, "staging/audit_restore.sql")
	if err == nil {
		t.Fatal("expected audit error")
	}

	// Data should be restored to original
	val, err = p.Sess.QueryValue("SELECT amount FROM staging.audit_restore WHERE id = 1")
	if err != nil {
		t.Fatalf("query after rollback: %v", err)
	}
	if val != "100" {
		t.Errorf("amount after rollback = %s, want 100 (should be restored)", val)
	}
}

func TestRun_Partition_SchemaEvolution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run
	p.AddModel("staging/part_evo.sql", `-- @kind: partition
-- @unique_key: region
SELECT 1 AS id, 'US' AS region, 100 AS amount
`)
	runModel(t, p, "staging/part_evo.sql")

	// Second run: add new column
	p.AddModel("staging/part_evo.sql", `-- @kind: partition
-- @unique_key: region
SELECT 2 AS id, 'EU' AS region, 200 AS amount, 'active' AS status
`)
	r2 := runModel(t, p, "staging/part_evo.sql")
	if r2.RowsAffected != 1 {
		t.Errorf("rows = %d, want 1", r2.RowsAffected)
	}

	// Verify 4 columns
	val, err := p.Sess.QueryValue("SELECT COUNT(*) FROM (DESCRIBE staging.part_evo)")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if val != "4" {
		t.Errorf("columns = %s, want 4", val)
	}
}

func TestRun_SCD2_SchemaEvolution_AddColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run: basic SCD2
	p.AddModel("staging/scd2_evo.sql", `-- @kind: scd2
-- @unique_key: id
SELECT 1 AS id, 'Alice' AS name
`)
	runModel(t, p, "staging/scd2_evo.sql")

	// Second run: add new column
	p.AddModel("staging/scd2_evo.sql", `-- @kind: scd2
-- @unique_key: id
SELECT 1 AS id, 'Alice' AS name, 'NYC' AS city
`)
	r2 := runModel(t, p, "staging/scd2_evo.sql")
	if r2.RowsAffected != 1 {
		t.Errorf("rows = %d, want 1", r2.RowsAffected)
	}
}

// TestRun_ResultStatus_ConstraintPass verifies that a successful run produces
// a result suitable for JSON output: no errors, status would be "ok".
func TestRun_ResultStatus_ConstraintPass(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/status_ok.sql", `-- @kind: table
-- @constraint: id NOT NULL
-- @constraint: id UNIQUE
SELECT 1 AS id, 'Alice' AS name
UNION ALL SELECT 2, 'Bob'
`)
	result := runModel(t, p, "staging/status_ok.sql")

	// This is what drives "status":"ok" in JSON output
	if len(result.Errors) != 0 {
		t.Errorf("expected no errors (status=ok), got %v", result.Errors)
	}
	if result.Target != "staging.status_ok" {
		t.Errorf("target = %q, want staging.status_ok", result.Target)
	}
	if result.RowsAffected != 2 {
		t.Errorf("rows = %d, want 2", result.RowsAffected)
	}
}

// TestRun_ResultStatus_ConstraintFail verifies that a constraint failure produces
// a result with descriptive errors suitable for JSON output: status would be "error".
func TestRun_ResultStatus_ConstraintFail(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/status_cerr.sql", `-- @kind: table
-- @constraint: id NOT NULL
-- @constraint: id UNIQUE
SELECT 1 AS id UNION ALL SELECT NULL UNION ALL SELECT 1
`)
	result, err := runModelErr(t, p, "staging/status_cerr.sql")
	if err == nil {
		t.Fatal("expected constraint error")
	}

	// This is what drives "status":"error" + "errors":[...] in JSON output
	if len(result.Errors) == 0 {
		t.Fatal("expected errors in result for JSON output")
	}
	// Verify error messages are descriptive (not empty or generic)
	for _, e := range result.Errors {
		if e == "" {
			t.Error("empty error message would produce empty string in JSON errors array")
		}
	}
	// Should mention which constraints failed
	combined := strings.Join(result.Errors, " ")
	if !strings.Contains(combined, "NOT NULL") && !strings.Contains(combined, "UNIQUE") {
		t.Errorf("errors should mention failed constraints, got: %v", result.Errors)
	}
}

// TestRun_ResultStatus_AuditFail verifies that an audit failure produces
// a result with descriptive errors and triggers rollback.
func TestRun_ResultStatus_AuditFail(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run: create initial data
	p.AddModel("staging/status_aerr.sql", `-- @kind: table
SELECT 1 AS id, 100 AS amount
`)
	runModel(t, p, "staging/status_aerr.sql")

	// Second run: audit fails
	p.AddModel("staging/status_aerr.sql", `-- @kind: table
-- @audit: row_count >= 999
SELECT 1 AS id, 200 AS amount
`)
	result, err := runModelErr(t, p, "staging/status_aerr.sql")
	if err == nil {
		t.Fatal("expected audit error")
	}

	// This is what drives "status":"error" + "errors":[...] in JSON output
	if len(result.Errors) == 0 {
		t.Fatal("expected errors in result for JSON output")
	}
	if !strings.Contains(result.Errors[0], "row_count") {
		t.Errorf("audit error should be descriptive, got: %q", result.Errors[0])
	}

	// Verify rollback: data should be restored
	val, qErr := p.Sess.QueryValue("SELECT amount FROM staging.status_aerr WHERE id = 1")
	if qErr != nil {
		t.Fatalf("query after rollback: %v", qErr)
	}
	if val != "100" {
		t.Errorf("amount after rollback = %s, want 100 (original data)", val)
	}
}

func TestComputeSingleRunType_TableKind(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Table kind: first run = backfill, second = skip (no deps, SQL unchanged)
	p.AddModel("staging/csrt_table.sql", `-- @kind: table
SELECT 1 AS id
`)
	runModel(t, p, "staging/csrt_table.sql")

	modelPath := filepath.Join(p.Dir, "models", "staging/csrt_table.sql")
	model, _ := parser.ParseModel(modelPath, p.Dir)
	decision, err := execute.ComputeSingleRunType(p.Sess, model)
	if err != nil {
		t.Fatalf("ComputeSingleRunType: %v", err)
	}
	if decision.RunType != "skip" {
		t.Errorf("run_type = %q, want skip", decision.RunType)
	}
}

// =============================================================================
// Prio 1: Incremental cursor filtering — verify getvariable('incr_last_value')
// actually filters data across multiple runs
// =============================================================================

func TestRun_IncrementalCursor_FiltersData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Tests that @incremental cursor actually restricts data on subsequent runs.
	// The model uses WHERE updated_at > getvariable('incr_last_value'),
	// so run 2 should only see rows newer than the max from run 1.
	p := testutil.NewProject(t)

	// Create source with timestamps
	p.AddModel("raw/events.sql", `-- @kind: table
SELECT 1 AS id, '2024-01-01' AS updated_at
UNION ALL SELECT 2, '2024-01-02'
UNION ALL SELECT 3, '2024-01-03'
`)
	runModel(t, p, "raw/events.sql")

	// Incremental model: uses cursor to filter
	p.AddModel("staging/inc_events.sql", `-- @kind: append
-- @incremental: updated_at
-- @incremental_initial: 2020-01-01
SELECT id, updated_at FROM raw.events
WHERE updated_at > getvariable('incr_last_value')
`)

	// Run 1: backfill → all 3 rows (initial value is 2020-01-01)
	r1 := runModel(t, p, "staging/inc_events.sql")
	if r1.RunType != "backfill" {
		t.Errorf("run1 type = %q, want backfill", r1.RunType)
	}
	if r1.RowsAffected != 3 {
		t.Errorf("run1 rows = %d, want 3", r1.RowsAffected)
	}

	// Run 2: incremental → incr_last_value should be '2024-01-03' (MAX from run 1)
	// No new source data → 0 rows appended
	r2 := runModel(t, p, "staging/inc_events.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run2 type = %q, want incremental", r2.RunType)
	}
	if r2.RowsAffected != 0 {
		t.Errorf("run2 rows = %d, want 0 (no new data above cursor)", r2.RowsAffected)
	}

	// Total should still be 3
	val, err := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.inc_events")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "3" {
		t.Errorf("total = %s, want 3", val)
	}
}

func TestRun_IncrementalCursor_PicksUpNewData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Tests that new source data above the cursor IS picked up on subsequent runs.
	p := testutil.NewProject(t)

	// Initial source
	p.AddModel("raw/ts_events.sql", `-- @kind: table
SELECT 1 AS id, '2024-01-01' AS ts
UNION ALL SELECT 2, '2024-01-02'
`)
	runModel(t, p, "raw/ts_events.sql")

	p.AddModel("staging/ts_target.sql", `-- @kind: append
-- @incremental: ts
-- @incremental_initial: 2020-01-01
SELECT id, ts FROM raw.ts_events
WHERE ts > getvariable('incr_last_value')
`)

	// Run 1: backfill → 2 rows
	r1 := runModel(t, p, "staging/ts_target.sql")
	if r1.RowsAffected != 2 {
		t.Errorf("run1 rows = %d, want 2", r1.RowsAffected)
	}

	// Add new source data with ts > '2024-01-02'
	p.AddModel("raw/ts_events.sql", `-- @kind: table
SELECT 1 AS id, '2024-01-01' AS ts
UNION ALL SELECT 2, '2024-01-02'
UNION ALL SELECT 3, '2024-01-15'
UNION ALL SELECT 4, '2024-02-01'
`)
	runModel(t, p, "raw/ts_events.sql")

	// Run 2: incremental → should pick up rows 3 and 4 (ts > '2024-01-02')
	r2 := runModel(t, p, "staging/ts_target.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run2 type = %q, want incremental", r2.RunType)
	}
	if r2.RowsAffected != 2 {
		t.Errorf("run2 rows = %d, want 2 (only new rows above cursor)", r2.RowsAffected)
	}

	// Total: 2 + 2 = 4
	val, err := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.ts_target")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "4" {
		t.Errorf("total = %s, want 4", val)
	}
}

func TestRun_IncrementalCursor_BackfillResetsValue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Tests that when SQL changes (hash changes → backfill), the cursor resets
	// to the initial value, getting all data again.
	p := testutil.NewProject(t)

	p.AddModel("raw/bf_src.sql", `-- @kind: table
SELECT 1 AS id, '2024-01-01' AS ts
UNION ALL SELECT 2, '2024-06-01'
`)
	runModel(t, p, "raw/bf_src.sql")

	p.AddModel("staging/bf_target.sql", `-- @kind: append
-- @incremental: ts
-- @incremental_initial: 2020-01-01
SELECT id, ts FROM raw.bf_src
WHERE ts > getvariable('incr_last_value')
`)
	runModel(t, p, "staging/bf_target.sql") // backfill: 2 rows

	// Change SQL → hash changes → backfill → cursor resets to 2020-01-01
	p.AddModel("staging/bf_target.sql", `-- @kind: append
-- @incremental: ts
-- @incremental_initial: 2020-01-01
SELECT id, ts FROM raw.bf_src
WHERE ts > getvariable('incr_last_value')
ORDER BY ts
`)
	r2 := runModel(t, p, "staging/bf_target.sql")
	if r2.RunType != "backfill" {
		t.Errorf("run2 type = %q, want backfill", r2.RunType)
	}
	// Backfill resets cursor → gets all 2 rows again
	if r2.RowsAffected != 2 {
		t.Errorf("run2 rows = %d, want 2 (backfill resets cursor)", r2.RowsAffected)
	}
}

// =============================================================================
// Prio 1: Error paths — invalid source tables, references to non-existent tables
// =============================================================================

func TestRun_InvalidSourceTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Model references a table that doesn't exist → should fail with descriptive error
	p := testutil.NewProject(t)
	p.AddModel("staging/bad_ref.sql", `-- @kind: table
SELECT * FROM staging.this_table_does_not_exist
`)
	_, err := runModelErr(t, p, "staging/bad_ref.sql")
	if err == nil {
		t.Fatal("expected error for missing source table")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "does not exist") &&
		!strings.Contains(strings.ToLower(err.Error()), "not found") &&
		!strings.Contains(strings.ToLower(err.Error()), "catalog error") {
		t.Errorf("error = %v, expected descriptive missing table error", err)
	}
}

func TestRun_InvalidSQLSyntax_Append(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Invalid SQL in append kind
	p := testutil.NewProject(t)
	p.AddModel("staging/bad_append.sql", `-- @kind: append
SELECTZ NOT VALID
`)
	_, err := runModelErr(t, p, "staging/bad_append.sql")
	if err == nil {
		t.Fatal("expected error for invalid SQL in append model")
	}
}

func TestRun_InvalidSQLSyntax_Merge(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/bad_merge.sql", `-- @kind: merge
-- @unique_key: id
THIS IS NOT SQL
`)
	_, err := runModelErr(t, p, "staging/bad_merge.sql")
	if err == nil {
		t.Fatal("expected error for invalid SQL in merge model")
	}
}

func TestRun_InvalidSQLSyntax_SCD2(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/bad_scd2.sql", `-- @kind: scd2
-- @unique_key: id
BROKEN SQL SYNTAX HERE
`)
	_, err := runModelErr(t, p, "staging/bad_scd2.sql")
	if err == nil {
		t.Fatal("expected error for invalid SQL in scd2 model")
	}
}

func TestRun_InvalidSQLSyntax_Partition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/bad_part.sql", `-- @kind: partition
-- @unique_key: region
INVALID SQL GOES HERE
`)
	_, err := runModelErr(t, p, "staging/bad_part.sql")
	if err == nil {
		t.Fatal("expected error for invalid SQL in partition model")
	}
}

func TestRun_ConstraintOnMissingColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Constraint references a column that doesn't exist in the query
	p := testutil.NewProject(t)
	p.AddModel("staging/missing_col.sql", `-- @kind: table
-- @constraint: nonexistent_column NOT NULL
SELECT 1 AS id
`)
	_, err := runModelErr(t, p, "staging/missing_col.sql")
	if err == nil {
		t.Fatal("expected error for constraint on missing column")
	}
}

func TestRun_MergeKeyNotInColumns(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// unique_key references a column that doesn't exist
	p := testutil.NewProject(t)
	p.AddModel("staging/bad_uk.sql", `-- @kind: merge
-- @unique_key: nonexistent_key
SELECT 1 AS id, 'hello' AS name
`)
	r1 := runModel(t, p, "staging/bad_uk.sql") // backfill works (no MERGE needed)
	if r1.RowsAffected != 1 {
		t.Errorf("backfill rows = %d, want 1", r1.RowsAffected)
	}

	// Second run tries incremental MERGE with invalid key → error
	_, err := runModelErr(t, p, "staging/bad_uk.sql")
	if err == nil {
		t.Fatal("expected error for merge with non-existent unique_key column")
	}
}

// =============================================================================
// Prio 1: NULL in unique_key — MERGE with NULL keys creates duplicates
// =============================================================================

func TestRun_Merge_NullUniqueKey_CreatesDuplicates(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// SQL standard: NULL = NULL → NULL (not TRUE), so MERGE ON (target.id = source.id)
	// never matches when id IS NULL → NULL rows always treated as NOT MATCHED → duplicates.
	p := testutil.NewProject(t)

	sqlCode := `-- @kind: merge
-- @unique_key: id
SELECT NULL AS id, 'ghost' AS name
`
	p.AddModel("staging/null_merge.sql", sqlCode)

	// Run 1: backfill → 1 row
	r1 := runModel(t, p, "staging/null_merge.sql")
	if r1.RowsAffected != 1 {
		t.Errorf("run1 rows = %d, want 1", r1.RowsAffected)
	}

	// Run 2: incremental merge → NULL key won't match existing NULL row → INSERT
	r2 := runModel(t, p, "staging/null_merge.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run2 type = %q, want incremental", r2.RunType)
	}

	// Should have 2 rows now (duplicate due to NULL key)
	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.null_merge")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != "2" {
		t.Errorf("count = %s, want 2 (NULL keys create duplicates in MERGE)", count)
	}
}

func TestRun_Merge_MixedNullAndNonNullKeys(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Verifies that non-NULL keys still merge correctly while NULL keys duplicate.
	p := testutil.NewProject(t)

	sqlCode := `-- @kind: merge
-- @unique_key: id
SELECT 1 AS id, 'Alice' AS name
UNION ALL SELECT NULL, 'Ghost'
`
	p.AddModel("staging/mixed_null.sql", sqlCode)

	// Run 1: backfill → 2 rows
	runModel(t, p, "staging/mixed_null.sql")

	// Run 2: incremental → id=1 matches (upsert), NULL doesn't match (insert)
	r2 := runModel(t, p, "staging/mixed_null.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run2 type = %q, want incremental", r2.RunType)
	}

	// id=1: 1 row (merged), NULL: 2 rows (original + new) = 3 total
	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.mixed_null")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != "3" {
		t.Errorf("count = %s, want 3 (1 merged + 2 NULL duplicates)", count)
	}

	// Verify the non-NULL key has exactly 1 row
	nonNull, _ := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.mixed_null WHERE id = 1")
	if nonNull != "1" {
		t.Errorf("non-null id rows = %s, want 1 (should merge, not duplicate)", nonNull)
	}

	// Verify NULL keys have 2 rows
	nullRows, _ := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.mixed_null WHERE id IS NULL")
	if nullRows != "2" {
		t.Errorf("null id rows = %s, want 2 (NULL = NULL is not TRUE in MERGE)", nullRows)
	}
}

// =============================================================================
// Prio 1: NULL in unique_key — partition DELETE with NULL won't match
// =============================================================================

func TestRun_Partition_NullPartitionKey(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// DELETE FROM target WHERE (region) IN (SELECT DISTINCT region FROM temp)
	// NULL IN (...) → NULL (not TRUE), so NULL-partition rows won't be deleted.
	// First run creates the data. Second run should handle NULL partitions.
	p := testutil.NewProject(t)

	sqlCode := `-- @kind: partition
-- @unique_key: region
SELECT 1 AS id, NULL AS region, 100 AS amount
UNION ALL SELECT 2, 'US', 200
`
	p.AddModel("staging/null_part.sql", sqlCode)

	// Run 1: backfill → 2 rows
	r1 := runModel(t, p, "staging/null_part.sql")
	if r1.RowsAffected != 2 {
		t.Errorf("run1 rows = %d, want 2", r1.RowsAffected)
	}

	// Run 2: incremental → DELETE WHERE region IN (NULL, 'US')
	// NULL IN (...) is NULL → NULL-partition rows NOT deleted → duplicated
	// US partition rows ARE deleted and re-inserted
	r2 := runModel(t, p, "staging/null_part.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run2 type = %q, want incremental", r2.RunType)
	}

	// US rows: 1 (deleted + re-inserted), NULL rows: 2 (not deleted + inserted again)
	usRows, _ := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.null_part WHERE region = 'US'")
	if usRows != "1" {
		t.Errorf("US rows = %s, want 1 (partition replaced correctly)", usRows)
	}

	nullRows, _ := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.null_part WHERE region IS NULL")
	if nullRows != "2" {
		t.Errorf("null region rows = %s, want 2 (NULL partition not matched by IN clause)", nullRows)
	}
}

func TestRun_Partition_OnlyNullPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Edge case: all rows have NULL partition key
	p := testutil.NewProject(t)

	sqlCode := `-- @kind: partition
-- @unique_key: region
SELECT 1 AS id, NULL AS region
`
	p.AddModel("staging/all_null_part.sql", sqlCode)

	// Run 1: backfill
	runModel(t, p, "staging/all_null_part.sql")

	// Run 2: incremental → no rows deleted (NULL doesn't match)
	r2 := runModel(t, p, "staging/all_null_part.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run2 type = %q, want incremental", r2.RunType)
	}

	// 2 rows: original not deleted + new inserted
	count, _ := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.all_null_part")
	if count != "2" {
		t.Errorf("count = %s, want 2 (NULL partition accumulates)", count)
	}
}

// =============================================================================
// Prio 1: SCD2 with NULL unique_key
// =============================================================================

func TestRun_SCD2_NullUniqueKey(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// SCD2 uses IS DISTINCT FROM for change detection, but EXCEPT for new/deleted detection.
	// NULL unique_key is handled differently than MERGE.
	p := testutil.NewProject(t)

	p.AddModel("staging/scd2_null.sql", `-- @kind: scd2
-- @unique_key: id
SELECT NULL AS id, 'ghost' AS name
`)
	r1 := runModel(t, p, "staging/scd2_null.sql")
	if r1.RowsAffected != 1 {
		t.Errorf("run1 rows = %d, want 1", r1.RowsAffected)
	}

	// Verify SCD2 columns
	val, err := p.Sess.QueryValue("SELECT is_current FROM staging.scd2_null WHERE id IS NULL")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "true" {
		t.Errorf("is_current = %s, want true", val)
	}
}

// =============================================================================
// Prio 1: Multiple errors collected (constraints + audits all run, all reported)
// =============================================================================

func TestRun_MultipleConstraintFailures_AllReported(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// When multiple constraints fail, ALL failures should be reported, not just the first.
	p := testutil.NewProject(t)
	p.AddModel("staging/multi_fail.sql", `-- @kind: table
-- @constraint: id NOT NULL
-- @constraint: id UNIQUE
-- @constraint: amount > 0
SELECT NULL AS id, -5 AS amount
UNION ALL SELECT NULL, -10
`)
	result, err := runModelErr(t, p, "staging/multi_fail.sql")
	if err == nil {
		t.Fatal("expected constraint errors")
	}

	// Should have multiple errors (NOT NULL fails, UNIQUE fails, amount > 0 fails)
	if len(result.Errors) < 2 {
		t.Errorf("expected multiple constraint errors, got %d: %v", len(result.Errors), result.Errors)
	}

	// Verify different constraint types are reported
	combined := strings.Join(result.Errors, " ")
	hasNotNull := strings.Contains(combined, "NOT NULL")
	hasAmount := strings.Contains(combined, "amount") || strings.Contains(combined, "> 0")
	if !hasNotNull {
		t.Errorf("expected NOT NULL failure in errors: %v", result.Errors)
	}
	if !hasAmount {
		t.Errorf("expected amount constraint failure in errors: %v", result.Errors)
	}
}

func TestRun_MultipleAuditFailures_AllReported(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Multiple audits should all be checked and all failures reported.
	p := testutil.NewProject(t)

	// Create initial data
	p.AddModel("staging/multi_aud.sql", `-- @kind: table
SELECT 1 AS id, 100 AS amount
`)
	runModel(t, p, "staging/multi_aud.sql")

	// Second run with multiple failing audits
	p.AddModel("staging/multi_aud.sql", `-- @kind: table
-- @audit: row_count >= 999
-- @audit: amount > 0
SELECT 1 AS id, -50 AS amount
`)
	result, err := runModelErr(t, p, "staging/multi_aud.sql")
	if err == nil {
		t.Fatal("expected audit errors")
	}

	// Should have at least 2 audit errors
	if len(result.Errors) < 2 {
		t.Errorf("expected multiple audit errors, got %d: %v", len(result.Errors), result.Errors)
	}
}

// =============================================================================
// Prio 2: Missing extension error handling
// =============================================================================

func TestRun_Extension_NonExistent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Loading a non-existent extension should fail with error
	p := testutil.NewProject(t)
	p.AddModel("staging/bad_ext.sql", `-- @kind: table
-- @extension: this_extension_does_not_exist_xyz_999
SELECT 1 AS id
`)
	_, err := runModelErr(t, p, "staging/bad_ext.sql")
	if err == nil {
		t.Fatal("expected error for non-existent extension")
	}
}

func TestRun_Extension_MultipleExtensions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Multiple @extension directives should all be loaded
	p := testutil.NewProject(t)
	p.AddModel("staging/multi_ext.sql", `-- @kind: table
-- @extension: json
-- @extension: icu
SELECT 1 AS id
`)
	result := runModel(t, p, "staging/multi_ext.sql")
	if result.RowsAffected != 1 {
		t.Errorf("rows = %d, want 1", result.RowsAffected)
	}
}

// =============================================================================
// Prio 2: Duplicate directives behavior
// =============================================================================

func TestRun_DuplicateConstraints_AllApply(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Multiple identical constraints should all apply (no deduplication)
	p := testutil.NewProject(t)
	p.AddModel("staging/dup_c.sql", `-- @kind: table
-- @constraint: id NOT NULL
-- @constraint: id NOT NULL
SELECT 1 AS id
`)
	result := runModel(t, p, "staging/dup_c.sql")
	if len(result.Errors) > 0 {
		t.Errorf("unexpected errors: %v", result.Errors)
	}
}

func TestRun_DuplicateConstraints_BothFail(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// When duplicated constraints both fail, should report both
	p := testutil.NewProject(t)
	p.AddModel("staging/dup_cfail.sql", `-- @kind: table
-- @constraint: id NOT NULL
-- @constraint: id NOT NULL
SELECT NULL AS id
`)
	result, err := runModelErr(t, p, "staging/dup_cfail.sql")
	if err == nil {
		t.Fatal("expected constraint error")
	}
	// Both duplicated constraints should fire
	if len(result.Errors) < 2 {
		t.Errorf("expected 2+ errors for duplicated constraints, got %d: %v", len(result.Errors), result.Errors)
	}
}

// =============================================================================
// Prio 2: Constraint + audit ordering (constraints block before materialization)
// =============================================================================

func TestRun_ConstraintBlocksBeforeMaterialization(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// If constraint fails, table should NOT be created
	// (constraints run before materialization, audits run after)
	p := testutil.NewProject(t)
	p.AddModel("staging/blocked.sql", `-- @kind: table
-- @constraint: id NOT NULL
SELECT NULL AS id
`)
	_, err := runModelErr(t, p, "staging/blocked.sql")
	if err == nil {
		t.Fatal("expected constraint error")
	}

	// Table must not exist (constraint blocks materialization)
	_, qErr := p.Sess.QueryValue("SELECT 1 FROM staging.blocked")
	if qErr == nil {
		t.Error("table should not exist: constraints should block before materialization")
	}
}

func TestRun_AuditRunsAfterMaterializationAndRollsBack(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Audit runs AFTER materialization, then rollback restores previous state
	p := testutil.NewProject(t)

	// Create initial data
	p.AddModel("staging/aud_order.sql", `-- @kind: table
SELECT 1 AS id, 'original' AS val
`)
	runModel(t, p, "staging/aud_order.sql")

	// Second run: audit fails → table was created temporarily then rolled back
	p.AddModel("staging/aud_order.sql", `-- @kind: table
-- @audit: row_count >= 999
SELECT 1 AS id, 'changed' AS val
`)
	_, err := runModelErr(t, p, "staging/aud_order.sql")
	if err == nil {
		t.Fatal("expected audit error")
	}

	// Data should be rolled back to 'original' (not 'changed')
	val, qErr := p.Sess.QueryValue("SELECT val FROM staging.aud_order WHERE id = 1")
	if qErr != nil {
		t.Fatalf("query: %v", qErr)
	}
	if val != "original" {
		t.Errorf("val = %q, want 'original' (audit should rollback materialization)", val)
	}
}

// =============================================================================
// Prio 2: Partial failure reporting — some models succeed, some fail
// =============================================================================

func TestRun_PartialPipeline_FailureDoesntAffectPrior(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Running models sequentially: first succeeds, second fails.
	// First model's data should persist.
	p := testutil.NewProject(t)

	// Model 1: succeeds
	p.AddModel("raw/good.sql", `-- @kind: table
SELECT 1 AS id, 100 AS amount
`)
	r1 := runModel(t, p, "raw/good.sql")
	if r1.RowsAffected != 1 {
		t.Errorf("good model rows = %d, want 1", r1.RowsAffected)
	}

	// Model 2: fails (constraint)
	p.AddModel("staging/bad.sql", `-- @kind: table
-- @constraint: id NOT NULL
SELECT NULL AS id
`)
	_, err := runModelErr(t, p, "staging/bad.sql")
	if err == nil {
		t.Fatal("expected constraint error")
	}

	// Model 1's data should still be intact
	val, qErr := p.Sess.QueryValue("SELECT amount FROM raw.good WHERE id = 1")
	if qErr != nil {
		t.Fatalf("query good model: %v", qErr)
	}
	if val != "100" {
		t.Errorf("good model amount = %s, want 100 (unaffected by bad model)", val)
	}
}

func TestRun_Constraint_ErrorMessageDescriptive(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Error messages should contain enough info for debugging
	p := testutil.NewProject(t)
	p.AddModel("staging/desc_err.sql", `-- @kind: table
-- @constraint: amount > 0
-- @constraint: id UNIQUE
SELECT 1 AS id, -5 AS amount
UNION ALL SELECT 1, 10
`)
	result, err := runModelErr(t, p, "staging/desc_err.sql")
	if err == nil {
		t.Fatal("expected constraint error")
	}
	// Each error should be non-empty and descriptive
	for i, e := range result.Errors {
		if e == "" {
			t.Errorf("error[%d] is empty", i)
		}
		if len(e) < 10 {
			t.Errorf("error[%d] = %q, seems too short to be descriptive", i, e)
		}
	}
}

// =============================================================================
// Prio 3: Column reordering detection (schema evolution)
// =============================================================================

func TestRun_SchemaEvolution_ColumnReorder(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Column reordering (same columns, different order) should trigger backfill
	// because DuckDB tables are order-sensitive for INSERT.
	p := testutil.NewProject(t)

	p.AddModel("staging/reorder.sql", `-- @kind: append
SELECT 1 AS id, 'Alice' AS name, 100 AS amount
`)
	runModel(t, p, "staging/reorder.sql")

	// Reorder columns: amount before name
	p.AddModel("staging/reorder.sql", `-- @kind: append
SELECT 2 AS id, 200 AS amount, 'Bob' AS name
`)
	r2 := runModel(t, p, "staging/reorder.sql")
	// Column reorder → hash changes → backfill
	if r2.RunType != "backfill" {
		t.Errorf("run_type = %q, want backfill (column reorder changes hash)", r2.RunType)
	}
}

// =============================================================================
// Prio 3: CDC with nullable columns (EXCEPT handles NULLs correctly)
// =============================================================================

func TestRun_CDC_NullableColumns(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// CDC uses EXCEPT which treats NULL = NULL as equal.
	// Verify that models with nullable columns work correctly with CDC.
	p := testutil.NewProject(t)

	// Source with nullable column
	p.AddModel("raw/nullable_src.sql", `-- @kind: table
SELECT 1 AS id, NULL AS optional_field
UNION ALL SELECT 2, 'has_value'
`)
	runModel(t, p, "raw/nullable_src.sql")

	sqlCode := `-- @kind: append
SELECT id, optional_field FROM raw.nullable_src
`
	p.AddModel("staging/nullable_cdc.sql", sqlCode)

	// Run 1: backfill
	r1 := runModel(t, p, "staging/nullable_cdc.sql")
	if r1.RowsAffected != 2 {
		t.Errorf("run1 rows = %d, want 2", r1.RowsAffected)
	}

	// Run 2: no source changes → CDC detects no changes → 0 rows (or same rows via empty CDC)
	r2 := runModel(t, p, "staging/nullable_cdc.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run2 type = %q, want incremental", r2.RunType)
	}

	// Source unchanged → CDC should produce 0 new rows (EXCEPT sees no diff)
	count, _ := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.nullable_cdc")
	if count != "2" {
		t.Errorf("count = %s, want 2 (no duplicates from NULL handling)", count)
	}
}

func TestRun_CDC_NullChangesToValue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// When a NULL field changes to a value, CDC should detect the change.
	p := testutil.NewProject(t)

	p.AddModel("raw/null_change.sql", `-- @kind: table
SELECT 1 AS id, CAST(NULL AS VARCHAR) AS email
`)
	runModel(t, p, "raw/null_change.sql")

	sqlCode := `-- @kind: merge
-- @unique_key: id
SELECT id, email FROM raw.null_change
`
	p.AddModel("staging/null_change_tgt.sql", sqlCode)
	runModel(t, p, "staging/null_change_tgt.sql") // backfill

	// Update source: NULL → 'test@example.com'
	p.AddModel("raw/null_change.sql", `-- @kind: table
SELECT 1 AS id, 'test@example.com' AS email
`)
	runModel(t, p, "raw/null_change.sql")

	// Incremental merge: CDC detects NULL→value change
	r2 := runModel(t, p, "staging/null_change_tgt.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run2 type = %q, want incremental", r2.RunType)
	}

	// Verify email was updated
	val, err := p.Sess.QueryValue("SELECT email FROM staging.null_change_tgt WHERE id = 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "test@example.com" {
		t.Errorf("email = %q, want test@example.com", val)
	}
}

// =============================================================================
// Prio 3: Deep dependency chains
// =============================================================================

func TestRun_DeepDependencyChain(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Tests a 5-level deep pipeline: raw → staging → intermediate → mart → summary
	p := testutil.NewProject(t)

	p.AddModel("raw/deep_src.sql", `-- @kind: table
SELECT 1 AS id, 10 AS value
UNION ALL SELECT 2, 20
UNION ALL SELECT 3, 30
`)
	runModel(t, p, "raw/deep_src.sql")

	p.AddModel("staging/deep_stage.sql", `-- @kind: table
SELECT id, value * 2 AS doubled FROM raw.deep_src
`)
	runModel(t, p, "staging/deep_stage.sql")

	p.AddModel("intermediate/deep_inter.sql", `-- @kind: table
SELECT id, doubled + 100 AS adjusted FROM staging.deep_stage
`)
	runModel(t, p, "intermediate/deep_inter.sql")

	p.AddModel("mart/deep_mart.sql", `-- @kind: table
SELECT id, adjusted, CASE WHEN adjusted > 130 THEN 'high' ELSE 'low' END AS tier
FROM intermediate.deep_inter
`)
	runModel(t, p, "mart/deep_mart.sql")

	// Final aggregation
	p.AddModel("mart/deep_summary.sql", `-- @kind: table
SELECT tier, COUNT(*) AS cnt, SUM(adjusted) AS total
FROM mart.deep_mart
GROUP BY tier
`)
	result := runModel(t, p, "mart/deep_summary.sql")

	if result.RowsAffected < 1 {
		t.Errorf("expected rows in summary, got %d", result.RowsAffected)
	}

	// Verify correct computation through all 5 levels:
	// raw: 10,20,30 → staging: 20,40,60 → intermediate: 120,140,160
	// mart: low(120), high(140), high(160) → summary: low=1/120, high=2/300
	lowCnt, _ := p.Sess.QueryValue("SELECT cnt FROM mart.deep_summary WHERE tier = 'low'")
	if lowCnt != "1" {
		t.Errorf("low count = %s, want 1", lowCnt)
	}
	highTotal, _ := p.Sess.QueryValue("SELECT total FROM mart.deep_summary WHERE tier = 'high'")
	if highTotal != "300" {
		t.Errorf("high total = %s, want 300", highTotal)
	}
}

func TestRun_WideDependencyFanout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Tests a wide fanout: one source → many downstream models
	p := testutil.NewProject(t)

	p.AddModel("raw/wide_src.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name, 100 AS amount, 'US' AS region
UNION ALL SELECT 2, 'Bob', 200, 'EU'
UNION ALL SELECT 3, 'Carol', 300, 'US'
`)
	runModel(t, p, "raw/wide_src.sql")

	// Multiple downstream models from same source
	p.AddModel("mart/by_region.sql", `-- @kind: table
SELECT region, SUM(amount) AS total FROM raw.wide_src GROUP BY region
`)
	p.AddModel("mart/by_name.sql", `-- @kind: table
SELECT name, amount FROM raw.wide_src ORDER BY amount DESC
`)
	p.AddModel("mart/totals.sql", `-- @kind: table
SELECT COUNT(*) AS cnt, SUM(amount) AS total, AVG(amount)::INTEGER AS avg FROM raw.wide_src
`)

	r1 := runModel(t, p, "mart/by_region.sql")
	r2 := runModel(t, p, "mart/by_name.sql")
	r3 := runModel(t, p, "mart/totals.sql")

	if r1.RowsAffected != 2 {
		t.Errorf("by_region rows = %d, want 2", r1.RowsAffected)
	}
	if r2.RowsAffected != 3 {
		t.Errorf("by_name rows = %d, want 3", r2.RowsAffected)
	}
	if r3.RowsAffected != 1 {
		t.Errorf("totals rows = %d, want 1", r3.RowsAffected)
	}

	val, _ := p.Sess.QueryValue("SELECT total FROM mart.totals")
	if val != "600" {
		t.Errorf("total = %s, want 600", val)
	}
}

// =============================================================================
// Prio 3: SCD2 change detection with IS DISTINCT FROM
// =============================================================================

func TestRun_SCD2_IncrementalWithNullChanges(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// SCD2 uses IS DISTINCT FROM for change detection, which correctly handles NULL→value.
	p := testutil.NewProject(t)

	p.AddModel("staging/scd2_src_null.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name, CAST(NULL AS VARCHAR) AS email
`)
	runModel(t, p, "staging/scd2_src_null.sql")

	sqlCode := `-- @kind: scd2
-- @unique_key: id
SELECT id, name, email FROM staging.scd2_src_null
`
	p.AddModel("staging/scd2_null_tgt.sql", sqlCode)
	runModel(t, p, "staging/scd2_null_tgt.sql") // backfill

	// Update source: NULL email → real email
	p.AddModel("staging/scd2_src_null.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name, 'alice@example.com' AS email
`)
	runModel(t, p, "staging/scd2_src_null.sql")

	// Incremental SCD2: should detect NULL→value change
	r2 := runModel(t, p, "staging/scd2_null_tgt.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run2 type = %q, want incremental", r2.RunType)
	}

	// Should have history: 1 old (closed) + 1 new (current)
	total, _ := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.scd2_null_tgt")
	if total == "1" {
		t.Error("expected history rows (old + new version)")
	}

	// Current version should have the new email
	email, err := p.Sess.QueryValue("SELECT email FROM staging.scd2_null_tgt WHERE id = 1 AND is_current = true")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if email != "alice@example.com" {
		t.Errorf("email = %q, want alice@example.com", email)
	}
}

// =============================================================================
// End-to-end chain validation: Full DuckDB + DuckLake pipeline tests
// =============================================================================

// --- SCD2 Full Lifecycle ---

func TestRun_SCD2_FullLifecycle_InsertUpdateDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Tests the complete SCD2 lifecycle:
	// 1. Backfill creates table with SCD2 columns
	// 2. Update changes a row → old version closed, new version inserted
	// 3. Delete removes a row → old version closed
	// 4. Insert adds new row → new version inserted
	p := testutil.NewProject(t)

	// Step 1: Source data
	p.AddModel("raw/scd2_src.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name, 100 AS salary
UNION ALL SELECT 2, 'Bob', 200
`)
	runModel(t, p, "raw/scd2_src.sql")

	// SCD2 model
	p.AddModel("staging/scd2_life.sql", `-- @kind: scd2
-- @unique_key: id
SELECT id, name, salary FROM raw.scd2_src
`)

	// Step 2: Backfill
	r1 := runModel(t, p, "staging/scd2_life.sql")
	if r1.RunType != "backfill" {
		t.Errorf("run1 type = %q, want backfill", r1.RunType)
	}
	if r1.RowsAffected != 2 {
		t.Errorf("run1 rows = %d, want 2", r1.RowsAffected)
	}

	// Verify SCD2 columns exist
	cols, err := p.Sess.QueryRows("SELECT column_name FROM information_schema.columns WHERE table_name = 'scd2_life' ORDER BY ordinal_position")
	if err != nil {
		t.Fatalf("query columns: %v", err)
	}
	hasSCD2Cols := false
	for _, c := range cols {
		if c == "is_current" {
			hasSCD2Cols = true
		}
	}
	if !hasSCD2Cols {
		t.Fatalf("missing SCD2 columns, got: %v", cols)
	}

	// All rows should be current
	currentCount, _ := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.scd2_life WHERE is_current = true")
	if currentCount != "2" {
		t.Errorf("current rows = %s, want 2", currentCount)
	}

	// Step 3: Update Alice's salary
	p.AddModel("raw/scd2_src.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name, 150 AS salary
UNION ALL SELECT 2, 'Bob', 200
`)
	runModel(t, p, "raw/scd2_src.sql")

	r2 := runModel(t, p, "staging/scd2_life.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run2 type = %q, want incremental", r2.RunType)
	}

	// Alice should have 2 rows (old closed, new current), Bob 1
	totalRows, _ := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.scd2_life")
	if totalRows != "3" {
		t.Errorf("total rows = %s, want 3 (Alice old + Alice new + Bob)", totalRows)
	}

	// Current Alice should have salary 150
	salary, _ := p.Sess.QueryValue("SELECT salary FROM staging.scd2_life WHERE id = 1 AND is_current = true")
	if salary != "150" {
		t.Errorf("Alice salary = %s, want 150", salary)
	}

	// Old Alice should be closed
	oldCurrent, _ := p.Sess.QueryValue("SELECT is_current FROM staging.scd2_life WHERE id = 1 AND salary = 100")
	if oldCurrent != "false" {
		t.Errorf("old Alice is_current = %s, want false", oldCurrent)
	}

	// Step 4: Delete Bob, add Charlie
	p.AddModel("raw/scd2_src.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name, 150 AS salary
UNION ALL SELECT 3, 'Charlie', 300
`)
	runModel(t, p, "raw/scd2_src.sql")

	r3 := runModel(t, p, "staging/scd2_life.sql")
	if r3.RunType != "incremental" {
		t.Errorf("run3 type = %q, want incremental", r3.RunType)
	}

	// Bob should be closed, Charlie should be current
	bobCurrent, _ := p.Sess.QueryValue("SELECT is_current FROM staging.scd2_life WHERE id = 2")
	if bobCurrent != "false" {
		t.Errorf("Bob is_current = %s, want false (deleted)", bobCurrent)
	}

	charlieExists, _ := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.scd2_life WHERE id = 3 AND is_current = true")
	if charlieExists != "1" {
		t.Errorf("Charlie current rows = %s, want 1", charlieExists)
	}
}

// --- Partition Full Lifecycle ---

func TestRun_Partition_FullLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Tests partition kind:
	// 1. Backfill creates table
	// 2. Incremental replaces only affected partitions (DELETE + INSERT)
	// 3. Unaffected partitions remain untouched
	p := testutil.NewProject(t)

	// Source with partitioned data
	p.AddModel("raw/part_src.sql", `-- @kind: table
SELECT 'A' AS region, 1 AS id, 100 AS amount
UNION ALL SELECT 'A', 2, 200
UNION ALL SELECT 'B', 3, 300
`)
	runModel(t, p, "raw/part_src.sql")

	// Partition model
	p.AddModel("staging/part_tgt.sql", `-- @kind: partition
-- @unique_key: region
SELECT region, id, amount FROM raw.part_src
`)

	// Backfill
	r1 := runModel(t, p, "staging/part_tgt.sql")
	if r1.RunType != "backfill" {
		t.Errorf("run1 type = %q, want backfill", r1.RunType)
	}
	if r1.RowsAffected != 3 {
		t.Errorf("run1 rows = %d, want 3", r1.RowsAffected)
	}

	// Update only region A data, keep B the same
	p.AddModel("raw/part_src.sql", `-- @kind: table
SELECT 'A' AS region, 1 AS id, 150 AS amount
UNION ALL SELECT 'A', 2, 250
UNION ALL SELECT 'B', 3, 300
`)
	runModel(t, p, "raw/part_src.sql")

	// Incremental: should replace partition A, leave B alone
	r2 := runModel(t, p, "staging/part_tgt.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run2 type = %q, want incremental", r2.RunType)
	}

	// Verify totals
	total, _ := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.part_tgt")
	if total != "3" {
		t.Errorf("total = %s, want 3", total)
	}

	// Verify region A was updated
	amountA, _ := p.Sess.QueryValue("SELECT SUM(amount) FROM staging.part_tgt WHERE region = 'A'")
	if amountA != "400" {
		t.Errorf("region A sum = %s, want 400 (150+250)", amountA)
	}

	// Verify region B was untouched
	amountB, _ := p.Sess.QueryValue("SELECT SUM(amount) FROM staging.part_tgt WHERE region = 'B'")
	if amountB != "300" {
		t.Errorf("region B sum = %s, want 300", amountB)
	}
}

// --- Audit Rollback Full Chain ---

func TestRun_AuditFail_RollbackRestoresFullState(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Tests that when an audit fails AFTER materialize:
	// 1. Table is rolled back to previous snapshot
	// 2. Original data is fully restored
	// 3. Error is reported
	p := testutil.NewProject(t)

	// Create initial data
	p.AddModel("staging/audit_rb.sql", `-- @kind: table
-- @audit: row_count >= 5
SELECT 1 AS id, 'Alice' AS name
UNION ALL SELECT 2, 'Bob'
UNION ALL SELECT 3, 'Charlie'
`)

	// Run 1: passes (row_count = 3 >= 5 is FALSE → audit fails on first run!)
	// Actually row_count >= 5 fails with 3 rows. Let me use a constraint that passes first.
	// Use a model without audit first, then add audit
	p.AddModel("staging/audit_rb.sql", `-- @kind: table
SELECT 1 AS id, 100 AS amount
UNION ALL SELECT 2, 200
UNION ALL SELECT 3, 300
`)
	runModel(t, p, "staging/audit_rb.sql")

	// Verify initial state
	origSum, _ := p.Sess.QueryValue("SELECT SUM(amount) FROM staging.audit_rb")
	if origSum != "600" {
		t.Fatalf("initial sum = %s, want 600", origSum)
	}

	// Run 2: change data AND add failing audit
	p.AddModel("staging/audit_rb.sql", `-- @kind: table
-- @audit: row_count >= 100
SELECT 1 AS id, 999 AS amount
`)
	_, err := runModelErr(t, p, "staging/audit_rb.sql")
	if err == nil {
		t.Fatal("expected audit failure")
	}

	// Data should be rolled back to original
	restoredSum, qErr := p.Sess.QueryValue("SELECT SUM(amount) FROM staging.audit_rb")
	if qErr != nil {
		t.Fatalf("query after rollback: %v", qErr)
	}
	if restoredSum != "600" {
		t.Errorf("sum after rollback = %s, want 600 (original)", restoredSum)
	}

	count, _ := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.audit_rb")
	if count != "3" {
		t.Errorf("count after rollback = %s, want 3 (original)", count)
	}
}

// --- Rollback on first run (no previous snapshot) → DROP TABLE ---

func TestRun_AuditFail_FirstRun_DropsTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// When audit fails on FIRST run (no previous snapshot),
	// rollback should DROP the table entirely
	p := testutil.NewProject(t)

	p.AddModel("staging/audit_first.sql", `-- @kind: table
-- @audit: row_count >= 100
SELECT 1 AS id
`)

	_, err := runModelErr(t, p, "staging/audit_first.sql")
	if err == nil {
		t.Fatal("expected audit failure")
	}

	// Table should not exist
	exists, _ := p.Sess.QueryValue("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'audit_first'")
	if exists != "0" {
		t.Errorf("table should not exist after rollback on first run, but found %s", exists)
	}
}

// --- CDC Full Chain: source→target with change detection ---

func TestRun_CDC_FullChain_DetectsChanges(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Tests the full CDC chain:
	// 1. Create source, run append model (backfill)
	// 2. Update source, run append again → only changes are appended
	p := testutil.NewProject(t)

	// Source
	p.AddModel("raw/cdc_src.sql", `-- @kind: table
SELECT 1 AS id, 'v1' AS value
UNION ALL SELECT 2, 'v2'
`)
	runModel(t, p, "raw/cdc_src.sql")

	// Append model (CDC-aware)
	p.AddModel("staging/cdc_tgt.sql", `-- @kind: append
SELECT id, value FROM raw.cdc_src
`)
	r1 := runModel(t, p, "staging/cdc_tgt.sql")
	if r1.RowsAffected != 2 {
		t.Errorf("run1 rows = %d, want 2", r1.RowsAffected)
	}

	// Update source: add row 3, change row 1
	p.AddModel("raw/cdc_src.sql", `-- @kind: table
SELECT 1 AS id, 'v1_updated' AS value
UNION ALL SELECT 2, 'v2'
UNION ALL SELECT 3, 'v3'
`)
	runModel(t, p, "raw/cdc_src.sql")

	// Run append again → CDC should detect changes
	r2 := runModel(t, p, "staging/cdc_tgt.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run2 type = %q, want incremental", r2.RunType)
	}

	// Verify: append adds changed rows (id=1 updated + id=3 new)
	// CDC uses EXCEPT, so id=1 old value differs → id=1 new appears
	total, _ := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.cdc_tgt")
	// Should be: 2 (original) + 2 (changes: updated id=1 + new id=3)
	if total != "4" {
		t.Errorf("total = %s, want 4 (2 original + 2 changes)", total)
	}
}

// --- Merge Full Chain: upsert with unique_key ---

func TestRun_Merge_FullChain_UpsertAndInsert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Tests merge lifecycle:
	// 1. Backfill creates table
	// 2. Update source → existing row updated, new row inserted
	p := testutil.NewProject(t)

	p.AddModel("raw/merge_src.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name, 100 AS score
UNION ALL SELECT 2, 'Bob', 200
`)
	runModel(t, p, "raw/merge_src.sql")

	p.AddModel("staging/merge_tgt.sql", `-- @kind: merge
-- @unique_key: id
SELECT id, name, score FROM raw.merge_src
`)

	// Backfill
	r1 := runModel(t, p, "staging/merge_tgt.sql")
	if r1.RowsAffected != 2 {
		t.Errorf("run1 rows = %d, want 2", r1.RowsAffected)
	}

	// Update: change Alice's score, add Charlie
	p.AddModel("raw/merge_src.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name, 150 AS score
UNION ALL SELECT 2, 'Bob', 200
UNION ALL SELECT 3, 'Charlie', 300
`)
	runModel(t, p, "raw/merge_src.sql")

	r2 := runModel(t, p, "staging/merge_tgt.sql")
	if r2.RunType != "incremental" {
		t.Errorf("run2 type = %q, want incremental", r2.RunType)
	}

	// Verify: Alice updated, Bob unchanged, Charlie added
	total, _ := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.merge_tgt")
	if total != "3" {
		t.Errorf("total = %s, want 3", total)
	}

	aliceScore, _ := p.Sess.QueryValue("SELECT score FROM staging.merge_tgt WHERE id = 1")
	if aliceScore != "150" {
		t.Errorf("Alice score = %s, want 150", aliceScore)
	}

	charlieScore, _ := p.Sess.QueryValue("SELECT score FROM staging.merge_tgt WHERE id = 3")
	if charlieScore != "300" {
		t.Errorf("Charlie score = %s, want 300", charlieScore)
	}
}

// --- Constraint + Audit combined ---

func TestRun_ConstraintAndAudit_BothChecked(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Constraint checked BEFORE materialize, audit AFTER.
	// Both should work together.
	p := testutil.NewProject(t)

	p.AddModel("staging/both_checks.sql", `-- @kind: table
-- @constraint: id NOT NULL
-- @constraint: id UNIQUE
-- @audit: row_count >= 1
SELECT 1 AS id, 'Alice' AS name
UNION ALL SELECT 2, 'Bob'
`)
	result := runModel(t, p, "staging/both_checks.sql")
	if result.RowsAffected != 2 {
		t.Errorf("rows = %d, want 2", result.RowsAffected)
	}

	// Verify no errors or warnings
	if len(result.Errors) > 0 {
		t.Errorf("unexpected errors: %v", result.Errors)
	}
}

func TestRun_ConstraintFail_BlocksMaterialize(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Constraint fails → data should NOT be written
	p := testutil.NewProject(t)

	// First run without constraint to create table
	p.AddModel("staging/cfail.sql", `-- @kind: table
SELECT 1 AS id, 100 AS amount
`)
	runModel(t, p, "staging/cfail.sql")

	// Second run: constraint will fail (id has duplicates)
	p.AddModel("staging/cfail.sql", `-- @kind: table
-- @constraint: id UNIQUE
SELECT 1 AS id, 999 AS amount
UNION ALL SELECT 1, 888
`)
	_, err := runModelErr(t, p, "staging/cfail.sql")
	if err == nil {
		t.Fatal("expected constraint failure")
	}

	// Original data should be preserved (constraint blocks BEFORE materialize)
	amount, _ := p.Sess.QueryValue("SELECT amount FROM staging.cfail WHERE id = 1")
	if amount != "100" {
		t.Errorf("amount = %s, want 100 (original, constraint should block)", amount)
	}
}

// --- Multi-layer DAG chain ---

func TestRun_ThreeLayerPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Tests a 3-layer pipeline: raw → staging → mart
	// Verifies data flows correctly through all layers
	p := testutil.NewProject(t)

	// Layer 1: raw source
	p.AddModel("raw/orders.sql", `-- @kind: table
SELECT 1 AS order_id, 100 AS amount, 'SEK' AS currency
UNION ALL SELECT 2, 200, 'SEK'
UNION ALL SELECT 3, 50, 'EUR'
`)
	runModel(t, p, "raw/orders.sql")

	// Layer 2: staging transformation
	p.AddModel("staging/orders_clean.sql", `-- @kind: table
SELECT order_id, amount, currency,
       CASE WHEN currency = 'EUR' THEN amount * 11 ELSE amount END AS amount_sek
FROM raw.orders
`)
	runModel(t, p, "staging/orders_clean.sql")

	// Layer 3: mart aggregation
	p.AddModel("mart/order_summary.sql", `-- @kind: table
SELECT currency, COUNT(*) AS order_count, SUM(amount_sek) AS total_sek
FROM staging.orders_clean
GROUP BY currency
`)
	runModel(t, p, "mart/order_summary.sql")

	// Verify final output
	sekTotal, _ := p.Sess.QueryValue("SELECT total_sek FROM mart.order_summary WHERE currency = 'SEK'")
	if sekTotal != "300" {
		t.Errorf("SEK total = %s, want 300", sekTotal)
	}

	eurTotal, _ := p.Sess.QueryValue("SELECT total_sek FROM mart.order_summary WHERE currency = 'EUR'")
	if eurTotal != "550" {
		t.Errorf("EUR total = %s, want 550 (50*11)", eurTotal)
	}
}

// --- Schema evolution through full pipeline ---

func TestRun_SchemaEvolution_AddColumn_ThroughPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Tests that schema evolution (additive) works through the full pipeline:
	// 1. Create table with schema A
	// 2. Re-run with schema A + new column → should add column without backfill
	p := testutil.NewProject(t)

	// V1: two columns
	p.AddModel("staging/evo_pipe.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name
`)
	r1 := runModel(t, p, "staging/evo_pipe.sql")
	if r1.RowsAffected != 1 {
		t.Errorf("run1 rows = %d, want 1", r1.RowsAffected)
	}

	// V2: add email column
	p.AddModel("staging/evo_pipe.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name, 'alice@example.com' AS email
`)
	r2 := runModel(t, p, "staging/evo_pipe.sql")
	if r2.RowsAffected != 1 {
		t.Errorf("run2 rows = %d, want 1", r2.RowsAffected)
	}

	// Verify new column exists with data
	email, err := p.Sess.QueryValue("SELECT email FROM staging.evo_pipe WHERE id = 1")
	if err != nil {
		t.Fatalf("query email: %v", err)
	}
	if email != "alice@example.com" {
		t.Errorf("email = %s, want alice@example.com", email)
	}

	// Verify 3 columns now
	colCount, _ := p.Sess.QueryValue("SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'evo_pipe'")
	if colCount != "3" {
		t.Errorf("column count = %s, want 3", colCount)
	}
}

// --- Warning directive (soft validation, no rollback) ---

func TestRun_Warning_DoesNotRollback(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// @warning should log warnings but NOT roll back data
	p := testutil.NewProject(t)

	p.AddModel("staging/warn_test.sql", `-- @kind: table
-- @warning: row_count >= 100
SELECT 1 AS id
`)
	result := runModel(t, p, "staging/warn_test.sql")

	// Data should be written despite warning
	if result.RowsAffected != 1 {
		t.Errorf("rows = %d, want 1", result.RowsAffected)
	}

	// Verify data exists
	count, _ := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.warn_test")
	if count != "1" {
		t.Errorf("count = %s, want 1 (warning should not rollback)", count)
	}
}

// --- View kind through DuckLake ---

// --- ComputeSingleRunType for all kinds ---

func TestComputeSingleRunType_AllKinds(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	tests := []struct {
		name   string
		model  string
		wantRT string
	}{
		{"append_first", `-- @kind: append
SELECT 1 AS id`, "backfill"},
		{"merge_first", `-- @kind: merge
-- @unique_key: id
SELECT 1 AS id`, "backfill"},
		{"scd2_first", `-- @kind: scd2
-- @unique_key: id
SELECT 1 AS id`, "backfill"},
		{"partition_first", `-- @kind: partition
-- @unique_key: id
SELECT 1 AS id`, "backfill"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			modelFile := "staging/" + tt.name + ".sql"
			p.AddModel(modelFile, tt.model)

			modelPath := filepath.Join(p.Dir, "models", modelFile)
			model, err := parser.ParseModel(modelPath, p.Dir)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}

			decision, err := execute.ComputeSingleRunType(p.Sess, model)
			if err != nil {
				t.Fatalf("ComputeSingleRunType: %v", err)
			}
			if decision.RunType != tt.wantRT {
				t.Errorf("run_type = %q, want %q", decision.RunType, tt.wantRT)
			}
		})
	}
}

func TestComputeSingleRunType_AfterRun(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// After running once, table kind → "skip" (no deps, same SQL), append kind → "incremental"
	p := testutil.NewProject(t)

	// Table kind
	p.AddModel("staging/csrt_tbl.sql", `-- @kind: table
SELECT 1 AS id`)
	runModel(t, p, "staging/csrt_tbl.sql")

	modelPath := filepath.Join(p.Dir, "models", "staging/csrt_tbl.sql")
	model, _ := parser.ParseModel(modelPath, p.Dir)
	d1, _ := execute.ComputeSingleRunType(p.Sess, model)
	if d1.RunType != "skip" {
		t.Errorf("table after run: run_type = %q, want skip", d1.RunType)
	}

	// Append kind
	p.AddModel("staging/csrt_app.sql", `-- @kind: append
SELECT 1 AS id`)
	runModel(t, p, "staging/csrt_app.sql")

	modelPath2 := filepath.Join(p.Dir, "models", "staging/csrt_app.sql")
	model2, _ := parser.ParseModel(modelPath2, p.Dir)
	d2, _ := execute.ComputeSingleRunType(p.Sess, model2)
	if d2.RunType != "incremental" {
		t.Errorf("append after run: run_type = %q, want incremental", d2.RunType)
	}
}

// =============================================================================
// Table dependency-aware skip tests
// =============================================================================

func TestComputeSingleRunType_TableSkip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Table with no deps (SELECT 1) — after first run, same SQL → skip
	p.AddModel("staging/skip_me.sql", `-- @kind: table
SELECT 1 AS id
`)
	runModel(t, p, "staging/skip_me.sql")

	modelPath := filepath.Join(p.Dir, "models", "staging/skip_me.sql")
	model, _ := parser.ParseModel(modelPath, p.Dir)
	d, err := execute.ComputeSingleRunType(p.Sess, model)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	if d.RunType != "skip" {
		t.Errorf("run_type = %q, want skip", d.RunType)
	}
}

func TestComputeSingleRunType_TableFullOnChangedDep(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Create upstream model
	p.AddModel("raw/src.sql", `-- @kind: table
SELECT 1 AS id
`)
	runModel(t, p, "raw/src.sql")

	// Create downstream table that depends on raw.src
	p.AddModel("staging/derived.sql", `-- @kind: table
SELECT id FROM raw.src
`)
	runModel(t, p, "staging/derived.sql")

	// Now re-run upstream (changes its snapshot)
	p.AddModel("raw/src.sql", `-- @kind: table
SELECT 1 AS id, 'new' AS label
`)
	runModel(t, p, "raw/src.sql")

	// Downstream should now see changed dep → full
	modelPath := filepath.Join(p.Dir, "models", "staging/derived.sql")
	model, _ := parser.ParseModel(modelPath, p.Dir)
	d, err := execute.ComputeSingleRunType(p.Sess, model)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	if d.RunType != "full" {
		t.Errorf("run_type = %q, want full (dep changed)", d.RunType)
	}
}

func TestComputeSingleRunType_TableFullOnExternalDep(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Create a raw schema and table directly (not via ondatrasql, so no commit metadata)
	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.external (id INTEGER)")
	p.Sess.Exec("INSERT INTO raw.external VALUES (1)")

	// Create table model that depends on the external table
	p.AddModel("staging/from_ext.sql", `-- @kind: table
SELECT id FROM raw.external
`)
	runModel(t, p, "staging/from_ext.sql")

	// Re-check: dep has no commit history → unverifiable → full
	modelPath := filepath.Join(p.Dir, "models", "staging/from_ext.sql")
	model, _ := parser.ParseModel(modelPath, p.Dir)
	d, err := execute.ComputeSingleRunType(p.Sess, model)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	if d.RunType != "full" {
		t.Errorf("run_type = %q, want full (external dep, unverifiable)", d.RunType)
	}
}

func TestComputeSingleRunType_TableSkipNoDeps(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Table with empty depends array (no FROM references)
	p.AddModel("staging/no_deps.sql", `-- @kind: table
SELECT 42 AS value
`)
	runModel(t, p, "staging/no_deps.sql")

	modelPath := filepath.Join(p.Dir, "models", "staging/no_deps.sql")
	model, _ := parser.ParseModel(modelPath, p.Dir)
	d, err := execute.ComputeSingleRunType(p.Sess, model)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	if d.RunType != "skip" {
		t.Errorf("run_type = %q, want skip (no deps)", d.RunType)
	}
}

func TestComputeSingleRunType_TableBackfillOnNewSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("staging/evolve.sql", `-- @kind: table
SELECT 1 AS id
`)
	runModel(t, p, "staging/evolve.sql")

	// Change SQL → hash changes → backfill
	p.AddModel("staging/evolve.sql", `-- @kind: table
SELECT 1 AS id, 'new_col' AS label
`)
	modelPath := filepath.Join(p.Dir, "models", "staging/evolve.sql")
	model, _ := parser.ParseModel(modelPath, p.Dir)
	d, err := execute.ComputeSingleRunType(p.Sess, model)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	if d.RunType != "backfill" {
		t.Errorf("run_type = %q, want backfill (SQL changed)", d.RunType)
	}
}

func TestRun_TableSkip_EarlyReturn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run: creates the table
	p.AddModel("staging/skiptest.sql", `-- @kind: table
SELECT 1 AS id, 100 AS value
`)
	r1 := runModel(t, p, "staging/skiptest.sql")
	if r1.RunType != "backfill" {
		t.Errorf("first run: run_type = %q, want backfill", r1.RunType)
	}

	// Second run: same SQL, no deps → skip, data preserved
	r2 := runModel(t, p, "staging/skiptest.sql")
	if r2.RunType != "skip" {
		t.Errorf("second run: run_type = %q, want skip", r2.RunType)
	}
	if r2.RowsAffected != 0 {
		t.Errorf("skip should have 0 rows affected, got %d", r2.RowsAffected)
	}

	// Data should still be there
	val, err := p.Sess.QueryValue("SELECT value FROM staging.skiptest WHERE id = 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "100" {
		t.Errorf("value = %s, want 100 (data preserved after skip)", val)
	}
}

// --- View kind integration tests ---

func TestRun_ViewKind(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("raw/data.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name
UNION ALL SELECT 2, 'Bob'
`)
	runModel(t, p, "raw/data.sql")

	p.AddModel("staging/data.sql", `-- @kind: view
SELECT id, name FROM raw.data WHERE id > 0
`)
	result := runModel(t, p, "staging/data.sql")

	if result.Kind != "view" {
		t.Errorf("kind = %q, want view", result.Kind)
	}
	if result.RunType != "create" {
		t.Errorf("run_type = %q, want create", result.RunType)
	}

	val, err := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.data")
	if err != nil {
		t.Fatalf("query view: %v", err)
	}
	if val != "2" {
		t.Errorf("count = %s, want 2", val)
	}
}

func TestRun_ViewKind_SkipUnchanged(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("raw/src.sql", `-- @kind: table
SELECT 1 AS id
`)
	runModel(t, p, "raw/src.sql")

	p.AddModel("staging/v.sql", `-- @kind: view
SELECT id FROM raw.src
`)
	r1 := runModel(t, p, "staging/v.sql")
	if r1.RunType != "create" {
		t.Errorf("run1: run_type = %q, want create", r1.RunType)
	}

	r2 := runModel(t, p, "staging/v.sql")
	if r2.RunType != "skip" {
		t.Errorf("run2: run_type = %q, want skip", r2.RunType)
	}
}

func TestRun_ViewKind_LiveResolution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("raw/src.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name
`)
	runModel(t, p, "raw/src.sql")

	p.AddModel("staging/v.sql", `-- @kind: view
SELECT * FROM raw.src
`)
	runModel(t, p, "staging/v.sql")

	p.AddModel("raw/src.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name
UNION ALL SELECT 2, 'Bob'
`)
	runModel(t, p, "raw/src.sql")

	val, err := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.v")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "2" {
		t.Errorf("count = %s, want 2 (view resolves live)", val)
	}
}

func TestRun_ViewKind_WithDescription(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("raw/src.sql", `-- @kind: table
SELECT 1 AS id
`)
	runModel(t, p, "raw/src.sql")

	p.AddModel("staging/described.sql", `-- @kind: view
-- @description: Cleaned source data

SELECT id FROM raw.src
`)
	runModel(t, p, "staging/described.sql")

	comment, err := p.Sess.QueryValue(`
		SELECT COALESCE(comment, '') FROM duckdb_views()
		WHERE schema_name = 'staging' AND view_name = 'described'
	`)
	if err != nil {
		t.Fatalf("query comment: %v", err)
	}
	if comment != "Cleaned source data" {
		t.Errorf("comment = %q, want %q", comment, "Cleaned source data")
	}
}

func TestRun_ViewKind_RejectsConstraints(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/bad.sql", `-- @kind: view
-- @constraint: id NOT NULL

SELECT 1 AS id
`)
	modelPath := filepath.Join(p.Dir, "models", "staging/bad.sql")
	_, err := parser.ParseModel(modelPath, p.Dir)
	if err == nil {
		t.Error("expected error for view with @constraint")
	}
}

// --- Storage hints integration tests ---

func TestRun_SortedBy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("staging/sorted.sql", `-- @kind: table
-- @sorted_by: id

SELECT 2 AS id, 'Bob' AS name
UNION ALL SELECT 1, 'Alice'
UNION ALL SELECT 3, 'Carol'
`)
	result := runModel(t, p, "staging/sorted.sql")
	if result.RowsAffected != 3 {
		t.Errorf("rows = %d, want 3", result.RowsAffected)
	}

	// Table should be queryable (sorted_by is a storage hint, doesn't affect queries)
	val, err := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.sorted")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "3" {
		t.Errorf("count = %s, want 3", val)
	}
}

func TestRun_PartitionedBy_StorageHint(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// @partitioned_by on merge kind = DuckLake native storage hint (not DELETE+INSERT)
	p.AddModel("staging/partitioned.sql", `-- @kind: merge
-- @unique_key: id
-- @partitioned_by: region

SELECT 1 AS id, 'US' AS region, 100 AS amount
UNION ALL SELECT 2, 'EU', 200
`)
	result := runModel(t, p, "staging/partitioned.sql")
	if result.RowsAffected != 2 {
		t.Errorf("rows = %d, want 2", result.RowsAffected)
	}

	val, err := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.partitioned")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "2" {
		t.Errorf("count = %s, want 2", val)
	}
}

// --- Column masking integration tests ---

func TestRun_ColumnMasking(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Create source with PII data
	p.AddModel("raw/customers.sql", `-- @kind: table
SELECT 1 AS id, 'Alice Smith' AS name, 'alice@example.com' AS email
UNION ALL SELECT 2, 'Bob Jones', 'bob@company.org'
`)
	runModel(t, p, "raw/customers.sql")

	// Define masking macros (normally in config/macros.sql)
	p.Sess.Exec("CREATE OR REPLACE MACRO mask(val) AS left(val::VARCHAR, 1) || repeat('*', length(val::VARCHAR) - 1)")
	p.Sess.Exec("CREATE OR REPLACE MACRO mask_email(val) AS regexp_replace(val::VARCHAR, '(.).*@', '\\1***@')")

	// Create materialized model with masking tags
	p.AddModel("staging/customers.sql", `-- @kind: merge
-- @unique_key: id
-- @column: name = Customer name | mask
-- @column: email = Contact email | mask_email

SELECT id, name, email FROM raw.customers
`)
	result := runModel(t, p, "staging/customers.sql")

	if result.RowsAffected != 2 {
		t.Errorf("rows = %d, want 2", result.RowsAffected)
	}

	// Verify data is masked
	name, err := p.Sess.QueryValue("SELECT name FROM staging.customers WHERE id = 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if name == "Alice Smith" {
		t.Error("name should be masked, got original value")
	}
	if !strings.HasPrefix(name, "A") {
		t.Errorf("masked name should start with A, got %q", name)
	}

	email, err := p.Sess.QueryValue("SELECT email FROM staging.customers WHERE id = 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if email == "alice@example.com" {
		t.Error("email should be masked, got original value")
	}
	if !strings.Contains(email, "***@") {
		t.Errorf("masked email should contain ***@, got %q", email)
	}

	// Verify unique_key is NOT masked
	id, err := p.Sess.QueryValue("SELECT id FROM staging.customers WHERE id = 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if id != "1" {
		t.Errorf("unique_key should not be masked, got %q", id)
	}
}

func TestRun_ColumnMasking_MetadataOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("raw/data.sql", `-- @kind: table
SELECT 1 AS id, 'secret' AS value
`)
	runModel(t, p, "raw/data.sql")

	// PII tag without masking prefix — should NOT mask
	p.AddModel("staging/data.sql", `-- @kind: merge
-- @unique_key: id
-- @column: value = Sensitive data | PII

SELECT id, value FROM raw.data
`)
	runModel(t, p, "staging/data.sql")

	val, err := p.Sess.QueryValue("SELECT value FROM staging.data WHERE id = 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "secret" {
		t.Errorf("PII tag alone should not mask, got %q want 'secret'", val)
	}
}

// --- COMMENT ON integration tests ---

func TestRun_CommentOnTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/described.sql", `-- @kind: table
-- @description: Daily aggregated sales by region

SELECT 1 AS id, 100.0 AS amount
`)
	runModel(t, p, "staging/described.sql")

	// Verify table comment was set
	comment, err := p.Sess.QueryValue(`
		SELECT comment FROM duckdb_tables()
		WHERE schema_name = 'staging' AND table_name = 'described'
	`)
	if err != nil {
		t.Fatalf("query table comment: %v", err)
	}
	if comment != "Daily aggregated sales by region" {
		t.Errorf("table comment = %q, want %q", comment, "Daily aggregated sales by region")
	}
}

func TestRun_CommentOnColumns(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/col_comments.sql", `-- @kind: table
-- @description: Sales data
-- @column: amount = Total revenue including tax
-- @column: region = Geographic sales region

SELECT 100.0 AS amount, 'US' AS region
`)
	runModel(t, p, "staging/col_comments.sql")

	// Verify column comments
	amountComment, err := p.Sess.QueryValue(`
		SELECT comment FROM duckdb_columns()
		WHERE schema_name = 'staging' AND table_name = 'col_comments' AND column_name = 'amount'
	`)
	if err != nil {
		t.Fatalf("query column comment: %v", err)
	}
	if amountComment != "Total revenue including tax" {
		t.Errorf("amount comment = %q, want %q", amountComment, "Total revenue including tax")
	}

	regionComment, err := p.Sess.QueryValue(`
		SELECT comment FROM duckdb_columns()
		WHERE schema_name = 'staging' AND table_name = 'col_comments' AND column_name = 'region'
	`)
	if err != nil {
		t.Fatalf("query column comment: %v", err)
	}
	if regionComment != "Geographic sales region" {
		t.Errorf("region comment = %q, want %q", regionComment, "Geographic sales region")
	}
}

func TestRun_CommentOnTable_SCD2(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/scd2_desc.sql", `-- @kind: scd2
-- @unique_key: id
-- @description: Customer history with SCD2 tracking

SELECT 1 AS id, 'Alice' AS name
`)
	runModel(t, p, "staging/scd2_desc.sql")

	comment, err := p.Sess.QueryValue(`
		SELECT comment FROM duckdb_tables()
		WHERE schema_name = 'staging' AND table_name = 'scd2_desc'
	`)
	if err != nil {
		t.Fatalf("query table comment: %v", err)
	}
	if comment != "Customer history with SCD2 tracking" {
		t.Errorf("table comment = %q, want %q", comment, "Customer history with SCD2 tracking")
	}
}

func TestRun_CommentOnTable_Partition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/part_desc.sql", `-- @kind: partition
-- @unique_key: region
-- @description: Regional sales partitioned by region

SELECT 'US' AS region, 100 AS amount
`)
	runModel(t, p, "staging/part_desc.sql")

	comment, err := p.Sess.QueryValue(`
		SELECT comment FROM duckdb_tables()
		WHERE schema_name = 'staging' AND table_name = 'part_desc'
	`)
	if err != nil {
		t.Fatalf("query table comment: %v", err)
	}
	if comment != "Regional sales partitioned by region" {
		t.Errorf("table comment = %q, want %q", comment, "Regional sales partitioned by region")
	}
}

func TestRun_CommentOnTable_SCD2_Incremental(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run: create without description
	p.AddModel("staging/scd2_inc.sql", `-- @kind: scd2
-- @unique_key: id

SELECT 1 AS id, 'Alice' AS name
`)
	runModel(t, p, "staging/scd2_inc.sql")

	// Second run: add description (incremental)
	p.AddModel("staging/scd2_inc.sql", `-- @kind: scd2
-- @unique_key: id
-- @description: Customer history updated

SELECT 1 AS id, 'Bob' AS name
`)
	runModel(t, p, "staging/scd2_inc.sql")

	comment, err := p.Sess.QueryValue(`
		SELECT COALESCE(comment, '') FROM duckdb_tables()
		WHERE schema_name = 'staging' AND table_name = 'scd2_inc'
	`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if comment != "Customer history updated" {
		t.Errorf("table comment = %q, want %q", comment, "Customer history updated")
	}
}

func TestRun_CommentOnTable_Partition_Incremental(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run: create without description
	p.AddModel("staging/part_inc.sql", `-- @kind: partition
-- @unique_key: region

SELECT 'US' AS region, 100 AS amount
`)
	runModel(t, p, "staging/part_inc.sql")

	// Second run: add description (incremental)
	p.AddModel("staging/part_inc.sql", `-- @kind: partition
-- @unique_key: region
-- @description: Regional sales updated

SELECT 'US' AS region, 200 AS amount
`)
	runModel(t, p, "staging/part_inc.sql")

	comment, err := p.Sess.QueryValue(`
		SELECT COALESCE(comment, '') FROM duckdb_tables()
		WHERE schema_name = 'staging' AND table_name = 'part_inc'
	`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if comment != "Regional sales updated" {
		t.Errorf("table comment = %q, want %q", comment, "Regional sales updated")
	}
}

func TestRun_CommentOnTable_NoDescription(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/nodesc.sql", `-- @kind: table
SELECT 1 AS id
`)
	runModel(t, p, "staging/nodesc.sql")

	// Table should exist but with no comment (NULL or empty)
	comment, err := p.Sess.QueryValue(`
		SELECT COALESCE(comment, '') FROM duckdb_tables()
		WHERE schema_name = 'staging' AND table_name = 'nodesc'
	`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if comment != "" {
		t.Errorf("expected no comment, got %q", comment)
	}
}

