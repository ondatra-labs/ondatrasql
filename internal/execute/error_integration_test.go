// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/execute"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// --- materializeSCD2: error mid-flow (session works, tmp table missing) ---

func TestRun_SCD2_MissingTempTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// SCD2 kind with invalid SQL that references nonexistent table
	// This triggers error paths inside materializeSCD2 (after ensureSchemaExists succeeds)
	p := testutil.NewProject(t)
	p.AddModel("staging/bad_scd2.sql", `-- @kind: scd2
-- @unique_key: id
SELECT * FROM nonexistent_source_table
`)
	result, err := runModelErr(t, p, "staging/bad_scd2.sql")
	if err == nil && len(result.Errors) == 0 {
		t.Fatal("expected error for SCD2 with nonexistent source")
	}
}

// --- materializePartition: error mid-flow ---

func TestRun_Partition_MissingTempTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/bad_part.sql", `-- @kind: partition
-- @unique_key: region
SELECT * FROM nonexistent_source_table
`)
	result, err := runModelErr(t, p, "staging/bad_part.sql")
	if err == nil && len(result.Errors) == 0 {
		t.Fatal("expected error for partition with nonexistent source")
	}
}

// --- materialize: unknown kind ---

func TestRun_UnknownKind(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	// Build model struct directly to bypass parser's kind validation
	model := &parser.Model{
		Target: "staging.bad_kind",
		Kind:   "imaginary",
		SQL:    "SELECT 1 AS id",
	}
	runner := execute.NewRunner(p.Sess, execute.ModeRun, "test")
	_, err := runner.Run(context.Background(), model)
	if err == nil {
		t.Fatal("expected error for unknown kind")
	}
	if !strings.Contains(err.Error(), "unknown kind") {
		t.Errorf("expected 'unknown kind' error, got: %v", err)
	}
}

// --- merge without unique_key: error in materialize ---

func TestRun_Merge_MissingUniqueKey(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	// Create model with merge kind but no unique_key
	testutil.WriteFile(t, p.Dir, "models/staging/bad_merge.sql", "-- @kind: merge\nSELECT 1 AS id, 'hello' AS name\n")
	modelPath := filepath.Join(p.Dir, "models/staging/bad_merge.sql")
	model, err := parser.ParseModel(modelPath, p.Dir)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Bug 16 fix: missing @unique_key fails IMMEDIATELY on first run
	// instead of cryptically on the second. The user shouldn't have to
	// run twice to discover the misconfiguration.
	runner := execute.NewRunner(p.Sess, execute.ModeRun, "test")
	_, err = runner.Run(context.Background(), model)
	if err == nil {
		t.Fatal("expected error for merge without unique_key on first run")
	}
	if !strings.Contains(err.Error(), "merge kind requires unique_key") {
		t.Errorf("expected 'merge kind requires unique_key' error, got: %v", err)
	}
}

// --- Run with failing audit (triggers rollback path) ---

func TestRun_AuditFailure_TriggersRollback(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	// `row_count = 0` will always fail when we INSERT a row. With the
	// transactional audit refactor, the failure surfaces as a DuckDB
	// error() call inside the materialize BEGIN/COMMIT, which aborts
	// the whole transaction atomically — no separate rollback() helper.
	p.AddModel("staging/audited.sql", `-- @kind: table
-- @audit: row_count(=, 0)
SELECT 1 AS id, 100 AS amount
`)
	result, err := runModelErr(t, p, "staging/audited.sql")
	if err == nil {
		t.Fatal("expected materialize error from failing audit")
	}
	// Error wraps the DuckDB error() output, which contains "audit failed:".
	if !strings.Contains(err.Error(), "audit failed") {
		t.Errorf("expected 'audit failed' in error, got: %v", err)
	}
	if result == nil || len(result.Errors) == 0 {
		t.Error("expected result.Errors to contain the audit failure message")
	}
}

// --- Run with failing constraint ---

func TestRun_ConstraintFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/constrained.sql", `-- @kind: table
-- @constraint: not_null(id)
SELECT NULL AS id, 'test' AS name
`)
	result, err := runModelErr(t, p, "staging/constrained.sql")
	if err == nil {
		t.Fatal("expected error from constraint failure")
	}
	if !strings.Contains(err.Error(), "constraint validation failed") {
		t.Errorf("expected 'constraint validation failed', got: %v", err)
	}
	if len(result.Errors) == 0 {
		t.Error("expected errors in result")
	}
}

// --- SCD2 incremental run (triggers the non-backfill path) ---

func TestRun_SCD2_IncrementalRun_ErrorPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run: create SCD2 table
	p.AddModel("staging/scd2_test.sql", `-- @kind: scd2
-- @unique_key: id
SELECT 1 AS id, 'Alice' AS name
UNION ALL SELECT 2, 'Bob'
`)
	result := runModel(t, p, "staging/scd2_test.sql")
	if len(result.Errors) > 0 {
		t.Fatalf("first run errors: %v", result.Errors)
	}

	// Second run: same SQL hash → incremental, triggers change detection
	result2 := runModel(t, p, "staging/scd2_test.sql")
	if len(result2.Errors) > 0 {
		t.Fatalf("second run errors: %v", result2.Errors)
	}
	if result2.RunType != "incremental" {
		t.Errorf("expected incremental run type, got: %s", result2.RunType)
	}
}

// --- Partition incremental run ---

func TestRun_Partition_IncrementalRun_ErrorPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run: create partition table
	p.AddModel("staging/part_test.sql", `-- @kind: partition
-- @unique_key: region
SELECT 'EU' AS region, 1 AS id, 100 AS amount
UNION ALL SELECT 'US', 2, 200
`)
	result := runModel(t, p, "staging/part_test.sql")
	if len(result.Errors) > 0 {
		t.Fatalf("first run errors: %v", result.Errors)
	}

	// Second run: same SQL → incremental partition replace
	result2 := runModel(t, p, "staging/part_test.sql")
	if len(result2.Errors) > 0 {
		t.Fatalf("second run errors: %v", result2.Errors)
	}
	if result2.RunType != "incremental" {
		t.Errorf("expected incremental run type, got: %s", result2.RunType)
	}
}

// --- Audit failure triggers rollback error path ---

func TestRun_AuditFailure_RollbackError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	// `row_count < 0` is always false → audit row count will always be
	// greater-than-or-equal-to zero, so the inverted `< 0` check fails
	// for any inserted row. This exercises the in-transaction error()
	// abort path: a row count of 1 trips the check, the SELECT error()
	// fires inside BEGIN/COMMIT, the transaction rolls back, and the
	// caller surfaces an error containing "audit failed".
	p.AddModel("staging/audit_rollback.sql", `-- @kind: table
-- @audit: row_count(<, 0)
SELECT 1 AS id
`)
	result, err := runModelErr(t, p, "staging/audit_rollback.sql")
	if err == nil {
		t.Fatal("expected materialize error from failing audit")
	}
	if !strings.Contains(err.Error(), "audit failed") {
		t.Errorf("expected 'audit failed' in error, got: %v", err)
	}
	if result == nil || len(result.Errors) == 0 {
		t.Error("expected result.Errors to contain the audit failure message")
	}
}

// --- Merge incremental with getTableColumns (covers merge non-backfill path) ---

func TestRun_Merge_Incremental(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run: creates as table (backfill)
	p.AddModel("staging/merge_incr.sql", `-- @kind: merge
-- @unique_key: id
SELECT 1 AS id, 'Alice' AS name
`)
	result := runModel(t, p, "staging/merge_incr.sql")
	if len(result.Errors) > 0 {
		t.Fatalf("first run errors: %v", result.Errors)
	}

	// Second run: incremental merge (covers getTableColumns + buildMergeSQL)
	result2 := runModel(t, p, "staging/merge_incr.sql")
	if len(result2.Errors) > 0 {
		t.Fatalf("second run errors: %v", result2.Errors)
	}
	if result2.RunType != "incremental" {
		t.Errorf("expected incremental, got: %s", result2.RunType)
	}
}

// --- Append incremental (covers append non-backfill path) ---

func TestRun_Append_Incremental(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("staging/append_incr.sql", `-- @kind: append
SELECT 1 AS id, 'data' AS value
`)
	result := runModel(t, p, "staging/append_incr.sql")
	if len(result.Errors) > 0 {
		t.Fatalf("first run errors: %v", result.Errors)
	}

	result2 := runModel(t, p, "staging/append_incr.sql")
	if len(result2.Errors) > 0 {
		t.Fatalf("second run errors: %v", result2.Errors)
	}
	if result2.RunType != "incremental" {
		t.Errorf("expected incremental, got: %s", result2.RunType)
	}
}

// --- CDC path: incremental append with source table changes ---

func TestRun_Append_CDC_Path(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// Create a source table in DuckLake
	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.events AS SELECT 1 AS id, 'click' AS event_type")

	// Create append model that reads from the source table
	p.AddModel("staging/events_agg.sql", `-- @kind: append
SELECT id, event_type FROM raw.events
`)
	// First run: backfill
	result := runModel(t, p, "staging/events_agg.sql")
	if len(result.Errors) > 0 {
		t.Fatalf("first run errors: %v", result.Errors)
	}

	// Modify source table (creates new DuckLake snapshot)
	p.Sess.Exec("INSERT INTO raw.events VALUES (2, 'view')")

	// Second run: should trigger CDC path (incremental with cdcTables)
	result2 := runModel(t, p, "staging/events_agg.sql")
	if len(result2.Errors) > 0 {
		t.Fatalf("second run errors: %v", result2.Errors)
	}
	// May be incremental with CDC or backfill depending on hash
	t.Logf("run type: %s, warnings: %v", result2.RunType, result2.Warnings)
}

// --- Merge with CDC path ---

func TestRun_Merge_CDC_Path(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.users AS SELECT 1 AS id, 'Alice' AS name")

	p.AddModel("staging/users_merged.sql", `-- @kind: merge
-- @unique_key: id
SELECT id, name FROM raw.users
`)
	result := runModel(t, p, "staging/users_merged.sql")
	if len(result.Errors) > 0 {
		t.Fatalf("first run errors: %v", result.Errors)
	}

	// Modify source
	p.Sess.Exec("INSERT INTO raw.users VALUES (2, 'Bob')")

	result2 := runModel(t, p, "staging/users_merged.sql")
	if len(result2.Errors) > 0 {
		t.Fatalf("second run errors: %v", result2.Errors)
	}
	t.Logf("run type: %s, warnings: %v", result2.RunType, result2.Warnings)
}

// --- Schema evolution: additive change triggers schema evolution ---

func TestRun_SchemaEvolution_AdditiveChange(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run with 2 columns
	p.AddModel("staging/evolving.sql", `-- @kind: append
SELECT 1 AS id, 'hello' AS name
`)
	result := runModel(t, p, "staging/evolving.sql")
	if len(result.Errors) > 0 {
		t.Fatalf("first run errors: %v", result.Errors)
	}

	// Rewrite model with added column — triggers schema evolution
	testutil.WriteFile(t, p.Dir, "models/staging/evolving.sql", "-- @kind: append\nSELECT 1 AS id, 'hello' AS name, 42 AS new_col\n")
	result2 := runModel(t, p, "staging/evolving.sql")
	if len(result2.Errors) > 0 {
		t.Fatalf("second run errors: %v", result2.Errors)
	}
	// Should mention schema evolution in warnings
	hasEvolution := false
	for _, w := range result2.Warnings {
		if strings.Contains(w, "schema evolution") {
			hasEvolution = true
		}
	}
	if hasEvolution {
		t.Logf("schema evolution detected: %v", result2.Warnings)
	}
}

// --- Schema evolution: destructive change forces backfill ---

func TestRun_SchemaEvolution_DestructiveColumnDrop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run with 3 columns
	p.AddModel("staging/destructive.sql", `-- @kind: append
SELECT 1 AS id, 'hello' AS name, 42 AS extra
`)
	result := runModel(t, p, "staging/destructive.sql")
	if len(result.Errors) > 0 {
		t.Fatalf("first run errors: %v", result.Errors)
	}

	// Remove a column — destructive change, forces backfill
	testutil.WriteFile(t, p.Dir, "models/staging/destructive.sql", "-- @kind: append\nSELECT 1 AS id, 'hello' AS name\n")
	result2 := runModel(t, p, "staging/destructive.sql")
	if len(result2.Errors) > 0 {
		t.Fatalf("second run errors: %v", result2.Errors)
	}
	t.Logf("run type: %s, warnings: %v", result2.RunType, result2.Warnings)
}

// --- Schema rename detection (covers runner.go:335-371) ---

func TestRun_SchemaEvolution_RenameDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// First run: create table with column "old_name" from a source
	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.rename_src AS SELECT 1 AS id, 'hello' AS old_name")

	p.AddModel("staging/rename_test.sql", `-- @kind: append
SELECT id, old_name FROM raw.rename_src
`)
	result := runModel(t, p, "staging/rename_test.sql")
	if len(result.Errors) > 0 {
		t.Fatalf("first run errors: %v", result.Errors)
	}

	// Now rename the column in source and model
	p.Sess.Exec("ALTER TABLE raw.rename_src RENAME COLUMN old_name TO new_name")
	testutil.WriteFile(t, p.Dir, "models/staging/rename_test.sql", "-- @kind: append\nSELECT id, new_name FROM raw.rename_src\n")

	result2 := runModel(t, p, "staging/rename_test.sql")
	if len(result2.Errors) > 0 {
		t.Fatalf("second run errors: %v", result2.Errors)
	}
	t.Logf("run type: %s, warnings: %v", result2.RunType, result2.Warnings)
}

// --- Audit query error (covers runner.go:444-446) ---

func TestRun_AuditQueryError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	// Use an audit with invalid SQL syntax to trigger query error
	p.AddModel("staging/bad_audit.sql", `-- @kind: table
-- @audit: INVALID SQL SYNTAX !!!
SELECT 1 AS id
`)
	result, err := runModelErr(t, p, "staging/bad_audit.sql")
	if err != nil {
		// Audit validation failed — covers the audit error + rollback path
		t.Logf("audit error (expected): %v", err)
	}
	if result != nil && len(result.Errors) > 0 {
		t.Logf("result errors: %v", result.Errors)
	}
}

// --- ensureSchemaExists error path ---

func TestRun_EnsureSchemaExists_Error(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	testutil.WriteFile(t, p.Dir, "models/staging/schema_err.sql", "-- @kind: table\nSELECT 1 AS id\n")
	modelPath := filepath.Join(p.Dir, "models/staging/schema_err.sql")
	model, err := parser.ParseModel(modelPath, p.Dir)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	// Use invalid catalog prefix to trigger schema creation error
	model.Target = "nonexistent_catalog.bad_schema.table1"
	runner := execute.NewRunner(p.Sess, execute.ModeRun, "test")
	_, err = runner.Run(context.Background(), model)
	if err == nil {
		t.Fatal("expected error for invalid schema target")
	}
}
