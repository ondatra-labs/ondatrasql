// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package backfill_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/backfill"
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

func TestCaptureSchema(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/users.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name, 30 AS age
`)
	runModel(t, p, "staging/users.sql")

	cols, err := backfill.CaptureSchema(p.Sess, "staging.users")
	if err != nil {
		t.Fatalf("CaptureSchema: %v", err)
	}
	if len(cols) != 3 {
		t.Fatalf("columns = %d, want 3", len(cols))
	}

	colMap := make(map[string]string)
	for _, c := range cols {
		colMap[c.Name] = c.Type
	}
	if colMap["id"] != "INTEGER" {
		t.Errorf("id type = %q, want INTEGER", colMap["id"])
	}
	if colMap["name"] != "VARCHAR" {
		t.Errorf("name type = %q, want VARCHAR", colMap["name"])
	}
	if colMap["age"] != "INTEGER" {
		t.Errorf("age type = %q, want INTEGER", colMap["age"])
	}
}

func TestCaptureSchema_SchemaTable(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("raw/events.sql", `-- @kind: table
SELECT 1 AS event_id, 'click' AS event_type, TIMESTAMP '2024-01-01' AS ts
`)
	runModel(t, p, "raw/events.sql")

	cols, err := backfill.CaptureSchema(p.Sess, "raw.events")
	if err != nil {
		t.Fatalf("CaptureSchema: %v", err)
	}
	if len(cols) != 3 {
		t.Fatalf("columns = %d, want 3", len(cols))
	}

	colMap := make(map[string]string)
	for _, c := range cols {
		colMap[c.Name] = c.Type
	}
	if _, ok := colMap["event_id"]; !ok {
		t.Error("missing column event_id")
	}
	if _, ok := colMap["event_type"]; !ok {
		t.Error("missing column event_type")
	}
	if _, ok := colMap["ts"]; !ok {
		t.Error("missing column ts")
	}
}

func TestGetPreviousSnapshot(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/orders.sql", `-- @kind: table
SELECT 1 AS id, 100.0 AS amount
`)
	runModel(t, p, "staging/orders.sql")

	snap, err := backfill.GetPreviousSnapshot(p.Sess, "staging.orders")
	if err != nil {
		t.Fatalf("GetPreviousSnapshot: %v", err)
	}
	if snap <= 0 {
		t.Errorf("snapshot = %d, want > 0", snap)
	}
}

func TestGetPreviousSnapshot_NoHistory(t *testing.T) {
	p := testutil.NewProject(t)

	// GetPreviousSnapshot returns the catalog's current snapshot ID,
	// not model-specific. A fresh DuckLake catalog may already be at snapshot >= 0.
	snap, err := backfill.GetPreviousSnapshot(p.Sess, "staging.nonexistent")
	if err != nil {
		t.Fatalf("GetPreviousSnapshot: %v", err)
	}
	if snap < 0 {
		t.Errorf("snapshot = %d, want >= 0", snap)
	}
}

func TestGetPreviousSchema(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/products.sql", `-- @kind: table
SELECT 1 AS id, 'Widget' AS name, 9.99 AS price
`)
	runModel(t, p, "staging/products.sql")

	cols, err := backfill.GetPreviousSchema(p.Sess, "staging.products")
	if err != nil {
		t.Fatalf("GetPreviousSchema: %v", err)
	}
	if cols == nil {
		t.Fatal("expected non-nil schema")
	}
	if len(cols) != 3 {
		t.Fatalf("columns = %d, want 3", len(cols))
	}

	colMap := make(map[string]string)
	for _, c := range cols {
		colMap[c.Name] = c.Type
	}
	if _, ok := colMap["id"]; !ok {
		t.Error("missing column id")
	}
	if _, ok := colMap["name"]; !ok {
		t.Error("missing column name")
	}
	if _, ok := colMap["price"]; !ok {
		t.Error("missing column price")
	}
}

func TestGetPreviousSchema_NoHistory(t *testing.T) {
	p := testutil.NewProject(t)

	cols, err := backfill.GetPreviousSchema(p.Sess, "staging.nonexistent")
	if err != nil {
		t.Fatalf("GetPreviousSchema: %v", err)
	}
	if cols != nil {
		t.Errorf("expected nil schema, got %v", cols)
	}
}

func TestGetModelCommitInfo(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/tracked.sql", `-- @kind: table
SELECT 1 AS id, 'test' AS value
`)
	runModel(t, p, "staging/tracked.sql")

	info, err := backfill.GetModelCommitInfo(p.Sess, "staging.tracked")
	if err != nil {
		t.Fatalf("GetModelCommitInfo: %v", err)
	}
	if info == nil {
		t.Fatal("expected non-nil commit info")
	}
	if info.Model != "staging.tracked" {
		t.Errorf("model = %q, want staging.tracked", info.Model)
	}
	if info.Kind != "table" {
		t.Errorf("kind = %q, want table", info.Kind)
	}
	if info.SQLHash == "" {
		t.Error("expected non-empty sql_hash")
	}
	if info.RunType == "" {
		t.Error("expected non-empty run_type")
	}
	if info.RowsAffected != 1 {
		t.Errorf("rows_affected = %d, want 1", info.RowsAffected)
	}
}

func TestGetModelCommitInfo_NoHistory(t *testing.T) {
	p := testutil.NewProject(t)

	info, err := backfill.GetModelCommitInfo(p.Sess, "staging.nonexistent")
	if err != nil {
		t.Fatalf("GetModelCommitInfo: %v", err)
	}
	if info != nil {
		t.Errorf("expected nil, got %+v", info)
	}
}

func TestGetDownstreamModels(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/source.sql", `-- @kind: table
SELECT 1 AS id, 10 AS value
`)
	runModel(t, p, "raw/source.sql")

	p.AddModel("staging/derived.sql", `-- @kind: table
SELECT id, value * 2 AS doubled FROM raw.source
`)
	runModel(t, p, "staging/derived.sql")

	downstream, err := backfill.GetDownstreamModels(p.Sess, "raw.source")
	if err != nil {
		t.Fatalf("GetDownstreamModels: %v", err)
	}

	found := false
	for _, m := range downstream {
		if m == "staging.derived" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected staging.derived in downstream, got %v", downstream)
	}
}

func TestGetIncrementalState_TableNotExists(t *testing.T) {
	p := testutil.NewProject(t)

	state, err := backfill.GetIncrementalState(p.Sess, "staging.missing", "updated_at", "2020-01-01T00:00:00Z")
	if err != nil {
		t.Fatalf("GetIncrementalState: %v", err)
	}
	if !state.IsBackfill {
		t.Error("expected IsBackfill=true for non-existent table")
	}
	if state.LastValue != "2020-01-01T00:00:00Z" {
		t.Errorf("LastValue = %q, want 2020-01-01T00:00:00Z", state.LastValue)
	}
	if state.Cursor != "updated_at" {
		t.Errorf("Cursor = %q, want updated_at", state.Cursor)
	}
}

func TestGetIncrementalState_TableExists(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("staging/incremental.sql", `-- @kind: table
SELECT 1 AS id, TIMESTAMP '2024-06-15 12:00:00' AS updated_at
UNION ALL SELECT 2, TIMESTAMP '2024-06-16 08:30:00'
`)
	runModel(t, p, "staging/incremental.sql")

	state, err := backfill.GetIncrementalState(p.Sess, "staging.incremental", "updated_at", "2020-01-01T00:00:00Z")
	if err != nil {
		t.Fatalf("GetIncrementalState: %v", err)
	}
	if state.IsBackfill {
		t.Error("expected IsBackfill=false for existing table")
	}
	if state.LastValue == "" || state.LastValue == "2020-01-01T00:00:00Z" {
		t.Errorf("LastValue = %q, expected actual max cursor value", state.LastValue)
	}
	if state.Cursor != "updated_at" {
		t.Errorf("Cursor = %q, want updated_at", state.Cursor)
	}
}

func TestGetIncrementalState_DefaultInitial(t *testing.T) {
	p := testutil.NewProject(t)

	state, err := backfill.GetIncrementalState(p.Sess, "staging.noexist", "ts", "")
	if err != nil {
		t.Fatalf("GetIncrementalState: %v", err)
	}
	if !state.IsBackfill {
		t.Error("expected IsBackfill=true")
	}
	if state.InitialValue != backfill.DefaultInitialValue {
		t.Errorf("InitialValue = %q, want %q", state.InitialValue, backfill.DefaultInitialValue)
	}
	if state.LastValue != backfill.DefaultInitialValue {
		t.Errorf("LastValue = %q, want %q", state.LastValue, backfill.DefaultInitialValue)
	}
}

func TestGetIncrementalState_SinglePartTarget(t *testing.T) {
	p := testutil.NewProject(t)

	// Single-part target uses ondatra_table_exists_simple
	state, err := backfill.GetIncrementalState(p.Sess, "simple_table", "ts", "2020-01-01")
	if err != nil {
		t.Fatalf("GetIncrementalState: %v", err)
	}
	if !state.IsBackfill {
		t.Error("expected IsBackfill=true for non-existent table")
	}
	if state.LastValue != "2020-01-01" {
		t.Errorf("LastValue = %q, want 2020-01-01", state.LastValue)
	}
}

func TestGetIncrementalState_ThreePartTarget(t *testing.T) {
	p := testutil.NewProject(t)

	// Three-part target uses ondatra_table_exists_full
	state, err := backfill.GetIncrementalState(p.Sess, "lake.staging.events", "ts", "2020-01-01")
	if err != nil {
		t.Fatalf("GetIncrementalState: %v", err)
	}
	if !state.IsBackfill {
		t.Error("expected IsBackfill=true for non-existent table")
	}
}

func TestGetIncrementalState_WithLastRun(t *testing.T) {
	p := testutil.NewProject(t)

	// Create and run a model to generate snapshot history
	p.AddModel("staging/with_lr.sql", `-- @kind: table
SELECT 1 AS id, TIMESTAMP '2024-01-01' AS updated_at
`)
	runModel(t, p, "staging/with_lr.sql")

	state, err := backfill.GetIncrementalState(p.Sess, "staging.with_lr", "updated_at", "2020-01-01")
	if err != nil {
		t.Fatalf("GetIncrementalState: %v", err)
	}
	if state.IsBackfill {
		t.Error("expected IsBackfill=false")
	}
	// LastRun should be populated from snapshot history
	if state.LastRun == "" {
		t.Error("expected non-empty LastRun")
	}
}

func TestCaptureSchema_SinglePart(t *testing.T) {
	p := testutil.NewProject(t)

	// Create a temp table (single-part name)
	p.Sess.Exec("CREATE TEMP TABLE single_part_test (id INT, name VARCHAR)")
	defer p.Sess.Exec("DROP TABLE IF EXISTS single_part_test")

	cols, err := backfill.CaptureSchema(p.Sess, "single_part_test")
	if err != nil {
		t.Fatalf("CaptureSchema single-part: %v", err)
	}
	if len(cols) != 2 {
		t.Errorf("columns = %d, want 2", len(cols))
	}
}

func TestCaptureSchema_TooManyParts(t *testing.T) {
	// 3+ parts are not supported (only table and schema.table)
	_, err := backfill.CaptureSchema(nil, "a.b.c")
	if err == nil {
		t.Error("expected error for 3+ parts")
	}
	_, err = backfill.CaptureSchema(nil, "a.b.c.d")
	if err == nil {
		t.Error("expected error for 4+ parts")
	}
}

func TestGetPreviousSchema_EmptyResult(t *testing.T) {
	p := testutil.NewProject(t)

	// Query a model that has no commit info → empty/null result
	cols, err := backfill.GetPreviousSchema(p.Sess, "staging.nonexistent_model")
	if err != nil {
		t.Fatalf("GetPreviousSchema: %v", err)
	}
	if cols != nil {
		t.Errorf("expected nil schema, got %v", cols)
	}
}

func TestGetPreviousSnapshot_NoMacro(t *testing.T) {
	// Without metadata macros, the query fails → returns 0, nil
	sess := testutil.NewSession(t)

	snap, err := backfill.GetPreviousSnapshot(sess, "staging.whatever")
	if err != nil {
		t.Fatalf("expected nil error on missing macro, got: %v", err)
	}
	if snap != 0 {
		t.Errorf("snapshot = %d, want 0", snap)
	}
}

func TestGetPreviousSchema_NoMacro(t *testing.T) {
	// Without metadata macros, the query fails → returns error
	sess := testutil.NewSession(t)

	_, err := backfill.GetPreviousSchema(sess, "staging.whatever")
	if err == nil {
		t.Fatal("expected error when macro is missing")
	}
}

func TestGetModelCommitInfo_NoMacro(t *testing.T) {
	// Without metadata macros, the query fails → returns error
	sess := testutil.NewSession(t)

	_, err := backfill.GetModelCommitInfo(sess, "staging.whatever")
	if err == nil {
		t.Fatal("expected error when macro is missing")
	}
}

func TestGetPreviousSnapshot_AfterMultipleRuns(t *testing.T) {
	p := testutil.NewProject(t)

	// Run a model twice to create multiple snapshots
	p.AddModel("staging/multi.sql", `-- @kind: table
SELECT 1 AS id
`)
	runModel(t, p, "staging/multi.sql")

	snap1, _ := backfill.GetPreviousSnapshot(p.Sess, "staging.multi")

	p.AddModel("staging/multi.sql", `-- @kind: table
SELECT 1 AS id UNION ALL SELECT 2
`)
	runModel(t, p, "staging/multi.sql")

	snap2, _ := backfill.GetPreviousSnapshot(p.Sess, "staging.multi")
	if snap2 <= snap1 {
		t.Errorf("snapshot should increase: snap1=%d, snap2=%d", snap1, snap2)
	}
}

func TestGetModelCommitInfo_Fields(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/info_check.sql", `-- @kind: append
SELECT 1 AS id, 'test' AS name
`)
	runModel(t, p, "staging/info_check.sql")

	info, err := backfill.GetModelCommitInfo(p.Sess, "staging.info_check")
	if err != nil {
		t.Fatalf("GetModelCommitInfo: %v", err)
	}
	if info == nil {
		t.Fatal("expected non-nil commit info")
	}
	if info.SchemaHash == "" {
		t.Error("expected non-empty schema_hash")
	}
	if info.DagRunID == "" {
		t.Error("expected non-empty dag_run_id")
	}
	if info.DuckDBVersion == "" {
		t.Error("expected non-empty duckdb_version")
	}
	if info.StartTime == "" {
		t.Error("expected non-empty start_time")
	}
	if info.EndTime == "" {
		t.Error("expected non-empty end_time")
	}
	if info.DurationMs <= 0 {
		t.Errorf("expected positive duration_ms, got %d", info.DurationMs)
	}
	if len(info.Columns) == 0 {
		t.Error("expected non-empty columns")
	}
}

func TestGetPreviousSchema_VerifyColumns(t *testing.T) {
	p := testutil.NewProject(t)

	// Run and then check the stored schema matches actual columns
	p.AddModel("staging/schema_verify.sql", `-- @kind: table
SELECT 1 AS user_id, 'Alice' AS username, TRUE AS active
`)
	runModel(t, p, "staging/schema_verify.sql")

	cols, err := backfill.GetPreviousSchema(p.Sess, "staging.schema_verify")
	if err != nil {
		t.Fatalf("GetPreviousSchema: %v", err)
	}
	if len(cols) != 3 {
		t.Fatalf("columns = %d, want 3", len(cols))
	}

	colMap := make(map[string]string)
	for _, c := range cols {
		colMap[c.Name] = c.Type
	}
	if colMap["user_id"] != "INTEGER" {
		t.Errorf("user_id type = %q, want INTEGER", colMap["user_id"])
	}
	if colMap["username"] != "VARCHAR" {
		t.Errorf("username type = %q, want VARCHAR", colMap["username"])
	}
	if colMap["active"] != "BOOLEAN" {
		t.Errorf("active type = %q, want BOOLEAN", colMap["active"])
	}
}

func TestGetIncrementalState_NoCursor(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/nocursor.sql", `-- @kind: table
SELECT 1 AS id
`)
	runModel(t, p, "staging/nocursor.sql")

	// Empty cursor: should still work, just use default initial value
	state, err := backfill.GetIncrementalState(p.Sess, "staging.nocursor", "", "")
	if err != nil {
		t.Fatalf("GetIncrementalState: %v", err)
	}
	if state.IsBackfill {
		t.Error("expected IsBackfill=false for existing table")
	}
	if state.LastValue != backfill.DefaultInitialValue {
		t.Errorf("LastValue = %q, want default", state.LastValue)
	}
}
