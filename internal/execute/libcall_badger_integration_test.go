// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/dag"
	"github.com/ondatra-labs/ondatrasql/internal/execute"
	"github.com/ondatra-labs/ondatrasql/internal/libregistry"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// runModelWithLib parses and runs a model with lib registry attached.
// Each call creates a fresh runner and registry to avoid Badger lock conflicts.
func runModelWithLib(t *testing.T, p *testutil.Project, relPath string) *execute.Result {
	t.Helper()
	result, err := runModelWithLibErr(t, p, relPath)
	if err != nil {
		t.Fatalf("run %s: %v", relPath, err)
	}
	return result
}

func runModelWithLibErr(t *testing.T, p *testutil.Project, relPath string) (*execute.Result, error) {
	t.Helper()
	modelPath := filepath.Join(p.Dir, "models", relPath)
	model, err := parser.ParseModel(modelPath, p.Dir)
	if err != nil {
		t.Fatalf("parse %s: %v", relPath, err)
	}

	reg, regErr := libregistry.Scan(p.Dir)
	if regErr != nil {
		t.Fatalf("load lib registry: %v", regErr)
	}

	runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
	runner.SetLibRegistry(reg)
	runner.SetProjectDir(p.Dir)

	return runner.Run(context.Background(), model)
}

// writeLib creates a .star file in the project's lib/ directory.
func writeLib(t *testing.T, p *testutil.Project, name, content string) {
	t.Helper()
	testutil.WriteFile(t, p.Dir, filepath.Join("lib", name+".star"), content)
}

// TestLibCall_Append_NoDuplication verifies that lib-call append models
// do not re-insert rows on incremental runs when there is no new data.
// Regression test for missing Badger claim ack in runner.go.
func TestLibCall_Append_NoDuplication(t *testing.T) {
	p := testutil.NewProject(t)

	// Create a simple lib that returns static rows.
	// On second call with is_backfill=False and last_value set,
	// it returns 0 rows (simulating "no new data").
	writeLib(t, p, "testapi", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["append"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    if not is_backfill and last_value != "":
        return {"rows": [], "next": None}
    return {
        "rows": [
            {"id": 1, "name": "Alice", "val": "2026-01-01"},
            {"id": 2, "name": "Bob", "val": "2026-01-02"},
            {"id": 3, "name": "Carol", "val": "2026-01-03"},
        ],
        "next": None,
    }
`)

	p.AddModel("raw/data.sql", `-- @kind: append
-- @fetch
-- @incremental: val
-- @incremental_initial: 2026-01-01
SELECT id::BIGINT AS id, name::VARCHAR AS name, val::VARCHAR AS val FROM testapi('items')
`)

	// First run — backfill, should insert 3 rows
	r1 := runModelWithLib(t, p, "raw/data.sql")
	if r1.RowsAffected != 3 {
		t.Fatalf("run 1: expected 3 rows, got %d", r1.RowsAffected)
	}

	// Second run — incremental, no new data, should insert 0 rows
	r2 := runModelWithLib(t, p, "raw/data.sql")
	if r2.RowsAffected != 0 {
		t.Fatalf("run 2: expected 0 rows (no new data), got %d", r2.RowsAffected)
	}

	// Third run — same, still 0
	r3 := runModelWithLib(t, p, "raw/data.sql")
	if r3.RowsAffected != 0 {
		t.Fatalf("run 3: expected 0 rows, got %d", r3.RowsAffected)
	}

	// Runs 4-5: verify no creep — claims fully consumed
	for run := 4; run <= 5; run++ {
		rn := runModelWithLib(t, p, "raw/data.sql")
		if rn.RowsAffected != 0 {
			t.Fatalf("run %d: expected 0 rows, got %d", run, rn.RowsAffected)
		}
	}

	// Verify total row count — should be exactly 3 after 5 runs
	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM raw.data")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != "3" {
		t.Fatalf("expected 3 total rows after 5 runs, got %s", count)
	}
}

// TestLibCall_CDC_SkipsTempTables verifies that CDC does not attempt
// table_changes() on lib-call temp tables (tmp__lib_*).
// Regression test for CDC warning on rewritten lib-call SQL.
func TestLibCall_CDC_SkipsTempTables(t *testing.T) {
	p := testutil.NewProject(t)

	writeLib(t, p, "testapi", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["append"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    if not is_backfill and last_value != "":
        return {"rows": [], "next": None}
    return {
        "rows": [
            {"id": 1, "name": "Alice", "val": "2026-01-01"},
        ],
        "next": None,
    }
`)

	p.AddModel("raw/data.sql", `-- @kind: append
-- @fetch
-- @incremental: val
-- @incremental_initial: 2026-01-01
SELECT id::BIGINT AS id, name::VARCHAR AS name, val::VARCHAR AS val FROM testapi('items')
`)

	// First run
	runModelWithLib(t, p, "raw/data.sql")

	// Second run — should NOT produce CDC warning about tmp__lib_*
	r2 := runModelWithLib(t, p, "raw/data.sql")
	for _, w := range r2.Warnings {
		if strings.Contains(w, "tmp__lib_") || strings.Contains(w, "CDC query failed") {
			t.Fatalf("CDC should not attempt table_changes on temp tables, got warning: %s", w)
		}
	}
}

// TestLibCall_MaterializeFailure_NacksClaims verifies that Badger claims
// are nacked when materialization fails, allowing retry on next run.
func TestLibCall_MaterializeFailure_NacksClaims(t *testing.T) {
	p := testutil.NewProject(t)

	writeLib(t, p, "testapi", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["append"],
    },
}

def fetch(resource, page):
    return {
        "rows": [
            {"id": 1, "name": "Alice"},
        ],
        "next": None,
    }
`)

	// Model with a constraint that will fail
	p.AddModel("raw/data.sql", `-- @kind: append
-- @fetch
-- @constraint: not_null(missing_col)
SELECT id::BIGINT AS id, name::VARCHAR AS name FROM testapi('items')
`)

	// First run — should fail on constraint
	modelPath := filepath.Join(p.Dir, "models", "raw/data.sql")
	model, err := parser.ParseModel(modelPath, p.Dir)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	reg, _ := libregistry.Scan(p.Dir)
	runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
	runner.SetLibRegistry(reg)
	runner.SetProjectDir(p.Dir)

	_, runErr := runner.Run(context.Background(), model)
	if runErr == nil {
		t.Fatal("expected constraint failure")
	}

	// Fix the model — remove bad constraint
	os.WriteFile(modelPath, []byte(`-- @kind: append
-- @fetch
SELECT id::BIGINT AS id, name::VARCHAR AS name FROM testapi('items')
`), 0644)

	// Second run — should succeed. Nacked claims from first run are retried,
	// plus fresh fetch produces new rows. Total = nacked + fresh.
	r2 := runModelWithLib(t, p, "raw/data.sql")
	if r2.RowsAffected == 0 {
		t.Fatal("run 2 after fix: expected rows (nacked claims retried), got 0")
	}
	// Verify rows actually landed in the table
	count, cerr := p.Sess.QueryValue("SELECT COUNT(*) FROM raw.data")
	if cerr != nil {
		t.Fatalf("count: %v", cerr)
	}
	if count == "0" {
		t.Fatal("expected rows in table after nack retry")
	}
}

// TestLibCall_EarlyExit_NacksClaims verifies that if a lib-call fails mid-execution
// (e.g. second lib-call errors after first already claimed), all claims are nacked.
// Regression test for early exit paths not cleaning up inflight claims.
func TestLibCall_EarlyExit_NacksClaims(t *testing.T) {
	p := testutil.NewProject(t)

	// Create a lib that always fails on fetch
	writeLib(t, p, "badapi", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["append"],
    },
}

def fetch(resource, page):
    fail("intentional fetch error for testing")
`)

	p.AddModel("raw/data.sql", `-- @kind: append
-- @fetch
SELECT id::BIGINT AS id FROM badapi('items')
`)

	// Run should fail
	_, err := runModelWithLibErr(t, p, "raw/data.sql")
	if err == nil {
		t.Fatal("expected error from failing lib")
	}

	// Verify Badger ingest directory is not locked (store was closed by defer)
	// by running again — if lock is held, this would fail with "Cannot acquire directory lock"
	_, err2 := runModelWithLibErr(t, p, "raw/data.sql")
	if err2 == nil {
		t.Fatal("expected error from failing lib on retry")
	}
	if strings.Contains(err2.Error(), "Cannot acquire directory lock") {
		t.Fatalf("Badger store not closed on early exit: %v", err2)
	}
}

// TestLibCall_AuditFailure_AcksClaims verifies that an audit failure acks claims
// (not nacks) because the data was valid — only the audit check prevented materialization.
// On retry, the blueprint should fetch fresh data, not replay old claims.
func TestLibCall_AuditFailure_AcksClaims(t *testing.T) {
	p := testutil.NewProject(t)

	writeLib(t, p, "countapi", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["table"],
    },
}

def fetch(resource, page):
    return {
        "rows": [
            {"id": 1, "val": 100},
        ],
        "next": None,
    }
`)

	// Model with audit that fails (val must be > 200)
	p.AddModel("raw/data.sql", `-- @kind: table
-- @fetch
-- @audit: compare(val, >, 200)
SELECT id::BIGINT AS id, val::BIGINT AS val FROM countapi('items')
`)

	// First run — should fail on audit
	_, runErr := runModelWithLibErr(t, p, "raw/data.sql")
	if runErr == nil {
		t.Fatal("expected audit failure")
	}
	if !strings.Contains(runErr.Error(), "audit") {
		t.Fatalf("expected audit error, got: %v", runErr)
	}

	// Pin libExtraPreSQL atomicity: the ack INSERT into _ondatra_acks is
	// run inside the materialize transaction, so an audit-driven rollback
	// must also remove the ack record. If a future change moves the ack
	// outside the transaction, the row would survive here and the next
	// run would silently skip the (now-missing) claim, losing data.
	ackCount, err := p.Sess.QueryValue("SELECT COUNT(*) FROM _ondatra_acks")
	if err == nil && ackCount != "0" {
		t.Errorf("after audit-rolled-back run: _ondatra_acks count = %s, want 0 (libExtraPreSQL must be inside materialize transaction)", ackCount)
	}

	// Fix model — remove audit
	modelPath := filepath.Join(p.Dir, "models", "raw/data.sql")
	os.WriteFile(modelPath, []byte(`-- @kind: table
-- @fetch
SELECT id::BIGINT AS id, val::BIGINT AS val FROM countapi('items')
`), 0644)

	// Second run — should succeed. Claims were acked (not nacked),
	// so fresh fetch runs. Should produce 1 row.
	r2 := runModelWithLib(t, p, "raw/data.sql")
	if r2.RowsAffected != 1 {
		t.Fatalf("expected 1 row after audit fix, got %d", r2.RowsAffected)
	}
}

// TestLibCall_AllEmpty_ZeroRows verifies that when all lib calls produce 0 rows
// (e.g. incremental with no new data after ack), the model materializes with
// 0 rows instead of failing with a Binder Error.
func TestLibCall_AllEmpty_ZeroRows(t *testing.T) {
	p := testutil.NewProject(t)

	// Lib that returns 0 rows when not backfill
	writeLib(t, p, "emptyapi", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["append"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    if not is_backfill:
        return {"rows": [], "next": None}
    return {
        "rows": [{"id": 1, "val": "2026-01-01"}],
        "next": None,
    }
`)

	p.AddModel("raw/data.sql", `-- @kind: append
-- @fetch
-- @incremental: val
-- @incremental_initial: 2026-01-01
SELECT id::BIGINT AS id, val::VARCHAR AS val FROM emptyapi('items')
`)

	// First run — backfill, 1 row
	r1 := runModelWithLib(t, p, "raw/data.sql")
	if r1.RowsAffected != 1 {
		t.Fatalf("run 1: expected 1 row, got %d", r1.RowsAffected)
	}

	// Second run — incremental, lib returns 0 rows.
	// Must NOT fail with "Referenced column ... not found" Binder Error.
	r2 := runModelWithLib(t, p, "raw/data.sql")
	if r2.RowsAffected != 0 {
		t.Fatalf("run 2: expected 0 rows, got %d", r2.RowsAffected)
	}

	// Third run — same
	r3 := runModelWithLib(t, p, "raw/data.sql")
	if r3.RowsAffected != 0 {
		t.Fatalf("run 3: expected 0 rows, got %d", r3.RowsAffected)
	}

	// Runs 4-5: verify claims are fully consumed — no creep over many runs
	for run := 4; run <= 5; run++ {
		rn := runModelWithLib(t, p, "raw/data.sql")
		if rn.RowsAffected != 0 {
			t.Fatalf("run %d: expected 0 rows (claims must be fully acked), got %d", run, rn.RowsAffected)
		}
	}

	// Total should still be 1 — no duplicates from any run
	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM raw.data")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != "1" {
		t.Fatalf("expected 1 total row after 5 runs, got %s", count)
	}
}

// TestLibCall_Tracked_NoDuplication verifies that tracked kind lib-call models
// with Badger correctly ack claims, and that extraPreSQL flows through
// materializeTracked's transaction path.
func TestLibCall_Tracked_NoDuplication(t *testing.T) {
	p := testutil.NewProject(t)

	writeLib(t, p, "testapi", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["tracked"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    if not is_backfill and last_value != "":
        return {"rows": [], "next": None}
    return {
        "rows": [
            {"grp": "A", "id": 1, "name": "Alice"},
            {"grp": "A", "id": 2, "name": "Bob"},
            {"grp": "B", "id": 3, "name": "Carol"},
        ],
        "next": None,
    }
`)

	p.AddModel("raw/data.sql", `-- @kind: tracked
-- @fetch
-- @group_key: grp
SELECT grp::VARCHAR AS grp, id::BIGINT AS id, name::VARCHAR AS name FROM testapi('items')
`)

	// First run — backfill
	r1 := runModelWithLib(t, p, "raw/data.sql")
	t.Logf("run 1: rows=%d warnings=%v errors=%v", r1.RowsAffected, r1.Warnings, r1.Errors)
	if r1.RowsAffected != 3 {
		t.Fatalf("run 1: expected 3 rows, got %d", r1.RowsAffected)
	}

	// Second run — incremental, lib returns 0 rows, should be 0 changes
	r2 := runModelWithLib(t, p, "raw/data.sql")
	t.Logf("run 2: rows=%d warnings=%v", r2.RowsAffected, r2.Warnings)
	// Tracked with 0 new rows = 0 changes (no groups changed)
	if r2.RowsAffected != 0 {
		t.Fatalf("run 2: expected 0 rows (no changes), got %d", r2.RowsAffected)
	}

	// Third run — verify no duplication (rows should still be 0, not 3 from replayed claims)
	r3 := runModelWithLib(t, p, "raw/data.sql")
	if r3.RowsAffected != 0 {
		t.Fatalf("run 3: expected 0 rows (claims acked, no replay), got %d", r3.RowsAffected)
	}
}

// TestLibCall_FirstRunZeroRows verifies that a lib-call model that returns
// 0 rows on its very first run succeeds. When column schema can be inferred
// from the SELECT list, the target table is created (empty). When no schema
// can be inferred, the run is skipped with a warning.
func TestLibCall_FirstRunZeroRows(t *testing.T) {
	p := testutil.NewProject(t)

	// Lib that always returns 0 rows
	writeLib(t, p, "emptyapi", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["table"],
    },
}

def fetch(resource, page):
    return {"rows": [], "next": None}
`)

	// Model with explicit columns — schema can be inferred from AST
	p.AddModel("raw/empty.sql", `-- @kind: table
-- @fetch
SELECT id::BIGINT AS id, name::VARCHAR AS name FROM emptyapi('items')
`)

	// First run with 0 rows — columns inferred from SELECT list,
	// so the target table is created (empty) via the normal pipeline.
	r1 := runModelWithLib(t, p, "raw/empty.sql")
	if r1.RowsAffected != 0 {
		t.Fatalf("run 1: expected 0 rows, got %d", r1.RowsAffected)
	}

	// Target table should exist now (created empty with inferred schema)
	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM raw.empty")
	if err != nil {
		t.Fatalf("target table should exist: %v", err)
	}
	if count != "0" {
		t.Fatalf("expected 0 rows in target, got %s", count)
	}

	// Second run — still 0, skip (hash unchanged)
	r2 := runModelWithLib(t, p, "raw/empty.sql")
	if r2.RowsAffected != 0 {
		t.Fatalf("run 2: expected 0 rows, got %d", r2.RowsAffected)
	}
}

// TestLibCall_SearchPath_Escaping verifies that schema names with special
// characters in SET search_path are escaped and don't cause SQL injection.
func TestLibCall_SearchPath_Escaping(t *testing.T) {
	p := testutil.NewProject(t)

	// Create a model that references a schema.table with normal names
	// and verify CDC doesn't crash on search_path manipulation.
	// This is a defense-in-depth test — schema names from AST are typically safe,
	// but escapeSQL() should be applied regardless.
	p.AddModel("raw/orders.sql", `-- @kind: append
SELECT 1 AS id, 'test' AS name
`)

	// First run
	r1 := runModel(t, p, "raw/orders.sql")
	if r1.RowsAffected != 1 {
		t.Fatalf("run 1: expected 1 row, got %d", r1.RowsAffected)
	}

	// Second run — triggers CDC path with search_path manipulation
	r2 := runModel(t, p, "raw/orders.sql")
	// Should not crash or produce warnings about search_path
	for _, w := range r2.Warnings {
		if strings.Contains(w, "search_path") {
			t.Fatalf("unexpected search_path warning: %s", w)
		}
	}
}

// TestLibCall_EmptyRun_PreservesRunType verifies that when all lib calls
// return 0 rows, the runner preserves the run_type from the backfill
// decision rather than hard-coding "incremental".
// Regression: runner.go previously overwrote result.RunType = "incremental"
// in the allLibsEmpty fast path.
func TestLibCall_EmptyRun_PreservesRunType(t *testing.T) {
	p := testutil.NewProject(t)

	// Phase 1: create a lib that returns data on first run, then empty.
	writeLib(t, p, "testapi", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["append"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    if not is_backfill:
        return {"rows": [], "next": None}
    return {
        "rows": [{"id": 1, "val": "2026-01-01"}],
        "next": None,
    }
`)

	p.AddModel("raw/data.sql", `-- @kind: append
-- @fetch
-- @incremental: val
-- @incremental_initial: 2026-01-01
SELECT id::BIGINT AS id, val::VARCHAR AS val FROM testapi('items')
`)

	// First run — backfill, creates target
	r1 := runModelWithLib(t, p, "raw/data.sql")
	if r1.RunType != "backfill" {
		t.Fatalf("run 1: expected run_type=backfill, got %s", r1.RunType)
	}

	// Second run — incremental, lib returns 0 rows
	r2 := runModelWithLib(t, p, "raw/data.sql")
	t.Logf("run 2: type=%s reason=%s rows=%d", r2.RunType, r2.RunReason, r2.RowsAffected)

	// Now change the SQL AND make the lib always return 0 rows.
	// The hash change triggers backfill, but is_backfill=True still
	// gets 0 rows — testing the empty-lib + backfill path.
	writeLib(t, p, "testapi", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["append"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    # API went dry — always returns empty regardless of backfill
    return {"rows": [], "next": None}
`)

	modelPath := filepath.Join(p.Dir, "models", "raw/data.sql")
	// Hash change is triggered by changing @incremental_initial — the SQL
	// itself stays valid under v0.30.0's no-WHERE strict-fetch rule.
	os.WriteFile(modelPath, []byte(`-- @kind: append
-- @fetch
-- @incremental: val
-- @incremental_initial: 2025-12-31
SELECT id::BIGINT AS id, val::VARCHAR AS val FROM testapi('items')
`), 0644)

	// Third run — SQL changed (hash change → backfill), lib returns 0 rows.
	// Run type must be "backfill" (preserved from decision), not "incremental".
	r3 := runModelWithLib(t, p, "raw/data.sql")
	t.Logf("run 3: type=%s reason=%s rows=%d", r3.RunType, r3.RunReason, r3.RowsAffected)
	if r3.RunType != "backfill" {
		t.Fatalf("run 3 (hash changed, 0 rows): expected run_type=backfill, got %s", r3.RunType)
	}
}

// TestLibCall_EmptyRun_SchemaEvolution verifies that when all lib calls
// return 0 rows but the model SQL adds a new column, schema evolution
// detects the change via the empty stub temp table (not via old target clone).
// Regression: the old allLibsEmpty fast path skipped detectSchemaEvolution.
func TestLibCall_EmptyRun_SchemaEvolution(t *testing.T) {
	p := testutil.NewProject(t)

	writeLib(t, p, "testapi", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["table"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    if not is_backfill:
        return {"rows": [], "next": None}
    return {
        "rows": [{"id": 1, "name": "Alice"}],
        "next": None,
    }
`)

	p.AddModel("raw/data.sql", `-- @kind: table
-- @fetch
SELECT id::BIGINT AS id, name::VARCHAR AS name FROM testapi('items')
`)

	// First run — creates target with columns (id, name)
	r1 := runModelWithLib(t, p, "raw/data.sql")
	if r1.RowsAffected != 1 {
		t.Fatalf("run 1: expected 1 row, got %d", r1.RowsAffected)
	}

	// Verify initial schema
	cols, err := p.Sess.QueryValue("SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema='raw' AND table_name='data'")
	if err != nil {
		t.Fatalf("schema query: %v", err)
	}
	if !strings.Contains(cols, "id") || !strings.Contains(cols, "name") {
		t.Fatalf("unexpected initial schema: %s", cols)
	}

	// Add a new column to the model SQL. Lib returns 0 rows on non-backfill,
	// but the stub should reflect the new query shape (id, name, email).
	writeLib(t, p, "testapi", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["table"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    if not is_backfill:
        return {"rows": [], "next": None}
    return {
        "rows": [{"id": 1, "name": "Alice", "email": "a@b.com"}],
        "next": None,
    }
`)

	modelPath := filepath.Join(p.Dir, "models", "raw/data.sql")
	os.WriteFile(modelPath, []byte(`-- @kind: table
-- @fetch
SELECT id::BIGINT AS id, name::VARCHAR AS name, email::VARCHAR AS email FROM testapi('items')
`), 0644)

	// Second run — hash changed (new column), lib returns 0 rows.
	// Schema evolution should detect the added "email" column.
	r2 := runModelWithLib(t, p, "raw/data.sql")
	t.Logf("run 2: type=%s rows=%d warnings=%v", r2.RunType, r2.RowsAffected, r2.Warnings)

	// Check if the target table now has the email column
	cols2, err := p.Sess.QueryValue("SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema='raw' AND table_name='data'")
	if err != nil {
		t.Fatalf("schema query after evolution: %v", err)
	}
	if !strings.Contains(cols2, "email") {
		t.Fatalf("schema evolution missed: expected 'email' column, got: %s", cols2)
	}
}

// TestLibCall_EmptyRun_ConstraintsStillRun verifies that constraints
// are evaluated even when all lib calls return 0 rows.
// Regression: the old allLibsEmpty fast path skipped constraints entirely.
func TestLibCall_EmptyRun_ConstraintsStillRun(t *testing.T) {
	p := testutil.NewProject(t)

	writeLib(t, p, "testapi", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["append"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    if not is_backfill:
        return {"rows": [], "next": None}
    return {
        "rows": [{"id": 1, "val": "2026-01-01"}],
        "next": None,
    }
`)

	p.AddModel("raw/data.sql", `-- @kind: append
-- @fetch
-- @incremental: val
-- @incremental_initial: 2026-01-01
SELECT id::BIGINT AS id, val::VARCHAR AS val FROM testapi('items')
`)

	// First run — backfill, creates target
	r1 := runModelWithLib(t, p, "raw/data.sql")
	if r1.RowsAffected != 1 {
		t.Fatalf("run 1: expected 1 row, got %d", r1.RowsAffected)
	}

	// Now add a constraint that references a non-existent column.
	// On an empty incremental run, the old code skipped constraints.
	// The fix ensures constraints run even with 0 lib rows.
	modelPath := filepath.Join(p.Dir, "models", "raw/data.sql")
	os.WriteFile(modelPath, []byte(`-- @kind: append
-- @fetch
-- @incremental: val
-- @incremental_initial: 2026-01-01
-- @constraint: not_null(nonexistent_col)
SELECT id::BIGINT AS id, val::VARCHAR AS val FROM testapi('items')
`), 0644)

	// Second run — 0 rows, but constraint should still be evaluated
	_, err := runModelWithLibErr(t, p, "raw/data.sql")
	if err == nil {
		t.Fatal("expected constraint failure on empty run, got nil")
	}
	if !strings.Contains(err.Error(), "constraint") {
		t.Fatalf("expected constraint error, got: %v", err)
	}
}

// TestLibCall_EmptyRun_AuditsStillRun verifies that audits are rendered
// even when all lib calls return 0 rows.
// Regression: the old allLibsEmpty fast path skipped audit rendering entirely.
func TestLibCall_EmptyRun_AuditsStillRun(t *testing.T) {
	p := testutil.NewProject(t)

	// Phase 1: lib returns data on first run, then always empty.
	writeLib(t, p, "auditapi", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["table"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    if not is_backfill:
        return {"rows": [], "next": None}
    return {
        "rows": [{"id": 1, "val": 100}],
        "next": None,
    }
`)

	p.AddModel("raw/auditdata.sql", `-- @kind: table
-- @fetch
SELECT id::BIGINT AS id, val::BIGINT AS val FROM auditapi('items')
`)

	// First run — creates target
	r1 := runModelWithLib(t, p, "raw/auditdata.sql")
	if r1.RowsAffected != 1 {
		t.Fatalf("run 1: expected 1 row, got %d", r1.RowsAffected)
	}

	// Phase 2: make lib always return 0 rows AND add an invalid audit.
	// The hash change triggers backfill, but the lib returns 0 rows.
	// The invalid audit should still be parsed and cause a failure.
	writeLib(t, p, "auditapi", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["table"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    return {"rows": [], "next": None}
`)

	modelPath := filepath.Join(p.Dir, "models", "raw/auditdata.sql")
	// Hash change is triggered by reordering the projection (v0.30.0
	// strict-fetch forbids WHERE; @audit is not in the hash, so the SQL
	// itself must change).
	os.WriteFile(modelPath, []byte(`-- @kind: table
-- @fetch
-- @audit: not a valid macro
SELECT val::BIGINT AS val, id::BIGINT AS id FROM auditapi('items')
`), 0644)

	// Second run — 0 rows, but audit parsing should still happen and fail
	_, err := runModelWithLibErr(t, p, "raw/auditdata.sql")
	if err == nil {
		t.Fatal("expected audit parse failure on empty run, got nil")
	}
	if !strings.Contains(err.Error(), "audit") {
		t.Fatalf("expected audit error, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Test 1: merge + lib + empty incremental + sink
// ---------------------------------------------------------------------------

// TestLibCall_Merge_EmptyIncremental_Push verifies that a lib-driven merge
// model with a downstream @push sink correctly handles empty incremental
// runs: no duplication, correct empty delta, sink semantics preserved.
//
// v0.30.0: @fetch + @push are now split across two models (a @fetch model
// is forbidden from also being a @push model). The lifecycle below
// alternates raw-fetch + downstream-push runs to exercise the same path.
func TestLibCall_Merge_EmptyIncremental_Push(t *testing.T) {
	p := testutil.NewProject(t)

	// Source lib: returns data on backfill, empty on incremental
	writeLib(t, p, "mergesrc", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["merge"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    if not is_backfill:
        return {"rows": [], "next": None}
    return {
        "rows": [
            {"id": 1, "name": "Alice", "score": 100},
            {"id": 2, "name": "Bob", "score": 200},
        ],
        "next": None,
    }
`)

	// Sink lib: atomic mode, just succeeds. We verify via SyncSucceeded count.
	writeLib(t, p, "recorder", `
API = {
    "push": {
        "batch_size": 100,
        "batch_mode": "atomic",
    },
}

def push(rows=[], batch_number=1, kind="", key_columns=[], columns=[]):
    pass
`)

	// Raw model: @fetch only, no @push
	p.AddModel("raw/merged.sql", `-- @kind: merge
-- @fetch
-- @unique_key: id
-- @incremental: score
-- @incremental_initial: 0
SELECT id::BIGINT AS id, name::VARCHAR AS name, score::BIGINT AS score FROM mergesrc('items')
`)

	// Downstream push model: reads from raw.merged, no lib in FROM
	p.AddModel("sync/merged.sql", `-- @kind: merge
-- @unique_key: id
-- @push: recorder
SELECT id::BIGINT AS id, name::VARCHAR AS name, score::BIGINT AS score FROM raw.merged
`)

	// Run 1 — backfill on both: 2 rows propagate, sink fires on push model
	rRaw1 := runModelWithLib(t, p, "raw/merged.sql")
	if rRaw1.RowsAffected != 2 {
		t.Fatalf("raw run 1: expected 2 rows, got %d", rRaw1.RowsAffected)
	}
	rSync1 := runModelWithLib(t, p, "sync/merged.sql")
	t.Logf("sync run 1: rows=%d type=%s sync_ok=%d", rSync1.RowsAffected, rSync1.RunType, rSync1.SyncSucceeded)
	if rSync1.RowsAffected != 2 {
		t.Fatalf("sync run 1: expected 2 rows, got %d", rSync1.RowsAffected)
	}
	if rSync1.SyncSucceeded == 0 {
		t.Fatalf("sync run 1: expected sink to succeed, got SyncSucceeded=0")
	}

	// Run 2 — incremental on both: lib returns 0 rows, raw is unchanged,
	// sync model sees no deltas, sink does NOT fire spuriously.
	rRaw2 := runModelWithLib(t, p, "raw/merged.sql")
	if rRaw2.RowsAffected != 0 {
		t.Fatalf("raw run 2: expected 0 rows (no new data), got %d", rRaw2.RowsAffected)
	}
	rSync2 := runModelWithLib(t, p, "sync/merged.sql")
	t.Logf("sync run 2: rows=%d type=%s sync_ok=%d", rSync2.RowsAffected, rSync2.RunType, rSync2.SyncSucceeded)
	if rSync2.RowsAffected != 0 {
		t.Fatalf("sync run 2: expected 0 rows, got %d", rSync2.RowsAffected)
	}

	// Run 3 — still empty, no duplication
	runModelWithLib(t, p, "raw/merged.sql")
	rSync3 := runModelWithLib(t, p, "sync/merged.sql")
	if rSync3.RowsAffected != 0 {
		t.Fatalf("sync run 3: expected 0 rows, got %d", rSync3.RowsAffected)
	}

	// Verify total row count on raw — should be exactly 2 (no duplication)
	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM raw.merged")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != "2" {
		t.Fatalf("expected 2 total rows in raw.merged after 3 runs, got %s", count)
	}
	count, err = p.Sess.QueryValue("SELECT COUNT(*) FROM sync.merged")
	if err != nil {
		t.Fatalf("count sync: %v", err)
	}
	if count != "2" {
		t.Fatalf("expected 2 total rows in sync.merged, got %s", count)
	}
}

// ---------------------------------------------------------------------------
// Test 2: tracked + lib + empty incremental + group changes
// ---------------------------------------------------------------------------

// TestLibCall_Tracked_GroupChanges verifies the full lifecycle of a tracked
// kind with lib calls: backfill, empty incremental, then group content change.
func TestLibCall_Tracked_GroupChanges(t *testing.T) {
	p := testutil.NewProject(t)

	// Phase 1: lib returns two groups
	writeLib(t, p, "trackedapi", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["tracked"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    if not is_backfill:
        return {"rows": [], "next": None}
    return {
        "rows": [
            {"region": "US", "product": "Widget", "amount": 100},
            {"region": "US", "product": "Gadget", "amount": 200},
            {"region": "EU", "product": "Widget", "amount": 150},
        ],
        "next": None,
    }
`)

	p.AddModel("raw/regional.sql", `-- @kind: tracked
-- @fetch
-- @group_key: region
SELECT region::VARCHAR AS region, product::VARCHAR AS product, amount::BIGINT AS amount FROM trackedapi('items')
`)

	// Run 1 — backfill: 3 rows, 2 groups
	r1 := runModelWithLib(t, p, "raw/regional.sql")
	t.Logf("run 1: rows=%d type=%s", r1.RowsAffected, r1.RunType)
	if r1.RowsAffected != 3 {
		t.Fatalf("run 1: expected 3 rows, got %d", r1.RowsAffected)
	}

	// Run 2 — incremental, lib returns 0 rows (no changes)
	r2 := runModelWithLib(t, p, "raw/regional.sql")
	t.Logf("run 2: rows=%d type=%s", r2.RowsAffected, r2.RunType)
	if r2.RowsAffected != 0 {
		t.Fatalf("run 2: expected 0 rows (no changes), got %d", r2.RowsAffected)
	}

	// Run 3 — verify claims are fully consumed (no replay)
	r3 := runModelWithLib(t, p, "raw/regional.sql")
	if r3.RowsAffected != 0 {
		t.Fatalf("run 3: expected 0 rows (claims acked), got %d", r3.RowsAffected)
	}

	// Phase 2: change lib to return modified group data.
	// US amount changed, EU unchanged, new group APAC added.
	writeLib(t, p, "trackedapi", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["tracked"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    return {
        "rows": [
            {"region": "US", "product": "Widget", "amount": 999},
            {"region": "US", "product": "Gadget", "amount": 200},
            {"region": "EU", "product": "Widget", "amount": 150},
            {"region": "APAC", "product": "Doohickey", "amount": 50},
        ],
        "next": None,
    }
`)

	// Change SQL to trigger hash change → backfill with new data.
	// Reorder columns to trigger the hash change without using WHERE
	// (forbidden in @fetch under v0.30.0 strict-fetch).
	modelPath := filepath.Join(p.Dir, "models", "raw/regional.sql")
	os.WriteFile(modelPath, []byte(`-- @kind: tracked
-- @fetch
-- @group_key: region
SELECT product::VARCHAR AS product, region::VARCHAR AS region, amount::BIGINT AS amount FROM trackedapi('items')
`), 0644)

	// Run 4 — backfill with changed data
	r4 := runModelWithLib(t, p, "raw/regional.sql")
	t.Logf("run 4: rows=%d type=%s", r4.RowsAffected, r4.RunType)
	if r4.RowsAffected != 4 {
		t.Fatalf("run 4: expected 4 rows (backfill), got %d", r4.RowsAffected)
	}

	// Verify US.Widget amount changed
	usAmount, err := p.Sess.QueryValue(
		"SELECT amount FROM raw.regional WHERE region='US' AND product='Widget'")
	if err != nil {
		t.Fatalf("query US amount: %v", err)
	}
	if usAmount != "999" {
		t.Fatalf("US Widget amount: expected 999, got %s", usAmount)
	}

	// Verify APAC exists
	apacCount, err := p.Sess.QueryValue(
		"SELECT COUNT(*) FROM raw.regional WHERE region='APAC'")
	if err != nil {
		t.Fatalf("query APAC: %v", err)
	}
	if apacCount != "1" {
		t.Fatalf("expected 1 APAC row, got %s", apacCount)
	}

	// Verify total: US(2) + EU(1) + APAC(1) = 4
	total, err := p.Sess.QueryValue("SELECT COUNT(*) FROM raw.regional")
	if err != nil {
		t.Fatalf("total count: %v", err)
	}
	if total != "4" {
		t.Fatalf("expected 4 total rows, got %s", total)
	}
}

// ---------------------------------------------------------------------------
// Test 3: multiple lib calls — one empty, one with data
// ---------------------------------------------------------------------------

// TestLibCall_MixedEmpty_TwoIndependentLibs verifies that two independent
// @fetch models — one with data, one returning 0 rows — both have correct
// claims lifecycle. Pre-v0.30 this was tested via JOIN of two libs in a
// single model; the strict-fetch contract now requires exactly one lib
// per @fetch model, so the lifecycle is verified per model independently.
// Cross-lib JOIN happens in a downstream staging model that reads from the
// raw tables.
func TestLibCall_MixedEmpty_TwoIndependentLibs(t *testing.T) {
	p := testutil.NewProject(t)

	// Lib A: always returns data
	writeLib(t, p, "users_api", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["table"],
    },
}

def fetch(resource, page):
    return {
        "rows": [
            {"user_id": 1, "name": "Alice"},
            {"user_id": 2, "name": "Bob"},
        ],
        "next": None,
    }
`)

	// Lib B: returns data on backfill, empty on incremental
	writeLib(t, p, "scores_api", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["table"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    if not is_backfill:
        return {"rows": [], "next": None}
    return {
        "rows": [
            {"user_id": 1, "score": 100},
            {"user_id": 2, "score": 200},
        ],
        "next": None,
    }
`)

	// Two independent @fetch models, one per lib.
	p.AddModel("raw/users.sql", `-- @kind: table
-- @fetch
SELECT user_id::BIGINT AS user_id, name::VARCHAR AS name FROM users_api('users')
`)
	p.AddModel("raw/scores.sql", `-- @kind: table
-- @fetch
SELECT user_id::BIGINT AS user_id, score::BIGINT AS score FROM scores_api('scores')
`)

	rUsers := runModelWithLib(t, p, "raw/users.sql")
	if rUsers.RowsAffected != 2 {
		t.Fatalf("users: expected 2 rows, got %d (warnings: %v)", rUsers.RowsAffected, rUsers.Warnings)
	}
	rScores := runModelWithLib(t, p, "raw/scores.sql")
	if rScores.RowsAffected != 2 {
		t.Fatalf("scores: expected 2 rows, got %d (warnings: %v)", rScores.RowsAffected, rScores.Warnings)
	}

	// Downstream JOIN model — regular SQL, no @fetch.
	p.AddModel("staging/joined.sql", `-- @kind: table
SELECT u.user_id::BIGINT AS user_id, u.name::VARCHAR AS name, s.score::BIGINT AS score
FROM raw.users u
JOIN raw.scores s ON u.user_id = s.user_id
`)
	rJoined := runModelWithLib(t, p, "staging/joined.sql")
	if rJoined.RowsAffected != 2 {
		t.Fatalf("joined: expected 2 rows, got %d (warnings: %v)", rJoined.RowsAffected, rJoined.Warnings)
	}

	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.joined")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != "2" {
		t.Fatalf("expected 2 rows in target, got %s", count)
	}
}

// ---------------------------------------------------------------------------
// Test 4: schema evolution after empty run (documented limitation)
// ---------------------------------------------------------------------------

// TestLibCall_SchemalessLib_EmptyThenSchemaChange documents the degraded-
// mode behavior: when a dynamic-column lib returns 0 rows and the target
// exists, the stub is cloned from the target. Schema changes (new columns)
// are NOT detected until the next non-empty run. This is a known limitation.
func TestLibCall_SchemalessLib_EmptyThenSchemaChange(t *testing.T) {
	p := testutil.NewProject(t)

	// Phase 1: lib returns data to create initial target
	writeLib(t, p, "dynapi", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["table"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    if not is_backfill:
        return {"rows": [], "next": None}
    return {
        "rows": [{"id": 1, "name": "Alice"}],
        "next": None,
    }
`)

	p.AddModel("raw/dyn.sql", `-- @kind: table
-- @fetch
SELECT id::BIGINT AS id, name::VARCHAR AS name FROM dynapi('items')
`)

	// Run 1 — creates target with (id, name)
	r1 := runModelWithLib(t, p, "raw/dyn.sql")
	if r1.RowsAffected != 1 {
		t.Fatalf("run 1: expected 1 row, got %d", r1.RowsAffected)
	}

	// Phase 2: lib always returns 0 rows, SQL adds a new column.
	// Target exists → stub cloned from target → email column NOT
	// detected (degraded mode). Run should succeed without crash.
	writeLib(t, p, "dynapi", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["table"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    return {"rows": [], "next": None}
`)

	modelPath := filepath.Join(p.Dir, "models", "raw/dyn.sql")
	os.WriteFile(modelPath, []byte(`-- @kind: table
-- @fetch
SELECT id::BIGINT AS id, name::VARCHAR AS name, email::VARCHAR AS email FROM dynapi('items')
`), 0644)

	// Run 2 — 0 rows, target-clone stub. Must not crash.
	// Schema change deferred until next run with data.
	r2 := runModelWithLib(t, p, "raw/dyn.sql")
	t.Logf("run 2 (degraded): rows=%d type=%s warnings=%v", r2.RowsAffected, r2.RunType, r2.Warnings)

	// Phase 3: lib returns data with the new schema → detected now
	writeLib(t, p, "dynapi", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["table"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    return {
        "rows": [{"id": 2, "name": "Bob", "email": "bob@test.com"}],
        "next": None,
    }
`)

	// Reorder columns to trigger hash change (WHERE forbidden in @fetch v0.30.0).
	os.WriteFile(modelPath, []byte(`-- @kind: table
-- @fetch
SELECT email::VARCHAR AS email, id::BIGINT AS id, name::VARCHAR AS name FROM dynapi('items')
`), 0644)

	r3 := runModelWithLib(t, p, "raw/dyn.sql")
	t.Logf("run 3: rows=%d type=%s warnings=%v", r3.RowsAffected, r3.RunType, r3.Warnings)
	if r3.RowsAffected != 1 {
		t.Fatalf("run 3: expected 1 row with new schema, got %d", r3.RowsAffected)
	}

	// Verify email column exists now (added on run 3 with data)
	cols, err := p.Sess.QueryValue(
		"SELECT string_agg(column_name, ',' ORDER BY ordinal_position) " +
			"FROM information_schema.columns WHERE table_schema='raw' AND table_name='dyn'")
	if err != nil {
		t.Fatalf("schema query: %v", err)
	}
	if !strings.Contains(cols, "email") {
		t.Fatalf("expected 'email' column after run with data, got: %s", cols)
	}
}

// ---------------------------------------------------------------------------
// Test 5: ack crash-recovery via _ondatra_acks table
// ---------------------------------------------------------------------------

// TestLibCall_AckCrashRecovery verifies the claim ack crash-recovery path.
// When lib claims are acked inside the materialize transaction (via extraPreSQL),
// the _ondatra_acks table records the ack atomically with the data commit.
// If the post-commit AckClaims() to Badger fails, the next run detects the
// ack record and skips re-processing those claims.
func TestLibCall_AckCrashRecovery(t *testing.T) {
	p := testutil.NewProject(t)

	writeLib(t, p, "crashapi", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["append"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    if not is_backfill:
        return {"rows": [], "next": None}
    return {
        "rows": [
            {"id": 1, "val": "2026-01-01"},
            {"id": 2, "val": "2026-01-02"},
        ],
        "next": None,
    }
`)

	p.AddModel("raw/crash.sql", `-- @kind: append
-- @fetch
-- @incremental: val
-- @incremental_initial: 2026-01-01
SELECT id::BIGINT AS id, val::VARCHAR AS val FROM crashapi('items')
`)

	// Run 1 — backfill, 2 rows
	r1 := runModelWithLib(t, p, "raw/crash.sql")
	if r1.RowsAffected != 2 {
		t.Fatalf("run 1: expected 2 rows, got %d", r1.RowsAffected)
	}

	// Verify that _ondatra_acks table exists and is clean after successful run
	// (ack records should be deleted after successful Badger ack)
	ackCount, err := p.Sess.QueryValue(
		"SELECT COUNT(*) FROM _ondatra_acks WHERE target = 'raw.crash'")
	if err != nil {
		// Table might not exist if no claims were used — that's OK for in-memory path
		t.Logf("_ondatra_acks query: %v (expected if in-memory mode)", err)
	} else if ackCount != "0" {
		t.Fatalf("expected 0 leftover ack records after clean run, got %s", ackCount)
	}

	// Run 2 — incremental, 0 rows
	r2 := runModelWithLib(t, p, "raw/crash.sql")
	if r2.RowsAffected != 0 {
		t.Fatalf("run 2: expected 0 rows, got %d", r2.RowsAffected)
	}

	// Run 3 — still 0, no claim replay
	r3 := runModelWithLib(t, p, "raw/crash.sql")
	if r3.RowsAffected != 0 {
		t.Fatalf("run 3: expected 0 rows, got %d", r3.RowsAffected)
	}

	// Verify total: exactly 2 rows (no duplication from ack/nack confusion)
	total, err := p.Sess.QueryValue("SELECT COUNT(*) FROM raw.crash")
	if err != nil {
		t.Fatalf("total count: %v", err)
	}
	if total != "2" {
		t.Fatalf("expected 2 total rows, got %s", total)
	}
}

// ---------------------------------------------------------------------------
// Stub edge cases: 0-row lib calls
// ---------------------------------------------------------------------------

// TestLibCall_Stub_TargetClone verifies that when a target exists and all
// lib calls return 0 rows, the stub is cloned from target (real types)
// and the full pipeline runs (schema evolution, constraints, audits, sink).
func TestLibCall_Stub_TargetClone(t *testing.T) {
	p := testutil.NewProject(t)

	writeLib(t, p, "src", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["append"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    if not is_backfill:
        return {"rows": [], "next": None}
    return {
        "rows": [{"id": 1, "val": "2026-01-01", "amount": 99.5}],
        "next": None,
    }
`)

	p.AddModel("raw/data.sql", `-- @kind: append
-- @fetch
-- @incremental: val
-- @incremental_initial: 2026-01-01
SELECT id::BIGINT AS id, val::VARCHAR AS val, amount::DOUBLE AS amount FROM src('items')
`)

	// Run 1 — backfill, creates target with BIGINT/DOUBLE types
	r1 := runModelWithLib(t, p, "raw/data.sql")
	if r1.RowsAffected != 1 {
		t.Fatalf("run 1: expected 1 row, got %d", r1.RowsAffected)
	}

	// Run 2 — incremental, 0 rows. Stub clones from target.
	// No false schema evolution warnings (types match target).
	r2 := runModelWithLib(t, p, "raw/data.sql")
	if r2.RowsAffected != 0 {
		t.Fatalf("run 2: expected 0 rows, got %d", r2.RowsAffected)
	}
	for _, w := range r2.Warnings {
		if strings.Contains(w, "schema evolution") {
			t.Fatalf("run 2: unexpected schema evolution warning: %s", w)
		}
	}

	// Verify data unchanged — exactly 1 row
	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM raw.data")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != "1" {
		t.Fatalf("expected 1 row, got %s", count)
	}
}

// TestLibCall_Stub_NoTarget_VarcharStubs verifies that when no target
// exists and lib returns 0 rows, VARCHAR stubs are created with
// input-shape columns and an empty target table is created.
func TestLibCall_Stub_NoTarget_VarcharStubs(t *testing.T) {
	p := testutil.NewProject(t)

	writeLib(t, p, "src", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["table"],
    },
}

def fetch(resource, page):
    return {"rows": [], "next": None}
`)

	p.AddModel("raw/empty.sql", `-- @kind: table
-- @fetch
SELECT id::BIGINT AS id, name::VARCHAR AS name FROM src('items')
`)

	// First run — 0 rows, no target. Stubs created from AST columns.
	// Target table should be created (empty).
	r1 := runModelWithLib(t, p, "raw/empty.sql")
	if r1.RowsAffected != 0 {
		t.Fatalf("run 1: expected 0 rows, got %d", r1.RowsAffected)
	}

	// Verify target exists with correct columns
	cols, err := p.Sess.QueryValue(
		"SELECT string_agg(column_name, ',' ORDER BY ordinal_position) " +
			"FROM information_schema.columns WHERE table_schema='raw' AND table_name='empty'")
	if err != nil {
		t.Fatalf("schema query: %v", err)
	}
	if !strings.Contains(cols, "id") || !strings.Contains(cols, "name") {
		t.Fatalf("expected id,name columns, got: %s", cols)
	}
}

// TestLibCall_Stub_NoFalseSchemaEvolution verifies that incremental runs
// with 0 lib rows do NOT produce false schema evolution warnings when
// the target has non-VARCHAR types (e.g. DOUBLE, BIGINT).
func TestLibCall_Stub_NoFalseSchemaEvolution(t *testing.T) {
	p := testutil.NewProject(t)

	writeLib(t, p, "src", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["append"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    if not is_backfill:
        return {"rows": [], "next": None}
    return {
        "rows": [
            {"series": "USD", "date": "2026-01-01", "value": 10.5},
            {"series": "EUR", "date": "2026-01-02", "value": 11.2},
        ],
        "next": None,
    }
`)

	p.AddModel("raw/rates.sql", `-- @kind: append
-- @fetch
-- @incremental: date
-- @incremental_initial: 2026-01-01
SELECT series::VARCHAR AS series, date::VARCHAR AS date, value::DOUBLE AS value FROM src('items')
`)

	// Run 1 — backfill, target gets DOUBLE for value
	r1 := runModelWithLib(t, p, "raw/rates.sql")
	if r1.RowsAffected != 2 {
		t.Fatalf("run 1: expected 2 rows, got %d", r1.RowsAffected)
	}

	// Run 2-5: incremental, 0 rows. Must NOT produce "DOUBLE → VARCHAR"
	for run := 2; run <= 5; run++ {
		r := runModelWithLib(t, p, "raw/rates.sql")
		if r.RowsAffected != 0 {
			t.Fatalf("run %d: expected 0 rows, got %d", run, r.RowsAffected)
		}
		for _, w := range r.Warnings {
			if strings.Contains(w, "schema evolution") {
				t.Fatalf("run %d: false schema evolution: %s", run, w)
			}
		}
	}

	// Verify still exactly 2 rows
	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM raw.rates")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != "2" {
		t.Fatalf("expected 2 rows after 5 runs, got %s", count)
	}
}

// TestPushUpdatePreimage_FromPreChangeSnapshot pins that update_preimage
// rows are read from the snapshot BEFORE the change (e.Snapshot - 1), not
// from current state. The bug class: someone changes the snapshot offset
// in readRowsByEvents and the preimage starts containing the post-change
// values, silently breaking change-aware push() semantics. The push()
// function fails the run if the preimage doesn't have the OLD value, so
// any drift in the snapshot calculation will surface as SyncFailed > 0.
func TestPushUpdatePreimage_FromPreChangeSnapshot(t *testing.T) {
	p := testutil.NewProject(t)

	// Source lib: row id=1 with score=100 (run 1), then score=200 (run 2)
	writeLib(t, p, "updsrc", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["merge"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    if is_backfill:
        return {"rows": [{"id": 1, "score": 100}], "next": None}
    return {"rows": [{"id": 1, "score": 200}], "next": None}
`)

	// Push: asserts that preimage has the OLD value (100), postimage the NEW (200).
	// If readRowsByEvents reads from the wrong snapshot, the preimage row will
	// carry score=200 (current state) and fail() will trip the test.
	writeLib(t, p, "checker", `
API = {
    "push": {
        "batch_size": 100,
        "batch_mode": "atomic",
    },
}

def push(rows=[], batch_number=1, kind="", key_columns=[], columns=[]):
    for row in rows:
        ct = row.get("__ondatra_change_type", "")
        if ct == "update_preimage":
            if row.get("score") != 100:
                fail("preimage score should be 100 (pre-change), got " + str(row.get("score")))
        elif ct == "update_postimage":
            if row.get("score") != 200:
                fail("postimage score should be 200 (post-change), got " + str(row.get("score")))
`)

	// v0.30.0: split into raw-fetch + downstream-push models.
	p.AddModel("raw/items.sql", `-- @kind: merge
-- @fetch
-- @unique_key: id
SELECT id::BIGINT AS id, score::BIGINT AS score FROM updsrc('items')
`)
	p.AddModel("sync/items.sql", `-- @kind: merge
-- @unique_key: id
-- @push: checker
SELECT id::BIGINT AS id, score::BIGINT AS score FROM raw.items
`)

	// Run 1: backfill — single row insert, sink sees insert event
	rRaw1 := runModelWithLib(t, p, "raw/items.sql")
	if rRaw1.RowsAffected != 1 {
		t.Fatalf("raw run 1: expected 1 row, got %d", rRaw1.RowsAffected)
	}
	rSync1 := runModelWithLib(t, p, "sync/items.sql")
	if rSync1.SyncFailed != 0 {
		t.Fatalf("sync run 1: SyncFailed=%d, expected 0 (warnings: %v)", rSync1.SyncFailed, rSync1.Warnings)
	}

	// Run 2: lib returns same id with new score. Raw merges → update.
	// Sync model picks up the merge → sees update_preimage + update_postimage.
	// The push() asserts preimage carries the pre-change value.
	runModelWithLib(t, p, "raw/items.sql")
	rSync2 := runModelWithLib(t, p, "sync/items.sql")
	if rSync2.SyncFailed != 0 {
		t.Fatalf("sync run 2: SyncFailed=%d — preimage/postimage rows had wrong values, indicating readRowsByEvents read from wrong snapshot. Warnings: %v", rSync2.SyncFailed, rSync2.Warnings)
	}
	if rSync2.SyncSucceeded == 0 {
		t.Fatalf("sync run 2: SyncSucceeded=0, expected sink to fire on update (warnings: %v)", rSync2.Warnings)
	}
}

// TestPreCommitSnapshot_AppendWithPush pins that the preCommitSnapshot
// capture is gated by `model.Push != ""` only — not narrowed to specific
// kinds (merge/tracked). The bug class: someone changes the gate to
// `if model.Kind == "merge" || ...`, breaking append + @sink. Without
// the pre-commit snapshot, createPushDelta would query table_changes
// from snapshot 1 instead of the right range, replaying every row of
// every previous run into the sink on every incremental run.
//
// This test runs an append + @sink model twice with a NEW row on the
// second run, and asserts the sink saw only the new row — not all rows.
func TestPreCommitSnapshot_AppendWithPush(t *testing.T) {
	p := testutil.NewProject(t)

	// Source lib: returns 2 rows on backfill, 1 NEW row on incremental.
	writeLib(t, p, "appendsrc", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["append"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    if is_backfill:
        return {"rows": [
            {"id": 1, "score": 100},
            {"id": 2, "score": 200},
        ], "next": None}
    # incremental: one new row beyond last_value
    return {"rows": [{"id": 3, "score": 300}], "next": None}
`)

	// Sink lib: just succeeds. We verify via SyncSucceeded.
	writeLib(t, p, "sinkrec", `
API = {
    "push": {
        "batch_size": 100,
        "batch_mode": "atomic",
    },
}

def push(rows=[], batch_number=1, kind="", key_columns=[], columns=[]):
    pass
`)

	// v0.30.0: split raw-fetch + downstream-push
	p.AddModel("raw/scores.sql", `-- @kind: append
-- @fetch
-- @incremental: id
-- @incremental_initial: 0
SELECT id::BIGINT AS id, score::BIGINT AS score FROM appendsrc('items')
`)
	p.AddModel("sync/scores.sql", `-- @kind: append
-- @incremental: id
-- @incremental_initial: 0
-- @push: sinkrec
SELECT id::BIGINT AS id, score::BIGINT AS score FROM raw.scores
`)

	// Run 1: backfill — 2 rows in raw, 2 in sync, sink fires 2 times
	rRaw1 := runModelWithLib(t, p, "raw/scores.sql")
	if rRaw1.RowsAffected != 2 {
		t.Fatalf("raw run 1: expected 2 rows, got %d", rRaw1.RowsAffected)
	}
	rSync1 := runModelWithLib(t, p, "sync/scores.sql")
	if rSync1.SyncSucceeded != 2 {
		t.Fatalf("sync run 1: expected SyncSucceeded=2, got %d (warnings: %v)", rSync1.SyncSucceeded, rSync1.Warnings)
	}

	// Run 2: incremental — 1 NEW row in raw. Sync model picks it up via
	// incremental cursor. Sink delta should only include the new row.
	// If preCommitSnapshot was not captured, the delta would also include
	// run 1's rows → SyncSucceeded would be 3, not 1.
	rRaw2 := runModelWithLib(t, p, "raw/scores.sql")
	if rRaw2.RowsAffected != 1 {
		t.Fatalf("raw run 2: expected 1 new row, got %d", rRaw2.RowsAffected)
	}
	rSync2 := runModelWithLib(t, p, "sync/scores.sql")
	if rSync2.SyncSucceeded != 1 {
		t.Fatalf("sync run 2: expected SyncSucceeded=1 (only the new row), got %d — preCommitSnapshot may be missing for append kind (warnings: %v)", rSync2.SyncSucceeded, rSync2.Warnings)
	}

	// Verify totals
	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM raw.scores")
	if err != nil {
		t.Fatalf("count raw: %v", err)
	}
	if count != "3" {
		t.Fatalf("expected 3 rows in raw.scores after 2 runs, got %s", count)
	}
	count, err = p.Sess.QueryValue("SELECT COUNT(*) FROM sync.scores")
	if err != nil {
		t.Fatalf("count sync: %v", err)
	}
	if count != "3" {
		t.Fatalf("expected 3 rows in sync.scores, got %s", count)
	}
}

// TestLibCall_Tracked_EmptyIncremental_Push mirrors the merge equivalent for
// the tracked kind: backfill produces rows + sink events, then an empty
// incremental run must NOT re-fire the sink. Tracked has its own change
// detection (group hashes), so the sink-side guard against spurious events
// must hold for it too.
func TestLibCall_Tracked_EmptyIncremental_Push(t *testing.T) {
	p := testutil.NewProject(t)

	writeLib(t, p, "tracksrc", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["tracked"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    # Tracked without @incremental: lib must return the full source state
    # every call. Gating on is_backfill would return empty on run 2 (because
    # the runner sets is_backfill=false once the target table exists), and
    # tracked would then treat all groups as "disappeared" and DELETE every
    # row. Returning full state lets tracked compare group hashes correctly.
    return {"rows": [
        {"region": "US", "amount": 100},
        {"region": "EU", "amount": 200},
    ], "next": None}
`)

	writeLib(t, p, "trackedsink", `
API = {
    "push": {
        "batch_size": 100,
        "batch_mode": "atomic",
    },
}

def push(rows=[], batch_number=1, kind="", key_columns=[], columns=[]):
    pass
`)

	// v0.30.0: split raw-fetch + downstream-push for tracked
	p.AddModel("raw/regional.sql", `-- @kind: tracked
-- @fetch
-- @group_key: region
SELECT region::VARCHAR AS region, amount::BIGINT AS amount FROM tracksrc('items')
`)
	p.AddModel("sync/regional.sql", `-- @kind: tracked
-- @group_key: region
-- @push: trackedsink
SELECT region::VARCHAR AS region, amount::BIGINT AS amount FROM raw.regional
`)

	rRaw1 := runModelWithLib(t, p, "raw/regional.sql")
	if rRaw1.RowsAffected != 2 {
		t.Fatalf("raw run 1: expected 2 rows, got %d (warnings: %v)", rRaw1.RowsAffected, rRaw1.Warnings)
	}
	rSync1 := runModelWithLib(t, p, "sync/regional.sql")
	if rSync1.SyncSucceeded == 0 {
		t.Fatalf("sync run 1: sink did not fire (SyncSucceeded=0, warnings: %v)", rSync1.Warnings)
	}

	// Run 2 — same source, no group hash changes. Tracked must not commit
	// new rows, materialize must succeed, sink must not fail.
	runModelWithLib(t, p, "raw/regional.sql")
	rSync2 := runModelWithLib(t, p, "sync/regional.sql")
	if rSync2.RowsAffected != 0 {
		t.Fatalf("sync run 2: expected 0 rows, got %d", rSync2.RowsAffected)
	}
	if rSync2.SyncFailed != 0 {
		t.Fatalf("sync run 2: SyncFailed=%d, expected 0 (warnings: %v)", rSync2.SyncFailed, rSync2.Warnings)
	}

	// Run 3 — still empty, still stable
	runModelWithLib(t, p, "raw/regional.sql")
	rSync3 := runModelWithLib(t, p, "sync/regional.sql")
	if rSync3.RowsAffected != 0 {
		t.Fatalf("sync run 3: expected 0 rows, got %d", rSync3.RowsAffected)
	}
	if rSync3.SyncFailed != 0 {
		t.Fatalf("sync run 3: SyncFailed=%d, expected 0", rSync3.SyncFailed)
	}

	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM raw.regional")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != "2" {
		t.Fatalf("expected 2 rows after 3 runs, got %s", count)
	}
}

// TestLibCall_Tracked_GroupHashChange_Push verifies that when a tracked
// group's content changes (hash differs), the sink fires only for the
// changed group's events — not for unchanged groups. This pins the
// "no spurious replay" guarantee at the tracked + sink interface.
func TestLibCall_Tracked_GroupHashChange_Push(t *testing.T) {
	p := testutil.NewProject(t)

	writeLib(t, p, "tgsrc", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["tracked"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    return {"rows": [
        {"region": "US", "amount": 100},
        {"region": "EU", "amount": 200},
    ], "next": None}
`)

	writeLib(t, p, "tgsink", `
API = {
    "push": {
        "batch_size": 100,
        "batch_mode": "atomic",
    },
}

def push(rows=[], batch_number=1, kind="", key_columns=[], columns=[]):
    pass
`)

	// v0.30.0: split raw-fetch + downstream-push
	p.AddModel("raw/groups.sql", `-- @kind: tracked
-- @fetch
-- @group_key: region
SELECT region::VARCHAR AS region, amount::BIGINT AS amount FROM tgsrc('items')
`)
	p.AddModel("sync/groups.sql", `-- @kind: tracked
-- @group_key: region
-- @push: tgsink
SELECT region::VARCHAR AS region, amount::BIGINT AS amount FROM raw.groups
`)

	rRaw1 := runModelWithLib(t, p, "raw/groups.sql")
	if rRaw1.RowsAffected != 2 {
		t.Fatalf("raw run 1: rows=%d, want 2 (warnings: %v)", rRaw1.RowsAffected, rRaw1.Warnings)
	}
	rSync1 := runModelWithLib(t, p, "sync/groups.sql")
	if rSync1.SyncSucceeded < 2 {
		t.Fatalf("sync run 1: SyncSucceeded=%d, want >=2 (warnings: %v)", rSync1.SyncSucceeded, rSync1.Warnings)
	}
	syncAfterRun1 := rSync1.SyncSucceeded

	// Run 2 — same lib, no changes. Sync should be quiet.
	runModelWithLib(t, p, "raw/groups.sql")
	rSync2 := runModelWithLib(t, p, "sync/groups.sql")
	if rSync2.SyncSucceeded != 0 {
		t.Fatalf("sync run 2: expected sink quiet, got SyncSucceeded=%d", rSync2.SyncSucceeded)
	}

	// Phase 2 — change US group's amount. EU unchanged. Tracked should detect
	// US hash change, replace US group's rows, sink should see only US events.
	writeLib(t, p, "tgsrc", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["tracked"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    return {"rows": [
        {"region": "US", "amount": 999},
        {"region": "EU", "amount": 200},
    ], "next": None}
`)

	rRaw3 := runModelWithLib(t, p, "raw/groups.sql")
	if rRaw3.RowsAffected == 0 {
		t.Fatalf("raw run 3: expected rows updated for US group, got 0 (warnings: %v)", rRaw3.Warnings)
	}
	rSync3 := runModelWithLib(t, p, "sync/groups.sql")
	if rSync3.SyncSucceeded == 0 {
		t.Fatalf("sync run 3: sink did not fire on group change (warnings: %v)", rSync3.Warnings)
	}
	if rSync3.SyncSucceeded > syncAfterRun1 {
		t.Errorf("sync run 3: sink fired more events (%d) than backfill (%d) — would mean entire table replayed instead of changed group only", rSync3.SyncSucceeded, syncAfterRun1)
	}

	// US row should have new amount in both raw and sync
	usAmount, err := p.Sess.QueryValue("SELECT amount FROM raw.groups WHERE region = 'US'")
	if err != nil || usAmount != "999" {
		t.Fatalf("raw US amount = %q, want 999 (err: %v)", usAmount, err)
	}
	usAmount, err = p.Sess.QueryValue("SELECT amount FROM sync.groups WHERE region = 'US'")
	if err != nil || usAmount != "999" {
		t.Fatalf("sync US amount = %q, want 999 (err: %v)", usAmount, err)
	}
}

// TestLibCall_Tracked_GroupDisappears_Push pins that when a group disappears
// from the source, tracked DELETEs its rows from the target and the sink
// receives delete-classified events. Without this, downstream systems would
// silently keep stale data after a group is dropped upstream.
func TestLibCall_Tracked_GroupDisappears_Push(t *testing.T) {
	p := testutil.NewProject(t)

	writeLib(t, p, "tdsrc", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["tracked"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    return {"rows": [
        {"region": "US", "amount": 100},
        {"region": "EU", "amount": 200},
    ], "next": None}
`)

	writeLib(t, p, "tdsink", `
API = {
    "push": {
        "batch_size": 100,
        "batch_mode": "atomic",
    },
}

def push(rows=[], batch_number=1, kind="", key_columns=[], columns=[]):
    pass
`)

	// v0.30.0: split raw-fetch + downstream-push
	p.AddModel("raw/regions.sql", `-- @kind: tracked
-- @fetch
-- @group_key: region
SELECT region::VARCHAR AS region, amount::BIGINT AS amount FROM tdsrc('items')
`)
	p.AddModel("sync/regions.sql", `-- @kind: tracked
-- @group_key: region
-- @push: tdsink
SELECT region::VARCHAR AS region, amount::BIGINT AS amount FROM raw.regions
`)

	rRaw1 := runModelWithLib(t, p, "raw/regions.sql")
	if rRaw1.RowsAffected != 2 {
		t.Fatalf("raw run 1: expected 2 rows, got %d", rRaw1.RowsAffected)
	}
	runModelWithLib(t, p, "sync/regions.sql")

	// Phase 2 — EU group disappears. Tracked on raw should DELETE EU's rows;
	// the sync model picks up the delete and fires a delete-classified event
	// to the sink.
	writeLib(t, p, "tdsrc", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["tracked"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    return {"rows": [
        {"region": "US", "amount": 100},
    ], "next": None}
`)

	runModelWithLib(t, p, "raw/regions.sql")
	rSync2 := runModelWithLib(t, p, "sync/regions.sql")

	// Both raw and sync should have only US row
	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM raw.regions")
	if err != nil {
		t.Fatalf("count raw: %v", err)
	}
	if count != "1" {
		t.Fatalf("after group disappears: raw expected 1 row, got %s", count)
	}
	count, err = p.Sess.QueryValue("SELECT COUNT(*) FROM sync.regions")
	if err != nil {
		t.Fatalf("count sync: %v", err)
	}
	if count != "1" {
		t.Fatalf("after group disappears: sync expected 1 row, got %s", count)
	}

	// Sink should have fired with at least the delete event for EU
	if rSync2.SyncSucceeded == 0 {
		t.Fatalf("sink did not fire for group-disappear case (SyncSucceeded=0, warnings: %v) — delete events lost", rSync2.Warnings)
	}
}

// TestLibCall_Stub_SQLShape_Typed pins that on a first run with no target,
// when the lib returns 0 rows, the stub uses the SQL projection's typed
// columns (BIGINT, DOUBLE, etc) — not VARCHAR for everything. The target
// table is then created with the typed schema, so subsequent runs with
// real data don't fire false schema-evolution warnings.
//
// Regression for v0.24.0 Item 2 (SQL-shape primary in 0-row stub).
func TestLibCall_Stub_SQLShape_Typed(t *testing.T) {
	p := testutil.NewProject(t)

	// Phase 1: lib that returns 0 rows on first call (e.g., upstream not
	// ready yet). Target does not exist → no target-clone fallback.
	writeLib(t, p, "src", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["table"],
    },
}

def fetch(resource, page):
    return {"rows": [], "next": None}
`)

	// SELECT has explicit casts: id BIGINT, amount DOUBLE
	p.AddModel("raw/typed.sql", `-- @kind: table
-- @fetch
SELECT id::BIGINT AS id, name::VARCHAR AS name, amount::DOUBLE AS amount FROM src('items')
`)

	// First run — 0 rows, no target. Stub built from SQL projection types.
	r1 := runModelWithLib(t, p, "raw/typed.sql")
	if r1.RowsAffected != 0 {
		t.Fatalf("run 1: expected 0 rows, got %d (warnings: %v)", r1.RowsAffected, r1.Warnings)
	}

	// Verify target was created with typed columns from the SQL,
	// not VARCHAR fallbacks.
	rows, err := p.Sess.QueryRowsMap(
		"SELECT column_name, data_type FROM information_schema.columns " +
			"WHERE table_schema='raw' AND table_name='typed' ORDER BY column_name")
	if err != nil {
		t.Fatalf("schema query: %v", err)
	}
	want := map[string]string{
		"id":     "BIGINT",
		"amount": "DOUBLE",
		"name":   "VARCHAR", // bare COLUMN_REF, no cast → VARCHAR
	}
	got := make(map[string]string)
	for _, row := range rows {
		got[row["column_name"]] = row["data_type"]
	}
	for col, expectedType := range want {
		if got[col] != expectedType {
			t.Errorf("column %q: got type %q, want %q (full schema: %v)", col, got[col], expectedType, got)
		}
	}

	// Phase 2: lib now returns real data. No false schema-evolution warning
	// because the target's typed schema already matches what the data fits.
	writeLib(t, p, "src", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["table"],
    },
}

def fetch(resource, page):
    return {"rows": [{"id": 1, "name": "Alice", "amount": 9.5}], "next": None}
`)

	// Touch the model so the hash changes and table re-runs.
	// Column reorder triggers hash change (WHERE forbidden in @fetch v0.30.0).
	modelPath := filepath.Join(p.Dir, "models", "raw/typed.sql")
	os.WriteFile(modelPath, []byte(`-- @kind: table
-- @fetch
SELECT name::VARCHAR AS name, id::BIGINT AS id, amount::DOUBLE AS amount FROM src('items')
`), 0644)

	r2 := runModelWithLib(t, p, "raw/typed.sql")
	if r2.RowsAffected != 1 {
		t.Fatalf("run 2: expected 1 row, got %d (warnings: %v)", r2.RowsAffected, r2.Warnings)
	}
	for _, w := range r2.Warnings {
		if strings.Contains(w, "schema evolution") {
			t.Fatalf("run 2: unexpected schema evolution warning (target was already typed): %s", w)
		}
	}
}

// TestLibCall_Tracked_EmptyResult_DefaultNoChange_PreservesTarget pins that
// when a tracked-kind lib returns 0 rows on an incremental run AND does not
// set empty_result (so it defaults to no_change), the target rows are
// preserved — the runner does not interpret an empty fetch as "every group
// disappeared from the source".
//
// This is the bug class hit by mistral_ocr's smart-skip pattern: lib detects
// "nothing changed upstream" via cache key, returns 0 rows; without this
// guard, tracked materialize would DELETE every group on each smart-skip
// run, then re-fetch on the next, oscillating data forever.
//
// Regression for v0.24.0 Item 1 (tracked empty_result fetch metadata).
func TestLibCall_Tracked_EmptyResult_DefaultNoChange_PreservesTarget(t *testing.T) {
	p := testutil.NewProject(t)

	// Phase 1: lib returns 3 rows across 2 groups (smart-skip will be
	// activated by rewriting the lib in phase 2).
	writeLib(t, p, "src", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["tracked"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    return {"rows": [
        {"region": "US", "id": 1, "name": "Alice"},
        {"region": "US", "id": 2, "name": "Bob"},
        {"region": "EU", "id": 3, "name": "Carol"},
    ], "next": None}
`)

	p.AddModel("raw/regional.sql", `-- @kind: tracked
-- @fetch
-- @group_key: region
SELECT region::VARCHAR AS region, id::BIGINT AS id, name::VARCHAR AS name FROM src('items')
`)

	// Run 1 — backfill: 3 rows, 2 groups
	r1 := runModelWithLib(t, p, "raw/regional.sql")
	if r1.RowsAffected != 3 {
		t.Fatalf("run 1: expected 3 rows, got %d (warnings: %v)", r1.RowsAffected, r1.Warnings)
	}

	// Phase 2: lib smart-skips — returns 0 rows with NO empty_result key
	// (defaults to no_change). Tracked materialize MUST preserve the
	// existing target.
	writeLib(t, p, "src", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["tracked"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    return {"rows": [], "next": None}
`)

	// Run 2 — smart-skip. RowsAffected==0, target unchanged. The runner
	// still goes through stub creation, schema-evolution, and materialize
	// so SQL-only changes (new columns, audit changes) would still apply
	// — only the delete-missing-groups branch is suppressed.
	r2 := runModelWithLib(t, p, "raw/regional.sql")
	if r2.RowsAffected != 0 {
		t.Fatalf("run 2: expected 0 rows, got %d", r2.RowsAffected)
	}
	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM raw.regional")
	if err != nil {
		t.Fatalf("count after run 2: %v", err)
	}
	if count != "3" {
		t.Fatalf("run 2: target preservation failed — expected 3 rows, got %s", count)
	}

	// Run 3 — same smart-skip, no oscillation
	r3 := runModelWithLib(t, p, "raw/regional.sql")
	if r3.RowsAffected != 0 {
		t.Fatalf("run 3: expected 0 rows, got %d", r3.RowsAffected)
	}
	count, err = p.Sess.QueryValue("SELECT COUNT(*) FROM raw.regional")
	if err != nil {
		t.Fatalf("count after run 3: %v", err)
	}
	if count != "3" {
		t.Fatalf("run 3: target preservation failed — expected 3 rows, got %s", count)
	}
}

// TestLibCall_Tracked_EmptyResult_DeleteMissing pins that a tracked-kind lib
// can opt in to legacy hard-delete-on-empty semantics by setting
// empty_result="delete_missing" in its fetch return. This is the explicit
// "fully-enumerated source, the empty really means empty" contract — every
// group gets deleted from the target.
//
// Regression for v0.24.0 Item 1 — opt-in path for the explicit hard-delete
// case (industry parallel: dbt hard_deletes, Airbyte CDC removal).
func TestLibCall_Tracked_EmptyResult_DeleteMissing(t *testing.T) {
	p := testutil.NewProject(t)

	// Phase 1: lib returns 2 rows across 2 groups
	writeLib(t, p, "src", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["tracked"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    return {"rows": [
        {"region": "US", "amount": 100},
        {"region": "EU", "amount": 200},
    ], "next": None}
`)

	p.AddModel("raw/regional.sql", `-- @kind: tracked
-- @fetch
-- @group_key: region
SELECT region::VARCHAR AS region, amount::BIGINT AS amount FROM src('items')
`)

	r1 := runModelWithLib(t, p, "raw/regional.sql")
	if r1.RowsAffected != 2 {
		t.Fatalf("run 1: expected 2 rows, got %d (warnings: %v)", r1.RowsAffected, r1.Warnings)
	}

	// Phase 2: lib explicitly declares the source is fully enumerated and
	// has gone empty. Tracked must delete all groups.
	writeLib(t, p, "src", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["tracked"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    return {"rows": [], "empty_result": "delete_missing", "next": None}
`)

	r2 := runModelWithLib(t, p, "raw/regional.sql")
	t.Logf("run 2: rows=%d type=%s warnings=%v", r2.RowsAffected, r2.RunType, r2.Warnings)

	// Target should now be empty — both groups were deleted.
	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM raw.regional")
	if err != nil {
		t.Fatalf("count after run 2: %v", err)
	}
	if count != "0" {
		t.Fatalf("delete_missing failed: expected 0 rows, got %s — opt-in hard-delete did not fire", count)
	}
}

// TestLibCall_Tracked_EmptyResult_NoChange_AuditBypassesFastPath pins
// that the no-cascade fast path is correctly *bypassed* when a tracked
// model declares an `@audit`. Audits are wrapped into the materialize
// transaction so a failing audit can roll back the schema ALTER + data
// write + commit metadata together; if the runtime took the fast path
// on a 0-row no_change run the audit would never execute and any
// invariant it pins would silently stop being checked.
//
// We verify by checking that a downstream model sees a dep change after
// the empty no_change run — proving materialize committed (which only
// happens when the fast path is bypassed). Compare with
// TestLibCall_Tracked_EmptyResult_NoChange_NoSnapshotCascade which uses
// the same shape minus the @audit and DOES skip downstream.
//
// Note: `@constraint` checks run before materialize (read-only) and
// therefore are independent of the fast-path decision — they are not
// part of this contract.
func TestLibCall_Tracked_EmptyResult_NoChange_AuditBypassesFastPath(t *testing.T) {
	p := testutil.NewProject(t)

	writeLib(t, p, "src", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["tracked"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    return {"rows": [
        {"region": "US", "amount": 100},
    ], "next": None}
`)

	// Model with an audit that always passes (vacuously true on 0 rows
	// too). The runner builds auditSQL != "", which must disable the
	// no-cascade fast path on the empty no_change run.
	p.AddModel("raw/audited.sql", `-- @kind: tracked
-- @fetch
-- @group_key: region
-- @audit: row_count(>=, 0)
SELECT region::VARCHAR AS region, amount::BIGINT AS amount FROM src('items')
`)

	p.AddModel("staging/derived.sql", `-- @kind: table
SELECT region, amount * 10 AS scaled FROM raw.audited
`)

	r1up := runModelWithLib(t, p, "raw/audited.sql")
	if r1up.RowsAffected != 1 {
		t.Fatalf("upstream run 1: expected 1 row, got %d (warnings: %v errors: %v)", r1up.RowsAffected, r1up.Warnings, r1up.Errors)
	}
	r1ds := runModelWithLib(t, p, "staging/derived.sql")
	if r1ds.RowsAffected != 1 {
		t.Fatalf("downstream run 1: expected 1 row, got %d", r1ds.RowsAffected)
	}

	// Phase 2: lib returns 0 rows. Audit must keep materialize on the
	// slow path (commit happens, snapshot bumps), so downstream sees the
	// dep as changed and re-runs.
	writeLib(t, p, "src", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["tracked"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    return {"rows": [], "next": None}
`)

	r2up := runModelWithLib(t, p, "raw/audited.sql")
	if r2up.RowsAffected != 0 {
		t.Fatalf("upstream run 2: expected 0 rows, got %d (warnings: %v errors: %v)", r2up.RowsAffected, r2up.Warnings, r2up.Errors)
	}
	// Target row preserved (audit doesn't change semantics — no_change
	// still suppresses the delete branch in materializeTracked).
	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM raw.audited")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != "1" {
		t.Fatalf("audited tracked + 0-row no_change: expected 1 row preserved, got %s", count)
	}

	// Downstream: dep changed (because audit forced a commit). RunType
	// must NOT be 'skip' — that would mean the fast path fired despite
	// the audit, leaving the audit unevaluated.
	r2ds := runModelWithLib(t, p, "staging/derived.sql")
	if r2ds.RunType == "skip" {
		t.Fatalf("downstream run 2: RunType=skip indicates upstream took the no-cascade fast path despite having an @audit")
	}
}

// TestLibCall_Tracked_EmptyResult_NoChange_NoSnapshotCascade pins that a
// tracked + lib + 0-row + no_change run does NOT create a new DuckLake
// snapshot when there is nothing to record (no schema evolution, no audits,
// no extra SQL). Without this fast path, every smart-skip would bump the
// model's snapshot id and trigger a downstream dep-change cascade — every
// dependent model would rebuild on every smart-skip, defeating the purpose
// of the whole no_change semantics.
//
// We verify by checking that a downstream `table` model marks the dep as
// unchanged after the smart-skip — if a snapshot got created, it would
// instead see "dep changed" and run.
//
// Regression for v0.24.0 efficiency: smart-skip must not cascade.
func TestLibCall_Tracked_EmptyResult_NoChange_NoSnapshotCascade(t *testing.T) {
	p := testutil.NewProject(t)

	writeLib(t, p, "src", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["tracked"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    return {"rows": [
        {"region": "US", "amount": 100},
    ], "next": None}
`)

	p.AddModel("raw/upstream.sql", `-- @kind: tracked
-- @fetch
-- @group_key: region
SELECT region::VARCHAR AS region, amount::BIGINT AS amount FROM src('items')
`)

	// Downstream depends on raw.upstream — should rebuild only when upstream
	// genuinely changes.
	p.AddModel("staging/downstream.sql", `-- @kind: table
SELECT region, amount * 2 AS doubled FROM raw.upstream
`)

	// Run 1: backfill upstream + downstream
	r1up := runModelWithLib(t, p, "raw/upstream.sql")
	if r1up.RowsAffected != 1 {
		t.Fatalf("upstream run 1: expected 1 row, got %d", r1up.RowsAffected)
	}
	r1ds := runModelWithLib(t, p, "staging/downstream.sql")
	if r1ds.RowsAffected != 1 {
		t.Fatalf("downstream run 1: expected 1 row, got %d", r1ds.RowsAffected)
	}

	// Phase 2: lib smart-skips upstream
	writeLib(t, p, "src", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["tracked"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    return {"rows": [], "next": None}
`)

	r2up := runModelWithLib(t, p, "raw/upstream.sql")
	if r2up.RowsAffected != 0 {
		t.Fatalf("upstream run 2: expected 0 rows, got %d", r2up.RowsAffected)
	}

	// Downstream run 2: dep upstream smart-skipped (no real change). The
	// runner should mark this as 'skip' — same dep snapshot id as before.
	r2ds := runModelWithLib(t, p, "staging/downstream.sql")
	if r2ds.RunType != "skip" {
		t.Errorf("downstream run 2: expected RunType=skip (smart-skip didn't change dep), got %q reason=%q",
			r2ds.RunType, r2ds.RunReason)
	}
}

// TestLibCall_Tracked_EmptyResult_NoChange_SchemaEvolves pins that even when
// a tracked-kind lib smart-skips with 0 rows + default no_change semantics,
// SQL-only changes (a new column added to the model SELECT) are still
// applied to the target. The runner must NOT take a global skip path that
// suppresses schema evolution — only the delete-missing-groups branch of
// tracked materialize should be suppressed.
//
// Regression for v0.24.0 reviewer concern #1.
func TestLibCall_Tracked_EmptyResult_NoChange_SchemaEvolves(t *testing.T) {
	p := testutil.NewProject(t)

	// Phase 1: lib returns rows with two value columns; tracked target gets
	// (region, id, val1) schema.
	writeLib(t, p, "src", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["tracked"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    return {"rows": [
        {"region": "US", "id": 1, "val1": "alpha"},
        {"region": "EU", "id": 2, "val1": "beta"},
    ], "next": None}
`)

	p.AddModel("raw/data.sql", `-- @kind: tracked
-- @fetch
-- @group_key: region
SELECT region::VARCHAR AS region, id::BIGINT AS id, val1::VARCHAR AS val1 FROM src('items')
`)

	r1 := runModelWithLib(t, p, "raw/data.sql")
	if r1.RowsAffected != 2 {
		t.Fatalf("run 1: expected 2 rows, got %d (warnings: %v)", r1.RowsAffected, r1.Warnings)
	}

	// Phase 2: lib smart-skips (returns 0 rows, no empty_result key →
	// default no_change). The model SELECT gains a new column `val2`.
	// Schema evolution must add `val2` to the target even though no
	// data is being written this run.
	writeLib(t, p, "src", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["tracked"],
    },
}

def fetch(resource, page, is_backfill=True, last_value=""):
    return {"rows": [], "next": None}
`)

	modelPath := filepath.Join(p.Dir, "models", "raw/data.sql")
	os.WriteFile(modelPath, []byte(`-- @kind: tracked
-- @fetch
-- @group_key: region
SELECT region::VARCHAR AS region, id::BIGINT AS id, val1::VARCHAR AS val1, val2::VARCHAR AS val2 FROM src('items')
`), 0644)

	r2 := runModelWithLib(t, p, "raw/data.sql")
	t.Logf("run 2: rows=%d type=%s warnings=%v", r2.RowsAffected, r2.RunType, r2.Warnings)

	// Existing target rows must still be there (smart-skip preserves data).
	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM raw.data")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != "2" {
		t.Fatalf("smart-skip + schema evolution: expected target preserved at 2 rows, got %s", count)
	}

	// Schema evolution must have added the new column even with 0-row run.
	cols, err := p.Sess.QueryValue(
		"SELECT string_agg(column_name, ',' ORDER BY ordinal_position) " +
			"FROM information_schema.columns WHERE table_schema='raw' AND table_name='data'")
	if err != nil {
		t.Fatalf("schema query: %v", err)
	}
	if !strings.Contains(cols, "val2") {
		t.Fatalf("smart-skip suppressed schema evolution: expected `val2` column in target, got %q", cols)
	}
}

// ===========================================================================
// v0.25.0: strict lib schema mode
// ===========================================================================

// TestStrictLibSchema_RejectsSelectStar pins that `SELECT *` against a
// dynamic-column lib is rejected. SQL is the schema authority for
// lib-backed models — the runtime no longer guesses output columns.
func TestStrictLibSchema_RejectsSelectStar(t *testing.T) {
	p := testutil.NewProject(t)

	writeLib(t, p, "src", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["table"],
    },
}

def fetch(resource, page):
    return {"rows": [{"id": 1, "name": "Alice"}], "next": None}
`)

	p.AddModel("raw/data.sql", `-- @kind: table
-- @fetch
SELECT * FROM src('items')
`)

	_, err := runModelWithLibErr(t, p, "raw/data.sql")
	if err == nil {
		t.Fatal("expected validation error for SELECT * against dynamic lib")
	}
	if !strings.Contains(err.Error(), "SELECT *") {
		t.Fatalf("error should mention SELECT *: %v", err)
	}
}

// TestStrictLibSchema_RejectsBareProjection pins that bare COLUMN_REF
// projections from a dynamic-column lib are rejected. The user must cast
// each output column.
func TestStrictLibSchema_RejectsBareProjection(t *testing.T) {
	p := testutil.NewProject(t)

	writeLib(t, p, "src", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["table"],
    },
}

def fetch(resource, page):
    return {"rows": [{"id": 1, "name": "Alice"}], "next": None}
`)

	p.AddModel("raw/data.sql", `-- @kind: table
-- @fetch
SELECT id, name FROM src('items')
`)

	_, err := runModelWithLibErr(t, p, "raw/data.sql")
	if err == nil {
		t.Fatal("expected validation error for bare projection")
	}
	if !strings.Contains(err.Error(), "not cast") {
		t.Fatalf("error should mention missing cast: %v", err)
	}
}

// TestStrictLibSchema_AcceptsTypedProjections pins the success path: every
// projected column from a dynamic-column lib has an explicit cast.
func TestStrictLibSchema_AcceptsTypedProjections(t *testing.T) {
	p := testutil.NewProject(t)

	writeLib(t, p, "src", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["table"],
    },
}

def fetch(resource, page):
    return {"rows": [{"id": 1, "name": "Alice", "amount": 9.5}], "next": None}
`)

	p.AddModel("raw/data.sql", `-- @kind: table
-- @fetch
SELECT id::BIGINT AS id, name::VARCHAR AS name, amount::DOUBLE AS amount FROM src('items')
`)

	r := runModelWithLib(t, p, "raw/data.sql")
	if r.RowsAffected != 1 {
		t.Fatalf("expected 1 row, got %d (warnings: %v errors: %v)", r.RowsAffected, r.Warnings, r.Errors)
	}
}

// TestStrictLibSchema_RejectsComputedExprWithoutCast pins that a computed
// expression that references a dynamic-column lib must be wrapped in a cast.
// `a + b AS total` is rejected; `(a + b)::BIGINT AS total` is accepted.
func TestStrictLibSchema_RejectsComputedExprWithoutCast(t *testing.T) {
	p := testutil.NewProject(t)

	writeLib(t, p, "src", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["table"],
    },
}

def fetch(resource, page):
    return {"rows": [{"id": 1, "amount": 100}], "next": None}
`)

	p.AddModel("raw/data.sql", `-- @kind: table
-- @fetch
SELECT id::BIGINT AS id, amount * 2 AS doubled FROM src('items')
`)

	_, err := runModelWithLibErr(t, p, "raw/data.sql")
	if err == nil {
		t.Fatal("expected validation error for computed expression without outer cast")
	}
	if !strings.Contains(err.Error(), "not cast") {
		t.Fatalf("error should mention missing cast: %v", err)
	}
}

// TestStrictLibSchema_RejectsCastWithoutAlias pins that the cast must be
// followed by an explicit `AS name`. Implicit names from underlying
// COLUMN_REFs do not satisfy the contract — the SELECT must declare every
// output column name.
func TestStrictLibSchema_RejectsCastWithoutAlias(t *testing.T) {
	p := testutil.NewProject(t)

	writeLib(t, p, "src", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["table"],
    },
}

def fetch(resource, page):
    return {"rows": [{"id": 1, "name": "Alice"}], "next": None}
`)

	p.AddModel("raw/data.sql", `-- @kind: table
-- @fetch
SELECT id::BIGINT, name::VARCHAR FROM src('items')
`)

	_, err := runModelWithLibErr(t, p, "raw/data.sql")
	if err == nil {
		t.Fatal("expected validation error for cast without explicit alias")
	}
	if !strings.Contains(err.Error(), "no explicit alias") {
		t.Fatalf("error should mention missing alias: %v", err)
	}
}

// TestStrictLibSchema_RejectsDuplicateAliases pins that two projections
// cannot share the same output name. The model's output schema must be
// unambiguous.
func TestStrictLibSchema_RejectsDuplicateAliases(t *testing.T) {
	p := testutil.NewProject(t)

	writeLib(t, p, "src", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["table"],
    },
}

def fetch(resource, page):
    return {"rows": [{"id": 1, "name": "Alice"}], "next": None}
`)

	p.AddModel("raw/dup.sql", `-- @kind: table
-- @fetch
SELECT id::BIGINT AS x, name::VARCHAR AS x FROM src('items')
`)

	_, err := runModelWithLibErr(t, p, "raw/dup.sql")
	if err == nil {
		t.Fatal("expected validation error for duplicate alias")
	}
	if !strings.Contains(err.Error(), "duplicate output column name") {
		t.Fatalf("error should mention duplicate name: %v", err)
	}
}

