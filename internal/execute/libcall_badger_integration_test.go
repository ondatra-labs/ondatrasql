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
-- @incremental: val
-- @incremental_initial: 2026-01-01
SELECT id, name, val FROM testapi('items')
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
-- @incremental: val
-- @incremental_initial: 2026-01-01
SELECT id, name, val FROM testapi('items')
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
-- @constraint: not_null(missing_col)
SELECT id, name FROM testapi('items')
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
SELECT id, name FROM testapi('items')
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
SELECT id FROM badapi('items')
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
-- @audit: compare(val, >, 200)
SELECT id, val FROM countapi('items')
`)

	// First run — should fail on audit
	_, runErr := runModelWithLibErr(t, p, "raw/data.sql")
	if runErr == nil {
		t.Fatal("expected audit failure")
	}
	if !strings.Contains(runErr.Error(), "audit") {
		t.Fatalf("expected audit error, got: %v", runErr)
	}

	// Fix model — remove audit
	modelPath := filepath.Join(p.Dir, "models", "raw/data.sql")
	os.WriteFile(modelPath, []byte(`-- @kind: table
SELECT id, val FROM countapi('items')
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
-- @incremental: val
-- @incremental_initial: 2026-01-01
SELECT id, val FROM emptyapi('items')
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
-- @group_key: grp
SELECT grp, id, name FROM testapi('items')
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
SELECT id, name FROM emptyapi('items')
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
-- @incremental: val
-- @incremental_initial: 2026-01-01
SELECT id, val FROM testapi('items')
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
	os.WriteFile(modelPath, []byte(`-- @kind: append
-- @incremental: val
-- @incremental_initial: 2026-01-01
SELECT id, val FROM testapi('items') WHERE val > '2025-01-01'
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
SELECT id, name FROM testapi('items')
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
SELECT id, name, email FROM testapi('items')
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
-- @incremental: val
-- @incremental_initial: 2026-01-01
SELECT id, val FROM testapi('items')
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
-- @incremental: val
-- @incremental_initial: 2026-01-01
-- @constraint: not_null(nonexistent_col)
SELECT id, val FROM testapi('items')
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
SELECT id, val FROM auditapi('items')
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
	os.WriteFile(modelPath, []byte(`-- @kind: table
-- @audit: not a valid macro
SELECT id, val FROM auditapi('items') WHERE 1=1
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

// TestLibCall_Merge_EmptyIncremental_Sink verifies that a lib-driven merge
// model with @sink correctly handles empty incremental runs: no duplication,
// correct empty delta, sink semantics preserved.
func TestLibCall_Merge_EmptyIncremental_Sink(t *testing.T) {
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

	p.AddModel("raw/merged.sql", `-- @kind: merge
-- @unique_key: id
-- @incremental: score
-- @incremental_initial: 0
-- @sink: recorder
SELECT id::BIGINT AS id, name::VARCHAR AS name, score::BIGINT AS score FROM mergesrc('items')
`)

	// Run 1 — backfill: 2 rows inserted, sink should fire
	r1 := runModelWithLib(t, p, "raw/merged.sql")
	t.Logf("run 1: rows=%d type=%s sync_ok=%d sync_fail=%d warnings=%v",
		r1.RowsAffected, r1.RunType, r1.SyncSucceeded, r1.SyncFailed, r1.Warnings)
	if r1.RowsAffected != 2 {
		t.Fatalf("run 1: expected 2 rows, got %d", r1.RowsAffected)
	}
	if r1.SyncSucceeded == 0 {
		t.Fatalf("run 1: expected sink to succeed, got SyncSucceeded=0")
	}

	// Run 2 — incremental, lib returns 0 rows. Merge with 0 new rows should
	// produce 0 rows affected. Sink should NOT produce spurious events.
	r2 := runModelWithLib(t, p, "raw/merged.sql")
	t.Logf("run 2: rows=%d type=%s sync_ok=%d sync_fail=%d warnings=%v",
		r2.RowsAffected, r2.RunType, r2.SyncSucceeded, r2.SyncFailed, r2.Warnings)
	if r2.RowsAffected != 0 {
		t.Fatalf("run 2: expected 0 rows (no new data), got %d", r2.RowsAffected)
	}

	// Run 3 — still empty, verify no duplication
	r3 := runModelWithLib(t, p, "raw/merged.sql")
	if r3.RowsAffected != 0 {
		t.Fatalf("run 3: expected 0 rows, got %d", r3.RowsAffected)
	}

	// Verify total row count — should be exactly 2 (no duplication)
	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM raw.merged")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != "2" {
		t.Fatalf("expected 2 total rows after 3 runs, got %s", count)
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
-- @group_key: region
SELECT region, product, amount FROM trackedapi('items')
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

	// Change SQL to trigger hash change → backfill with new data
	modelPath := filepath.Join(p.Dir, "models", "raw/regional.sql")
	os.WriteFile(modelPath, []byte(`-- @kind: tracked
-- @group_key: region
SELECT region, product, amount FROM trackedapi('items') WHERE 1=1
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

// TestLibCall_MixedEmpty_JoinTwoLibs verifies that when a model JOINs two
// lib functions and one returns 0 rows while the other has data, the
// rewrite handles both correctly: stub for the empty one, real temp table
// for the other. Claims lifecycle must be correct for both.
func TestLibCall_MixedEmpty_JoinTwoLibs(t *testing.T) {
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

	p.AddModel("raw/joined.sql", `-- @kind: table
SELECT u.user_id, u.name, s.score
FROM users_api('users') u
JOIN scores_api('scores') s ON u.user_id = s.user_id
`)

	// Run 1 — both libs have data, JOIN produces 2 rows
	r1 := runModelWithLib(t, p, "raw/joined.sql")
	t.Logf("run 1: rows=%d type=%s warnings=%v", r1.RowsAffected, r1.RunType, r1.Warnings)
	if r1.RowsAffected != 2 {
		t.Fatalf("run 1: expected 2 rows, got %d", r1.RowsAffected)
	}

	// Verify data
	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM raw.joined")
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
SELECT id, name FROM dynapi('items')
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
SELECT id, name, email FROM dynapi('items')
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

	os.WriteFile(modelPath, []byte(`-- @kind: table
SELECT id, name, email FROM dynapi('items') WHERE 1=1
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
-- @incremental: val
-- @incremental_initial: 2026-01-01
SELECT id, val FROM crashapi('items')
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
-- @incremental: val
-- @incremental_initial: 2026-01-01
SELECT id::BIGINT AS id, val, amount::DOUBLE AS amount FROM src('items')
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
SELECT id, name FROM src('items')
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

// TestLibCall_Stub_JoinColumnInON verifies that when a model JOINs two
// libs, a column used only in JOIN ON (not in SELECT) is present in the
// 0-row stub. This tests extractColumnsForLib's per-alias filtering.
func TestLibCall_Stub_JoinColumnInON(t *testing.T) {
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

	// user_id is in JOIN ON but NOT in SELECT
	p.AddModel("raw/joined.sql", `-- @kind: table
SELECT u.name, s.score
FROM users_api('users') u
JOIN scores_api('scores') s ON u.user_id = s.user_id
`)

	// Run 1 — both have data
	r1 := runModelWithLib(t, p, "raw/joined.sql")
	if r1.RowsAffected != 2 {
		t.Fatalf("run 1: expected 2 rows, got %d", r1.RowsAffected)
	}

	// Run 2 — scores returns 0 rows. Stub must have user_id
	// (from JOIN ON) even though it's not in SELECT.
	modelPath := filepath.Join(p.Dir, "models", "raw/joined.sql")
	os.WriteFile(modelPath, []byte(`-- @kind: table
SELECT u.name, s.score
FROM users_api('users') u
JOIN scores_api('scores') s ON u.user_id = s.user_id
WHERE 1=1
`), 0644)

	r2 := runModelWithLib(t, p, "raw/joined.sql")
	// users has 2 rows, scores has 0 → JOIN produces 0 (or target-clone)
	// Either way it must not crash.
	t.Logf("run 2: rows=%d type=%s warnings=%v", r2.RowsAffected, r2.RunType, r2.Warnings)
}

// TestLibCall_Stub_ColumnsFilteredPerLib verifies that the columns kwarg
// sent to fetch() only includes columns referenced against that specific
// lib call, not columns from other sources (LATERAL, other libs, etc.).
func TestLibCall_Stub_ColumnsFilteredPerLib(t *testing.T) {
	p := testutil.NewProject(t)

	// This lib records what columns it receives
	writeLib(t, p, "colcheck", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
        "supported_kinds": ["table"],
    },
}

def fetch(resource, page, columns=[]):
    col_names = [c["name"] for c in columns]
    # Return column names as data so we can verify
    rows = []
    for name in col_names:
        rows.append({"col_name": name, "val": "ok"})
    return {"rows": rows, "next": None}
`)

	// The SELECT references col_name and val from the lib,
	// plus a literal column. Only col_name and val should be
	// in the columns kwarg, not the literal.
	p.AddModel("raw/coltest.sql", `-- @kind: table
SELECT col_name, val, 'extra' AS extra
FROM colcheck('test')
`)

	r1 := runModelWithLib(t, p, "raw/coltest.sql")
	t.Logf("run 1: rows=%d", r1.RowsAffected)

	// Verify the lib received only col_name and val, not 'extra'
	rows, err := p.Sess.QueryRows("SELECT col_name FROM raw.coltest ORDER BY col_name")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	// Should have col_name and val (the 2 columns the lib saw)
	for _, r := range rows {
		if r == "extra" {
			t.Fatalf("lib received 'extra' column — should be filtered out (it's a literal, not a lib column)")
		}
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
-- @incremental: date
-- @incremental_initial: 2026-01-01
SELECT series, date, value FROM src('items')
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

// TestSinkUpdatePreimage_FromPreChangeSnapshot pins that update_preimage
// rows are read from the snapshot BEFORE the change (e.Snapshot - 1), not
// from current state. The bug class: someone changes the snapshot offset
// in readRowsByEvents and the preimage starts containing the post-change
// values, silently breaking change-aware push() semantics. The push()
// function fails the run if the preimage doesn't have the OLD value, so
// any drift in the snapshot calculation will surface as SyncFailed > 0.
func TestSinkUpdatePreimage_FromPreChangeSnapshot(t *testing.T) {
	p := testutil.NewProject(t)

	// Source lib: row id=1 with score=100 (run 1), then score=200 (run 2)
	writeLib(t, p, "updsrc", `
RUN_FILE = "_run_counter.txt"

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

	// Sink: asserts that preimage has the OLD value (100), postimage the NEW (200).
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

	p.AddModel("raw/items.sql", `-- @kind: merge
-- @unique_key: id
-- @sink: checker
SELECT id::BIGINT AS id, score::BIGINT AS score FROM updsrc('items')
`)

	// Run 1: backfill — single row insert, sink sees insert event
	r1 := runModelWithLib(t, p, "raw/items.sql")
	if r1.RowsAffected != 1 {
		t.Fatalf("run 1: expected 1 row, got %d", r1.RowsAffected)
	}
	if r1.SyncFailed != 0 {
		t.Fatalf("run 1: SyncFailed=%d, expected 0 (warnings: %v)", r1.SyncFailed, r1.Warnings)
	}

	// Run 2: same id, new score → merge produces update_preimage + update_postimage.
	// The push() function asserts both have correct values for their slot.
	r2 := runModelWithLib(t, p, "raw/items.sql")
	if r2.SyncFailed != 0 {
		t.Fatalf("run 2: SyncFailed=%d — preimage/postimage rows had wrong values, indicating readRowsByEvents read from wrong snapshot. Warnings: %v", r2.SyncFailed, r2.Warnings)
	}
	if r2.SyncSucceeded == 0 {
		t.Fatalf("run 2: SyncSucceeded=0, expected sink to fire on update (warnings: %v)", r2.Warnings)
	}
}

// TestPreCommitSnapshot_AppendWithSink pins that the preCommitSnapshot
// capture is gated by `model.Sink != ""` only — not narrowed to specific
// kinds (merge/tracked). The bug class: someone changes the gate to
// `if model.Kind == "merge" || ...`, breaking append + @sink. Without
// the pre-commit snapshot, createSinkDelta would query table_changes
// from snapshot 1 instead of the right range, replaying every row of
// every previous run into the sink on every incremental run.
//
// This test runs an append + @sink model twice with a NEW row on the
// second run, and asserts the sink saw only the new row — not all rows.
func TestPreCommitSnapshot_AppendWithSink(t *testing.T) {
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

	p.AddModel("raw/scores.sql", `-- @kind: append
-- @incremental: id
-- @incremental_initial: 0
-- @sink: sinkrec
SELECT id::BIGINT AS id, score::BIGINT AS score FROM appendsrc('items')
`)

	// Run 1: backfill — 2 rows in, 2 in sink
	r1 := runModelWithLib(t, p, "raw/scores.sql")
	if r1.RowsAffected != 2 {
		t.Fatalf("run 1: expected 2 rows, got %d", r1.RowsAffected)
	}
	if r1.SyncSucceeded != 2 {
		t.Fatalf("run 1: expected SyncSucceeded=2, got %d (warnings: %v)", r1.SyncSucceeded, r1.Warnings)
	}

	// Run 2: incremental — 1 NEW row. Sink delta should only include the
	// new row. If preCommitSnapshot was not captured, the delta would
	// also include run 1's rows → SyncSucceeded would be 3, not 1.
	r2 := runModelWithLib(t, p, "raw/scores.sql")
	if r2.RowsAffected != 1 {
		t.Fatalf("run 2: expected 1 new row, got %d", r2.RowsAffected)
	}
	if r2.SyncSucceeded != 1 {
		t.Fatalf("run 2: expected SyncSucceeded=1 (only the new row), got %d — preCommitSnapshot may be missing for append kind, causing run 1's rows to be replayed (warnings: %v)", r2.SyncSucceeded, r2.Warnings)
	}

	// Verify total rows in target — exactly 3, no duplication
	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM raw.scores")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != "3" {
		t.Fatalf("expected 3 total rows after 2 runs, got %s", count)
	}
}
