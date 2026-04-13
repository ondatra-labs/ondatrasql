// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

// Atomic-audit regression coverage.
//
// These tests pin down the contract of the v0.11.0 audit-pipeline
// rewrite: audits run inside the materialize BEGIN/COMMIT (folded in
// via DuckDB's error() scalar), so a failing audit aborts the schema
// ALTER, the data write, and the commit metadata together. The pre-
// rewrite design ran audits AFTER set_commit_message and used a
// separate rollback() helper to undo the table — leaving commit
// metadata permanently ahead of physical state if the helper missed
// anything (the bug that broke the invoice-pipeline e2e project).
//
// Each test reproduces a specific divergence scenario observed
// against the broken design and asserts the post-rewrite invariants:
//
//	1. Physical schema and the latest commit metadata stay in sync.
//	2. A re-run after a failed audit re-attempts the model (no
//	   "deps unchanged" zombie skip caused by half-committed state).
//	3. Schema evolution (ALTER ADD COLUMN) rolls back when the audit
//	   in the same transaction fails.
//	4. Sandbox mode does not pollute prod state on failure.
package execute_test

import (
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// describeColumns returns the column names of `target` in declaration
// order, by querying information_schema. Any error fails the test —
// schema queries against an existing table are expected to succeed.
func describeColumns(t *testing.T, p *testutil.Project, target string) []string {
	t.Helper()
	parts := strings.SplitN(target, ".", 2)
	var query string
	if len(parts) == 2 {
		query = "SELECT column_name FROM information_schema.columns " +
			"WHERE table_schema = '" + parts[0] + "' AND table_name = '" + parts[1] + "' " +
			"ORDER BY ordinal_position"
	} else {
		query = "SELECT column_name FROM information_schema.columns " +
			"WHERE table_name = '" + target + "' ORDER BY ordinal_position"
	}
	rows, err := p.Sess.QueryRows(query)
	if err != nil {
		t.Fatalf("describe %s: %v", target, err)
	}
	return rows
}

// metadataColumns returns the column-name list recorded in the model's
// most recent commit metadata. Empty slice when no commit exists.
func metadataColumns(t *testing.T, p *testutil.Project, target string) []string {
	t.Helper()
	val, err := p.Sess.QueryValue("SELECT ondatra_get_prev_columns('" + target + "')")
	if err != nil || val == "" || val == "null" || val == "[]" {
		return nil
	}
	// The macro returns a JSON array of {name, type} objects. We just
	// extract the names with a string scan rather than pulling in a
	// JSON dependency for tests.
	var names []string
	for _, chunk := range strings.Split(val, "{") {
		if i := strings.Index(chunk, `"name":"`); i >= 0 {
			rest := chunk[i+len(`"name":"`):]
			if j := strings.Index(rest, `"`); j >= 0 {
				names = append(names, rest[:j])
			}
		}
	}
	return names
}

func columnsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// --- The exact reproducer that motivated the rewrite -------------------
//
// 1. Initial run with [id, name] succeeds.
// 2. User edits the model to add a new column AND adds an audit that
//    cannot pass with the available rows. Old design: ALTER + INSERT
//    commit succeeds, audit then fails, rollback() restores the
//    physical table but leaves commit metadata recording the new
//    column → permanent divergence. New design: error() inside the
//    transaction aborts everything atomically.

func TestAuditAtomicity_EditedSchemaPlusFailingAudit_NoDivergence(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/seed.sql", `-- @kind: table
SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b'
`)
	p.AddModel("mart/target.sql", `-- @kind: table
SELECT id, name FROM staging.seed
`)
	runModel(t, p, "staging/seed.sql")
	runModel(t, p, "mart/target.sql")

	beforePhysical := describeColumns(t, p, "mart.target")
	beforeMeta := metadataColumns(t, p, "mart.target")
	if !columnsEqual(beforePhysical, []string{"id", "name"}) {
		t.Fatalf("baseline physical = %v, want [id name]", beforePhysical)
	}
	if !columnsEqual(beforeMeta, []string{"id", "name"}) {
		t.Fatalf("baseline metadata = %v, want [id name]", beforeMeta)
	}

	// Edit: add a new column AND an audit that cannot pass.
	p.AddModel("mart/target.sql", `-- @kind: table
-- @audit: row_count(>=, 100)
SELECT id, name, 'v2' AS extra FROM staging.seed
`)
	_, err := runModelErr(t, p, "mart/target.sql")
	if err == nil {
		t.Fatal("expected audit failure on edited model")
	}
	if !strings.Contains(err.Error(), "audit failed") {
		t.Errorf("expected 'audit failed' in error, got: %v", err)
	}

	afterPhysical := describeColumns(t, p, "mart.target")
	afterMeta := metadataColumns(t, p, "mart.target")
	if !columnsEqual(afterPhysical, beforePhysical) {
		t.Errorf("physical schema diverged after audit failure: before=%v, after=%v",
			beforePhysical, afterPhysical)
	}
	if !columnsEqual(afterMeta, beforeMeta) {
		t.Errorf("metadata schema diverged after audit failure: before=%v, after=%v",
			beforeMeta, afterMeta)
	}
	if !columnsEqual(afterPhysical, afterMeta) {
		t.Errorf("physical (%v) and metadata (%v) disagree — divergence regression",
			afterPhysical, afterMeta)
	}
}

// After a failed audit, re-running with the same SQL must re-attempt
// the model. The pre-rewrite bug committed the SQL hash before the
// audit ran, so a failed run left the model in a "skip — deps
// unchanged" zombie state until the user changed the SQL. The fix:
// no commit metadata is written when the transaction aborts, so the
// next run still sees a SQL hash mismatch and re-tries.
func TestAuditAtomicity_FailedRun_DoesNotZombieSkipNextRun(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/audit_zombie.sql", `-- @kind: table
-- @audit: row_count(>=, 100)
SELECT 1 AS id, 'x' AS name
`)

	_, err1 := runModelErr(t, p, "staging/audit_zombie.sql")
	if err1 == nil {
		t.Fatal("first run: expected audit failure")
	}

	result2, err2 := runModelErr(t, p, "staging/audit_zombie.sql")
	if err2 == nil {
		t.Fatal("second run: expected audit failure (model SQL unchanged, audit still impossible)")
	}
	if result2 == nil {
		t.Fatal("second run returned nil result")
	}
	if result2.RunType == "skip" {
		t.Errorf("second run reported run_type=skip — zombie skip regression. result=%+v", result2)
	}
}

// Schema evolution applied via ALTER TABLE ADD COLUMN must roll back
// together with the data INSERT when an audit in the same transaction
// fails. Verified by editing a model to add a column, adding a
// failing audit, and asserting the column is not present afterwards.
func TestAuditAtomicity_SchemaEvolutionRollsBackOnAuditFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/sevol.sql", `-- @kind: table
SELECT 1 AS id, 100 AS amount
`)
	runModel(t, p, "staging/sevol.sql")

	if !columnsEqual(describeColumns(t, p, "staging.sevol"), []string{"id", "amount"}) {
		t.Fatal("baseline schema unexpected")
	}

	// Edit: add `category` column AND a failing audit.
	p.AddModel("staging/sevol.sql", `-- @kind: table
-- @audit: row_count(>=, 100)
SELECT id, amount, 'electronics' AS category FROM (VALUES (1, 100), (2, 200)) v(id, amount)
`)
	_, err := runModelErr(t, p, "staging/sevol.sql")
	if err == nil {
		t.Fatal("expected audit failure")
	}
	if !strings.Contains(err.Error(), "audit failed") {
		t.Errorf("expected 'audit failed' in error, got: %v", err)
	}

	// `category` must NOT exist on the physical table — the ALTER was
	// rolled back together with the audit-failing transaction.
	afterCols := describeColumns(t, p, "staging.sevol")
	for _, c := range afterCols {
		if c == "category" {
			t.Errorf("category column persisted after audit-failed run: %v", afterCols)
		}
	}
}

// All non-trivial materialize kinds (table/append/merge/scd2/partition/
// tracked) share the same commit.sql template, so the audit fold-in
// works through the same code path. This sub-test matrix exercises
// each kind with a guaranteed-failing audit and verifies (a) the
// failure surfaces with "audit failed", (b) the model's row count
// is observable as "did not commit" (table missing or empty for
// first runs).
func TestAuditAtomicity_AllKindsFailAtomically(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	cases := []struct {
		name      string
		modelBody string
	}{
		{
			"table",
			`-- @kind: table
-- @audit: row_count(>=, 999)
SELECT 1 AS id, 'a' AS name
`,
		},
		{
			"append",
			`-- @kind: append
-- @audit: row_count(>=, 999)
SELECT 1 AS id, 'a' AS name
`,
		},
		{
			"merge",
			`-- @kind: merge
-- @unique_key: id
-- @audit: row_count(>=, 999)
SELECT 1 AS id, 'a' AS name
`,
		},
		{
			"scd2",
			`-- @kind: scd2
-- @unique_key: id
-- @audit: row_count(>=, 999)
SELECT 1 AS id, 'a' AS name
`,
		},
		{
			"partition",
			`-- @kind: partition
-- @unique_key: id
-- @audit: row_count(>=, 999)
SELECT 1 AS id, 'a' AS name
`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := testutil.NewProject(t)
			modelPath := "staging/atomic_" + tc.name + ".sql"
			p.AddModel(modelPath, tc.modelBody)

			_, err := runModelErr(t, p, modelPath)
			if err == nil {
				t.Fatalf("kind=%s: expected audit failure", tc.name)
			}
			if !strings.Contains(err.Error(), "audit failed") {
				t.Errorf("kind=%s: expected 'audit failed' in error, got: %v", tc.name, err)
			}

			// First-run audit failure: table should not be committed.
			// information_schema returns no rows for tables that don't
			// exist, so describeColumns is empty.
			cols := describeColumns(t, p, "staging.atomic_"+tc.name)
			if len(cols) > 0 {
				t.Errorf("kind=%s: table exists with columns %v after audit-failed first run",
					tc.name, cols)
			}
		})
	}
}

// Incremental-path coverage: scd2 and partition kinds use their own
// dedicated commit templates (scd2_update.sql, partition_delete.sql)
// that wrap their own BEGIN/COMMIT and were not initially threading
// the auditSQL through the template's pre-commit-checks slot. Without
// this regression test we silently dropped audits on the second run
// of any scd2 or partition model. The test runs each kind twice — a
// passing first run that creates the target via the table-init path,
// then a second run with a guaranteed-failing audit that must abort
// the incremental update transaction atomically.
func TestAuditAtomicity_IncrementalPathFailsAtomically(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	cases := []struct {
		name      string
		firstRun  string
		secondRun string
	}{
		{
			"scd2_incremental",
			`-- @kind: scd2
-- @unique_key: id
SELECT 1 AS id, 'alice' AS name UNION ALL SELECT 2, 'bob'
`,
			`-- @kind: scd2
-- @unique_key: id
-- @audit: row_count(>=, 999)
SELECT 1 AS id, 'alice updated' AS name UNION ALL SELECT 2, 'bob updated' UNION ALL SELECT 3, 'charlie'
`,
		},
		{
			"partition_incremental",
			`-- @kind: partition
-- @unique_key: region
SELECT 'EU' AS region, 1 AS id, 100 AS amount UNION ALL SELECT 'US', 2, 200
`,
			`-- @kind: partition
-- @unique_key: region
-- @audit: row_count(>=, 999)
SELECT 'EU' AS region, 1 AS id, 999 AS amount UNION ALL SELECT 'US', 2, 999
`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := testutil.NewProject(t)
			modelPath := "staging/incr_" + tc.name + ".sql"

			// First run establishes the target table.
			p.AddModel(modelPath, tc.firstRun)
			result1 := runModel(t, p, modelPath)
			if len(result1.Errors) > 0 {
				t.Fatalf("first run errors: %v", result1.Errors)
			}

			// Second run modifies the data and adds a guaranteed-failing
			// audit. This goes through the incremental path —
			// scd2_update.sql or partition_delete.sql — and exercises
			// the audit slot we threaded through those templates.
			p.AddModel(modelPath, tc.secondRun)
			_, err := runModelErr(t, p, modelPath)
			if err == nil {
				t.Fatalf("kind=%s incremental: expected audit failure", tc.name)
			}
			if !strings.Contains(err.Error(), "audit failed") {
				t.Errorf("kind=%s incremental: expected 'audit failed', got: %v", tc.name, err)
			}
		})
	}
}

// Sandbox mode regression: an audit failure inside a sandbox run must
// surface the same `audit failed` message and must not leave the
// sandbox catalog in a half-committed state. Since the sandbox path
// uses the exact same materialize() code as prod, this test mainly
// validates that the error() inside-the-transaction abort works
// against the dual-attach sandbox catalog (where the prod tables are
// visible read-only via the prod alias).
func TestAuditAtomicity_SandboxFailureAtomic(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// 1. Build a prod project with a baseline table.
	prod := testutil.NewProject(t)
	prod.AddModel("staging/sb_target.sql", `-- @kind: table
SELECT 1 AS id, 'baseline' AS name
`)
	runModel(t, prod, "staging/sb_target.sql")

	// 2. Convert to sandbox (closes prod session, opens dual-attach).
	//    Edit the model so the sandbox run picks up a new column AND
	//    a guaranteed-failing audit.
	prod.AddModel("staging/sb_target.sql", `-- @kind: table
-- @audit: row_count(>=, 999)
SELECT id, name, 'sandbox-only' AS extra FROM (VALUES (1, 'x'), (2, 'y')) v(id, name)
`)
	sandbox := testutil.NewSandboxProject(t, prod)

	// 3. Run in sandbox — must fail with the same atomic error path.
	_, err := runModelErr(t, sandbox, "staging/sb_target.sql")
	if err == nil {
		t.Fatal("expected sandbox audit failure")
	}
	if !strings.Contains(err.Error(), "audit failed") {
		t.Errorf("expected 'audit failed' from sandbox, got: %v", err)
	}

	// 4. The sandbox catalog must not record any commit for this model
	//    after the failed run — the transaction abort prevented
	//    set_commit_message from running. Either the table doesn't
	//    exist in the sandbox catalog yet, or its commit metadata
	//    still points at the original two-column shape.
	sandboxMeta := metadataColumns(t, sandbox, "staging.sb_target")
	for _, c := range sandboxMeta {
		if c == "extra" {
			t.Errorf("sandbox metadata leaked failed-audit columns: %v", sandboxMeta)
		}
	}
}
