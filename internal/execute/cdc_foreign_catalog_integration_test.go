// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute_test

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// TestCDC_ForeignCatalogSourceIsSkipped pins that a source table living in
// another attached catalog is left out of the CDC gate.
//
// table_changes() is a DuckLake function, so it cannot run against a table in
// an ATTACHed Postgres (or any other foreign catalog). Before this, such a
// table still entered cdcTables: the gate qualified it as
// <lakeAlias>.<schema>.<table>, failed to resolve it, and fell back to a full
// query — after emitting two warnings per model on every run. The fallback was
// correct, so this is about reaching it quietly, and about not training the
// operator to ignore warnings.
func TestCDC_ForeignCatalogSourceIsSkipped(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// A second catalog attached under the alias `crm`, standing in for
	// `ATTACH 'postgresql://…' AS crm (TYPE postgres, READ_ONLY)`. The alias is
	// what matters: the model then reads `crm.decision`, which the CDC gate
	// splits as schema `crm` + table `decision` and qualifies against the lake.
	foreign := filepath.Join(p.Dir, "foreign.duckdb")
	if err := p.Sess.Exec("ATTACH '" + foreign + "' AS crm"); err != nil {
		t.Fatalf("attach foreign catalog: %v", err)
	}
	for _, stmt := range []string{
		"CREATE TABLE crm.decision (id BIGINT, note VARCHAR)",
		"INSERT INTO crm.decision VALUES (1, 'first')",
	} {
		if err := p.Sess.Exec(stmt); err != nil {
			t.Fatalf("seed foreign catalog (%s): %v", stmt, err)
		}
	}

	p.AddModel("raw/crm_decision.sql", `-- @kind: merge
-- @unique_key: id
SELECT id::BIGINT AS id, note::VARCHAR AS note FROM crm.decision
`)

	// First run establishes the target; CDC only engages from the second.
	if r := runModel(t, p, "raw/crm_decision.sql"); r.RunType != "backfill" {
		t.Fatalf("first run: want backfill, got %q (%s)", r.RunType, r.RunReason)
	}

	if err := p.Sess.Exec("INSERT INTO crm.decision VALUES (2, 'second')"); err != nil {
		t.Fatalf("add a row to the foreign source: %v", err)
	}

	r := runModel(t, p, "raw/crm_decision.sql")
	if r.RunType != "incremental" {
		t.Errorf("second run: want incremental, got %q (%s)", r.RunType, r.RunReason)
	}
	if r.RowsAffected != 2 {
		t.Errorf("full query should read both rows, got %d", r.RowsAffected)
	}

	sawSkip := false
	for _, step := range r.Trace {
		if strings.Contains(step.Name, "cdc.skip_foreign_catalog:crm.decision") {
			sawSkip = true
		}
	}
	if !sawSkip {
		t.Error("the skip must be traced — it replaces the warnings that used to signal the fallback")
	}

	for _, w := range r.Warnings {
		if strings.Contains(w, "search_path for table_changes gate") ||
			strings.Contains(w, "CDC query failed") ||
			strings.Contains(w, "table_changes gate failed") ||
			strings.Contains(w, "cdc catalog check") {
			t.Errorf("a foreign-catalog source must not reach the CDC gate; got warning: %s", w)
		}
	}
}

// TestCDC_LakeSourceStillGetsCDC is the negative control for the test above.
// Without it, a change making tableInLakeCatalog always answer false would
// leave that test green while silently disabling CDC everywhere.
func TestCDC_LakeSourceStillGetsCDC(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("raw/src.sql", `-- @kind: table
SELECT 1 AS id, 'a' AS note
`)
	runModel(t, p, "raw/src.sql")

	p.AddModel("staging/derived.sql", `-- @kind: merge
-- @unique_key: id
SELECT id::BIGINT AS id, note::VARCHAR AS note FROM raw.src
`)
	runModel(t, p, "staging/derived.sql")
	r := runModel(t, p, "staging/derived.sql")

	for _, step := range r.Trace {
		if strings.Contains(step.Name, "cdc.skip_foreign_catalog") {
			t.Errorf("a lake source must not be treated as foreign; trace: %s", step.Name)
		}
	}
}

// TestCDC_NameShapesAreClassified pins the shapes lineage can produce for
// TableRef.Table. An unqualified name is unknowable, so it must keep the prior
// behaviour rather than be dropped; a catalog-qualified lake name must be
// recognised as a lake table rather than parsed as schema `lake`.
func TestCDC_NameShapesAreClassified(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	t.Run("unqualified source keeps CDC", func(t *testing.T) {
		p := testutil.NewProject(t)
		if err := p.Sess.Exec("CREATE TABLE main.bare (id BIGINT, note VARCHAR)"); err != nil {
			t.Fatalf("seed bare table: %v", err)
		}
		if err := p.Sess.Exec("INSERT INTO main.bare VALUES (1, 'a')"); err != nil {
			t.Fatalf("seed row: %v", err)
		}
		p.AddModel("staging/bare_src.sql", `-- @kind: merge
-- @unique_key: id
SELECT id::BIGINT AS id, note::VARCHAR AS note FROM bare
`)
		runModel(t, p, "staging/bare_src.sql")
		r := runModel(t, p, "staging/bare_src.sql")
		for _, step := range r.Trace {
			if strings.Contains(step.Name, "cdc.skip_foreign_catalog") {
				t.Errorf("an unqualified name is not classifiable and must not be dropped; trace: %s", step.Name)
			}
		}
	})

	t.Run("catalog-qualified lake source gets CDC", func(t *testing.T) {
		p := testutil.NewProject(t)
		p.AddModel("raw/src.sql", `-- @kind: table
SELECT 1 AS id, 'a' AS note
`)
		runModel(t, p, "raw/src.sql")

		p.AddModel("staging/qualified.sql", `-- @kind: merge
-- @unique_key: id
SELECT id::BIGINT AS id, note::VARCHAR AS note FROM lake.raw.src
`)
		runModel(t, p, "staging/qualified.sql")

		// cdc.applied is gated on hasChanges, so give it a change to find.
		if err := p.Sess.Exec("INSERT INTO raw.src VALUES (2, 'b')"); err != nil {
			t.Fatalf("add a row to the lake source: %v", err)
		}
		r := runModel(t, p, "staging/qualified.sql")

		for _, step := range r.Trace {
			if strings.Contains(step.Name, "cdc.skip_foreign_catalog") {
				t.Errorf("lake.raw.src is a lake table; trace: %s", step.Name)
			}
		}

		// The cdc.applied trace is not evidence: applySmartCDC returns the SQL
		// unmodified when it matches nothing, and the trace fires either way.
		// Row count is the observable difference — CDC narrows the source to
		// the one changed row, a full query re-reads both.
		if r.RowsAffected != 1 {
			t.Errorf("CDC must actually rewrite the source: want 1 changed row, got %d", r.RowsAffected)
		}
	})
}

// TestCDC_QualifiedNameDoesNotCaptureForeignTwin pins that a CDC rewrite of a
// catalog-qualified lake source leaves a foreign source in the same query alone.
//
// The sharper collision this started from — a foreign catalog literally named
// `raw` next to the lake's own `raw` schema — cannot be built: DuckDB refuses
// the two-part reference itself with `Binder Error: Ambiguous reference to
// catalog or schema "raw" - use a fully qualified path`. So the unsafe case is
// not reachable through a two-part name, and the foreign catalog here is named
// distinctly. What remains worth pinning is the mixed query: one source the
// rewrite must touch, one it must not, both in the same statement.
func TestCDC_QualifiedNameDoesNotCaptureForeignTwin(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	// A foreign catalog holding its own `src`, read side by side with the
	// lake's `raw.src` in one model.
	foreign := filepath.Join(p.Dir, "twin.duckdb")
	if err := p.Sess.Exec("ATTACH '" + foreign + "' AS raw2"); err != nil {
		t.Fatalf("attach foreign catalog: %v", err)
	}
	for _, stmt := range []string{
		"CREATE TABLE raw2.src (id BIGINT, note VARCHAR)",
		"INSERT INTO raw2.src VALUES (99, 'foreign')",
	} {
		if err := p.Sess.Exec(stmt); err != nil {
			t.Fatalf("seed foreign catalog (%s): %v", stmt, err)
		}
	}

	p.AddModel("raw/src.sql", `-- @kind: table
SELECT 1 AS id, 'lake' AS note
`)
	runModel(t, p, "raw/src.sql")

	p.AddModel("staging/both.sql", `-- @kind: merge
-- @unique_key: id
SELECT l.id::BIGINT AS id, f.note::VARCHAR AS note
FROM lake.raw.src l
JOIN raw2.src f ON true
`)
	runModel(t, p, "staging/both.sql")
	if err := p.Sess.Exec("INSERT INTO raw.src VALUES (2, 'lake2')"); err != nil {
		t.Fatalf("change the lake source: %v", err)
	}

	r := runModel(t, p, "staging/both.sql")

	// The foreign table has no snapshot history. If the lake entry captured it,
	// the rewrite would produce a time-travel subquery against it and the model
	// would fail or return wrong rows.
	if len(r.Errors) > 0 {
		t.Errorf("the foreign twin must not be rewritten: %v", r.Errors)
	}
	for _, w := range r.Warnings {
		if strings.Contains(w, "CDC failed") || strings.Contains(w, "CDC query failed") {
			t.Errorf("the foreign twin must not be rewritten; got: %s", w)
		}
	}
}

// TestSandbox_ForeignCatalogIsNotQualifiedToProd pins the sandbox half of the
// fix, which the CDC tests above do not reach.
//
// In a sandbox run every table that is absent from the fork is stamped with the
// prod catalog, because DuckDB resolves schema-qualified names in the current
// catalog only. A table in an ATTACHed foreign catalog is absent from the fork
// too, but it is not in prod either — qualifying it produces <prod>.<name>,
// which does not resolve, and the model fails.
//
// The reference is written two-part (`crm.decision`), which is what
// `ATTACH '…' AS crm` plus a model reading `crm.decision` actually produces:
// DuckDB parses the leading segment as a schema, so nothing downstream can see
// a catalog there without asking which catalogs are ATTACHed. A three-part name
// is already protected by qualifyTablesInAST leaving an explicit catalog alone.
//
// The kind here is `table` deliberately: cdcHandled only ever shielded
// append/merge on an incremental run, so this path was never covered.
func TestSandbox_ForeignCatalogIsNotQualifiedToProd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	prod := testutil.NewProject(t)

	// A lake source, materialised in prod so the sandbox run has to reach
	// across to it — this is the reference that legitimately needs qualifying.
	prod.AddModel("raw/src.sql", `-- @kind: table
SELECT 1 AS id, 'lake' AS note
`)
	runModel(t, prod, "raw/src.sql")

	// The foreign catalog file is built here, while prod's session is still
	// open; it is ATTACHed on the sandbox session below.
	foreign := filepath.Join(prod.Dir, "sandbox_foreign.duckdb")
	if err := prod.Sess.Exec("ATTACH '" + foreign + "' AS crm"); err != nil {
		t.Fatalf("attach foreign catalog: %v", err)
	}
	for _, stmt := range []string{
		"CREATE TABLE crm.decision (id BIGINT, verdict VARCHAR)",
		"INSERT INTO crm.decision VALUES (1, 'approved')",
	} {
		if err := prod.Sess.Exec(stmt); err != nil {
			t.Fatalf("seed foreign catalog (%s): %v", stmt, err)
		}
	}
	if err := prod.Sess.Exec("DETACH crm"); err != nil {
		t.Fatalf("detach before sandbox conversion: %v", err)
	}

	prod.AddModel("staging/mixed.sql", `-- @kind: table
SELECT l.id::BIGINT AS id, f.verdict::VARCHAR AS verdict
FROM raw.src l
JOIN crm.decision f ON f.id = l.id
`)

	sandbox := testutil.NewSandboxProject(t, prod)
	if err := sandbox.Sess.Exec("ATTACH '" + foreign + "' AS crm (READ_ONLY)"); err != nil {
		t.Fatalf("attach foreign catalog in sandbox: %v", err)
	}

	r, err := runModelErr(t, sandbox, "staging/mixed.sql")
	if err != nil {
		t.Fatalf("foreign source must survive sandbox qualification: %v", err)
	}
	if len(r.Errors) > 0 {
		t.Fatalf("foreign source must survive sandbox qualification: %v", r.Errors)
	}

	// The lake reference still had to be qualified across to prod, so a row
	// only appears if both halves resolved.
	rows, qErr := sandbox.Sess.QueryRows("SELECT verdict FROM staging.mixed ORDER BY id")
	if qErr != nil {
		t.Fatalf("read sandbox result: %v", qErr)
	}
	if len(rows) != 1 || rows[0] != "approved" {
		t.Errorf("expected one row joining lake and foreign source, got %v", rows)
	}
}

// TestCDC_ForeignSourceDisablesCDCForWholeModel pins that skipping a foreign
// source is not a per-table decision.
//
// A model joining a lake source and a foreign one has one source whose changes
// CDC cannot observe. If CDC still ran on the lake source alone, the gate would
// answer "nothing changed" whenever only the foreign source moved, the model
// would read an empty delta, and the change would be lost with no error. The
// failing gate used to force a full query by accident; the skip has to keep
// that outcome deliberately.
func TestCDC_ForeignSourceDisablesCDCForWholeModel(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	foreign := filepath.Join(p.Dir, "mixed.duckdb")
	if err := p.Sess.Exec("ATTACH '" + foreign + "' AS crm"); err != nil {
		t.Fatalf("attach foreign catalog: %v", err)
	}
	for _, stmt := range []string{
		"CREATE TABLE crm.decision (id BIGINT, verdict VARCHAR)",
		"INSERT INTO crm.decision VALUES (1, 'pending')",
	} {
		if err := p.Sess.Exec(stmt); err != nil {
			t.Fatalf("seed foreign catalog (%s): %v", stmt, err)
		}
	}

	p.AddModel("raw/src.sql", `-- @kind: table
SELECT 1 AS id, 'lake' AS note
`)
	runModel(t, p, "raw/src.sql")

	p.AddModel("staging/mixed_cdc.sql", `-- @kind: merge
-- @unique_key: id
SELECT l.id::BIGINT AS id, f.verdict::VARCHAR AS verdict
FROM raw.src l
JOIN crm.decision f ON f.id = l.id
`)
	runModel(t, p, "staging/mixed_cdc.sql")

	// Only the foreign source changes. The lake source is untouched, so a CDC
	// gate scoped to it alone reports no work to do.
	if err := p.Sess.Exec("UPDATE crm.decision SET verdict = 'approved' WHERE id = 1"); err != nil {
		t.Fatalf("change the foreign source: %v", err)
	}

	r := runModel(t, p, "staging/mixed_cdc.sql")
	if len(r.Errors) > 0 {
		t.Fatalf("rerun failed: %v", r.Errors)
	}

	rows, qErr := p.Sess.QueryRows("SELECT verdict FROM staging.mixed_cdc ORDER BY id")
	if qErr != nil {
		t.Fatalf("read result: %v", qErr)
	}
	if len(rows) != 1 || rows[0] != "approved" {
		t.Errorf("a change in the foreign source must reach the model, got %v", rows)
	}
}

// TestCDC_AppendWithForeignSourceDoesNotDuplicate pins the materialization half
// of disabling CDC.
//
// Clearing cdcTables makes the model run its full source query. On an `append`
// kind that must materialize as a rebuild, not an append, or every run inserts
// the whole source again and the target grows without bound — 2 rows, then 4,
// then 6. The other two CDC-abandonment paths in the runner already escalate to
// backfill for exactly this reason.
func TestCDC_AppendWithForeignSourceDoesNotDuplicate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	foreign := filepath.Join(p.Dir, "append_src.duckdb")
	if err := p.Sess.Exec("ATTACH '" + foreign + "' AS crm"); err != nil {
		t.Fatalf("attach foreign catalog: %v", err)
	}
	for _, stmt := range []string{
		"CREATE TABLE crm.decision (id BIGINT, verdict VARCHAR)",
		"INSERT INTO crm.decision VALUES (1, 'a'), (2, 'b')",
	} {
		if err := p.Sess.Exec(stmt); err != nil {
			t.Fatalf("seed foreign catalog (%s): %v", stmt, err)
		}
	}

	p.AddModel("staging/appended.sql", `-- @kind: append
SELECT f.id::BIGINT AS id, f.verdict::VARCHAR AS verdict
FROM crm.decision f
`)

	count := func(when string) int64 {
		t.Helper()
		rows, err := p.Sess.QueryRows("SELECT count(*)::BIGINT FROM staging.appended")
		if err != nil {
			t.Fatalf("count %s: %v", when, err)
		}
		var n int64
		if _, err := fmt.Sscanf(rows[0], "%d", &n); err != nil {
			t.Fatalf("parse count %s (%q): %v", when, rows[0], err)
		}
		return n
	}

	runModel(t, p, "staging/appended.sql")
	if got := count("after first run"); got != 2 {
		t.Fatalf("first run should load 2 rows, got %d", got)
	}
	runModel(t, p, "staging/appended.sql")
	if got := count("after rerun"); got != 2 {
		t.Fatalf("rerunning an unchanged foreign source must not duplicate, got %d rows", got)
	}
	runModel(t, p, "staging/appended.sql")
	if got := count("after third run"); got != 2 {
		t.Fatalf("third run must still hold 2 rows, got %d", got)
	}

	// A genuinely new row in the foreign source must still arrive — the
	// rebuild has to reflect the source, not freeze the target.
	if err := p.Sess.Exec("INSERT INTO crm.decision VALUES (3, 'c')"); err != nil {
		t.Fatalf("add a row to the foreign source: %v", err)
	}
	runModel(t, p, "staging/appended.sql")
	if got := count("after source grew"); got != 3 {
		t.Errorf("a new row in the foreign source must reach the model, got %d rows", got)
	}
}

// TestIncremental_LateBackfillEscalationKeepsHistory pins that escalating to a
// rebuild mid-run resets the incremental cursor with it.
//
// applyIncrementalVars runs early, off the run-type decision. Every escalation
// to backfill happens later — a foreign source disabling CDC, an upstream
// schema change, a failed CDC query — and each one switches materialize to
// TRUNCATE + INSERT while the model's SQL is still filtered by
// `getvariable('incr_last_value')`. The rebuild then sees only the rows after
// the cursor and destroys every row before it: a target holding 1 and 2 came
// back holding only 3.
func TestIncremental_LateBackfillEscalationKeepsHistory(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	foreign := filepath.Join(p.Dir, "incremental_src.duckdb")
	if err := p.Sess.Exec("ATTACH '" + foreign + "' AS crm"); err != nil {
		t.Fatalf("attach foreign catalog: %v", err)
	}
	for _, stmt := range []string{
		"CREATE TABLE crm.decision (id BIGINT, verdict VARCHAR)",
		"INSERT INTO crm.decision VALUES (1, 'a'), (2, 'b')",
	} {
		if err := p.Sess.Exec(stmt); err != nil {
			t.Fatalf("seed foreign catalog (%s): %v", stmt, err)
		}
	}

	// A foreign source forces the CDC-disabled path, so the escalation fires
	// on every run rather than only on a schema change or a query failure.
	p.AddModel("staging/incremental_hist.sql", `-- @kind: append
-- @incremental: id
-- @incremental_initial: 0
SELECT f.id::BIGINT AS id, f.verdict::VARCHAR AS verdict
FROM crm.decision f
WHERE f.id > getvariable('incr_last_value')
`)

	rows := func(when string) []string {
		t.Helper()
		got, err := p.Sess.QueryRows("SELECT id || '=' || verdict FROM staging.incremental_hist ORDER BY id")
		if err != nil {
			t.Fatalf("read %s: %v", when, err)
		}
		return got
	}
	same := func(got, want []string) bool {
		if len(got) != len(want) {
			return false
		}
		for i := range got {
			if got[i] != want[i] {
				return false
			}
		}
		return true
	}

	runModel(t, p, "staging/incremental_hist.sql")
	if got := rows("after first run"); !same(got, []string{"1=a", "2=b"}) {
		t.Fatalf("first run should load both rows, got %v", got)
	}

	if err := p.Sess.Exec("INSERT INTO crm.decision VALUES (3, 'c')"); err != nil {
		t.Fatalf("add a row to the foreign source: %v", err)
	}

	r := runModel(t, p, "staging/incremental_hist.sql")
	if len(r.Errors) > 0 {
		t.Fatalf("rerun failed: %v", r.Errors)
	}
	if got := rows("after rerun"); !same(got, []string{"1=a", "2=b", "3=c"}) {
		t.Errorf("a mid-run rebuild must not drop rows before the cursor, got %v", got)
	}
	// And the cursor must still be doing its job. Escalating a foreign-source
	// model to a rebuild would reset it and re-read the whole source every
	// run — which is the opposite of what `@incremental` is recommended for
	// on exactly these sources.
	if r.RowsAffected != 1 {
		t.Errorf("only the row past the cursor should be read, got %d rows", r.RowsAffected)
	}
}
