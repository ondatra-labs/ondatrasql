// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
)

// mustClosedSession creates and immediately closes a DuckDB session.
// Panics on creation failure (acceptable in tests).
func mustClosedSession() *duckdb.Session {
	s, err := duckdb.NewSession(":memory:")
	if err != nil {
		panic("create session: " + err.Error())
	}
	s.Close()
	return s
}

// --- Rapid generators for error path testing ---

func genErrModelKind() *rapid.Generator[string] {
	return rapid.SampledFrom([]string{"table", "append", "merge", "scd2", "tracked"})
}

func genErrTarget() *rapid.Generator[string] {
	schema := rapid.SampledFrom([]string{"raw", "staging", "mart", "analytics"})
	table := rapid.StringMatching(`^[a-z][a-z0-9_]{1,8}$`)
	return rapid.Custom(func(t *rapid.T) string {
		return schema.Draw(t, "schema") + "." + table.Draw(t, "table")
	})
}

func genErrSQL() *rapid.Generator[string] {
	return rapid.SampledFrom([]string{
		"SELECT 1 AS id",
		"SELECT 1 AS id, 'hello' AS name",
		"SELECT 42 AS val, true AS flag",
		"INVALID SQL",
		"",
		"SELECT * FROM nonexistent",
	})
}

func genErrModel() *rapid.Generator[*parser.Model] {
	return rapid.Custom(func(t *rapid.T) *parser.Model {
		kind := genErrModelKind().Draw(t, "kind")
		target := genErrTarget().Draw(t, "target")
		sql := genErrSQL().Draw(t, "sql")

		m := &parser.Model{
			Target: target,
			Kind:   kind,
			SQL:    sql,
		}

		// Add required directives for specific kinds
		if kind == "merge" || kind == "scd2" {
			m.UniqueKey = rapid.SampledFrom([]string{"id", "key", "uid"}).Draw(t, "uk")
		}
		if kind == "tracked" {
			m.GroupKey = rapid.SampledFrom([]string{"region", "date", "category"}).Draw(t, "group_key")
		}

		// Optionally add extensions
		if rapid.Bool().Draw(t, "has_ext") {
			m.Extensions = []string{rapid.SampledFrom([]string{"spatial", "httpfs", "json"}).Draw(t, "ext")}
		}

		return m
	})
}

// --- Property: Run never panics on closed session, always returns error ---

func TestRapid_Run_ClosedSession_NeverPanics(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	rapid.Check(t, func(rt *rapid.T) {
		sess := mustClosedSession()
		runner := NewRunner(sess, ModeRun, "test-rapid")
		model := genErrModel().Draw(rt, "model")

		_, err := runner.Run(context.Background(), model)
		if err == nil {
			rt.Fatalf("expected error for kind=%q target=%q with closed session", model.Kind, model.Target)
		}
	})
}

// --- Property: ComputeSingleRunType never panics on closed session ---

func TestRapid_ComputeSingleRunType_ClosedSession(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	rapid.Check(t, func(rt *rapid.T) {
		sess := mustClosedSession()
		model := genErrModel().Draw(rt, "model")

		_, err := ComputeSingleRunType(sess, model)
		if err == nil {
			rt.Fatal("expected error on closed session")
		}
	})
}

// --- Property: ComputeRunTypeDecisions never panics on closed session ---

func TestRapid_ComputeRunTypeDecisions_ClosedSession(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	rapid.Check(t, func(rt *rapid.T) {
		sess := mustClosedSession()
		n := rapid.IntRange(1, 5).Draw(rt, "n_models")
		var models []*parser.Model
		for i := 0; i < n; i++ {
			models = append(models, genErrModel().Draw(rt, "model"))
		}

		_, err := ComputeRunTypeDecisions(sess, models)
		if err == nil {
			rt.Fatal("expected error from closed session")
		}
	})
}

// --- Property: materialize never panics on closed session with any kind ---

func TestRapid_Materialize_ClosedSession(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	rapid.Check(t, func(rt *rapid.T) {
		sess := mustClosedSession()
		runner := NewRunner(sess, ModeRun, "test-rapid")
		model := genErrModel().Draw(rt, "model")
		result := &Result{Target: model.Target}

		_, err := runner.materialize(model, "tmp_nonexistent", true, nil, "", "hash", "backfill", result, time.Now(), trackedRunOpts{})
		if err == nil {
			rt.Fatalf("expected error for kind=%q on closed session", model.Kind)
		}
	})
}

// --- Property: materializeSCD2 never panics on closed session ---

func TestRapid_MaterializeSCD2_ClosedSession(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	rapid.Check(t, func(rt *rapid.T) {
		sess := mustClosedSession()
		runner := NewRunner(sess, ModeRun, "test-rapid")
		target := genErrTarget().Draw(rt, "target")
		uk := rapid.SampledFrom([]string{"id", "key", "uid"}).Draw(rt, "uk")
		model := &parser.Model{Target: target, Kind: "scd2", UniqueKey: uk, SQL: "SELECT 1 AS id"}
		result := &Result{Target: target}

		_, err := runner.materializeSCD2(model, "tmp_nonexistent", rapid.Bool().Draw(rt, "backfill"), "", "", "hash", "backfill", result, time.Now())
		if err == nil {
			rt.Fatal("expected error on closed session")
		}
	})
}

// --- Property: materializeTracked never panics on closed session ---

func TestRapid_MaterializePartition_ClosedSession(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	rapid.Check(t, func(rt *rapid.T) {
		sess := mustClosedSession()
		runner := NewRunner(sess, ModeRun, "test-rapid")
		target := genErrTarget().Draw(rt, "target")
		partCol := rapid.SampledFrom([]string{"region", "date", "category"}).Draw(rt, "part")
		model := &parser.Model{Target: target, Kind: "tracked", GroupKey: partCol, SQL: "SELECT 1 AS id"}
		result := &Result{Target: target}

		_, err := runner.materializeTracked(model, "tmp_nonexistent", rapid.Bool().Draw(rt, "backfill"), "", "", "hash", "backfill", result, time.Now(), trackedRunOpts{})
		if err == nil {
			rt.Fatal("expected error on closed session")
		}
	})
}

// --- Property: loadExtension never panics on closed session ---

func TestRapid_LoadExtension_ClosedSession(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	rapid.Check(t, func(rt *rapid.T) {
		sess := mustClosedSession()
		runner := NewRunner(sess, ModeRun, "test-rapid")

		ext := rapid.SampledFrom([]string{
			"spatial",
			"httpfs",
			"json",
			"httpfs FROM community",
			"delta FROM core_nightly",
			"myext FROM 'https://example.com/ext'",
		}).Draw(rt, "ext")

		err := runner.loadExtension(ext)
		if err == nil {
			rt.Fatal("expected error on closed session")
		}
	})
}

// --- Property: extractLineage never panics on closed session ---

func TestRapid_ExtractLineage_ClosedSession(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	rapid.Check(t, func(rt *rapid.T) {
		sess := mustClosedSession()
		runner := NewRunner(sess, ModeRun, "test-rapid")
		sql := genErrSQL().Draw(rt, "sql")

		_, _, err := runner.extractLineage(sql)
		if err == nil {
			rt.Fatal("expected error on closed session")
		}
	})
}

// TestRunDAG_PropagatesDecisionError regression-tests the dag_runner.go
// fix that surfaces ComputeRunTypeDecisions failures via the
// `_validation` errors-map slot. Previously the error was silently
// dropped (`decisions, _ := ...`) so the DAG would proceed with an
// empty decision map, causing every model to recompute via the
// fall-back path even though the batch query was broken.
func TestRunDAG_PropagatesDecisionError(t *testing.T) {
	sess := mustClosedSession()

	models := []*parser.Model{
		{Target: "staging.x", Kind: "table", SQL: "SELECT 1 AS id"},
	}
	results, errors := RunDAG(context.Background(), sess, models, nil, "test-dag",
		"", "", "", "", nil, nil)

	if results != nil {
		t.Errorf("results = %v, want nil when decisions failed", results)
	}
	if errors == nil || errors["_validation"] == nil {
		t.Fatalf("expected _validation error key, got %v", errors)
	}
}

// TestRunner_Run_PropagatesRunTypeError regression-tests the runner.go
// fix that aborts on a failed ComputeSingleRunType instead of silently
// falling back to a destructive `backfill` decision. The previous code
// appended the error to result.Errors but kept executing — letting a
// transient DB hiccup force a full re-fetch.
func TestRunner_Run_PropagatesRunTypeError(t *testing.T) {
	sess := mustClosedSession()
	runner := NewRunner(sess, ModeRun, "test-runtype-abort")
	model := &parser.Model{Target: "staging.x", Kind: "table", SQL: "SELECT 1 AS id"}

	result, err := runner.Run(context.Background(), model)
	if err == nil {
		t.Fatal("expected error from Run() with closed session, got nil")
	}
	// The result should carry the error message rather than be a
	// successful "backfill" run that hides the failure.
	if result == nil || len(result.Errors) == 0 {
		t.Errorf("expected result.Errors populated, got result=%+v", result)
	}
}

// TestDeleteSyncAck_ReturnsError regression-tests the sync_ack.go fix
// that promotes deleteSyncAck from a side-effect-only helper to one
// that returns its DELETE error so the caller can surface a warning.
// Previously a closed-session DELETE silently no-op'd.
func TestDeleteSyncAck_ReturnsError(t *testing.T) {
	sess := mustClosedSession()
	if err := deleteSyncAck(sess, "claim-123"); err == nil {
		t.Fatal("expected error from deleteSyncAck on closed session, got nil")
	}
}

// TestRecordSyncAckCleanupWarning_SuccessNoOp asserts the warning
// helper extracted from ackAll does nothing when deleteSyncAck
// succeeded — the previous (inlined) code already had this behaviour
// but the extraction must preserve it.
func TestRecordSyncAckCleanupWarning_SuccessNoOp(t *testing.T) {
	r := &Result{}
	recordSyncAckCleanupWarning(r, nil)
	if len(r.Warnings) != 0 {
		t.Errorf("Warnings = %v, want empty when err == nil", r.Warnings)
	}
}

// TestRecordSyncAckCleanupWarning_AppendsOnFailure regression-tests
// the push.go fix that promotes a swallowed deleteSyncAck failure to
// a result.Warning. Pre-fix `deleteSyncAck(...)` was called for side
// effects and any error was lost; ackAll therefore silently leaked
// stale _sync_acked rows whenever the cleanup DELETE failed.
func TestRecordSyncAckCleanupWarning_AppendsOnFailure(t *testing.T) {
	r := &Result{}
	recordSyncAckCleanupWarning(r, errors.New("disk full"))
	if len(r.Warnings) != 1 {
		t.Fatalf("Warnings = %d, want 1", len(r.Warnings))
	}
	if !strings.Contains(r.Warnings[0], "_sync_acked cleanup failed") {
		t.Errorf("warning %q missing '_sync_acked cleanup failed' phrase", r.Warnings[0])
	}
	if !strings.Contains(r.Warnings[0], "disk full") {
		t.Errorf("warning %q missing underlying error message", r.Warnings[0])
	}
}


