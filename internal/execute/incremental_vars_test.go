// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"errors"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/backfill"
)

// fakeExecer counts Exec calls and fails on the failOn-th invocation
// (1-indexed). Records the SQL of every call so the test can assert
// the loop aborted at exactly the failing statement and never invoked
// the rest.
type fakeExecer struct {
	calls  []string
	failOn int // 0 = never fail
}

func (f *fakeExecer) Exec(sql string) error {
	f.calls = append(f.calls, sql)
	if f.failOn > 0 && len(f.calls) == f.failOn {
		return errors.New("boom")
	}
	return nil
}

// TestApplyIncrementalVars_HappyPath asserts all four SET VARIABLE
// statements run in order and no error surfaces when every Exec
// succeeds.
func TestApplyIncrementalVars_HappyPath(t *testing.T) {
	exec := &fakeExecer{}
	state := &backfill.IncrementalState{
		LastValue:  "2024-01-01",
		LastRun:    "2024-06-15 12:00:00",
		IsBackfill: false,
		Cursor:     "updated_at",
	}
	if err := applyIncrementalVars(exec, state); err != nil {
		t.Fatalf("applyIncrementalVars: %v", err)
	}
	if len(exec.calls) != 4 {
		t.Fatalf("calls = %d, want 4 (incr_last_value, incr_last_run, incr_is_backfill, incr_cursor)", len(exec.calls))
	}
	wantPrefixes := []string{
		"SET VARIABLE incr_last_value",
		"SET VARIABLE incr_last_run",
		"SET VARIABLE incr_is_backfill",
		"SET VARIABLE incr_cursor",
	}
	for i, want := range wantPrefixes {
		if !strings.HasPrefix(exec.calls[i], want) {
			t.Errorf("call[%d] = %q, want prefix %q", i, exec.calls[i], want)
		}
	}
}

// TestApplyIncrementalVars_AbortsOnFailure regression-tests the
// runner.go fix that converts the SET VARIABLE block from four silent
// Exec calls into a fail-fast loop. The pre-fix code discarded all
// four errors and let the model proceed with stale incremental
// variables — guaranteeing wrong rows for every kind of failure.
//
// For each of the four statement positions, verify that:
//   - applyIncrementalVars returns a wrapped "set incremental variable" error
//   - the loop does NOT continue past the failing call (no later Exec invocations)
func TestApplyIncrementalVars_AbortsOnFailure(t *testing.T) {
	state := &backfill.IncrementalState{
		LastValue: "2024-01-01", LastRun: "2024-06-15 12:00:00",
		IsBackfill: false, Cursor: "updated_at",
	}
	for failAt := 1; failAt <= 4; failAt++ {
		exec := &fakeExecer{failOn: failAt}
		err := applyIncrementalVars(exec, state)
		if err == nil {
			t.Fatalf("failAt=%d: expected error, got nil", failAt)
		}
		if !strings.Contains(err.Error(), "set incremental variable") {
			t.Errorf("failAt=%d: error %q missing 'set incremental variable' wrap", failAt, err)
		}
		// Error must name which of the four variables failed so callers
		// don't have to count Exec invocations to localise the failure.
		// (R6 finding: pre-fix wrap was generic.)
		varNames := []string{"incr_last_value", "incr_last_run", "incr_is_backfill", "incr_cursor"}
		wantName := varNames[failAt-1]
		if !strings.Contains(err.Error(), wantName) {
			t.Errorf("failAt=%d: error %q missing variable name %q", failAt, err, wantName)
		}
		if !errors.Is(err, errors.Unwrap(err)) && err.Error() == "" {
			t.Errorf("failAt=%d: error chain broken: %v", failAt, err)
		}
		if len(exec.calls) != failAt {
			t.Errorf("failAt=%d: calls=%d, want %d (loop must abort at failure, not run later statements)",
				failAt, len(exec.calls), failAt)
		}
	}
}
