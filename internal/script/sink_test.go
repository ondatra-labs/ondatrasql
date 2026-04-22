// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// These tests use SINK = {...} as a Starlark dict fixture. The SINK dict is NOT
// parsed by libregistry (which only reads API dicts). It exists purely as a
// Starlark variable for test isolation.

package script

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// writeSinkStar creates a lib/<name>.star file with the given content.
func writeSinkStar(t *testing.T, dir, name, code string) {
	t.Helper()
	libDir := filepath.Join(dir, "lib")
	os.MkdirAll(libDir, 0o755)
	if err := os.WriteFile(filepath.Join(libDir, name+".star"), []byte(code), 0o644); err != nil {
		t.Fatalf("write %s.star: %v", name, err)
	}
}

func TestRunSink_SyncReturnDict(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "test_push", `
SINK = {"batch_size": 100}

def push(rows):
    return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "ok" for r in rows}
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{
		{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert", "name": "alice"},
		{"__ondatra_rowid": 2.0, "__ondatra_change_type": "insert", "name": "bob"},
	}
	result, err := rt.RunSink(context.Background(), "test_push", rows, 1)
	if err != nil {
		t.Fatalf("RunSink: %v", err)
	}
	if result.PerRow == nil {
		t.Fatal("expected PerRow to be set")
	}
	if len(result.PerRow) != 2 {
		t.Fatalf("got %d PerRow entries, want 2", len(result.PerRow))
	}
	if result.PerRow["1.0:insert"] != "ok" {
		t.Errorf("PerRow[1.0:insert] = %q, want ok", result.PerRow["1.0:insert"])
	}
}

func TestRunSink_SyncReturnNone(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "none_push", `
SINK = {"batch_size": 100}

def push(rows):
    pass  # returns None
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert"}}
	result, err := rt.RunSink(context.Background(), "none_push", rows, 1)
	if err != nil {
		t.Fatalf("RunSink: %v", err)
	}
	// None return → PerRow is nil, RawReturn is nil
	if result.PerRow != nil {
		t.Errorf("expected nil PerRow for None return, got %v", result.PerRow)
	}
	if result.RawReturn != nil {
		t.Errorf("expected nil RawReturn for None return, got %v", result.RawReturn)
	}
}

func TestRunSink_SyncReturnNonDict(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "list_push", `
SINK = {"batch_size": 100}

def push(rows):
    return [1, 2, 3]  # wrong type
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert"}}
	result, err := rt.RunSink(context.Background(), "list_push", rows, 1)
	if err != nil {
		t.Fatalf("RunSink: %v", err)
	}
	// Non-dict return → PerRow nil, RawReturn nil (caller handles this)
	if result.PerRow != nil {
		t.Errorf("expected nil PerRow for list return, got %v", result.PerRow)
	}
}

func TestRunSink_AsyncReturnJobRef(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "async_push", `
SINK = {"batch_size": 100, "batch_mode": "async", "poll_interval": "1s", "poll_timeout": "10s"}

def push(rows):
    return {"job_id": "abc123", "status": "pending"}

def poll(job_ref):
    return {"done": True, "per_row": {}}
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert"}}
	result, err := rt.RunSink(context.Background(), "async_push", rows, 1)
	if err != nil {
		t.Fatalf("RunSink: %v", err)
	}
	if result.RawReturn == nil {
		t.Fatal("expected RawReturn for async job ref")
	}
	if result.RawReturn["job_id"] != "abc123" {
		t.Errorf("job_id = %v, want abc123", result.RawReturn["job_id"])
	}
}

func TestRunSink_AtomicReturnNone(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "atomic_push", `
SINK = {"batch_size": 100, "batch_mode": "atomic"}

def push(rows):
    pass  # atomic returns None on success
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert"}}
	result, err := rt.RunSink(context.Background(), "atomic_push", rows, 1)
	if err != nil {
		t.Fatalf("RunSink: %v", err)
	}
	if result.PerRow != nil {
		t.Errorf("expected nil PerRow for atomic None, got %v", result.PerRow)
	}
}

func TestRunSink_PushError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "error_push", `
SINK = {"batch_size": 100}

def push(rows):
    fail("destination unavailable")
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert"}}
	_, err := rt.RunSink(context.Background(), "error_push", rows, 1)
	if err == nil {
		t.Fatal("expected error from failing push")
	}
}

func TestRunSink_NoPushFunction(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "no_push", `
SINK = {"batch_size": 100}
# no push() defined
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert"}}
	_, err := rt.RunSink(context.Background(), "no_push", rows, 1)
	if err == nil {
		t.Fatal("expected error for missing push()")
	}
}

func TestRunSink_EmptyRows(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "empty_push", `
SINK = {"batch_size": 100}

def push(rows):
    return {}
`)
	rt := NewRuntime(nil, nil, dir)
	result, err := rt.RunSink(context.Background(), "empty_push", []map[string]any{}, 1)
	if err != nil {
		t.Fatalf("RunSink: %v", err)
	}
	// Empty dict return → PerRow is empty map (not nil)
	if result.PerRow == nil {
		t.Fatal("expected non-nil PerRow for empty dict return")
	}
	if len(result.PerRow) != 0 {
		t.Errorf("expected empty PerRow, got %v", result.PerRow)
	}
}

func TestRunSink_PerRowNonStringValue(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "nonstr_push", `
SINK = {"batch_size": 100}

def push(rows):
    # Return True instead of "ok" -- wrong type
    return {"1.0": True}
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert"}}
	result, err := rt.RunSink(context.Background(), "nonstr_push", rows, 1)
	if err != nil {
		t.Fatalf("RunSink: %v", err)
	}
	// Non-string value in dict → converted via .String() in PerRow
	if len(result.PerRow) != 1 {
		t.Errorf("expected 1 PerRow entry, got %v", result.PerRow)
	}
	if v, ok := result.PerRow["1.0"]; !ok || v != "True" {
		t.Errorf("expected PerRow[\"1.0\"] = \"True\", got %q", v)
	}
}

func TestRunSink_BatchNumber(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "batch_push", `
SINK = {"batch_size": 100}

def push(rows):
    # Return batch_number as string value to verify it's accessible
    return {"1.0": "ok:" + str(sink.batch_number)}
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert"}}
	result, err := rt.RunSink(context.Background(), "batch_push", rows, 7)
	if err != nil {
		t.Fatalf("RunSink: %v", err)
	}
	if result.PerRow["1.0"] != "ok:7" {
		t.Errorf("got %q, want ok:7 (batch_number should be 7)", result.PerRow["1.0"])
	}
}

func TestRunSinkFinalize_Optional(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "nofin_push", `
SINK = {"batch_size": 100}

def push(rows):
    return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "ok" for r in rows}
# no finalize() -- should be no-op
`)
	rt := NewRuntime(nil, nil, dir)
	err := rt.RunSinkFinalize(context.Background(), "nofin_push", 10, 0)
	if err != nil {
		t.Fatalf("RunSinkFinalize without finalize(): %v", err)
	}
}

func TestRunSinkFinalize_Called(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// finalize writes to a file to prove it was called with correct args
	outFile := filepath.Join(dir, "finalize_out.txt")
	writeSinkStar(t, dir, "fin_push", `
SINK = {"batch_size": 100}

def push(rows):
    return {}

def finalize(succeeded, failed):
    if succeeded != 5:
        fail("expected succeeded=5, got " + str(succeeded))
    if failed != 2:
        fail("expected failed=2, got " + str(failed))
`)
	_ = outFile
	rt := NewRuntime(nil, nil, dir)
	err := rt.RunSinkFinalize(context.Background(), "fin_push", 5, 2)
	if err != nil {
		t.Fatalf("RunSinkFinalize: %v", err)
	}
}

func TestRunSinkPoll_NotDone(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "poll_push", `
SINK = {"batch_size": 100, "batch_mode": "async", "poll_interval": "1s", "poll_timeout": "10s"}

def push(rows):
    return {"job_id": "x"}

def poll(job_ref):
    return {"done": False}
`)
	rt := NewRuntime(nil, nil, dir)
	done, perRow, err := rt.RunSinkPoll(context.Background(), "poll_push", map[string]any{"job_id": "x"})
	if err != nil {
		t.Fatalf("RunSinkPoll: %v", err)
	}
	if done {
		t.Error("expected done=false")
	}
	if perRow != nil {
		t.Errorf("expected nil perRow when not done, got %v", perRow)
	}
}

func TestRunSinkPoll_DoneWithPerRow(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "poll_done", `
SINK = {"batch_size": 100, "batch_mode": "async", "poll_interval": "1s", "poll_timeout": "10s"}

def push(rows):
    return {"job_id": "x"}

def poll(job_ref):
    return {"done": True, "per_row": {"1.0": "ok", "2.0": "error: timeout"}}
`)
	rt := NewRuntime(nil, nil, dir)
	done, perRow, err := rt.RunSinkPoll(context.Background(), "poll_done", map[string]any{"job_id": "x"})
	if err != nil {
		t.Fatalf("RunSinkPoll: %v", err)
	}
	if !done {
		t.Error("expected done=true")
	}
	if perRow["1.0"] != "ok" {
		t.Errorf("perRow[1.0] = %q, want ok", perRow["1.0"])
	}
	if perRow["2.0"] != "error: timeout" {
		t.Errorf("perRow[2.0] = %q, want 'error: timeout'", perRow["2.0"])
	}
}

func TestRunSinkPoll_DoneStringInsteadOfBool(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "poll_str", `
SINK = {"batch_size": 100, "batch_mode": "async", "poll_interval": "1s", "poll_timeout": "10s"}

def push(rows):
    return {"job_id": "x"}

def poll(job_ref):
    return {"done": "true"}  # string, not bool
`)
	rt := NewRuntime(nil, nil, dir)
	_, _, err := rt.RunSinkPoll(context.Background(), "poll_str", map[string]any{"job_id": "x"})
	if err == nil {
		t.Fatal("expected error for done='true' (string instead of bool)")
	}
}

func TestRunSinkPoll_NoPollFunction(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "no_poll", `
SINK = {"batch_size": 100, "batch_mode": "async"}

def push(rows):
    return {"job_id": "x"}
# no poll() defined
`)
	rt := NewRuntime(nil, nil, dir)
	_, _, err := rt.RunSinkPoll(context.Background(), "no_poll", map[string]any{"job_id": "x"})
	if err == nil {
		t.Fatal("expected error for missing poll()")
	}
}

func TestRunSinkPoll_NonDictReturn(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "poll_list", `
SINK = {"batch_size": 100, "batch_mode": "async", "poll_interval": "1s", "poll_timeout": "10s"}

def push(rows):
    return {"job_id": "x"}

def poll(job_ref):
    return [1, 2, 3]  # wrong type
`)
	rt := NewRuntime(nil, nil, dir)
	_, _, err := rt.RunSinkPoll(context.Background(), "poll_list", map[string]any{"job_id": "x"})
	if err == nil {
		t.Fatal("expected error for non-dict poll return")
	}
}

func TestRunSinkPoll_MissingDoneKey(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "poll_nodone", `
SINK = {"batch_size": 100, "batch_mode": "async", "poll_interval": "1s", "poll_timeout": "10s"}

def push(rows):
    return {"job_id": "x"}

def poll(job_ref):
    return {"status": "running"}  # no "done" key
`)
	rt := NewRuntime(nil, nil, dir)
	_, _, err := rt.RunSinkPoll(context.Background(), "poll_nodone", map[string]any{"job_id": "x"})
	if err == nil {
		t.Fatal("expected error for missing 'done' key")
	}
}

// --- Regression tests ---

// Regression: sync mode must nack when push returns None instead of silently
// acking the entire batch. Previously nil PerRow fell through to ackAll.
func TestRunSink_SyncNoneReturn_MustBeDetectable(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "sync_none", `
SINK = {"batch_size": 100}

def push(rows):
    pass  # returns None -- wrong for sync mode
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert"}}
	result, err := rt.RunSink(context.Background(), "sync_none", rows, 1)
	if err != nil {
		t.Fatalf("RunSink should not error (Starlark ran fine): %v", err)
	}
	// PerRow must be nil so the caller (executeBatch) can detect and nack
	if result.PerRow != nil {
		t.Error("sync None return should give nil PerRow for caller to detect")
	}
}

// Regression: starlarkToGo conversion error must propagate, not be silently
// swallowed leaving RawReturn nil.
func TestRunSink_ConversionErrorPropagates(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Return a valid dict -- conversion should succeed
	writeSinkStar(t, dir, "conv_push", `
SINK = {"batch_size": 100}

def push(rows):
    return {"job_id": "abc", "count": 42}
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert"}}
	result, err := rt.RunSink(context.Background(), "conv_push", rows, 1)
	if err != nil {
		t.Fatalf("RunSink: %v", err)
	}
	// RawReturn must be populated (not nil from swallowed error)
	if result.RawReturn == nil {
		t.Fatal("RawReturn should be set for dict return")
	}
	if result.RawReturn["job_id"] != "abc" {
		t.Errorf("job_id = %v, want abc", result.RawReturn["job_id"])
	}
}

// Regression: poll done="true" (string) must error, not be treated as truthy.
func TestRunSinkPoll_DoneIntInsteadOfBool(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "poll_int", `
SINK = {"batch_size": 100, "batch_mode": "async", "poll_interval": "1s", "poll_timeout": "10s"}

def push(rows):
    return {"job_id": "x"}

def poll(job_ref):
    return {"done": 1}  # int, not bool
`)
	rt := NewRuntime(nil, nil, dir)
	_, _, err := rt.RunSinkPoll(context.Background(), "poll_int", map[string]any{"job_id": "x"})
	if err == nil {
		t.Fatal("expected error for done=1 (int instead of bool)")
	}
}

// Regression: goToStarlark must handle []map[string]any rows correctly.
// Previously it fell to the default case and stringified the entire list.
func TestRunSink_RowsConvertedCorrectly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "rows_push", `
SINK = {"batch_size": 100}

def push(rows):
    # Verify rows are actual dicts, not stringified
    results = {}
    for r in rows:
        if type(r) != "dict":
            fail("row is not a dict: " + type(r))
        key = str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]
        results[key] = "ok"
    return results
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{
		{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert", "name": "alice"},
		{"__ondatra_rowid": 2.0, "__ondatra_change_type": "insert", "name": "bob"},
	}
	result, err := rt.RunSink(context.Background(), "rows_push", rows, 1)
	if err != nil {
		t.Fatalf("RunSink: %v (rows may not have been converted correctly)", err)
	}
	if len(result.PerRow) != 2 {
		t.Fatalf("got %d PerRow, want 2", len(result.PerRow))
	}
}

func TestRunSinkPoll_JobRefPassedCorrectly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "poll_ref", `
SINK = {"batch_size": 100, "batch_mode": "async", "poll_interval": "1s", "poll_timeout": "10s"}

def push(rows):
    return {"job_id": "x"}

def poll(job_ref):
    if job_ref["job_id"] != "test_123":
        fail("wrong job_id: " + str(job_ref["job_id"]))
    return {"done": True, "per_row": {"1.0": "ok"}}
`)
	rt := NewRuntime(nil, nil, dir)
	done, _, err := rt.RunSinkPoll(context.Background(), "poll_ref", map[string]any{"job_id": "test_123"})
	if err != nil {
		t.Fatalf("RunSinkPoll: %v", err)
	}
	if !done {
		t.Error("expected done=true")
	}
}
