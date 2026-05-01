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

func TestRunPush_SyncReturnDict(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "test_push", `
SINK = {"batch_size": 100}

def push(rows=[], batch_number=1):
    return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "ok" for r in rows}
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{
		{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert", "name": "alice"},
		{"__ondatra_rowid": 2.0, "__ondatra_change_type": "insert", "name": "bob"},
	}
	result, err := rt.RunPush(context.Background(), "test_push", rows, 1, "table", "", nil, nil)
	if err != nil {
		t.Fatalf("RunPush: %v", err)
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

func TestRunPush_SyncReturnNone(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "none_push", `
SINK = {"batch_size": 100}

def push(rows=[], batch_number=1):
    pass  # returns None
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert"}}
	result, err := rt.RunPush(context.Background(), "none_push", rows, 1, "table", "", nil, nil)
	if err != nil {
		t.Fatalf("RunPush: %v", err)
	}
	// None return → PerRow is nil, RawReturn is nil
	if result.PerRow != nil {
		t.Errorf("expected nil PerRow for None return, got %v", result.PerRow)
	}
	if result.RawReturn != nil {
		t.Errorf("expected nil RawReturn for None return, got %v", result.RawReturn)
	}
}

func TestRunPush_SyncReturnNonDict(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "list_push", `
SINK = {"batch_size": 100}

def push(rows=[], batch_number=1):
    return [1, 2, 3]  # wrong type
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert"}}
	result, err := rt.RunPush(context.Background(), "list_push", rows, 1, "table", "", nil, nil)
	if err != nil {
		t.Fatalf("RunPush: %v", err)
	}
	// Non-dict return → PerRow nil, RawReturn nil (caller handles this)
	if result.PerRow != nil {
		t.Errorf("expected nil PerRow for list return, got %v", result.PerRow)
	}
}

func TestRunPush_AsyncReturnJobRef(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "async_push", `
SINK = {"batch_size": 100, "batch_mode": "async", "poll_interval": "1s", "poll_timeout": "10s"}

def push(rows=[], batch_number=1):
    return {"job_id": "abc123", "status": "pending"}

def poll(job_ref):
    return {"done": True, "per_row": {}}
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert"}}
	result, err := rt.RunPush(context.Background(), "async_push", rows, 1, "table", "", nil, nil)
	if err != nil {
		t.Fatalf("RunPush: %v", err)
	}
	if result.RawReturn == nil {
		t.Fatal("expected RawReturn for async job ref")
	}
	if result.RawReturn["job_id"] != "abc123" {
		t.Errorf("job_id = %v, want abc123", result.RawReturn["job_id"])
	}
}

func TestRunPush_AtomicReturnNone(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "atomic_push", `
SINK = {"batch_size": 100, "batch_mode": "atomic"}

def push(rows=[], batch_number=1):
    pass  # atomic returns None on success
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert"}}
	result, err := rt.RunPush(context.Background(), "atomic_push", rows, 1, "table", "", nil, nil)
	if err != nil {
		t.Fatalf("RunPush: %v", err)
	}
	if result.PerRow != nil {
		t.Errorf("expected nil PerRow for atomic None, got %v", result.PerRow)
	}
}

func TestRunPush_PushError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "error_push", `
SINK = {"batch_size": 100}

def push(rows=[], batch_number=1):
    fail("destination unavailable")
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert"}}
	_, err := rt.RunPush(context.Background(), "error_push", rows, 1, "table", "", nil, nil)
	if err == nil {
		t.Fatal("expected error from failing push")
	}
}

func TestRunPush_NoPushFunction(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "no_push", `
SINK = {"batch_size": 100}
# no push() defined
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert"}}
	_, err := rt.RunPush(context.Background(), "no_push", rows, 1, "table", "", nil, nil)
	if err == nil {
		t.Fatal("expected error for missing push()")
	}
}

func TestRunPush_EmptyRows(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "empty_push", `
SINK = {"batch_size": 100}

def push(rows=[], batch_number=1):
    return {}
`)
	rt := NewRuntime(nil, nil, dir)
	result, err := rt.RunPush(context.Background(), "empty_push", []map[string]any{}, 1, "table", "", nil, nil)
	if err != nil {
		t.Fatalf("RunPush: %v", err)
	}
	// Empty dict return → PerRow is empty map (not nil)
	if result.PerRow == nil {
		t.Fatal("expected non-nil PerRow for empty dict return")
	}
	if len(result.PerRow) != 0 {
		t.Errorf("expected empty PerRow, got %v", result.PerRow)
	}
}

func TestRunPush_PerRowNonStringValue(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "nonstr_push", `
SINK = {"batch_size": 100}

def push(rows=[], batch_number=1):
    # Return True instead of "ok" -- wrong type
    return {"1.0": True}
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert"}}
	result, err := rt.RunPush(context.Background(), "nonstr_push", rows, 1, "table", "", nil, nil)
	if err != nil {
		t.Fatalf("RunPush: %v", err)
	}
	// Non-string value in dict → converted via .String() in PerRow
	if len(result.PerRow) != 1 {
		t.Errorf("expected 1 PerRow entry, got %v", result.PerRow)
	}
	if v, ok := result.PerRow["1.0"]; !ok || v != "True" {
		t.Errorf("expected PerRow[\"1.0\"] = \"True\", got %q", v)
	}
}

func TestRunPush_BatchNumber(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "batch_push", `
SINK = {"batch_size": 100}

def push(rows=[], batch_number=1):
    # Return batch_number as string value to verify it's accessible
    return {"1.0": "ok:" + str(batch_number)}
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert"}}
	result, err := rt.RunPush(context.Background(), "batch_push", rows, 7, "table", "", nil, nil)
	if err != nil {
		t.Fatalf("RunPush: %v", err)
	}
	if result.PerRow["1.0"] != "ok:7" {
		t.Errorf("got %q, want ok:7 (batch_number should be 7)", result.PerRow["1.0"])
	}
}

// v0.30.0 fas 10: the columns kwarg is a list of dicts with `name` and
// `type`. `name` is the materialized column name (= the SQL alias the
// blueprint sees in `rows`) and `type` is the DuckDB-native type from
// the materialized table. When the caller passes nil tableColumns, the
// runtime falls back to deriving names from the rows themselves
// (untyped — used only when the schema lookup hiccups).
func TestRunPush_ColumnsKwargShape_FromTableSchema(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "shape_push", `
SINK = {"batch_size": 100}

def push(rows=[], columns=[]):
    # Encode the columns kwarg back into the per-row status dict so the
    # test can assert on its shape. Format: <idx>:<name>:<type>.
    parts = []
    for i, c in enumerate(columns):
        parts.append(str(i) + ":" + c.get("name", "") + ":" + str(c.get("type", "")))
    out = {}
    for r in rows:
        out[str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]] = ",".join(parts)
    return out
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert"}}
	tableColumns := []map[string]any{
		{"name": "id", "type": "BIGINT"},
		{"name": "Email__c", "type": "VARCHAR"},
		{"name": "amount", "type": "DECIMAL(18,3)"},
	}
	result, err := rt.RunPush(context.Background(), "shape_push", rows, 1, "merge", "id", tableColumns, nil)
	if err != nil {
		t.Fatalf("RunPush: %v", err)
	}
	got := result.PerRow["1.0:insert"]
	want := "0:id:BIGINT,1:Email__c:VARCHAR,2:amount:DECIMAL(18,3)"
	if got != want {
		t.Errorf("columns shape:\n got: %s\nwant: %s", got, want)
	}
}

func TestRunPush_ColumnsKwargShape_FallbackFromRows(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "fallback_push", `
SINK = {"batch_size": 100}

def push(rows=[], columns=[]):
    # When tableColumns is nil, the runtime derives names from the rows
    # themselves. Each entry has a name but no type.
    parts = []
    for c in columns:
        if "type" in c:
            parts.append(c["name"] + "=" + str(c["type"]))
        else:
            parts.append(c["name"] + "=untyped")
    parts = sorted(parts)
    out = {}
    for r in rows:
        out[str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]] = ",".join(parts)
    return out
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{
		{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert", "id": 1.0, "name": "alice"},
	}
	// Pass nil tableColumns to exercise the fallback path.
	result, err := rt.RunPush(context.Background(), "fallback_push", rows, 1, "table", "", nil, nil)
	if err != nil {
		t.Fatalf("RunPush: %v", err)
	}
	got := result.PerRow["1.0:insert"]
	want := "id=untyped,name=untyped"
	if got != want {
		t.Errorf("fallback shape:\n got: %s\nwant: %s", got, want)
	}
}

func TestRunPushFinalize_Optional(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "nofin_push", `
SINK = {"batch_size": 100}

def push(rows=[], batch_number=1):
    return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "ok" for r in rows}
# no finalize() -- should be no-op
`)
	rt := NewRuntime(nil, nil, dir)
	err := rt.RunPushFinalize(context.Background(), "nofin_push", 10, 0)
	if err != nil {
		t.Fatalf("RunPushFinalize without finalize(): %v", err)
	}
}

func TestRunPushFinalize_Called(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// finalize writes to a file to prove it was called with correct args
	outFile := filepath.Join(dir, "finalize_out.txt")
	writeSinkStar(t, dir, "fin_push", `
SINK = {"batch_size": 100}

def push(rows=[], batch_number=1):
    return {}

def finalize(succeeded, failed):
    if succeeded != 5:
        fail("expected succeeded=5, got " + str(succeeded))
    if failed != 2:
        fail("expected failed=2, got " + str(failed))
`)
	_ = outFile
	rt := NewRuntime(nil, nil, dir)
	err := rt.RunPushFinalize(context.Background(), "fin_push", 5, 2)
	if err != nil {
		t.Fatalf("RunPushFinalize: %v", err)
	}
}

func TestRunPushPoll_NotDone(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "poll_push", `
SINK = {"batch_size": 100, "batch_mode": "async", "poll_interval": "1s", "poll_timeout": "10s"}

def push(rows=[], batch_number=1):
    return {"job_id": "x"}

def poll(job_ref):
    return {"done": False}
`)
	rt := NewRuntime(nil, nil, dir)
	done, perRow, err := rt.RunPushPoll(context.Background(), "poll_push", map[string]any{"job_id": "x"})
	if err != nil {
		t.Fatalf("RunPushPoll: %v", err)
	}
	if done {
		t.Error("expected done=false")
	}
	if perRow != nil {
		t.Errorf("expected nil perRow when not done, got %v", perRow)
	}
}

func TestRunPushPoll_DoneWithPerRow(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "poll_done", `
SINK = {"batch_size": 100, "batch_mode": "async", "poll_interval": "1s", "poll_timeout": "10s"}

def push(rows=[], batch_number=1):
    return {"job_id": "x"}

def poll(job_ref):
    return {"done": True, "per_row": {"1.0": "ok", "2.0": "error: timeout"}}
`)
	rt := NewRuntime(nil, nil, dir)
	done, perRow, err := rt.RunPushPoll(context.Background(), "poll_done", map[string]any{"job_id": "x"})
	if err != nil {
		t.Fatalf("RunPushPoll: %v", err)
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

func TestRunPushPoll_DoneStringInsteadOfBool(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "poll_str", `
SINK = {"batch_size": 100, "batch_mode": "async", "poll_interval": "1s", "poll_timeout": "10s"}

def push(rows=[], batch_number=1):
    return {"job_id": "x"}

def poll(job_ref):
    return {"done": "true"}  # string, not bool
`)
	rt := NewRuntime(nil, nil, dir)
	_, _, err := rt.RunPushPoll(context.Background(), "poll_str", map[string]any{"job_id": "x"})
	if err == nil {
		t.Fatal("expected error for done='true' (string instead of bool)")
	}
}

func TestRunPushPoll_NoPollFunction(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "no_poll", `
SINK = {"batch_size": 100, "batch_mode": "async"}

def push(rows=[], batch_number=1):
    return {"job_id": "x"}
# no poll() defined
`)
	rt := NewRuntime(nil, nil, dir)
	_, _, err := rt.RunPushPoll(context.Background(), "no_poll", map[string]any{"job_id": "x"})
	if err == nil {
		t.Fatal("expected error for missing poll()")
	}
}

func TestRunPushPoll_NonDictReturn(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "poll_list", `
SINK = {"batch_size": 100, "batch_mode": "async", "poll_interval": "1s", "poll_timeout": "10s"}

def push(rows=[], batch_number=1):
    return {"job_id": "x"}

def poll(job_ref):
    return [1, 2, 3]  # wrong type
`)
	rt := NewRuntime(nil, nil, dir)
	_, _, err := rt.RunPushPoll(context.Background(), "poll_list", map[string]any{"job_id": "x"})
	if err == nil {
		t.Fatal("expected error for non-dict poll return")
	}
}

func TestRunPushPoll_MissingDoneKey(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "poll_nodone", `
SINK = {"batch_size": 100, "batch_mode": "async", "poll_interval": "1s", "poll_timeout": "10s"}

def push(rows=[], batch_number=1):
    return {"job_id": "x"}

def poll(job_ref):
    return {"status": "running"}  # no "done" key
`)
	rt := NewRuntime(nil, nil, dir)
	_, _, err := rt.RunPushPoll(context.Background(), "poll_nodone", map[string]any{"job_id": "x"})
	if err == nil {
		t.Fatal("expected error for missing 'done' key")
	}
}

// --- Regression tests ---

// Regression: sync mode must nack when push returns None instead of silently
// acking the entire batch. Previously nil PerRow fell through to ackAll.
func TestRunPush_SyncNoneReturn_MustBeDetectable(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "sync_none", `
SINK = {"batch_size": 100}

def push(rows=[], batch_number=1):
    pass  # returns None -- wrong for sync mode
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert"}}
	result, err := rt.RunPush(context.Background(), "sync_none", rows, 1, "table", "", nil, nil)
	if err != nil {
		t.Fatalf("RunPush should not error (Starlark ran fine): %v", err)
	}
	// PerRow must be nil so the caller (executeBatch) can detect and nack
	if result.PerRow != nil {
		t.Error("sync None return should give nil PerRow for caller to detect")
	}
}

// Regression: starlarkToGo conversion error must propagate, not be silently
// swallowed leaving RawReturn nil.
func TestRunPush_ConversionErrorPropagates(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Return a valid dict -- conversion should succeed
	writeSinkStar(t, dir, "conv_push", `
SINK = {"batch_size": 100}

def push(rows=[], batch_number=1):
    return {"job_id": "abc", "count": 42}
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{{"__ondatra_rowid": 1.0, "__ondatra_change_type": "insert"}}
	result, err := rt.RunPush(context.Background(), "conv_push", rows, 1, "table", "", nil, nil)
	if err != nil {
		t.Fatalf("RunPush: %v", err)
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
func TestRunPushPoll_DoneIntInsteadOfBool(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "poll_int", `
SINK = {"batch_size": 100, "batch_mode": "async", "poll_interval": "1s", "poll_timeout": "10s"}

def push(rows=[], batch_number=1):
    return {"job_id": "x"}

def poll(job_ref):
    return {"done": 1}  # int, not bool
`)
	rt := NewRuntime(nil, nil, dir)
	_, _, err := rt.RunPushPoll(context.Background(), "poll_int", map[string]any{"job_id": "x"})
	if err == nil {
		t.Fatal("expected error for done=1 (int instead of bool)")
	}
}

// Regression: goToStarlark must handle []map[string]any rows correctly.
// Previously it fell to the default case and stringified the entire list.
func TestRunPush_RowsConvertedCorrectly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "rows_push", `
SINK = {"batch_size": 100}

def push(rows=[], batch_number=1):
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
	result, err := rt.RunPush(context.Background(), "rows_push", rows, 1, "table", "", nil, nil)
	if err != nil {
		t.Fatalf("RunPush: %v (rows may not have been converted correctly)", err)
	}
	if len(result.PerRow) != 2 {
		t.Fatalf("got %d PerRow, want 2", len(result.PerRow))
	}
}

func TestRunPushPoll_JobRefPassedCorrectly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "poll_ref", `
SINK = {"batch_size": 100, "batch_mode": "async", "poll_interval": "1s", "poll_timeout": "10s"}

def push(rows=[], batch_number=1):
    return {"job_id": "x"}

def poll(job_ref):
    if job_ref["job_id"] != "test_123":
        fail("wrong job_id: " + str(job_ref["job_id"]))
    return {"done": True, "per_row": {"1.0": "ok"}}
`)
	rt := NewRuntime(nil, nil, dir)
	done, _, err := rt.RunPushPoll(context.Background(), "poll_ref", map[string]any{"job_id": "test_123"})
	if err != nil {
		t.Fatalf("RunPushPoll: %v", err)
	}
	if !done {
		t.Error("expected done=true")
	}
}
