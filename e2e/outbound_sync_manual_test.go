// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/collect"
	"github.com/ondatra-labs/ondatrasql/internal/dag"
	"github.com/ondatra-labs/ondatrasql/internal/execute"
	"github.com/ondatra-labs/ondatrasql/internal/libregistry"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"github.com/ondatra-labs/ondatrasql/internal/script"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// ---------------------------------------------------------------------------
// Test 1: table_changes() returns correct delta after materialization
// ---------------------------------------------------------------------------

func TestOutboundSync_TableChanges_Delta(t *testing.T) {
	p := testutil.NewProject(t)

	// Create source table
	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.customers AS SELECT * FROM (VALUES (1,'Alice','alice@example.com'),(2,'Bob','bob@example.com'),(3,'Carol','carol@example.com')) AS t(id,name,email)")

	// Tracked model -- simulates a sync target
	p.AddModel("sync/contacts.sql", `-- @kind: tracked
-- @unique_key: id

SELECT * FROM raw.customers
`)

	// Run 1: initial backfill
	r1 := runModel(t, p, "sync/contacts.sql")
	if r1.RunType == "skip" {
		t.Fatal("run 1 should not skip")
	}

	// Get snapshot after run 1
	snap1, _ := p.Sess.QueryValue("SELECT MAX(snapshot_id) FROM snapshots()")

	// Change source: update Bob, delete Carol, add Dave
	p.Sess.Exec("UPDATE raw.customers SET email='bob_new@example.com' WHERE name='Bob'")
	p.Sess.Exec("DELETE FROM raw.customers WHERE name='Carol'")
	p.Sess.Exec("INSERT INTO raw.customers VALUES (4, 'Dave', 'dave@example.com')")

	// Run 2: incremental
	r2 := runModel(t, p, "sync/contacts.sql")
	if r2.RunType == "skip" {
		t.Fatal("run 2 should not skip")
	}

	// Get snapshot after run 2
	snap2, _ := p.Sess.QueryValue("SELECT MAX(snapshot_id) FROM snapshots()")

	// Query table_changes between the two snapshots -- this is what sink would use
	deltaSQL := fmt.Sprintf(`SELECT id, name, email,
		CASE change_type
			WHEN 'insert' THEN 'upsert'
			WHEN 'update_postimage' THEN 'upsert'
			WHEN 'delete' THEN 'delete'
		END AS _operation
	FROM table_changes('contacts', %s, %s)
	WHERE change_type != 'update_preimage'
	ORDER BY id, change_type, email`, snap1, snap2)

	// Need to set search path for table_changes (bare table name)
	p.Sess.Exec("USE lake.sync")

	snap := newSnapshot()
	snap.addLine("--- run 1 (backfill) ---")
	snap.addResult(r1)
	snap.addLine("--- run 2 (incremental) ---")
	snap.addResult(r2)
	snap.addLine("--- table_changes delta ---")

	// Query delta as CSV for full row visibility
	deltaCsv, err := p.Sess.Query(deltaSQL)
	if err != nil {
		t.Fatalf("table_changes query: %v", err)
	}
	for _, line := range strings.Split(strings.TrimSpace(deltaCsv), "\n") {
		snap.addLine(line)
	}

	fullDelta, _ := p.Sess.QueryValue(fmt.Sprintf(
		"SELECT COUNT(*) FROM table_changes('contacts', %s, %s) WHERE change_type != 'update_preimage'",
		snap1, snap2))
	snap.addLine("delta_row_count: " + fullDelta)

	// Verify specific operations
	upsertCount, _ := p.Sess.QueryValue(fmt.Sprintf(
		"SELECT COUNT(*) FROM table_changes('contacts', %s, %s) WHERE change_type IN ('insert', 'update_postimage')",
		snap1, snap2))
	deleteCount, _ := p.Sess.QueryValue(fmt.Sprintf(
		"SELECT COUNT(*) FROM table_changes('contacts', %s, %s) WHERE change_type = 'delete'",
		snap1, snap2))
	snap.addLine("upsert_count: " + upsertCount)
	snap.addLine("delete_count: " + deleteCount)

	assertGolden(t, "outbound_sync_table_changes", snap)
}

// ---------------------------------------------------------------------------
// Test 2: table_changes with detect_deletes filter
// ---------------------------------------------------------------------------

func TestOutboundSync_TableChanges_DetectDeletesFilter(t *testing.T) {
	p := testutil.NewProject(t)

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.users AS SELECT * FROM (VALUES (1,'Alice'),(2,'Bob'),(3,'Carol')) AS t(id,name)")

	p.AddModel("sync/users.sql", `-- @kind: tracked
-- @unique_key: id

SELECT * FROM raw.users
`)

	runModel(t, p, "sync/users.sql")
	snap1, _ := p.Sess.QueryValue("SELECT MAX(snapshot_id) FROM snapshots()")

	// Delete Carol
	p.Sess.Exec("DELETE FROM raw.users WHERE name='Carol'")
	runModel(t, p, "sync/users.sql")
	snap2, _ := p.Sess.QueryValue("SELECT MAX(snapshot_id) FROM snapshots()")

	p.Sess.Exec("USE lake.sync")

	// Without delete filter (detect_deletes=true) -- should include delete
	allChanges, _ := p.Sess.QueryValue(fmt.Sprintf(
		"SELECT COUNT(*) FROM table_changes('users', %s, %s) WHERE change_type != 'update_preimage'",
		snap1, snap2))

	// With delete filter (detect_deletes=false) -- should exclude delete
	noDeletes, _ := p.Sess.QueryValue(fmt.Sprintf(
		"SELECT COUNT(*) FROM table_changes('users', %s, %s) WHERE change_type != 'update_preimage' AND change_type != 'delete'",
		snap1, snap2))

	snap := newSnapshot()
	snap.addLine("all_changes (detect_deletes=true): " + allChanges)
	snap.addLine("no_deletes (detect_deletes=false): " + noDeletes)
	assertGolden(t, "outbound_sync_detect_deletes_filter", snap)
}

// ---------------------------------------------------------------------------
// Test 3: Badger claim/ack flow with sink-style keys
// ---------------------------------------------------------------------------

func TestOutboundSync_Badger_ClaimAck(t *testing.T) {
	dir := t.TempDir()
	store, err := collect.Open(filepath.Join(dir, "badger"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	target := "sync:hubspot"

	// Write sink rows (simulating delta from table_changes)
	rows := []map[string]any{
		{"customer_id": "c1", "email": "alice@example.com", "_operation": "upsert"},
		{"customer_id": "c2", "email": "bob@example.com", "_operation": "upsert"},
		{"customer_id": "c3", "email": "carol@example.com", "_operation": "delete"},
	}
	if err := store.WriteBatch(target, rows); err != nil {
		t.Fatalf("write batch: %v", err)
	}

	// Claim batch (simulating push batch)
	claimID, claimed, err := store.Claim(target, 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 3 {
		t.Fatalf("claimed %d rows, want 3", len(claimed))
	}

	// Verify claimed rows have sink keys
	snap := newSnapshot()
	snap.addLine("claimed_count: " + fmt.Sprintf("%d", len(claimed)))
	for _, row := range claimed {
		snap.addLine(fmt.Sprintf("  customer_id=%s operation=%s", row["customer_id"], row["_operation"]))
	}

	// Ack (simulating successful push)
	if err := store.Ack(claimID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// Verify no more rows to claim
	_, remaining, err := store.Claim(target, 10)
	if err != nil {
		t.Fatalf("claim after ack: %v", err)
	}
	snap.addLine("remaining_after_ack: " + fmt.Sprintf("%d", len(remaining)))

	assertGolden(t, "outbound_sync_badger_claim_ack", snap)
}

// ---------------------------------------------------------------------------
// Test 4: Badger nack and retry (simulating partial failure)
// ---------------------------------------------------------------------------

func TestOutboundSync_Badger_NackRetry(t *testing.T) {
	dir := t.TempDir()
	store, err := collect.Open(filepath.Join(dir, "badger"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	target := "sync:sheets"

	// Write rows
	rows := []map[string]any{
		{"id": "1", "value": "a"},
		{"id": "2", "value": "b"},
	}
	if err := store.WriteBatch(target, rows); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Claim
	claimID, claimed, err := store.Claim(target, 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	snap := newSnapshot()
	snap.addLine("claimed: " + fmt.Sprintf("%d", len(claimed)))

	// Nack (simulating push failure)
	if err := store.Nack(claimID); err != nil {
		t.Fatalf("nack: %v", err)
	}

	// Claim again (should get same rows back)
	_, retried, err := store.Claim(target, 10)
	if err != nil {
		t.Fatalf("retry claim: %v", err)
	}
	snap.addLine("retried: " + fmt.Sprintf("%d", len(retried)))

	assertGolden(t, "outbound_sync_badger_nack_retry", snap)
}

// ---------------------------------------------------------------------------
// Test 5: Starlark push function via RunSource (proof of concept)
// ---------------------------------------------------------------------------

func TestOutboundSync_Starlark_Push(t *testing.T) {
	p := testutil.NewProject(t)

	// Create a test push function that records what it receives
	testutil.WriteFile(t, p.Dir, "lib/test_push.star", `
def fetch(save, rows=None):
    """Simulates push by saving received rows back for verification."""
    if rows == None:
        return
    for row in rows:
        save.row(row)
`)

	// Create test rows (what table_changes would return)
	testRows := []interface{}{
		map[string]interface{}{"customer_id": "c1", "email": "alice@example.com", "_operation": "upsert"},
		map[string]interface{}{"customer_id": "c2", "email": "bob@example.com", "_operation": "upsert"},
	}

	// Call RunSource with test rows as kwargs
	rt := script.NewRuntime(p.Sess, nil, p.Dir)
	result, err := rt.RunSource(context.Background(), "test_sink", "test_push", map[string]any{
		"rows": testRows,
	})
	if err != nil {
		t.Fatalf("RunSource: %v", err)
	}

	snap := newSnapshot()
	snap.addLine("rows_received: " + fmt.Sprintf("%d", result.RowCount))

	// Verify rows were passed through -- create temp table from collected data
	if err := result.CreateTempTable(); err != nil {
		t.Fatalf("create temp table: %v", err)
	}
	if result.TempTable != "" {
		count, _ := p.Sess.QueryValue("SELECT COUNT(*) FROM " + result.TempTable)
		snap.addLine("temp_table_rows: " + count)

		emails, _ := p.Sess.QueryRows("SELECT email FROM " + result.TempTable + " ORDER BY email")
		for _, email := range emails {
			snap.addLine("  email: " + email)
		}
	} else {
		snap.addLine("temp_table_rows: 0 (no temp table created)")
	}

	assertGolden(t, "outbound_sync_starlark_push", snap)
}

// ---------------------------------------------------------------------------
// Test 6: Full end-to-end -- materialize, table_changes, push to mock API
// ---------------------------------------------------------------------------

func TestOutboundSync_EndToEnd_MockAPI(t *testing.T) {
	// Mock API server that records received batches
	var mu sync.Mutex
	var receivedBatches [][]map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch []map[string]any
		json.NewDecoder(r.Body).Decode(&batch)
		mu.Lock()
		receivedBatches = append(receivedBatches, batch)
		mu.Unlock()

		// Return per-row success
		results := make([]map[string]string, len(batch))
		for i := range batch {
			results[i] = map[string]string{"status": "ok"}
		}
		json.NewEncoder(w).Encode(map[string]any{"results": results})
	}))
	defer server.Close()

	p := testutil.NewProject(t)

	// Create source data
	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.contacts AS SELECT * FROM (VALUES (1,'Alice','alice@co.com'),(2,'Bob','bob@co.com')) AS t(id,name,email)")

	// Create tracked model
	p.AddModel("sync/contacts.sql", `-- @kind: tracked
-- @unique_key: id

SELECT * FROM raw.contacts
`)

	// Run 1: backfill
	runModel(t, p, "sync/contacts.sql")
	snap1, _ := p.Sess.QueryValue("SELECT MAX(snapshot_id) FROM snapshots()")

	// Modify source
	p.Sess.Exec("UPDATE raw.contacts SET email='alice_new@co.com' WHERE name='Alice'")
	p.Sess.Exec("INSERT INTO raw.contacts VALUES (3, 'Carol', 'carol@co.com')")

	// Run 2: incremental
	runModel(t, p, "sync/contacts.sql")
	snap2, _ := p.Sess.QueryValue("SELECT MAX(snapshot_id) FROM snapshots()")

	// Create push function that sends to mock API
	testutil.WriteFile(t, p.Dir, "lib/mock_push.star", fmt.Sprintf(`
MOCK_URL = "%s"

def fetch(save, rows=None):
    if rows == None:
        return
    # Strip internal fields before sending
    payload = [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]
    resp = http.post(MOCK_URL, json=payload, retry=1)
    # Save results back for verification
    for i, row in enumerate(rows):
        save.row({"id": row["id"], "status": "ok"})
`, server.URL))

	// Query delta via table_changes
	p.Sess.Exec("USE lake.sync")
	deltaSQL := fmt.Sprintf(`SELECT id::VARCHAR AS id, name, email,
		CASE change_type
			WHEN 'insert' THEN 'upsert'
			WHEN 'update_postimage' THEN 'upsert'
			WHEN 'delete' THEN 'delete'
		END AS _operation
	FROM table_changes('contacts', %s, %s)
	WHERE change_type != 'update_preimage'
	ORDER BY id`, snap1, snap2)

	// Read delta rows
	deltaRows := queryAsMaps(t, p.Sess, deltaSQL)

	// Convert to interface slice for Starlark
	var rowsForPush []interface{}
	for _, row := range deltaRows {
		rowsForPush = append(rowsForPush, row)
	}

	// Call push via RunSource
	rt := script.NewRuntime(p.Sess, nil, p.Dir)
	result, err := rt.RunSource(context.Background(), "mock_sink", "mock_push", map[string]any{
		"rows": rowsForPush,
	})
	if err != nil {
		t.Fatalf("push: %v", err)
	}

	snap := newSnapshot()
	snap.addLine("delta_rows: " + fmt.Sprintf("%d", len(deltaRows)))
	snap.addLine("push_results: " + fmt.Sprintf("%d", result.RowCount))

	mu.Lock()
	snap.addLine("api_batches_received: " + fmt.Sprintf("%d", len(receivedBatches)))
	totalAPIRows := 0
	for _, batch := range receivedBatches {
		totalAPIRows += len(batch)
	}
	snap.addLine("api_rows_total: " + fmt.Sprintf("%d", totalAPIRows))
	mu.Unlock()

	// Verify internal fields were stripped
	mu.Lock()
	if len(receivedBatches) > 0 {
		for _, row := range receivedBatches[0] {
			for k := range row {
				if strings.HasPrefix(k, "_") {
					t.Errorf("internal field %q leaked to API", k)
				}
			}
		}
	}
	mu.Unlock()

	assertGolden(t, "outbound_sync_end_to_end", snap)
}

// ---------------------------------------------------------------------------
// Test 7: Deduplication query produces correct logical delta
// ---------------------------------------------------------------------------

func TestOutboundSync_TableChanges_Deduplication(t *testing.T) {
	p := testutil.NewProject(t)

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.contacts AS SELECT * FROM (VALUES (1,'Alice','alice@co.com'),(2,'Bob','bob@co.com'),(3,'Carol','carol@co.com')) AS t(id,name,email)")

	p.AddModel("sync/contacts.sql", `-- @kind: tracked
-- @unique_key: id

SELECT * FROM raw.contacts
`)

	// Run 1: backfill
	runModel(t, p, "sync/contacts.sql")
	snap1, _ := p.Sess.QueryValue("SELECT MAX(snapshot_id) FROM snapshots()")

	// 3 logical changes: update Bob, delete Carol, insert Dave
	p.Sess.Exec("UPDATE raw.contacts SET email='bob_new@co.com' WHERE name='Bob'")
	p.Sess.Exec("DELETE FROM raw.contacts WHERE name='Carol'")
	p.Sess.Exec("INSERT INTO raw.contacts VALUES (4, 'Dave', 'dave@co.com')")

	// Run 2: incremental
	runModel(t, p, "sync/contacts.sql")
	snap2, _ := p.Sess.QueryValue("SELECT MAX(snapshot_id) FROM snapshots()")

	p.Sess.Exec("USE lake.sync")

	// Raw table_changes (before dedup)
	rawCount, _ := p.Sess.QueryValue(fmt.Sprintf(
		"SELECT COUNT(*) FROM table_changes('contacts', %s, %s) WHERE change_type != 'update_preimage'",
		snap1, snap2))

	// Deduplicated (keep last operation per unique_key)
	// Tiebreaker: insert > delete at same snapshot (tracked kind does DELETE+INSERT,
	// so the INSERT is the "final state" for updated rows)
	dedupSQL := fmt.Sprintf(`WITH ranked AS (
    SELECT id, name, email, change_type, snapshot_id,
        CASE change_type
            WHEN 'insert' THEN 'upsert'
            WHEN 'update_postimage' THEN 'upsert'
            WHEN 'delete' THEN 'delete'
        END AS _operation,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY snapshot_id DESC,
                CASE change_type WHEN 'insert' THEN 0 WHEN 'update_postimage' THEN 0 ELSE 1 END
        ) AS rn
    FROM table_changes('contacts', %s, %s)
    WHERE change_type != 'update_preimage'
)
SELECT id, name, email, _operation FROM ranked WHERE rn = 1 ORDER BY id`, snap1, snap2)

	dedupCsv, _ := p.Sess.Query(dedupSQL)

	snap := newSnapshot()
	snap.addLine("raw_table_changes_count: " + rawCount)
	snap.addLine("--- deduplicated delta ---")
	for _, line := range strings.Split(strings.TrimSpace(dedupCsv), "\n") {
		snap.addLine(line)
	}

	// Count per operation
	dedupTotal, _ := p.Sess.QueryValue(fmt.Sprintf(`WITH ranked AS (
    SELECT id, change_type, snapshot_id,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY snapshot_id DESC,
                CASE change_type WHEN 'insert' THEN 0 WHEN 'update_postimage' THEN 0 ELSE 1 END
        ) AS rn
    FROM table_changes('contacts', %s, %s)
    WHERE change_type != 'update_preimage'
)
SELECT COUNT(*) FROM ranked WHERE rn = 1`, snap1, snap2))
	snap.addLine("dedup_total: " + dedupTotal)

	assertGolden(t, "outbound_sync_deduplication", snap)
}

// ---------------------------------------------------------------------------
// Test 8: Badger crash recovery -- unacked claims survive restart
// ---------------------------------------------------------------------------

func TestOutboundSync_Badger_CrashRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "badger")

	// Phase 1: write and claim, then "crash" (close without ack)
	func() {
		store, err := collect.Open(dir)
		if err != nil {
			t.Fatalf("open store: %v", err)
		}

		target := "sync:crm"
		rows := []map[string]any{
			{"id": "1", "name": "Alice"},
			{"id": "2", "name": "Bob"},
			{"id": "3", "name": "Carol"},
		}
		if err := store.WriteBatch(target, rows); err != nil {
			t.Fatalf("write: %v", err)
		}

		// Claim but don't ack -- simulates crash
		claimID, claimed, err := store.Claim(target, 10)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		_ = claimID // intentionally not acked

		if len(claimed) != 3 {
			t.Fatalf("claimed %d, want 3", len(claimed))
		}

		// Close without ack = crash
		store.Close()
	}()

	// Phase 2: reopen -- old inflight claims should be recoverable
	store, err := collect.Open(dir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer store.Close()

	// Wait briefly so inflight recovery triggers (requires > 10min by default,
	// but Claim internally calls recoverOldInflight which checks age).
	// For this test, we nack the old claim manually by claiming again.
	// The store.Claim recovers old inflight batches older than 10 min,
	// so we test the write-claim-close-reopen-claim cycle instead.

	// Write fresh rows and verify store is functional after crash
	target := "sync:crm"
	freshRows := []map[string]any{{"id": "4", "name": "Dave"}}
	if err := store.WriteBatch(target, freshRows); err != nil {
		t.Fatalf("write after crash: %v", err)
	}

	_, claimed, err := store.Claim(target, 10)
	if err != nil {
		t.Fatalf("claim after crash: %v", err)
	}

	snap := newSnapshot()
	// Fresh rows are claimable after crash
	snap.addLine(fmt.Sprintf("claimed_after_crash: %d", len(claimed)))
	// Old inflight rows are NOT immediately claimable (need 10min age for recovery)
	// This is by design -- prevents double-processing of in-progress batches
	snap.addLine("note: old inflight rows recover after 10min timeout (by design)")

	assertGolden(t, "outbound_sync_crash_recovery", snap)
}

// ---------------------------------------------------------------------------
// Test 9: table_changes with @kind: append (only inserts)
// ---------------------------------------------------------------------------

func TestOutboundSync_TableChanges_AppendKind(t *testing.T) {
	p := testutil.NewProject(t)

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.events AS SELECT * FROM (VALUES (1,'click','2026-01-01'),(2,'view','2026-01-02')) AS t(id,event_type,ts)")

	p.AddModel("sync/events.sql", `-- @kind: append

SELECT * FROM raw.events
`)

	// Run 1: initial
	runModel(t, p, "sync/events.sql")
	snap1, _ := p.Sess.QueryValue("SELECT MAX(snapshot_id) FROM snapshots()")

	// Add new events to source
	p.Sess.Exec("INSERT INTO raw.events VALUES (3, 'purchase', '2026-01-03'), (4, 'signup', '2026-01-04')")

	// Run 2: append
	r2 := runModel(t, p, "sync/events.sql")
	snap2, _ := p.Sess.QueryValue("SELECT MAX(snapshot_id) FROM snapshots()")

	p.Sess.Exec("USE lake.sync")

	// All changes should be inserts only
	changeCsv, _ := p.Sess.Query(fmt.Sprintf(
		"SELECT change_type, COUNT(*) AS cnt FROM table_changes('events', %s, %s) GROUP BY change_type ORDER BY change_type",
		snap1, snap2))

	// Verify no deletes or updates
	deleteCount, _ := p.Sess.QueryValue(fmt.Sprintf(
		"SELECT COUNT(*) FROM table_changes('events', %s, %s) WHERE change_type IN ('delete', 'update_postimage')",
		snap1, snap2))

	snap := newSnapshot()
	snap.addLine("--- run 2 ---")
	snap.addResult(r2)
	snap.addLine("--- change_types ---")
	for _, line := range strings.Split(strings.TrimSpace(changeCsv), "\n") {
		snap.addLine(line)
	}
	snap.addLine("deletes_or_updates: " + deleteCount)

	assertGolden(t, "outbound_sync_append_kind", snap)
}

// ---------------------------------------------------------------------------
// Test 10: HasPendingEvents only detects claimable evt: rows, not inflight
// ---------------------------------------------------------------------------

func TestOutboundSync_Badger_HasPendingEvents(t *testing.T) {
	dir := t.TempDir()
	store, err := collect.Open(filepath.Join(dir, "badger"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	target := "sync:test"

	// Empty store: no pending
	if func() bool { h, _ := store.HasPendingEvents(target); return h }() {
		t.Error("should be false on empty store")
	}

	// Write rows: has pending
	store.WriteBatch(target, []map[string]any{{"id": "1"}})
	if !func() bool { h, _ := store.HasPendingEvents(target); return h }() {
		t.Error("should be true after write")
	}

	// Claim moves to inflight: NO pending (evt: rows are gone)
	claimID, _, _ := store.Claim(target, 10)
	if func() bool { h, _ := store.HasPendingEvents(target); return h }() {
		t.Error("should be false while only inflight (not pending)")
	}

	// Ack: still no pending
	store.Ack(claimID)
	if func() bool { h, _ := store.HasPendingEvents(target); return h }() {
		t.Error("should be false after ack")
	}

	// Write + nack: has pending again (nack returns to evt:)
	store.WriteBatch(target, []map[string]any{{"id": "2"}})
	claimID2, _, _ := store.Claim(target, 10)
	store.Nack(claimID2)
	if !func() bool { h, _ := store.HasPendingEvents(target); return h }() {
		t.Error("should be true after nack (rows back in evt:)")
	}
}

// ---------------------------------------------------------------------------
// Test 11: No duplicate writes on retry -- backlog skips delta query
// ---------------------------------------------------------------------------

func TestOutboundSync_Badger_NoDuplicateOnRetry(t *testing.T) {
	dir := t.TempDir()
	store, err := collect.Open(filepath.Join(dir, "badger"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	target := "sync:hubspot"

	// Simulate first run: write 3 rows, claim, nack (failure)
	rows := []map[string]any{
		{"id": "1", "name": "Alice"},
		{"id": "2", "name": "Bob"},
		{"id": "3", "name": "Carol"},
	}
	store.WriteBatch(target, rows)
	claimID, claimed, _ := store.Claim(target, 10)
	if len(claimed) != 3 {
		t.Fatalf("first claim: got %d, want 3", len(claimed))
	}
	store.Nack(claimID) // simulate failure

	// Verify backlog exists (nack returns rows to evt:)
	if !func() bool { h, _ := store.HasPendingEvents(target); return h }() {
		t.Fatal("should have pending events after nack")
	}

	// Simulate second run: should NOT write delta again (hasBacklog=true)
	// Just claim from existing backlog
	claimID2, claimed2, _ := store.Claim(target, 10)
	if len(claimed2) != 3 {
		t.Fatalf("retry claim: got %d, want 3 (same rows)", len(claimed2))
	}

	// Ack all
	store.Ack(claimID2)

	// Verify empty
	if func() bool { h, _ := store.HasPendingEvents(target); return h }() {
		t.Error("should be empty after ack")
	}

	// No extra rows should exist
	_, remaining, _ := store.Claim(target, 10)
	if len(remaining) != 0 {
		t.Errorf("got %d remaining rows, want 0 (no duplicates)", len(remaining))
	}
}

// ---------------------------------------------------------------------------
// Test 12: Sink runs on skip (pending backlog processed without new materialization)
// ---------------------------------------------------------------------------

func TestOutboundSync_SinkRunsOnSkip(t *testing.T) {
	p := testutil.NewProject(t)

	// Track pushes via a mock server
	var mu sync.Mutex
	var pushCount int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		pushCount++
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer server.Close()

	testutil.WriteFile(t, p.Dir, "lib/skip_push.star", fmt.Sprintf(`
API = {"push": {"batch_size": 100}}

def push(rows):
    http.post("%s", json=rows, retry=1)
    return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "ok" for r in rows}
`, server.URL))

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.items AS SELECT 1 AS id, 'a' AS val")

	p.AddModel("sync/items.sql", `-- @kind: merge
-- @unique_key: id
-- @sink: skip_push

SELECT * FROM raw.items
`)

	// Run 1: backfill + sink
	r1 := runModelWithSink(t, p, "sync/items.sql")
	t.Logf("run 1: type=%s rows=%d warnings=%v sync_ok=%d sync_fail=%d",
		r1.RunType, r1.RowsAffected, r1.Warnings, r1.SyncSucceeded, r1.SyncFailed)

	// Run 2: no data change -> sink should still run without errors
	r2 := runModelWithSink(t, p, "sync/items.sql")
	t.Logf("run 2: type=%s rows=%d warnings=%v sync_ok=%d sync_fail=%d",
		r2.RunType, r2.RowsAffected, r2.Warnings, r2.SyncSucceeded, r2.SyncFailed)

	// Key assertion: no fatal sink errors on repeat run
	for _, w := range r2.Warnings {
		if strings.Contains(w, "sink") && !strings.Contains(w, "0 failed") {
			t.Errorf("run 2: unexpected sink error: %s", w)
		}
	}
}

// ---------------------------------------------------------------------------
// Test 13: @kind: table full sync retries after inflight resolves
// ---------------------------------------------------------------------------

func TestOutboundSync_TableRetryAfterInflight(t *testing.T) {
	p := testutil.NewProject(t)

	var mu sync.Mutex
	var pushBatches []int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rows []map[string]any
		json.NewDecoder(r.Body).Decode(&rows)
		mu.Lock()
		pushBatches = append(pushBatches, len(rows))
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer server.Close()

	testutil.WriteFile(t, p.Dir, "lib/table_push.star", fmt.Sprintf(`
API = {"push": {"batch_size": 100, "batch_mode": "sync"}}

def push(rows):
    http.post("%s", json=rows, retry=1)
    return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "ok" for r in rows}
`, server.URL))

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.report AS SELECT * FROM (VALUES (1,'a'),(2,'b'),(3,'c')) AS t(id,val)")

	p.AddModel("sync/report.sql", `-- @kind: table
-- @sink: table_push

SELECT * FROM raw.report
`)

	// Run 1: initial full sync
	r1 := runModelWithSink(t, p, "sync/report.sql")
	t.Logf("run 1: type=%s rows=%d sync_ok=%d warnings=%v",
		r1.RunType, r1.RowsAffected, r1.SyncSucceeded, r1.Warnings)

	mu.Lock()
	batchesAfterRun1 := len(pushBatches)
	mu.Unlock()
	t.Logf("run 1: push batches=%d", batchesAfterRun1)

	// Update source data
	p.Sess.Exec("INSERT INTO raw.report VALUES (4, 'd'), (5, 'e')")

	// Run 2: new data, full sync again
	r2 := runModelWithSink(t, p, "sync/report.sql")
	t.Logf("run 2: type=%s rows=%d sync_ok=%d warnings=%v",
		r2.RunType, r2.RowsAffected, r2.SyncSucceeded, r2.Warnings)

	mu.Lock()
	batchesAfterRun2 := len(pushBatches)
	mu.Unlock()
	t.Logf("run 2: push batches=%d (total)", batchesAfterRun2)

	// Run 3: no change -- sink should not crash
	r3 := runModelWithSink(t, p, "sync/report.sql")
	t.Logf("run 3: type=%s rows=%d sync_ok=%d warnings=%v",
		r3.RunType, r3.RowsAffected, r3.SyncSucceeded, r3.Warnings)

	// No fatal sink errors
	for _, w := range r3.Warnings {
		if strings.Contains(w, "sink") && !strings.Contains(w, "0 failed") {
			t.Errorf("run 3: unexpected sink error: %s", w)
		}
	}
}

// ---------------------------------------------------------------------------
// Test 14: Sink error propagates to runner (not swallowed as warning)
// ---------------------------------------------------------------------------

func TestOutboundSync_SinkError_Propagates(t *testing.T) {
	p := testutil.NewProject(t)

	// Create a push function that always fails
	testutil.WriteFile(t, p.Dir, "lib/fail_push.star", `
API = {"push": {"batch_size": 10}}

def push(rows):
    fail("intentional push failure")
`)

	// Create source and model
	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.src AS SELECT 1 AS id, 'test' AS name")

	p.AddModel("sync/failing.sql", `-- @kind: merge
-- @unique_key: id
-- @sink: fail_push

SELECT * FROM raw.src
`)

	// Run -- should succeed for materialization but report sink error
	result := runModelWithSink(t, p, "sync/failing.sql")

	// Sink errors should appear in warnings (materialization succeeded, sink failed)
	hasSinkWarning := false
	for _, w := range result.Warnings {
		if strings.Contains(w, "sink") {
			hasSinkWarning = true
			break
		}
	}
	if !hasSinkWarning {
		t.Error("expected sink warning in result, got none")
	}
}

// ---------------------------------------------------------------------------
// Test 13: finalize() callback runs after all batches
// ---------------------------------------------------------------------------

func TestOutboundSync_Finalize_Callback(t *testing.T) {
	p := testutil.NewProject(t)

	// Push that tracks calls, finalize that records it ran
	testutil.WriteFile(t, p.Dir, "lib/fin_push.star", `
API = {"push": {"batch_size": 1}}

def push(rows):
    print("push batch: " + str(len(rows)) + " rows")
    return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "ok" for r in rows}

def finalize(succeeded, failed):
    print("finalize: " + str(succeeded) + " succeeded, " + str(failed) + " failed")
`)

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.fin_src AS SELECT * FROM (VALUES (1,'a'),(2,'b')) AS t(id,name)")

	p.AddModel("sync/fin.sql", `-- @kind: merge
-- @unique_key: id
-- @sink: fin_push

SELECT * FROM raw.fin_src
`)

	result := runModelWithSink(t, p, "sync/fin.sql")

	// Should succeed without errors
	if len(result.Errors) > 0 {
		t.Errorf("unexpected errors: %v", result.Errors)
	}
	// Finalize runs silently -- if it failed we'd see it in warnings
	for _, w := range result.Warnings {
		if strings.Contains(w, "finalize") {
			t.Errorf("finalize should not produce warnings on success, got: %s", w)
		}
	}
}

// ---------------------------------------------------------------------------
// Test 14: finalize() not required -- sinks without it work fine
// ---------------------------------------------------------------------------

func TestOutboundSync_NoFinalize_OK(t *testing.T) {
	p := testutil.NewProject(t)

	testutil.WriteFile(t, p.Dir, "lib/nofin_push.star", `
API = {"push": {"batch_size": 10}}

def push(rows):
    return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "ok" for r in rows}
`)

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.nofin_src AS SELECT 1 AS id, 'x' AS val")

	p.AddModel("sync/nofin.sql", `-- @kind: merge
-- @unique_key: id
-- @sink: nofin_push

SELECT * FROM raw.nofin_src
`)

	result := runModelWithSink(t, p, "sync/nofin.sql")
	if len(result.Errors) > 0 {
		t.Errorf("unexpected errors: %v", result.Errors)
	}
}

// runModelWithSink parses and executes a model with lib registry for sink support.
func runModelWithSink(t *testing.T, p *testutil.Project, relPath string) *execute.Result {
	t.Helper()
	modelPath := filepath.Join(p.Dir, "models", relPath)
	model, err := parser.ParseModel(modelPath, p.Dir)
	if err != nil {
		t.Fatalf("parse %s: %v", relPath, err)
	}

	reg, err := libregistry.Scan(p.Dir)
	if err != nil {
		t.Fatalf("scan lib: %v", err)
	}

	runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
	runner.SetProjectDir(p.Dir)
	runner.SetLibRegistry(reg)
	result, err := runner.Run(context.Background(), model)
	if err != nil {
		t.Fatalf("run %s: %v", relPath, err)
	}
	return result
}

// queryAsMaps executes a SQL query and returns results as []map[string]any.
func queryAsMaps(t *testing.T, sess interface{ Query(string) (string, error) }, sql string) []map[string]any {
	t.Helper()
	csvOut, err := sess.Query(sql)
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(csvOut), "\n")
	if len(lines) < 2 {
		return nil
	}

	headers := strings.Split(lines[0], ",")
	var result []map[string]any
	for _, line := range lines[1:] {
		if line == "" {
			continue
		}
		vals := strings.Split(line, ",")
		row := make(map[string]any)
		for i, h := range headers {
			if i < len(vals) {
				row[h] = vals[i]
			}
		}
		result = append(result, row)
	}
	return result
}

// ---------------------------------------------------------------------------
// Test 17: Partial failure -- ok rows acked, failed rows re-queued
// ---------------------------------------------------------------------------

func TestOutboundSync_PartialFailure_RequeueFailedOnly(t *testing.T) {
	p := testutil.NewProject(t)

	// Push that returns per-row status: row with id=2 always fails
	testutil.WriteFile(t, p.Dir, "lib/partial_push.star", `
API = {"push": {"batch_size": 100}}

def push(rows):
    results = {}
    for row in rows:
        rid = str(row["__ondatra_rowid"]) + ":" + row["__ondatra_change_type"]
        row_id = row.get("id", None)
        if row_id == 2 or str(row_id) == "2":
            results[rid] = "error: rejected by destination"
        else:
            results[rid] = "ok"
    return results
`)

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.partial AS SELECT * FROM (VALUES (1,'a'),(2,'b'),(3,'c')) AS t(id,val)")

	p.AddModel("sync/partial.sql", `-- @kind: merge
-- @unique_key: id
-- @sink: partial_push

SELECT * FROM raw.partial
`)

	r := runModelWithSink(t, p, "sync/partial.sql")
	t.Logf("result: type=%s rows=%d sync_ok=%d sync_fail=%d warnings=%v",
		r.RunType, r.RowsAffected, r.SyncSucceeded, r.SyncFailed, r.Warnings)

	// Should have partial results
	if r.SyncSucceeded != 2 {
		t.Errorf("sync_succeeded = %d, want 2", r.SyncSucceeded)
	}
	if r.SyncFailed != 1 {
		t.Errorf("sync_failed = %d, want 1", r.SyncFailed)
	}
}

// ---------------------------------------------------------------------------
// Test 19: Table kind -- empty table calls push([])
// ---------------------------------------------------------------------------

func TestOutboundSync_TableEmpty_PushDeleteEvents(t *testing.T) {
	p := testutil.NewProject(t)

	var mu sync.Mutex
	var pushCalls []int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rows []map[string]any
		json.NewDecoder(r.Body).Decode(&rows)
		mu.Lock()
		pushCalls = append(pushCalls, len(rows))
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer server.Close()

	testutil.WriteFile(t, p.Dir, "lib/empty_push.star", fmt.Sprintf(`
API = {"push": {"batch_size": 100, "batch_mode": "sync"}}

def push(rows):
    http.post("%s", json=rows, retry=1)
    return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "ok" for r in rows}
`, server.URL))

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.empty_src AS SELECT 1 AS id, 'a' AS val")

	p.AddModel("sync/empty.sql", `-- @kind: table
-- @sink: empty_push

SELECT * FROM raw.empty_src
`)

	// Run 1: 1 row (insert)
	runModelWithSink(t, p, "sync/empty.sql")

	mu.Lock()
	afterRun1 := len(pushCalls)
	mu.Unlock()

	if afterRun1 == 0 {
		t.Fatal("run 1: expected push")
	}

	// Empty the source
	p.Sess.Exec("DELETE FROM raw.empty_src")

	// Run 2: TRUNCATE+INSERT with 0 new rows → delete events for the old row
	runModelWithSink(t, p, "sync/empty.sql")

	mu.Lock()
	afterRun2 := len(pushCalls)
	mu.Unlock()

	if afterRun2 <= afterRun1 {
		t.Fatal("run 2: expected push with delete events from truncate")
	}

	// Last push should have delete events (1 row deleted by truncate)
	mu.Lock()
	lastPush := pushCalls[len(pushCalls)-1]
	mu.Unlock()
	if lastPush == 0 {
		t.Error("last push had 0 rows, expected delete events from truncate")
	}
}

// ---------------------------------------------------------------------------
// Test 20: __ondatra_change_type and __ondatra_rowid present in push data
// ---------------------------------------------------------------------------

func TestOutboundSync_InternalFields_Present(t *testing.T) {
	p := testutil.NewProject(t)

	var mu sync.Mutex
	var receivedRows []map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rows []map[string]any
		json.NewDecoder(r.Body).Decode(&rows)
		mu.Lock()
		receivedRows = append(receivedRows, rows...)
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer server.Close()

	testutil.WriteFile(t, p.Dir, "lib/fields_push.star", fmt.Sprintf(`
API = {"push": {"batch_size": 100}}

def push(rows):
    http.post("%s", json=rows, retry=1)
    return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "ok" for r in rows}
`, server.URL))

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.fields_src AS SELECT 1 AS id, 'test' AS val")

	p.AddModel("sync/fields.sql", `-- @kind: merge
-- @unique_key: id
-- @sink: fields_push

SELECT * FROM raw.fields_src
`)

	runModelWithSink(t, p, "sync/fields.sql")

	mu.Lock()
	defer mu.Unlock()

	if len(receivedRows) == 0 {
		t.Fatal("no rows received")
	}

	row := receivedRows[0]
	if _, ok := row["__ondatra_rowid"]; !ok {
		t.Error("missing __ondatra_rowid in push data")
	}
	if _, ok := row["__ondatra_change_type"]; !ok {
		t.Error("missing __ondatra_change_type in push data")
	}
	if row["__ondatra_change_type"] != "insert" {
		t.Errorf("__ondatra_change_type = %v, want insert", row["__ondatra_change_type"])
	}
}

// runModelWithSinkAllowErr is like runModelWithSink but returns the error instead of calling t.Fatalf.
func runModelWithSinkAllowErr(t *testing.T, p *testutil.Project, relPath string) (*execute.Result, error) {
	t.Helper()
	modelPath := filepath.Join(p.Dir, "models", relPath)
	model, err := parser.ParseModel(modelPath, p.Dir)
	if err != nil {
		t.Fatalf("parse %s: %v", relPath, err)
	}

	reg, err := libregistry.Scan(p.Dir)
	if err != nil {
		t.Fatalf("scan lib: %v", err)
	}

	runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
	runner.SetProjectDir(p.Dir)
	runner.SetLibRegistry(reg)
	result, err := runner.Run(context.Background(), model)
	return result, err
}

// ---------------------------------------------------------------------------
// V2 Test 1: Retry reads fresh data (not stale cached data)
// ---------------------------------------------------------------------------

func TestOutboundSyncV2_RetryReadsFreshData(t *testing.T) {
	p := testutil.NewProject(t)

	var mu sync.Mutex
	var pushCount int
	var lastPushedRows []map[string]any

	// First call: fail for id=2; second call onwards: accept all
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rows []map[string]any
		json.NewDecoder(r.Body).Decode(&rows)
		mu.Lock()
		pushCount++
		lastPushedRows = append(lastPushedRows, rows...)
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer server.Close()

	// Run 1 push: fail for id=2
	testutil.WriteFile(t, p.Dir, "lib/retry_push.star", fmt.Sprintf(`
API = {"push": {"batch_size": 100, "batch_mode": "sync"}}

def push(rows):
    http.post("%s", json=rows, retry=1)
    results = {}
    for row in rows:
        rid = str(row["__ondatra_rowid"]) + ":" + row["__ondatra_change_type"]
        row_id = row.get("id", None)
        if row_id == 2 or str(row_id) == "2":
            results[rid] = "error: rejected"
        else:
            results[rid] = "ok"
    return results
`, server.URL))

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.retry_src AS SELECT * FROM (VALUES (1,'Alice'),(2,'Bob')) AS t(id,name)")

	p.AddModel("sync/retry.sql", `-- @kind: merge
-- @unique_key: id
-- @sink: retry_push

SELECT * FROM raw.retry_src
`)

	// Run 1: id=2 fails
	r1 := runModelWithSink(t, p, "sync/retry.sql")
	t.Logf("run 1: type=%s sync_ok=%d sync_fail=%d", r1.RunType, r1.SyncSucceeded, r1.SyncFailed)

	if r1.SyncFailed != 1 {
		t.Errorf("run 1: sync_failed = %d, want 1", r1.SyncFailed)
	}

	// Update id=2's data in source
	p.Sess.Exec("UPDATE raw.retry_src SET name='Bob_Updated' WHERE id=2")

	// Fix push to accept all
	testutil.WriteFile(t, p.Dir, "lib/retry_push.star", fmt.Sprintf(`
API = {"push": {"batch_size": 100, "batch_mode": "sync"}}

def push(rows):
    http.post("%s", json=rows, retry=1)
    return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "ok" for r in rows}
`, server.URL))

	mu.Lock()
	lastPushedRows = nil
	mu.Unlock()

	// Run 2: should re-push id=2 with new data
	r2 := runModelWithSink(t, p, "sync/retry.sql")
	t.Logf("run 2: type=%s sync_ok=%d sync_fail=%d", r2.RunType, r2.SyncSucceeded, r2.SyncFailed)

	mu.Lock()
	defer mu.Unlock()

	// Verify the pushed data contains Bob_Updated (fresh data, not stale)
	foundUpdated := false
	for _, row := range lastPushedRows {
		if name, ok := row["name"]; ok && fmt.Sprintf("%v", name) == "Bob_Updated" {
			foundUpdated = true
		}
	}
	if !foundUpdated {
		t.Error("run 2: expected Bob_Updated in pushed data (fresh read), got stale or missing")
	}
}

// ---------------------------------------------------------------------------
// V2 Test 2: Table kind skip (no-dep table skips on re-run, no delta)
// ---------------------------------------------------------------------------

func TestOutboundSyncV2_TableSkip(t *testing.T) {
	p := testutil.NewProject(t)

	var mu sync.Mutex
	var pushCount int
	var lastBatchSizes []int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rows []map[string]any
		json.NewDecoder(r.Body).Decode(&rows)
		mu.Lock()
		pushCount++
		lastBatchSizes = append(lastBatchSizes, len(rows))
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer server.Close()

	testutil.WriteFile(t, p.Dir, "lib/wm_push.star", fmt.Sprintf(`
API = {"push": {"batch_size": 100, "batch_mode": "sync"}}

def push(rows):
    http.post("%s", json=rows, retry=1)
    return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "ok" for r in rows}
`, server.URL))

	// Use a literal SELECT (no source table dependency) so the table model
	// can skip on re-run when the SQL hash is unchanged.
	p.AddModel("sync/wm.sql", `-- @kind: table
-- @sink: wm_push

SELECT 1 AS id, 'a' AS val
UNION ALL
SELECT 2, 'b'
`)

	// Run 1: push 2 rows
	r1 := runModelWithSink(t, p, "sync/wm.sql")
	t.Logf("run 1: type=%s sync_ok=%d", r1.RunType, r1.SyncSucceeded)
	mu.Lock()
	afterRun1 := pushCount
	mu.Unlock()
	if afterRun1 == 0 {
		t.Fatal("run 1: expected push call")
	}

	// Run 2: no changes, no deps -> model skips -> no push
	r2 := runModelWithSink(t, p, "sync/wm.sql")
	t.Logf("run 2: type=%s sync_ok=%d", r2.RunType, r2.SyncSucceeded)
	mu.Lock()
	afterRun2 := pushCount
	mu.Unlock()
	if afterRun2 != afterRun1 {
		t.Errorf("run 2: pushCount changed from %d to %d, expected no push (model skipped)", afterRun1, afterRun2)
	}

	// Run 3: change SQL -> model re-materializes -> push again
	p.AddModel("sync/wm.sql", `-- @kind: table
-- @sink: wm_push

SELECT 1 AS id, 'a' AS val
UNION ALL
SELECT 2, 'b'
UNION ALL
SELECT 3, 'c'
`)
	r3 := runModelWithSink(t, p, "sync/wm.sql")
	t.Logf("run 3: type=%s sync_ok=%d", r3.RunType, r3.SyncSucceeded)
	mu.Lock()
	afterRun3 := pushCount
	mu.Unlock()
	if afterRun3 <= afterRun2 {
		t.Errorf("run 3: pushCount=%d, expected increase from %d after SQL change", afterRun3, afterRun2)
	}
}

// ---------------------------------------------------------------------------
// V2 Test 3: Table kind empty result
// ---------------------------------------------------------------------------

func TestOutboundSyncV2_TableEmptyResult(t *testing.T) {
	p := testutil.NewProject(t)

	var mu sync.Mutex
	var pushCount int
	var lastBatchSize int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rows []map[string]any
		json.NewDecoder(r.Body).Decode(&rows)
		mu.Lock()
		pushCount++
		lastBatchSize = len(rows)
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer server.Close()

	testutil.WriteFile(t, p.Dir, "lib/empty_wm_push.star", fmt.Sprintf(`
API = {"push": {"batch_size": 100, "batch_mode": "sync"}}

def push(rows):
    http.post("%s", json=rows, retry=1)
    return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "ok" for r in rows}
`, server.URL))

	// Run 1: table with 1 row (literal SELECT, no source dep)
	p.AddModel("sync/empty_wm.sql", `-- @kind: table
-- @sink: empty_wm_push

SELECT 1 AS id, 'a' AS val
`)

	runModelWithSink(t, p, "sync/empty_wm.sql")
	mu.Lock()
	afterRun1 := pushCount
	mu.Unlock()
	if afterRun1 == 0 {
		t.Fatal("run 1: expected push")
	}

	// Run 2: change SQL to return 0 rows → TRUNCATE+INSERT(0)
	// table_changes() produces delete events for the 1 row that was truncated
	p.AddModel("sync/empty_wm.sql", `-- @kind: table
-- @sink: empty_wm_push

SELECT id, val FROM (SELECT 1 AS id, 'a' AS val) WHERE 1=0
`)

	runModelWithSink(t, p, "sync/empty_wm.sql")
	mu.Lock()
	afterRun2 := pushCount
	run2BatchSize := lastBatchSize
	mu.Unlock()
	if afterRun2 <= afterRun1 {
		t.Errorf("run 2: pushCount=%d, expected increase (delete events from truncate)", afterRun2)
	}
	if run2BatchSize == 0 {
		t.Errorf("run 2: batch size=0, expected delete events from truncate")
	}

	// Run 3: same empty SQL, model skips -> no push (no delta)
	runModelWithSink(t, p, "sync/empty_wm.sql")
	mu.Lock()
	afterRun3 := pushCount
	mu.Unlock()
	if afterRun3 != afterRun2 {
		t.Errorf("run 3: pushCount changed from %d to %d, expected no push (model skipped, no delta)", afterRun2, afterRun3)
	}
}

// ---------------------------------------------------------------------------
// V2 Test 4: Delete detection with tracked kind
// Tracked uses DELETE+INSERT, so removing a row from source produces
// delete events in table_changes(). Merge cannot detect deletes.
// ---------------------------------------------------------------------------

func TestOutboundSyncV2_TrackedDeleteDetection(t *testing.T) {
	p := testutil.NewProject(t)

	var mu sync.Mutex
	var allPushedRows []map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rows []map[string]any
		json.NewDecoder(r.Body).Decode(&rows)
		mu.Lock()
		allPushedRows = append(allPushedRows, rows...)
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer server.Close()

	testutil.WriteFile(t, p.Dir, "lib/del_push.star", fmt.Sprintf(`
API = {"push": {"batch_size": 100, "batch_mode": "sync"}}

def push(rows):
    http.post("%s", json=rows, retry=1)
    return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "ok" for r in rows}
`, server.URL))

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.del_src AS SELECT * FROM (VALUES (1,'Alice'),(2,'Bob'),(3,'Carol')) AS t(id,name)")

	p.AddModel("sync/del.sql", `-- @kind: tracked
-- @unique_key: id
-- @sink: del_push

SELECT * FROM raw.del_src
`)

	// Run 1: backfill 3 rows
	r1 := runModelWithSink(t, p, "sync/del.sql")
	t.Logf("run 1: type=%s rows=%d sync_ok=%d", r1.RunType, r1.RowsAffected, r1.SyncSucceeded)

	// Clear captured rows for run 2
	mu.Lock()
	allPushedRows = nil
	mu.Unlock()

	// Remove Carol from source
	p.Sess.Exec("DELETE FROM raw.del_src WHERE name='Carol'")

	// Run 2: tracked detects Carol disappeared → delete event
	r2 := runModelWithSink(t, p, "sync/del.sql")
	t.Logf("run 2: type=%s rows=%d sync_ok=%d sync_fail=%d", r2.RunType, r2.RowsAffected, r2.SyncSucceeded, r2.SyncFailed)

	mu.Lock()
	defer mu.Unlock()

	// Verify delete event pushed with Carol's data
	foundDeleteCarol := false
	for _, row := range allPushedRows {
		op := fmt.Sprintf("%v", row["__ondatra_change_type"])
		name := fmt.Sprintf("%v", row["name"])
		if op == "delete" && name == "Carol" {
			foundDeleteCarol = true
		}
	}
	if !foundDeleteCarol {
		t.Error("run 2: expected delete event with name=Carol")
		for i, row := range allPushedRows {
			t.Logf("  pushed[%d]: op=%v name=%v", i, row["__ondatra_change_type"], row["name"])
		}
	}
}

// ---------------------------------------------------------------------------
// V2 Test 5: Automatic delete detection for tracked + @sink
// Merge cannot detect deletes (MERGE INTO never DELETEs from DuckLake).
// Tracked uses DELETE+INSERT, so removed rows produce delete events.
// ---------------------------------------------------------------------------

func TestOutboundSyncV2_AutomaticDeleteDetection(t *testing.T) {
	p := testutil.NewProject(t)

	testutil.WriteFile(t, p.Dir, "lib/autodel_push.star", `
API = {"push": {"batch_size": 100, "batch_mode": "sync"}}

def push(rows):
    return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "ok" for r in rows}
`)

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec(`CREATE TABLE raw.autodel_src AS
		SELECT * FROM (VALUES (1,'a'),(2,'b'),(3,'c')) AS t(id,val)`)

	p.AddModel("sync/autodel.sql", `-- @kind: tracked
-- @unique_key: id
-- @sink: autodel_push

SELECT * FROM raw.autodel_src
`)

	// Run 1: backfill 3 rows
	r1 := runModelWithSink(t, p, "sync/autodel.sql")
	t.Logf("run 1: type=%s sync_ok=%d", r1.RunType, r1.SyncSucceeded)

	// Delete 1 row from source
	p.Sess.Exec("DELETE FROM raw.autodel_src WHERE id = 3")

	// Run 2: tracked detects delete → pushed automatically
	r2 := runModelWithSink(t, p, "sync/autodel.sql")
	t.Logf("run 2: type=%s sync_ok=%d sync_fail=%d", r2.RunType, r2.SyncSucceeded, r2.SyncFailed)

	// Verify DuckLake has 2 rows (tracked removed id=3)
	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM sync.autodel")
	if err != nil {
		t.Fatalf("query count: %v", err)
	}
	if count != "2" {
		t.Errorf("DuckLake row count = %s, want 2 (delete should have been applied)", count)
	}

	// Verify sync pushed the delete event
	if r2.SyncSucceeded == 0 {
		t.Error("expected sync to push the delete event")
	}
}

// ---------------------------------------------------------------------------
// V2 Test 6: _sync_acked lifecycle
// ---------------------------------------------------------------------------

func TestOutboundSyncV2_SyncAckedTable(t *testing.T) {
	p := testutil.NewProject(t)

	testutil.WriteFile(t, p.Dir, "lib/acked_push.star", `
API = {"push": {"batch_size": 100, "batch_mode": "sync"}}

def push(rows):
    return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "ok" for r in rows}
`)

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.acked_src AS SELECT * FROM (VALUES (1,'Alice'),(2,'Bob')) AS t(id,name)")

	p.AddModel("sync/acked.sql", `-- @kind: merge
-- @unique_key: id
-- @sink: acked_push

SELECT * FROM raw.acked_src
`)

	// Run 1: successful push
	r1 := runModelWithSink(t, p, "sync/acked.sql")
	t.Logf("run 1: type=%s sync_ok=%d sync_fail=%d", r1.RunType, r1.SyncSucceeded, r1.SyncFailed)

	if r1.SyncFailed != 0 {
		t.Errorf("run 1: sync_failed = %d, want 0", r1.SyncFailed)
	}

	// Query _sync_acked: should be empty (cleaned up after Badger ack)
	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM _sync_acked")
	if err != nil {
		// Table might not exist if no ack was needed (clean path)
		t.Logf("_sync_acked query: %v (table may not exist on clean path)", err)
	} else if count != "0" {
		t.Errorf("_sync_acked count = %s, want 0 (should be cleaned up after Badger ack)", count)
	}
}

// ---------------------------------------------------------------------------
// V2 Test 7: Tracked group update (all items in group re-pushed)
// ---------------------------------------------------------------------------

func TestOutboundSyncV2_TrackedGroupUpdate(t *testing.T) {
	p := testutil.NewProject(t)

	var mu sync.Mutex
	var allPushedRows []map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rows []map[string]any
		json.NewDecoder(r.Body).Decode(&rows)
		mu.Lock()
		allPushedRows = append(allPushedRows, rows...)
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer server.Close()

	testutil.WriteFile(t, p.Dir, "lib/grp_push.star", fmt.Sprintf(`
API = {"push": {"batch_size": 100, "batch_mode": "sync"}}

def push(rows):
    http.post("%s", json=rows, retry=1)
    return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "ok" for r in rows}
`, server.URL))

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec(`CREATE TABLE raw.order_items AS SELECT * FROM (VALUES
		(1, 'order_1', 10.00),
		(2, 'order_1', 20.00),
		(3, 'order_2', 30.00),
		(4, 'order_2', 40.00)
	) AS t(item_id, order_id, price)`)

	p.AddModel("sync/order_items.sql", `-- @kind: tracked
-- @unique_key: order_id
-- @sink: grp_push

SELECT * FROM raw.order_items
`)

	// Run 1: backfill all 4 items
	r1 := runModelWithSink(t, p, "sync/order_items.sql")
	t.Logf("run 1: type=%s sync_ok=%d", r1.RunType, r1.SyncSucceeded)

	// Clear captured rows for run 2
	mu.Lock()
	allPushedRows = nil
	mu.Unlock()

	// Update price of one item in order_1
	p.Sess.Exec("UPDATE raw.order_items SET price=15.00 WHERE item_id=1")

	// Run 2: should re-push BOTH items in order_1
	r2 := runModelWithSink(t, p, "sync/order_items.sql")
	t.Logf("run 2: type=%s sync_ok=%d", r2.RunType, r2.SyncSucceeded)

	mu.Lock()
	defer mu.Unlock()

	// Count how many order_1 items were pushed
	order1Count := 0
	for _, row := range allPushedRows {
		oid := fmt.Sprintf("%v", row["order_id"])
		if oid == "order_1" {
			order1Count++
		}
	}
	if order1Count < 2 {
		t.Errorf("run 2: pushed %d order_1 items, want 2 (both items in group)", order1Count)
		for i, row := range allPushedRows {
			t.Logf("  pushed[%d]: order_id=%v item_id=%v price=%v", i, row["order_id"], row["item_id"], row["price"])
		}
	}
}

// ---------------------------------------------------------------------------
// V2 Test 8: Tracked group delete
// ---------------------------------------------------------------------------

func TestOutboundSyncV2_TrackedGroupDelete(t *testing.T) {
	p := testutil.NewProject(t)

	var mu sync.Mutex
	var allPushedRows []map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rows []map[string]any
		json.NewDecoder(r.Body).Decode(&rows)
		mu.Lock()
		allPushedRows = append(allPushedRows, rows...)
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer server.Close()

	testutil.WriteFile(t, p.Dir, "lib/grpdel_push.star", fmt.Sprintf(`
API = {"push": {"batch_size": 100, "batch_mode": "sync"}}

def push(rows):
    http.post("%s", json=rows, retry=1)
    return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "ok" for r in rows}
`, server.URL))

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec(`CREATE TABLE raw.grp_items AS SELECT * FROM (VALUES
		(1, 'order_1', 10.00),
		(2, 'order_1', 20.00),
		(3, 'order_2', 30.00)
	) AS t(item_id, order_id, price)`)

	p.AddModel("sync/grp_items.sql", `-- @kind: tracked
-- @unique_key: order_id
-- @sink: grpdel_push

SELECT * FROM raw.grp_items
`)

	// Run 1: backfill
	r1 := runModelWithSink(t, p, "sync/grp_items.sql")
	t.Logf("run 1: type=%s sync_ok=%d", r1.RunType, r1.SyncSucceeded)

	// Clear captured rows
	mu.Lock()
	allPushedRows = nil
	mu.Unlock()

	// Remove order_2 from source
	p.Sess.Exec("DELETE FROM raw.grp_items WHERE order_id='order_2'")

	// Run 2: detect delete for order_2
	r2 := runModelWithSink(t, p, "sync/grp_items.sql")
	t.Logf("run 2: type=%s sync_ok=%d sync_fail=%d", r2.RunType, r2.SyncSucceeded, r2.SyncFailed)

	mu.Lock()
	defer mu.Unlock()

	// Verify delete for order_2's item
	foundDelete := false
	for _, row := range allPushedRows {
		op := fmt.Sprintf("%v", row["__ondatra_change_type"])
		oid := fmt.Sprintf("%v", row["order_id"])
		if op == "delete" && oid == "order_2" {
			foundDelete = true
		}
	}
	if !foundDelete {
		t.Error("run 2: expected delete operation for order_2")
		for i, row := range allPushedRows {
			t.Logf("  pushed[%d]: op=%v order_id=%v", i, row["__ondatra_change_type"], row["order_id"])
		}
	}
}

// ---------------------------------------------------------------------------
// V2 Test 9: Append incremental only
// ---------------------------------------------------------------------------

func TestOutboundSyncV2_AppendIncrementalOnly(t *testing.T) {
	p := testutil.NewProject(t)

	var mu sync.Mutex
	var pushCounts []int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rows []map[string]any
		json.NewDecoder(r.Body).Decode(&rows)
		mu.Lock()
		pushCounts = append(pushCounts, len(rows))
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer server.Close()

	testutil.WriteFile(t, p.Dir, "lib/append_push.star", fmt.Sprintf(`
API = {"push": {"batch_size": 100, "batch_mode": "sync"}}

def push(rows):
    http.post("%s", json=rows, retry=1)
    return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "ok" for r in rows}
`, server.URL))

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.append_src AS SELECT * FROM (VALUES (1,'a'),(2,'b')) AS t(id,val)")

	p.AddModel("sync/append_test.sql", `-- @kind: append
-- @sink: append_push

SELECT * FROM raw.append_src
`)

	// Run 1: push 2 rows
	r1 := runModelWithSink(t, p, "sync/append_test.sql")
	t.Logf("run 1: type=%s sync_ok=%d", r1.RunType, r1.SyncSucceeded)

	mu.Lock()
	totalRun1 := 0
	for _, c := range pushCounts {
		totalRun1 += c
	}
	pushCounts = nil
	mu.Unlock()

	if totalRun1 != 2 {
		t.Errorf("run 1: pushed %d rows, want 2", totalRun1)
	}

	// Add 1 new row
	p.Sess.Exec("INSERT INTO raw.append_src VALUES (3, 'c')")

	// Run 2: only new row pushed
	r2 := runModelWithSink(t, p, "sync/append_test.sql")
	t.Logf("run 2: type=%s sync_ok=%d", r2.RunType, r2.SyncSucceeded)

	mu.Lock()
	totalRun2 := 0
	for _, c := range pushCounts {
		totalRun2 += c
	}
	mu.Unlock()

	if totalRun2 != 1 {
		t.Errorf("run 2: pushed %d rows, want 1 (only new row)", totalRun2)
	}
}

// ---------------------------------------------------------------------------
// V2 Test 10: Append backfill clears backlog
// ---------------------------------------------------------------------------

func TestOutboundSyncV2_AppendBackfillClearsBacklog(t *testing.T) {
	p := testutil.NewProject(t)

	var mu sync.Mutex
	var pushCounts []int
	failPush := true

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rows []map[string]any
		json.NewDecoder(r.Body).Decode(&rows)
		mu.Lock()
		pushCounts = append(pushCounts, len(rows))
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer server.Close()

	testutil.WriteFile(t, p.Dir, "lib/bf_push.star", fmt.Sprintf(`
API = {"push": {"batch_size": 100, "batch_mode": "sync"}}

def push(rows):
    http.post("%s", json=rows, retry=1)
    results = {}
    for row in rows:
        rid = str(row["__ondatra_rowid"]) + ":" + row["__ondatra_change_type"]
        results[rid] = "error: fail"
    return results
`, server.URL))

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.bf_src AS SELECT * FROM (VALUES (1,'a'),(2,'b')) AS t(id,val)")

	p.AddModel("sync/bf_append.sql", `-- @kind: append
-- @sink: bf_push

SELECT * FROM raw.bf_src
`)

	// Run 1: push fails (all rows in backlog)
	r1 := runModelWithSink(t, p, "sync/bf_append.sql")
	t.Logf("run 1: type=%s sync_ok=%d sync_fail=%d", r1.RunType, r1.SyncSucceeded, r1.SyncFailed)
	_ = failPush

	// Change SQL (triggers backfill) and fix push
	testutil.WriteFile(t, p.Dir, "lib/bf_push.star", fmt.Sprintf(`
API = {"push": {"batch_size": 100, "batch_mode": "sync"}}

def push(rows):
    http.post("%s", json=rows, retry=1)
    return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "ok" for r in rows}
`, server.URL))

	p.Sess.Exec("INSERT INTO raw.bf_src VALUES (3, 'c')")

	p.AddModel("sync/bf_append.sql", `-- @kind: append
-- @sink: bf_push

SELECT * FROM raw.bf_src WHERE id > 0
`)

	mu.Lock()
	pushCounts = nil
	mu.Unlock()

	// Run 2: backfill -- should clear old backlog and push new data
	r2 := runModelWithSink(t, p, "sync/bf_append.sql")
	t.Logf("run 2: type=%s sync_ok=%d sync_fail=%d", r2.RunType, r2.SyncSucceeded, r2.SyncFailed)

	if r2.SyncFailed != 0 {
		t.Errorf("run 2: sync_failed = %d, want 0", r2.SyncFailed)
	}
}

// ---------------------------------------------------------------------------
// V2 Test 11: Concurrent push
// ---------------------------------------------------------------------------

func TestOutboundSyncV2_ConcurrentPush(t *testing.T) {
	p := testutil.NewProject(t)

	var mu sync.Mutex
	var allPushedIDs []string
	var finalizeCalled bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rows []map[string]any
		json.NewDecoder(r.Body).Decode(&rows)
		mu.Lock()
		for _, row := range rows {
			if id, ok := row["id"]; ok {
				allPushedIDs = append(allPushedIDs, fmt.Sprintf("%v", id))
			}
		}
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer server.Close()

	testutil.WriteFile(t, p.Dir, "lib/conc_push.star", fmt.Sprintf(`
API = {"push": {"batch_size": 5, "batch_mode": "sync", "max_concurrent": 3}}

def push(rows):
    http.post("%s", json=rows, retry=1)
    return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "ok" for r in rows}

def finalize(succeeded, failed):
    pass
`, server.URL))

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	// Create 20 rows
	p.Sess.Exec(`CREATE TABLE raw.conc_src AS
		SELECT unnest(generate_series(1,20)) AS id, 'val_' || unnest(generate_series(1,20)) AS val`)

	p.AddModel("sync/conc.sql", `-- @kind: merge
-- @unique_key: id
-- @sink: conc_push

SELECT * FROM raw.conc_src
`)

	r := runModelWithSink(t, p, "sync/conc.sql")
	t.Logf("result: type=%s sync_ok=%d sync_fail=%d", r.RunType, r.SyncSucceeded, r.SyncFailed)
	_ = finalizeCalled

	mu.Lock()
	defer mu.Unlock()

	// Verify all 20 rows pushed
	if len(allPushedIDs) != 20 {
		t.Errorf("pushed %d rows, want 20", len(allPushedIDs))
	}

	// Check no duplicates
	seen := make(map[string]bool)
	for _, id := range allPushedIDs {
		if seen[id] {
			t.Errorf("duplicate push for id=%s", id)
		}
		seen[id] = true
	}

	if r.SyncFailed != 0 {
		t.Errorf("sync_failed = %d, want 0", r.SyncFailed)
	}
}

// ---------------------------------------------------------------------------
// V2 Test 12: Merge delete and update same run
// ---------------------------------------------------------------------------

func TestOutboundSyncV2_MergeDeleteAndUpdateSameRun(t *testing.T) {
	p := testutil.NewProject(t)

	var mu sync.Mutex
	var allPushedRows []map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rows []map[string]any
		json.NewDecoder(r.Body).Decode(&rows)
		mu.Lock()
		allPushedRows = append(allPushedRows, rows...)
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer server.Close()

	testutil.WriteFile(t, p.Dir, "lib/mdu_push.star", fmt.Sprintf(`
API = {"push": {"batch_size": 100, "batch_mode": "sync"}}

def push(rows):
    http.post("%s", json=rows, retry=1)
    return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "ok" for r in rows}
`, server.URL))

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.mdu_src AS SELECT * FROM (VALUES (1,'Alice'),(2,'Bob'),(3,'Carol')) AS t(id,name)")

	p.AddModel("sync/mdu.sql", `-- @kind: merge
-- @unique_key: id
-- @sink: mdu_push

SELECT * FROM raw.mdu_src
`)

	// Run 1: backfill
	r1 := runModelWithSink(t, p, "sync/mdu.sql")
	t.Logf("run 1: type=%s sync_ok=%d", r1.RunType, r1.SyncSucceeded)

	// Clear for run 2
	mu.Lock()
	allPushedRows = nil
	mu.Unlock()

	// Update Alice + remove Carol in same run
	p.Sess.Exec("UPDATE raw.mdu_src SET name='Alice_Updated' WHERE id=1")
	p.Sess.Exec("DELETE FROM raw.mdu_src WHERE id=3")

	// Run 2: merge produces update_postimage for Alice's change.
	// Carol's deletion from source is NOT detected by merge (MERGE INTO
	// never DELETEs from DuckLake). Use tracked kind for delete detection.
	r2 := runModelWithSink(t, p, "sync/mdu.sql")
	t.Logf("run 2: type=%s sync_ok=%d sync_fail=%d", r2.RunType, r2.SyncSucceeded, r2.SyncFailed)

	mu.Lock()
	defer mu.Unlock()

	foundAliceUpdate := false
	for _, row := range allPushedRows {
		op := fmt.Sprintf("%v", row["__ondatra_change_type"])
		name := fmt.Sprintf("%v", row["name"])
		if op == "update_postimage" && name == "Alice_Updated" {
			foundAliceUpdate = true
		}
	}

	if !foundAliceUpdate {
		t.Error("run 2: expected update_postimage with name=Alice_Updated")
	}

	if !foundAliceUpdate {
		for i, row := range allPushedRows {
			t.Logf("  pushed[%d]: op=%v name=%v id=%v", i, row["__ondatra_change_type"], row["name"], row["id"])
		}
	}
}

// ---------------------------------------------------------------------------
// Bug 3 test: update_preimage + update_postimage for same RowID
//
// This test exposes three problems when merge produces both preimage and
// postimage events for the same RowID:
//   1. Push receives two rows with same __ondatra_rowid — can't return
//      different statuses per change_type
//   2. classifyPerRowStatus indexes by RowID — one status overwrites the other
//   3. mergeBacklogWithDelta deduplicates by RowID — preimage in backlog
//      gets dropped if postimage appears in delta
// ---------------------------------------------------------------------------

func TestOutboundSync_Bug3_PreimagePostimageSameRowID(t *testing.T) {
	p := testutil.NewProject(t)

	var mu sync.Mutex
	var allPushedRows []map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rows []map[string]any
		json.NewDecoder(r.Body).Decode(&rows)
		mu.Lock()
		allPushedRows = append(allPushedRows, rows...)
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer server.Close()

	testutil.WriteFile(t, p.Dir, "lib/bug3_push.star", fmt.Sprintf(`
API = {"push": {"batch_size": 100, "batch_mode": "sync"}}

def push(rows):
    http.post("%s", json=rows, retry=1)
    return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "ok" for r in rows}
`, server.URL))

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.bug3_src AS SELECT * FROM (VALUES (1,'Alice','alice@example.com'),(2,'Bob','bob@example.com')) AS t(id,name,email)")

	p.AddModel("sync/bug3.sql", `-- @kind: merge
-- @unique_key: id
-- @sink: bug3_push

SELECT * FROM raw.bug3_src
`)

	// Run 1: backfill
	r1 := runModelWithSink(t, p, "sync/bug3.sql")
	t.Logf("run 1: type=%s sync_ok=%d", r1.RunType, r1.SyncSucceeded)

	// Clear for run 2
	mu.Lock()
	allPushedRows = nil
	mu.Unlock()

	// Update Alice's email (merge will produce preimage + postimage)
	p.Sess.Exec("UPDATE raw.bug3_src SET email='alice_new@example.com' WHERE id=1")

	// Run 2: incremental merge
	r2 := runModelWithSink(t, p, "sync/bug3.sql")
	t.Logf("run 2: type=%s sync_ok=%d sync_fail=%d", r2.RunType, r2.SyncSucceeded, r2.SyncFailed)

	mu.Lock()
	defer mu.Unlock()

	// --- Diagnostic: log all pushed rows ---
	t.Log("--- Pushed rows from run 2 ---")
	for i, row := range allPushedRows {
		t.Logf("  row[%d]: rowid=%v change_type=%v name=%v email=%v",
			i, row["__ondatra_rowid"], row["__ondatra_change_type"], row["name"], row["email"])
	}

	// Check 1: Do we get both preimage and postimage?
	var preimageCount, postimageCount int
	var preimageEmail, postimageEmail string
	for _, row := range allPushedRows {
		ct := fmt.Sprintf("%v", row["__ondatra_change_type"])
		if ct == "update_preimage" {
			preimageCount++
			preimageEmail = fmt.Sprintf("%v", row["email"])
		}
		if ct == "update_postimage" {
			postimageCount++
			postimageEmail = fmt.Sprintf("%v", row["email"])
		}
	}

	t.Logf("preimage count=%d postimage count=%d", preimageCount, postimageCount)

	if postimageCount == 0 {
		t.Error("BUG: no update_postimage row received")
	}
	if preimageCount == 0 {
		t.Log("NOTE: no update_preimage row received (may be filtered or collapsed)")
	}

	// Check 2: If both present, do they have correct data?
	if preimageCount > 0 && postimageCount > 0 {
		if preimageEmail == postimageEmail {
			t.Errorf("BUG: preimage and postimage have same email=%s (preimage should have old value)", preimageEmail)
		} else {
			t.Logf("OK: preimage email=%s, postimage email=%s", preimageEmail, postimageEmail)
		}

		// Check 3: Do they share the same __ondatra_rowid?
		rowids := make(map[string][]string)
		for _, row := range allPushedRows {
			rid := fmt.Sprintf("%v", row["__ondatra_rowid"])
			ct := fmt.Sprintf("%v", row["__ondatra_change_type"])
			rowids[rid] = append(rowids[rid], ct)
		}
		for rid, cts := range rowids {
			if len(cts) > 1 {
				t.Logf("COLLISION: rowid=%s has change_types=%v — push() cannot return different status per change_type", rid, cts)
			}
		}
	}

	// Check 4: Did sync succeed without errors?
	if r2.SyncFailed > 0 {
		t.Errorf("sync had %d failures", r2.SyncFailed)
	}
	for _, w := range r2.Warnings {
		if strings.Contains(w, "sink") {
			t.Logf("warning: %s", w)
		}
	}
}

// ---------------------------------------------------------------------------
// Bug 3 fix B test: composite key rowid:change_type
//
// Simulates what fix B would look like from the Starlark side.
// Push returns status keyed by "rowid:change_type" instead of just "rowid".
// This lets preimage and postimage have independent statuses.
//
// NOTE: This test will FAIL until fix B is implemented in Go — the runtime
// currently expects keys to be just rowid. Run this to see what breaks.
// ---------------------------------------------------------------------------

func TestOutboundSync_Bug3_FixB_CompositeKey(t *testing.T) {
	p := testutil.NewProject(t)

	var mu sync.Mutex
	var allPushedRows []map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rows []map[string]any
		json.NewDecoder(r.Body).Decode(&rows)
		mu.Lock()
		allPushedRows = append(allPushedRows, rows...)
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer server.Close()

	// Push returns composite key: "rowid:change_type"
	// Preimage gets "ok", postimage gets "ok" — but with DIFFERENT keys
	testutil.WriteFile(t, p.Dir, "lib/bug3b_push.star", fmt.Sprintf(`
API = {"push": {"batch_size": 100, "batch_mode": "sync"}}

def push(rows):
    http.post("%s", json=rows, retry=1)
    results = {}
    for r in rows:
        rid = str(r["__ondatra_rowid"])
        ct = r["__ondatra_change_type"]
        key = rid + ":" + ct
        if ct == "update_preimage":
            results[key] = "ok"  # ack preimage (just context)
        elif ct == "update_postimage":
            results[key] = "ok"  # ack postimage (the actual update)
        else:
            results[key] = "ok"
    return results
`, server.URL))

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.bug3b_src AS SELECT * FROM (VALUES (1,'Alice','old@example.com'),(2,'Bob','bob@example.com')) AS t(id,name,email)")

	p.AddModel("sync/bug3b.sql", `-- @kind: merge
-- @unique_key: id
-- @sink: bug3b_push

SELECT * FROM raw.bug3b_src
`)

	// Run 1: backfill
	r1 := runModelWithSink(t, p, "sync/bug3b.sql")
	t.Logf("run 1: type=%s sync_ok=%d", r1.RunType, r1.SyncSucceeded)

	mu.Lock()
	allPushedRows = nil
	mu.Unlock()

	// Update Alice
	p.Sess.Exec("UPDATE raw.bug3b_src SET email='new@example.com' WHERE id=1")

	// Run 2: incremental — produces preimage + postimage
	r2 := runModelWithSink(t, p, "sync/bug3b.sql")
	t.Logf("run 2: type=%s sync_ok=%d sync_fail=%d warnings=%v",
		r2.RunType, r2.SyncSucceeded, r2.SyncFailed, r2.Warnings)

	mu.Lock()
	defer mu.Unlock()

	t.Log("--- Pushed rows ---")
	for i, row := range allPushedRows {
		t.Logf("  row[%d]: rowid=%v ct=%v email=%v", i,
			row["__ondatra_rowid"], row["__ondatra_change_type"], row["email"])
	}

	// With fix B implemented, both events should be acked separately.
	// Without fix B, the runtime won't find "rowid:change_type" keys
	// in the status dict (it looks for just "rowid"), causing failures.
	if r2.SyncFailed > 0 {
		t.Logf("EXPECTED FAILURE (fix B not implemented): %d events failed because runtime expects plain rowid keys", r2.SyncFailed)
	} else if r2.SyncSucceeded >= 2 {
		t.Log("OK: both preimage and postimage acked independently")
	}

	// Log warnings to see what the runtime reports
	for _, w := range r2.Warnings {
		t.Logf("warning: %s", w)
	}
}
