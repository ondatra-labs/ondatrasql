// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package script

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBadgerCollector_WriteAndClaim(t *testing.T) {
	dir := t.TempDir()
	ingestDir := filepath.Join(dir, "ingest")

	bc, err := newBadgerCollector("test.target", ingestDir, sharedSess)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer bc.close()

	// Write some rows
	for i := range 5 {
		if err := bc.add(map[string]interface{}{"id": i, "name": "row"}); err != nil {
			t.Fatalf("add: %v", err)
		}
	}

	if bc.count() != 5 {
		t.Fatalf("count = %d, want 5", bc.count())
	}

	// Create temp table from claimed events
	tmpTable, rowCount, claimIDs, err := bc.createTempTable()
	if err != nil {
		t.Fatalf("createTempTable: %v", err)
	}

	if tmpTable == "" {
		t.Fatal("expected non-empty temp table name")
	}
	if rowCount != 5 {
		t.Errorf("rowCount = %d, want 5", rowCount)
	}
	if len(claimIDs) == 0 {
		t.Fatal("expected at least one claim ID")
	}

	// Verify data in temp table
	rows, err := sharedSess.QueryRowsMap("SELECT COUNT(*) AS cnt FROM " + tmpTable)
	if err != nil {
		t.Fatalf("query temp table: %v", err)
	}
	if rows[0]["cnt"] != "5" {
		t.Errorf("temp table count = %s, want 5", rows[0]["cnt"])
	}

	// Ack claims
	if err := bc.ack(claimIDs); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// Clean up temp table
	sharedSess.Exec("DROP TABLE IF EXISTS " + tmpTable)
}

func TestBadgerCollector_EmptyStore(t *testing.T) {
	dir := t.TempDir()
	ingestDir := filepath.Join(dir, "ingest")

	bc, err := newBadgerCollector("test.empty", ingestDir, sharedSess)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer bc.close()

	// No rows written
	tmpTable, rowCount, claimIDs, err := bc.createTempTable()
	if err != nil {
		t.Fatalf("createTempTable: %v", err)
	}
	if tmpTable != "" {
		t.Errorf("expected empty temp table, got %q", tmpTable)
	}
	if rowCount != 0 {
		t.Errorf("rowCount = %d, want 0", rowCount)
	}
	if len(claimIDs) != 0 {
		t.Errorf("expected no claim IDs, got %d", len(claimIDs))
	}
}

func TestBadgerCollector_CrashRecovery(t *testing.T) {
	dir := t.TempDir()
	ingestDir := filepath.Join(dir, "ingest")

	// Simulate a crash: write events, claim them, but don't ack (crash before DuckDB commit)
	bc1, err := newBadgerCollector("test.crash", ingestDir, sharedSess)
	if err != nil {
		t.Fatalf("new bc1: %v", err)
	}
	for i := range 3 {
		bc1.add(map[string]interface{}{"id": i})
	}
	// Claim but don't ack (simulates crash between claim and DuckDB commit)
	_, _, _, err = bc1.createTempTable()
	if err != nil {
		t.Fatalf("createTempTable bc1: %v", err)
	}
	bc1.close()

	// Open again — inflight events should be recovered back to evt: and available
	bc2, err := newBadgerCollector("test.crash", ingestDir, sharedSess)
	if err != nil {
		t.Fatalf("new bc2: %v", err)
	}
	defer bc2.close()

	// Recovered events should be claimable in the new run
	tmpTable, rowCount, claimIDs, err := bc2.createTempTable()
	if err != nil {
		t.Fatalf("createTempTable bc2: %v", err)
	}
	if rowCount != 3 {
		t.Errorf("expected 3 recovered rows, got %d", rowCount)
	}
	if tmpTable == "" {
		t.Error("expected non-empty temp table from recovered events")
	}

	// Ack to clean up
	if len(claimIDs) > 0 {
		bc2.ack(claimIDs)
	}
	if tmpTable != "" {
		sharedSess.Exec("DROP TABLE IF EXISTS " + tmpTable)
	}
}

func TestBadgerCollector_PreExistingEvents(t *testing.T) {
	dir := t.TempDir()
	ingestDir := filepath.Join(dir, "ingest")

	// Write events and close WITHOUT claiming (simulates events buffered but not yet flushed)
	bc1, err := newBadgerCollector("test.preexist", ingestDir, sharedSess)
	if err != nil {
		t.Fatalf("new bc1: %v", err)
	}
	for i := range 2 {
		bc1.add(map[string]interface{}{"id": i, "source": "run1"})
	}
	bc1.close()

	// Open again — pre-existing events should still be available
	bc2, err := newBadgerCollector("test.preexist", ingestDir, sharedSess)
	if err != nil {
		t.Fatalf("new bc2: %v", err)
	}
	defer bc2.close()

	// Add new events in the current run
	bc2.add(map[string]interface{}{"id": 10, "source": "run2"})

	// createTempTable should include BOTH pre-existing and new events
	tmpTable, rowCount, claimIDs, err := bc2.createTempTable()
	if err != nil {
		t.Fatalf("createTempTable bc2: %v", err)
	}
	if rowCount != 3 {
		t.Errorf("expected 3 rows (2 pre-existing + 1 new), got %d", rowCount)
	}

	// Ack and clean up
	if len(claimIDs) > 0 {
		bc2.ack(claimIDs)
	}
	if tmpTable != "" {
		sharedSess.Exec("DROP TABLE IF EXISTS " + tmpTable)
	}
}

func TestBadgerCollector_AckPreventsReClaim(t *testing.T) {
	dir := t.TempDir()
	ingestDir := filepath.Join(dir, "ingest")

	bc, err := newBadgerCollector("test.ack", ingestDir, sharedSess)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer bc.close()

	// Write and claim
	bc.add(map[string]interface{}{"id": 1})
	_, _, claimIDs, err := bc.createTempTable()
	if err != nil {
		t.Fatalf("createTempTable: %v", err)
	}

	// Ack
	if err := bc.ack(claimIDs); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// Second claim should return nothing (events already acked)
	tmpTable2, rowCount2, _, err := bc.createTempTable()
	if err != nil {
		t.Fatalf("second createTempTable: %v", err)
	}
	if tmpTable2 != "" || rowCount2 != 0 {
		t.Errorf("expected empty after ack, got table=%q rows=%d", tmpTable2, rowCount2)
	}
}

func TestBadgerCollector_RuntimeIntegration(t *testing.T) {
	// Test that Runtime with ingestDir uses Badger collector
	dir := t.TempDir()
	ingestDir := filepath.Join(dir, "ingest")
	os.MkdirAll(ingestDir, 0o755)

	rt := NewRuntime(sharedSess, nil)
	rt.SetIngestDir(ingestDir)

	code := `
save.row({"id": 1, "name": "alice"})
save.row({"id": 2, "name": "bob"})
`
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := rt.Run(ctx, "test.badger_rt", code)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	defer result.Close()

	if result.RowCount != 2 {
		t.Errorf("RowCount = %d, want 2", result.RowCount)
	}

	// Create temp table
	if err := result.CreateTempTable(); err != nil {
		t.Fatalf("CreateTempTable: %v", err)
	}

	if result.TempTable == "" {
		t.Fatal("expected non-empty TempTable")
	}
	if len(result.ClaimIDs) == 0 {
		t.Fatal("expected ClaimIDs to be populated")
	}

	// Verify data
	rows, err := sharedSess.QueryRowsMap("SELECT COUNT(*) AS cnt FROM " + result.TempTable)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if rows[0]["cnt"] != "2" {
		t.Errorf("count = %s, want 2", rows[0]["cnt"])
	}

	// Ack and cleanup
	result.AckClaims()
	sharedSess.Exec("DROP TABLE IF EXISTS " + result.TempTable)
}

func TestAckTable(t *testing.T) {
	// Test ack table creation and queries
	if err := EnsureAckTable(sharedSess); err != nil {
		t.Fatalf("EnsureAckTable: %v", err)
	}

	// Insert an ack record
	sql := AckSQL("test_claim_123", "test.target", 42)
	if err := sharedSess.Exec(sql); err != nil {
		t.Fatalf("insert ack: %v", err)
	}

	// Check isAcked
	acked, err := IsAcked(sharedSess, "test_claim_123")
	if err != nil {
		t.Fatalf("isAcked: %v", err)
	}
	if !acked {
		t.Error("expected test_claim_123 to be acked")
	}

	// Non-existent claim
	acked2, err := IsAcked(sharedSess, "nonexistent_claim")
	if err != nil {
		t.Fatalf("isAcked nonexistent: %v", err)
	}
	if acked2 {
		t.Error("expected nonexistent claim to not be acked")
	}

	// Cleanup
	sharedSess.Exec("DELETE FROM _ondatra_acks WHERE claim_id = 'test_claim_123'")
}

func TestBadgerCollector_CrashBetweenCommitAndAck(t *testing.T) {
	// Simulates: DuckDB commit succeeded (ack record in txn), but process
	// crashed before Badger ack. On restart, the events are recovered from
	// inflight, but createTempTable should detect the ack record and skip them.
	dir := t.TempDir()
	ingestDir := filepath.Join(dir, "ingest")

	// Step 1: Write events and claim them
	bc1, err := newBadgerCollector("test.crashack", ingestDir, sharedSess)
	if err != nil {
		t.Fatalf("new bc1: %v", err)
	}
	bc1.add(map[string]interface{}{"id": 1})
	bc1.add(map[string]interface{}{"id": 2})

	_, _, claimIDs, err := bc1.createTempTable()
	if err != nil {
		t.Fatalf("createTempTable: %v", err)
	}
	if len(claimIDs) == 0 {
		t.Fatal("expected claim IDs")
	}

	// Step 2: Simulate DuckDB commit with ack record (but no Badger ack)
	EnsureAckTable(sharedSess)
	for _, id := range claimIDs {
		sharedSess.Exec(AckSQL(id, "test.crashack", 2))
	}
	// Close WITHOUT acking Badger — simulates crash
	bc1.close()

	// Step 3: Restart — events are inflight, recovered back to evt:
	bc2, err := newBadgerCollector("test.crashack", ingestDir, sharedSess)
	if err != nil {
		t.Fatalf("new bc2: %v", err)
	}
	defer bc2.close()

	// createTempTable should detect ack records and skip already-committed claims
	tmpTable, rowCount, pendingClaims, err := bc2.createTempTable()
	if err != nil {
		t.Fatalf("createTempTable bc2: %v", err)
	}
	if rowCount != 0 {
		t.Errorf("expected 0 pending rows (all already committed), got %d", rowCount)
	}
	if tmpTable != "" {
		t.Errorf("expected no temp table, got %q", tmpTable)
		sharedSess.Exec("DROP TABLE IF EXISTS " + tmpTable)
	}
	if len(pendingClaims) != 0 {
		t.Errorf("expected 0 pending claims, got %d", len(pendingClaims))
	}

	// Cleanup ack records
	for _, id := range claimIDs {
		sharedSess.Exec(fmt.Sprintf("DELETE FROM _ondatra_acks WHERE claim_id = '%s'", escapeAckSQL(id)))
	}
}
