// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package collect

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func TestStore_WriteAndClaim(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "raw.events"

	// Write 3 events
	for i := 0; i < 3; i++ {
		if err := store.Write(target, map[string]any{"i": i}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	// Claim all
	claimID, events, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3", len(events))
	}
	if claimID == "" {
		t.Fatal("empty claim ID")
	}

	// Claim again should return 0
	_, events2, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim2: %v", err)
	}
	if len(events2) != 0 {
		t.Fatalf("got %d events on second claim, want 0", len(events2))
	}
}

func TestStore_Ack(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "raw.events"
	store.Write(target, map[string]any{"x": 1})

	claimID, _, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	// Ack should remove inflight events
	if err := store.Ack(claimID); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// RecoverInflight should find nothing
	if err := store.RecoverInflight(target); err != nil {
		t.Fatalf("RecoverInflight: %v", err)
	}

	// Claim should return 0 (events were acked, not recovered)
	_, events, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim after ack: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("got %d events after ack+recover, want 0", len(events))
	}
}

func TestStore_Nack(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "raw.events"
	store.Write(target, map[string]any{"x": 1})
	store.Write(target, map[string]any{"x": 2})

	claimID, _, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	// Nack should return events to queue
	if err := store.Nack(claimID); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	// Claim again should get the events back
	_, events, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim after nack: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events after nack, want 2", len(events))
	}
}

func TestStore_RecoverInflight(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "raw.events"
	store.Write(target, map[string]any{"x": 1})

	// Claim (simulates runner grabbing events)
	_, _, err = store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	// Simulate crash: RecoverInflight should move back to evt:
	if err := store.RecoverInflight(target); err != nil {
		t.Fatalf("RecoverInflight: %v", err)
	}

	// Events should be claimable again
	_, events, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim after recover: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events after recover, want 1", len(events))
	}
}

func TestStore_RecoverAllInflight(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	// Write to two different targets
	store.Write("raw.events", map[string]any{"x": 1})
	store.Write("raw.clicks", map[string]any{"y": 2})

	store.Claim("raw.events", 100)
	store.Claim("raw.clicks", 100)

	// RecoverAll should recover both
	if err := store.RecoverAllInflight(); err != nil {
		t.Fatalf("RecoverAllInflight: %v", err)
	}

	_, events1, _ := store.Claim("raw.events", 100)
	_, events2, _ := store.Claim("raw.clicks", 100)
	if len(events1) != 1 {
		t.Fatalf("raw.events: got %d, want 1", len(events1))
	}
	if len(events2) != 1 {
		t.Fatalf("raw.clicks: got %d, want 1", len(events2))
	}
}

func TestStore_ClaimLimit(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "raw.events"
	for i := 0; i < 10; i++ {
		store.Write(target, map[string]any{"i": i})
	}

	// Claim with limit 3
	claimID1, events1, err := store.Claim(target, 3)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(events1) != 3 {
		t.Fatalf("got %d events, want 3", len(events1))
	}

	// Ack first batch
	store.Ack(claimID1)

	// Claim remaining
	claimID2, events2, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim2: %v", err)
	}
	if len(events2) != 7 {
		t.Fatalf("got %d remaining events, want 7", len(events2))
	}
	store.Ack(claimID2)
}

func TestStore_DifferentTargets(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	store.Write("raw.events", map[string]any{"a": 1})
	store.Write("raw.events", map[string]any{"a": 2})
	store.Write("raw.clicks", map[string]any{"b": 1})

	// Claim only raw.events
	_, events, err := store.Claim("raw.events", 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events for raw.events, want 2", len(events))
	}

	// raw.clicks should be untouched
	_, clicks, err := store.Claim("raw.clicks", 100)
	if err != nil {
		t.Fatalf("Claim clicks: %v", err)
	}
	if len(clicks) != 1 {
		t.Fatalf("got %d events for raw.clicks, want 1", len(clicks))
	}
}

// --- New tests below ---

func TestStore_WriteBatch(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "raw.events"
	batch := []map[string]any{
		{"i": 0},
		{"i": 1},
		{"i": 2},
	}
	if err := store.WriteBatch(target, batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	_, events, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3", len(events))
	}
}

func TestStore_ClearAll(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "raw.events"

	// Write pending events
	store.Write(target, map[string]any{"x": 1})
	store.Write(target, map[string]any{"x": 2})

	// Claim some to create inflight
	store.Write(target, map[string]any{"x": 3})
	claimID, _, err := store.Claim(target, 2)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	_ = claimID

	// Save a job ref
	store.SaveJobRef(target, map[string]any{"job_id": "abc"}, "hash123")

	// ClearAll should remove everything
	if err := store.ClearAll(target); err != nil {
		t.Fatalf("ClearAll: %v", err)
	}

	// No pending events
	if has, _ := store.HasPendingEvents(target); has {
		t.Fatal("expected no pending events after ClearAll")
	}

	// No inflight (recover should find nothing to move back)
	store.RecoverInflight(target)
	_, events, _ := store.Claim(target, 100)
	if len(events) != 0 {
		t.Fatalf("got %d events after ClearAll+Recover, want 0", len(events))
	}

	// No job ref
	ref, _, err := store.LoadJobRef(target)
	if err != nil {
		t.Fatalf("LoadJobRef: %v", err)
	}
	if ref != nil {
		t.Fatalf("expected nil job ref after ClearAll, got %v", ref)
	}
}

func TestStore_ClearAllAndWrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "raw.events"

	// Write old events and claim some to create inflight
	store.Write(target, map[string]any{"old": 1})
	store.Write(target, map[string]any{"old": 2})
	store.Claim(target, 1) // moves one to inflight

	// Save a job ref
	store.SaveJobRef(target, map[string]any{"job_id": "old"}, "oldhash")

	// ClearAllAndWrite replaces everything with new snapshot
	newEvents := []map[string]any{{"new": 1}, {"new": 2}, {"new": 3}}
	if err := store.ClearAllAndWrite(target, newEvents); err != nil {
		t.Fatalf("ClearAllAndWrite: %v", err)
	}

	// Should have exactly 3 new pending events
	pending, err := store.ReadPending(target)
	if err != nil {
		t.Fatalf("ReadPending: %v", err)
	}
	if len(pending) != 3 {
		t.Fatalf("got %d pending, want 3", len(pending))
	}

	// No inflight should remain
	store.RecoverInflight(target)
	all, err := store.ReadPending(target)
	if err != nil {
		t.Fatalf("ReadPending after recover: %v", err)
	}
	// Should still be 3 (no inflight was recovered)
	if len(all) != 3 {
		t.Fatalf("got %d after recover, want 3", len(all))
	}

	// Job ref should be cleared
	ref, _, _ := store.LoadJobRef(target)
	if ref != nil {
		t.Fatalf("expected nil job ref after ClearAllAndWrite, got %v", ref)
	}
}

func TestStore_ClearAllAndWrite_PreservesOtherTargets(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	// Write events for two targets
	store.Write("raw.events", map[string]any{"a": 1})
	store.Write("raw.clicks", map[string]any{"b": 1})

	// ClearAllAndWrite only affects raw.events
	if err := store.ClearAllAndWrite("raw.events", []map[string]any{{"a": 99}}); err != nil {
		t.Fatalf("ClearAllAndWrite: %v", err)
	}

	// raw.clicks should be untouched
	pending, err := store.ReadPending("raw.clicks")
	if err != nil {
		t.Fatalf("ReadPending clicks: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("got %d pending for raw.clicks, want 1", len(pending))
	}
}

func TestStore_ReadPending(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "raw.events"
	store.Write(target, map[string]any{"x": 1})
	store.Write(target, map[string]any{"x": 2})

	// ReadPending should return events without claiming them
	events, err := store.ReadPending(target)
	if err != nil {
		t.Fatalf("ReadPending: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}

	// Events should still be claimable (ReadPending is non-destructive)
	_, claimed, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("got %d claimed after ReadPending, want 2", len(claimed))
	}
}

func TestStore_ReadAllEvents(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "raw.events"
	store.Write(target, map[string]any{"x": 1})
	store.Write(target, map[string]any{"x": 2})
	store.Write(target, map[string]any{"x": 3})

	// Claim 2 events (moves them to inflight)
	_, _, err = store.Claim(target, 2)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	// ReadAllEvents should return all 3 (1 pending + 2 inflight)
	all, err := store.ReadAllEvents(target)
	if err != nil {
		t.Fatalf("ReadAllEvents: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("got %d events, want 3", len(all))
	}
}

func TestStore_ReadAllEvents_CorruptEntry(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "raw.events"

	// Write a corrupt entry directly via badger
	err = store.db.Update(func(txn *badger.Txn) error {
		key := []byte(prefixEvt + target + ":999_1")
		return txn.Set(key, []byte("not valid json{{{"))
	})
	if err != nil {
		t.Fatalf("write corrupt entry: %v", err)
	}

	// ReadAllEvents should return an error on corrupt data
	_, err = store.ReadAllEvents(target)
	if err == nil {
		t.Fatal("expected error for corrupt evt entry")
	}
}

func TestStore_HasRecentInflight(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "raw.events"

	// No inflight initially
	if has, _ := store.HasRecentInflight(target); has {
		t.Fatal("expected no recent inflight initially")
	}

	// Write and claim
	store.Write(target, map[string]any{"x": 1})
	_, _, err = store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	// Freshly claimed events are always "recent" in tests (age < inflightMaxAge)
	if has, _ := store.HasRecentInflight(target); !has {
		t.Fatal("expected recent inflight after Claim")
	}
}

func TestStore_RecoverOldInflight(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "raw.events"
	store.Write(target, map[string]any{"x": 1})

	// Claim the event
	_, _, err = store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	// RecoverOldInflight should NOT recover recent claims
	if err := store.RecoverOldInflight(target); err != nil {
		t.Fatalf("RecoverOldInflight: %v", err)
	}

	// Event should still be inflight (not recovered to evt:)
	pending, _ := store.ReadPending(target)
	if len(pending) != 0 {
		t.Fatalf("got %d pending, want 0 (recent claims should not be recovered)", len(pending))
	}

	// RecoverInflight (all) should recover it
	if err := store.RecoverInflight(target); err != nil {
		t.Fatalf("RecoverInflight: %v", err)
	}
	pending, _ = store.ReadPending(target)
	if len(pending) != 1 {
		t.Fatalf("got %d pending after RecoverInflight, want 1", len(pending))
	}
}

func TestStore_AckAndRequeue(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "raw.events"
	store.Write(target, map[string]any{"id": 1, "ok": true})
	store.Write(target, map[string]any{"id": 2, "ok": false})
	store.Write(target, map[string]any{"id": 3, "ok": true})

	claimID, _, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	// Requeue only the failed row
	failedRows := []map[string]any{{"id": 2, "ok": false}}
	if err := store.AckAndRequeue(claimID, target, failedRows, false); err != nil {
		t.Fatalf("AckAndRequeue: %v", err)
	}

	// Inflight should be gone (acked)
	store.RecoverInflight(target)

	// Only the failed row should be pending
	_, events, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim after requeue: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
}

func TestStore_AckAndRequeue_WithJobRef(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "raw.events"
	store.Write(target, map[string]any{"id": 1})

	claimID, _, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	// Save a job ref
	store.SaveJobRef(target, map[string]any{"job_id": "j1"}, "hash1")

	// AckAndRequeue with deleteJobRef=true should also remove the job ref
	failedRows := []map[string]any{{"id": 1}}
	if err := store.AckAndRequeue(claimID, target, failedRows, true); err != nil {
		t.Fatalf("AckAndRequeue: %v", err)
	}

	ref, _, err := store.LoadJobRef(target)
	if err != nil {
		t.Fatalf("LoadJobRef: %v", err)
	}
	if ref != nil {
		t.Fatalf("expected nil job ref after AckAndRequeue with deleteJobRef=true, got %v", ref)
	}
}

func TestStore_SaveLoadDeleteJobRef(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "raw.events"

	// Load non-existent returns nil
	ref, hash, err := store.LoadJobRef(target)
	if err != nil {
		t.Fatalf("LoadJobRef (empty): %v", err)
	}
	if ref != nil || hash != "" {
		t.Fatalf("expected nil/empty for non-existent job ref, got %v / %q", ref, hash)
	}

	// Save
	jobRef := map[string]any{"job_id": "abc123", "status": "pending"}
	if err := store.SaveJobRef(target, jobRef, "rowshash"); err != nil {
		t.Fatalf("SaveJobRef: %v", err)
	}

	// Load
	ref, hash, err = store.LoadJobRef(target)
	if err != nil {
		t.Fatalf("LoadJobRef: %v", err)
	}
	if ref["job_id"] != "abc123" {
		t.Fatalf("got job_id=%v, want abc123", ref["job_id"])
	}
	if hash != "rowshash" {
		t.Fatalf("got hash=%q, want rowshash", hash)
	}

	// Delete
	if err := store.DeleteJobRef(target); err != nil {
		t.Fatalf("DeleteJobRef: %v", err)
	}

	// Load after delete
	ref, hash, err = store.LoadJobRef(target)
	if err != nil {
		t.Fatalf("LoadJobRef after delete: %v", err)
	}
	if ref != nil || hash != "" {
		t.Fatalf("expected nil/empty after delete, got %v / %q", ref, hash)
	}
}

func TestStore_ClearPendingAndWrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "raw.events"

	// Write old events
	store.Write(target, map[string]any{"old": 1})
	store.Write(target, map[string]any{"old": 2})

	// Claim one to create inflight
	store.Claim(target, 1)

	// ClearPendingAndWrite should replace only pending, preserve inflight
	newEvents := []map[string]any{{"new": 1}}
	if err := store.ClearPendingAndWrite(target, newEvents); err != nil {
		t.Fatalf("ClearPendingAndWrite: %v", err)
	}

	// Pending should be just the new event
	pending, _ := store.ReadPending(target)
	if len(pending) != 1 {
		t.Fatalf("got %d pending, want 1", len(pending))
	}

	// Inflight should still exist (recover to verify)
	store.RecoverInflight(target)
	pending, _ = store.ReadPending(target)
	if len(pending) != 2 {
		t.Fatalf("got %d pending after recover, want 2 (1 new + 1 recovered inflight)", len(pending))
	}
}

func TestStore_HasPendingEvents(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "raw.events"

	// No events initially
	if has, _ := store.HasPendingEvents(target); has {
		t.Fatal("expected no pending events initially")
	}

	// Write an event
	store.Write(target, map[string]any{"x": 1})
	if has, _ := store.HasPendingEvents(target); !has {
		t.Fatal("expected pending events after Write")
	}

	// Claim moves to inflight: HasPendingEvents should return false
	_, _, err = store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if has, _ := store.HasPendingEvents(target); has {
		t.Fatal("expected no pending events after Claim (only inflight)")
	}
}

func TestStore_TouchClaim_Heartbeat(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "raw.events"
	store.Write(target, map[string]any{"x": 1})

	claimID, _, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	// Touch the claim with a heartbeat
	if err := store.TouchClaim(claimID, 5*time.Minute); err != nil {
		t.Fatalf("TouchClaim: %v", err)
	}

	// HasRecentInflight should still be true (heartbeat keeps it alive)
	if has, _ := store.HasRecentInflight(target); !has {
		t.Fatal("expected recent inflight after TouchClaim")
	}

	// RecoverOldInflight should skip heartbeated claims even if they were old
	if err := store.RecoverOldInflight(target); err != nil {
		t.Fatalf("RecoverOldInflight: %v", err)
	}

	// Event should still be inflight (not recovered)
	pending, _ := store.ReadPending(target)
	if len(pending) != 0 {
		t.Fatalf("got %d pending, want 0 (heartbeated claim should not be recovered)", len(pending))
	}
}

func TestStore_Claim_CorruptEvent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "raw.events"

	// Write a corrupt entry directly via badger
	err = store.db.Update(func(txn *badger.Txn) error {
		key := []byte(prefixEvt + target + ":999_1")
		return txn.Set(key, []byte("not valid json"))
	})
	if err != nil {
		t.Fatalf("write corrupt entry: %v", err)
	}

	// Claim should return error (not silently delete the entry)
	_, _, err = store.Claim(target, 100)
	if err == nil {
		t.Fatal("expected error for corrupt event in Claim")
	}

	// The corrupt entry should still exist (txn rolled back on error)
	if has, _ := store.HasPendingEvents(target); !has {
		t.Fatal("corrupt entry should still be pending after failed Claim")
	}
}

func TestStore_ReadPending_CorruptEntry(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "raw.events"

	// Write one good and one corrupt entry
	store.Write(target, map[string]any{"x": 1})
	err = store.db.Update(func(txn *badger.Txn) error {
		key := []byte(prefixEvt + target + ":999_1")
		return txn.Set(key, []byte("{invalid"))
	})
	if err != nil {
		t.Fatalf("write corrupt: %v", err)
	}

	// ReadPending should return error (not silently skip)
	_, err = store.ReadPending(target)
	if err == nil {
		t.Fatal("expected error for corrupt entry in ReadPending")
	}
}

func TestStore_Nack_PreservesAllEvents(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "raw.events"
	store.Write(target, map[string]any{"a": 1})
	store.Write(target, map[string]any{"a": 2})
	store.Write(target, map[string]any{"a": 3})

	claimID, _, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	// Nack should return ALL events back to queue
	if err := store.Nack(claimID); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	// All 3 should be claimable again
	_, events, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim after nack: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("got %d events after Nack, want 3 (all preserved)", len(events))
	}
}

func TestStore_ClearAll_WithColonInTarget(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	// Target with colon (sync:schema.table format)
	target := "sync:staging.orders"
	store.Write(target, map[string]any{"x": 1})
	store.Write(target, map[string]any{"x": 2})

	// Claim to create inflight
	_, _, err = store.Claim(target, 1)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	// ClearAll should handle colons in target correctly
	if err := store.ClearAll(target); err != nil {
		t.Fatalf("ClearAll: %v", err)
	}

	if has, _ := store.HasPendingEvents(target); has {
		t.Fatal("expected no pending after ClearAll with colon target")
	}
	store.RecoverInflight(target)
	_, events, _ := store.Claim(target, 100)
	if len(events) != 0 {
		t.Fatalf("got %d events after ClearAll+Recover, want 0", len(events))
	}
}

func TestStore_ClearAllAndWrite_Atomicity(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "raw.events"
	store.Write(target, map[string]any{"old": 1})

	// ClearAllAndWrite with valid data should succeed atomically
	newEvents := []map[string]any{{"new": 1}, {"new": 2}}
	if err := store.ClearAllAndWrite(target, newEvents); err != nil {
		t.Fatalf("ClearAllAndWrite: %v", err)
	}

	// Should have exactly 2 new events (old one cleared)
	pending, _ := store.ReadPending(target)
	if len(pending) != 2 {
		t.Fatalf("got %d pending, want 2", len(pending))
	}
}

func TestStore_AckAndRequeue_NoFailedRows(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "raw.events"
	store.Write(target, map[string]any{"x": 1})

	claimID, _, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	// AckAndRequeue with nil failedRows = pure ack (used by async poll success)
	if err := store.AckAndRequeue(claimID, target, nil, false); err != nil {
		t.Fatalf("AckAndRequeue (nil): %v", err)
	}

	// Nothing should remain
	store.RecoverInflight(target)
	_, events, _ := store.Claim(target, 100)
	if len(events) != 0 {
		t.Fatalf("got %d events, want 0", len(events))
	}
}

func TestStore_Ack_EmptyClaim(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	// Ack on non-existent claim should succeed (no-op)
	if err := store.Ack("nonexistent_claim_id"); err != nil {
		t.Fatalf("Ack on empty claim: %v", err)
	}
}

// --- Async recovery tests ---

func TestStore_ClearAllAndWrite_DeletesJobRef(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "sync:staging.orders"

	// Save a job_ref (simulates async push in progress)
	store.SaveJobRef(target, map[string]any{"job_id": "j1"}, "hash1")

	// ClearAllAndWrite should delete job_ref
	if err := store.ClearAllAndWrite(target, []map[string]any{{"x": 1}}); err != nil {
		t.Fatalf("ClearAllAndWrite: %v", err)
	}

	ref, _, _ := store.LoadJobRef(target)
	if ref != nil {
		t.Fatalf("expected nil job_ref after ClearAllAndWrite, got %v", ref)
	}
}

func TestStore_WriteBatch_PreservesJobRef(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "sync:staging.orders"

	// Save a job_ref (simulates async push in progress)
	store.SaveJobRef(target, map[string]any{"job_id": "j1"}, "hash1")

	// WriteBatch should NOT touch job_ref
	if err := store.WriteBatch(target, []map[string]any{{"x": 1}}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	ref, hash, err := store.LoadJobRef(target)
	if err != nil {
		t.Fatalf("LoadJobRef: %v", err)
	}
	if ref == nil {
		t.Fatal("WriteBatch should preserve job_ref")
	}
	if ref["job_id"] != "j1" || hash != "hash1" {
		t.Errorf("job_ref corrupted: %v / %s", ref, hash)
	}
}

func TestStore_WriteBatch_PreservesInflight(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "sync:staging.orders"

	// Create inflight (simulates async push claimed and running)
	store.Write(target, map[string]any{"old": 1})
	store.Write(target, map[string]any{"old": 2})
	_, _, err = store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	// WriteBatch adds new events without touching inflight
	if err := store.WriteBatch(target, []map[string]any{{"new": 1}}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	// Should have 1 pending (new) + 2 inflight
	all, err := store.ReadAllEvents(target)
	if err != nil {
		t.Fatalf("ReadAllEvents: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("got %d events (1 pending + 2 inflight), want 3", len(all))
	}
}

func TestStore_AsyncRecovery_JobRefSurvivesWriteBatch(t *testing.T) {
	// Simulates the full async crash recovery flow:
	// 1. Process A: push → job_ref saved → claim inflight → crash
	// 2. Process B: new delta → WriteBatch (preserves job_ref + inflight)
	// 3. Heartbeat expires → RecoverOldInflight → nack → back to evt:
	// 4. Process C: Claim → LoadJobRef → hash matches → resume polling
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "sync:staging.orders"
	rows := []map[string]any{{"id": 1}, {"id": 2}}

	// Step 1: Process A writes events, claims them, saves job_ref
	store.WriteBatch(target, rows)
	claimID, _, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	store.SaveJobRef(target, map[string]any{"job_id": "async_job_1"}, "hash_abc")
	_ = claimID

	// Step 2: Process B arrives with new delta, uses WriteBatch (async preserves state)
	store.WriteBatch(target, []map[string]any{{"id": 3}})

	// Verify: job_ref still exists
	ref, hash, _ := store.LoadJobRef(target)
	if ref == nil || ref["job_id"] != "async_job_1" {
		t.Fatalf("job_ref should survive WriteBatch, got %v", ref)
	}
	if hash != "hash_abc" {
		t.Fatalf("hash should survive WriteBatch, got %s", hash)
	}

	// Step 3: Heartbeat expires → RecoverInflight (simulates RecoverOldInflight after timeout)
	if err := store.RecoverInflight(target); err != nil {
		t.Fatalf("RecoverInflight: %v", err)
	}

	// Step 4: All events now in evt: (2 recovered + 1 new)
	pending, _ := store.ReadPending(target)
	if len(pending) != 3 {
		t.Fatalf("got %d pending after recovery, want 3 (2 recovered + 1 new)", len(pending))
	}

	// Step 4b: job_ref still loadable for resume
	ref, hash, _ = store.LoadJobRef(target)
	if ref == nil {
		t.Fatal("job_ref should still exist after RecoverInflight")
	}
	if ref["job_id"] != "async_job_1" {
		t.Errorf("job_id = %v, want async_job_1", ref["job_id"])
	}
}

func TestStore_ClearAll_DeletesJobRef(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "sync:staging.orders"
	store.SaveJobRef(target, map[string]any{"job_id": "j1"}, "h1")
	store.Write(target, map[string]any{"x": 1})

	if err := store.ClearAll(target); err != nil {
		t.Fatalf("ClearAll: %v", err)
	}

	ref, _, _ := store.LoadJobRef(target)
	if ref != nil {
		t.Fatalf("ClearAll should delete job_ref, got %v", ref)
	}
}

// --- Regression tests ---

// Regression: ClearAllAndWrite must be atomic. Previously ClearAll + WriteBatch
// were two separate transactions, so a crash between them would lose the delta.
func TestStore_ClearAllAndWrite_IsAtomic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "sync:staging.orders"
	store.Write(target, map[string]any{"old": 1})
	store.Write(target, map[string]any{"old": 2})

	// Claim one to create inflight
	store.Claim(target, 1)

	// ClearAllAndWrite should clear evt: + inflight: + jobref: and write new in ONE txn
	store.SaveJobRef(target, map[string]any{"job_id": "j1"}, "h1")
	newEvents := []map[string]any{{"new": 1}, {"new": 2}, {"new": 3}}
	if err := store.ClearAllAndWrite(target, newEvents); err != nil {
		t.Fatalf("ClearAllAndWrite: %v", err)
	}

	// Exactly 3 new pending events (old cleared)
	pending, _ := store.ReadPending(target)
	if len(pending) != 3 {
		t.Fatalf("got %d pending, want 3", len(pending))
	}

	// No inflight
	store.RecoverInflight(target)
	pending2, _ := store.ReadPending(target)
	if len(pending2) != 3 {
		t.Fatalf("got %d after recover, want 3 (no inflight to recover)", len(pending2))
	}

	// No jobref
	ref, _, _ := store.LoadJobRef(target)
	if ref != nil {
		t.Fatal("expected nil jobref after ClearAllAndWrite")
	}
}

// Regression: Nack must preserve ALL events. Previously it silently skipped
// events with parse/read errors via continue, causing data loss.
func TestStore_Nack_NeverLosesEvents(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "sync:staging.orders"
	for i := range 5 {
		store.Write(target, map[string]any{"id": i, "data": "value"})
	}

	claimID, _, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	if err := store.Nack(claimID); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	// All 5 must be back in evt:
	_, events, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim after nack: %v", err)
	}
	if len(events) != 5 {
		t.Fatalf("Nack lost events: got %d, want 5", len(events))
	}
}

// Regression: Claim must return hard error on corrupt JSON, not silently
// delete the entry. Previously corrupt events were deleted via continue.
func TestStore_Claim_CorruptPreservesEntry(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "sync:staging.orders"

	// Write corrupt + valid entries
	store.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(prefixEvt+target+":100_1"), []byte("corrupt{"))
	})
	store.Write(target, map[string]any{"good": true})

	// Claim should fail (corrupt entry)
	_, _, err = store.Claim(target, 100)
	if err == nil {
		t.Fatal("expected error for corrupt event")
	}

	// Both entries should still be pending (txn rolled back)
	if has, _ := store.HasPendingEvents(target); !has {
		t.Fatal("events should still be pending after failed Claim")
	}
}

// Regression: ReadAllEvents must return error on corrupt entries, not skip.
// Previously corrupt entries were silently skipped, and ClearAllAndWrite
// would then delete them permanently.
func TestStore_ReadAllEvents_CorruptInflight(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "sync:staging.orders"

	// Write corrupt inflight entry directly
	store.db.Update(func(txn *badger.Txn) error {
		key := prefixInflight + "fakeclaim:" + target + ":999_1"
		return txn.Set([]byte(key), []byte("not json"))
	})

	_, err = store.ReadAllEvents(target)
	if err == nil {
		t.Fatal("expected error for corrupt inflight entry")
	}
}

// Regression: RecoverOldInflight must recover claims with unparseable
// timestamps instead of leaving them stuck forever.
func TestStore_RecoverOldInflight_UnparseableClaimID(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "sync:staging.orders"

	// Write an inflight entry with unparseable claim ID directly
	store.db.Update(func(txn *badger.Txn) error {
		key := prefixInflight + "notanumber_1:" + target + ":999_1"
		val, _ := json.Marshal(map[string]any{"id": 1})
		return txn.Set([]byte(key), val)
	})

	// RecoverOldInflight should recover it (not leave it stuck)
	if err := store.RecoverOldInflight(target); err != nil {
		t.Fatalf("RecoverOldInflight: %v", err)
	}

	// Event should be back in evt:
	pending, _ := store.ReadPending(target)
	if len(pending) != 1 {
		t.Fatalf("got %d pending, want 1 (unparseable claim should be recovered)", len(pending))
	}
}

// Regression: HasPendingEvents and HasRecentInflight must return errors,
// not silently return false on Badger failures.
func TestStore_HasPendingEvents_ReturnsError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	store.Write("raw.events", map[string]any{"x": 1})

	// Close the store to make View fail
	store.Close()

	_, err = store.HasPendingEvents("raw.events")
	if err == nil {
		t.Fatal("expected error after store closed")
	}
}

func TestStore_ClearPendingAndWrite_PreservesJobRef(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	target := "sync:staging.orders"
	store.SaveJobRef(target, map[string]any{"job_id": "j1"}, "h1")
	store.Write(target, map[string]any{"old": 1})

	if err := store.ClearPendingAndWrite(target, []map[string]any{{"new": 1}}); err != nil {
		t.Fatalf("ClearPendingAndWrite: %v", err)
	}

	// ClearPendingAndWrite only clears evt:, should preserve job_ref
	ref, _, _ := store.LoadJobRef(target)
	if ref == nil {
		t.Fatal("ClearPendingAndWrite should preserve job_ref")
	}
	if ref["job_id"] != "j1" {
		t.Errorf("job_id = %v, want j1", ref["job_id"])
	}
}

// --- Regression: WriteBatch visibility (buffered Write must be visible to reads) ---

func TestStore_WriteBatch_VisibleToHasPending(t *testing.T) {
	t.Parallel()
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	store.Write("raw.events", map[string]any{"event": "pageview"})

	// HasPendingEvents must see the buffered write without explicit flush
	has, err := store.HasPendingEvents("raw.events")
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Fatal("HasPendingEvents should return true after Write (buffered)")
	}
}

func TestStore_WriteBatch_VisibleToReadPending(t *testing.T) {
	t.Parallel()
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	store.Write("raw.events", map[string]any{"event": "click"})

	events, err := store.ReadPending("raw.events")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("ReadPending got %d events, want 1", len(events))
	}
	if events[0]["event"] != "click" {
		t.Errorf("event = %v, want click", events[0]["event"])
	}
}

func TestStore_WriteBatch_VisibleToClaim(t *testing.T) {
	t.Parallel()
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	store.Write("raw.events", map[string]any{"event": "signup"})

	_, events, err := store.Claim("raw.events", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("Claim got %d events, want 1", len(events))
	}
}

func TestStore_WriteBatch_ClearAllAndWrite_NoGhosts(t *testing.T) {
	t.Parallel()
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Write a buffered event
	store.Write("raw.events", map[string]any{"event": "old"})

	// ClearAllAndWrite should clear the buffered event and write new ones
	err = store.ClearAllAndWrite("raw.events", []map[string]any{{"event": "new"}})
	if err != nil {
		t.Fatal(err)
	}

	// Only the new event should exist — the buffered "old" must not resurrect
	events, err := store.ReadPending("raw.events")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if events[0]["event"] != "new" {
		t.Errorf("event = %v, want new (old event resurrected)", events[0]["event"])
	}
}

func TestStore_WriteBatch_ClearPendingAndWrite_NoGhosts(t *testing.T) {
	t.Parallel()
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	store.Write("raw.events", map[string]any{"event": "old"})

	err = store.ClearPendingAndWrite("raw.events", []map[string]any{{"event": "new"}})
	if err != nil {
		t.Fatal(err)
	}

	events, err := store.ReadPending("raw.events")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if events[0]["event"] != "new" {
		t.Errorf("event = %v, want new", events[0]["event"])
	}
}
