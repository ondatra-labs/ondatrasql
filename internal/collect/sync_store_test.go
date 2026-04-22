// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package collect

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func openTestSyncStore(t *testing.T) *SyncStore {
	t.Helper()
	store, err := OpenSyncStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSyncStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestSyncStore_WriteBatchAndClaim(t *testing.T) {
	t.Parallel()
	store := openTestSyncStore(t)
	target := "raw.events"

	events := []SyncEvent{
		{ChangeType: "insert", RowID: 1, Snapshot: 100},
		{ChangeType: "insert", RowID: 2, Snapshot: 100},
		{ChangeType: "delete", RowID: 3, Snapshot: 100},
	}
	if err := store.WriteBatch(target, events); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	claimID, claimed, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if claimID == "" {
		t.Fatal("empty claim ID")
	}
	if len(claimed) != 3 {
		t.Fatalf("got %d events, want 3", len(claimed))
	}

	// Second claim should return 0
	_, claimed2, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim2: %v", err)
	}
	if len(claimed2) != 0 {
		t.Fatalf("got %d events on second claim, want 0", len(claimed2))
	}
}

func TestSyncStore_ClearAllAndWrite(t *testing.T) {
	t.Parallel()
	store := openTestSyncStore(t)
	target := "raw.events"

	// Seed existing events and claim some to create inflight state
	store.WriteBatch(target, []SyncEvent{
		{ChangeType: "insert", RowID: 1, Snapshot: 100},
		{ChangeType: "insert", RowID: 2, Snapshot: 100},
	})
	claimID, _, _ := store.Claim(target, 1)
	// Save a job ref too
	store.SaveJobRef(target, map[string]any{"id": "j1"}, "hash1")

	// ClearAllAndWrite replaces everything
	newEvents := []SyncEvent{
		{ChangeType: "insert", RowID: 10, Snapshot: 200},
	}
	if err := store.ClearAllAndWrite(target, newEvents); err != nil {
		t.Fatalf("ClearAllAndWrite: %v", err)
	}

	// Old inflight should be gone
	if err := store.Ack(claimID); err != nil {
		t.Fatalf("Ack old claim: %v", err)
	}

	// Job ref should be gone
	ref, _, err := store.LoadJobRef(target)
	if err != nil {
		t.Fatalf("LoadJobRef: %v", err)
	}
	if ref != nil {
		t.Fatalf("expected nil job ref, got %v", ref)
	}

	// Only the new event should be claimable
	_, claimed, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("got %d events, want 1", len(claimed))
	}
	if claimed[0].RowID != 10 {
		t.Fatalf("got RowID %d, want 10", claimed[0].RowID)
	}
}

func TestSyncStore_ClearAllAndWrite_PreservesOtherTargets(t *testing.T) {
	t.Parallel()
	store := openTestSyncStore(t)

	store.WriteBatch("target_a", []SyncEvent{
		{ChangeType: "insert", RowID: 1, Snapshot: 100},
	})
	store.WriteBatch("target_b", []SyncEvent{
		{ChangeType: "insert", RowID: 2, Snapshot: 100},
	})

	// Clear target_a
	if err := store.ClearAllAndWrite("target_a", []SyncEvent{
		{ChangeType: "insert", RowID: 99, Snapshot: 200},
	}); err != nil {
		t.Fatalf("ClearAllAndWrite: %v", err)
	}

	// target_b should be untouched
	_, claimed, err := store.Claim("target_b", 100)
	if err != nil {
		t.Fatalf("Claim target_b: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("target_b: got %d events, want 1", len(claimed))
	}
	if claimed[0].RowID != 2 {
		t.Fatalf("target_b: got RowID %d, want 2", claimed[0].RowID)
	}
}

func TestSyncStore_ClearAll(t *testing.T) {
	t.Parallel()
	store := openTestSyncStore(t)
	target := "raw.events"

	store.WriteBatch(target, []SyncEvent{
		{ChangeType: "insert", RowID: 1, Snapshot: 100},
	})
	store.Claim(target, 1) // creates inflight
	store.SaveJobRef(target, map[string]any{"id": "j1"}, "h1")

	if err := store.ClearAll(target); err != nil {
		t.Fatalf("ClearAll: %v", err)
	}

	// Nothing should remain
	_, claimed, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("got %d events after ClearAll, want 0", len(claimed))
	}

	ref, _, err := store.LoadJobRef(target)
	if err != nil {
		t.Fatalf("LoadJobRef: %v", err)
	}
	if ref != nil {
		t.Fatal("expected nil job ref after ClearAll")
	}
}

func TestSyncStore_ReadAllEvents(t *testing.T) {
	t.Parallel()
	store := openTestSyncStore(t)
	target := "raw.events"

	store.WriteBatch(target, []SyncEvent{
		{ChangeType: "insert", RowID: 1, Snapshot: 100},
		{ChangeType: "insert", RowID: 2, Snapshot: 100},
		{ChangeType: "delete", RowID: 3, Snapshot: 100},
	})
	// Claim 1 to move it to inflight
	store.Claim(target, 1)

	all, err := store.ReadAllEvents(target)
	if err != nil {
		t.Fatalf("ReadAllEvents: %v", err)
	}
	// Should see both pending (2) + inflight (1) = 3
	if len(all) != 3 {
		t.Fatalf("got %d events, want 3", len(all))
	}
}

func TestSyncStore_ReadAllEvents_CorruptEntry(t *testing.T) {
	t.Parallel()
	store := openTestSyncStore(t)
	target := "raw.events"

	// Write a corrupt entry directly via Badger
	key := fmt.Sprintf("%s%s:%d_%d", syncPrefixEvt, target, time.Now().UnixNano(), 999)
	err := store.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), []byte("not-json"))
	})
	if err != nil {
		t.Fatalf("write corrupt entry: %v", err)
	}

	_, err = store.ReadAllEvents(target)
	if err == nil {
		t.Fatal("expected error on corrupt entry, got nil")
	}
}

func TestSyncStore_Nack_PreservesAllEvents(t *testing.T) {
	t.Parallel()
	store := openTestSyncStore(t)
	target := "raw.events"

	events := []SyncEvent{
		{ChangeType: "insert", RowID: 1, Snapshot: 100},
		{ChangeType: "insert", RowID: 2, Snapshot: 100},
		{ChangeType: "delete", RowID: 3, Snapshot: 100},
	}
	store.WriteBatch(target, events)

	claimID, _, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	if err := store.Nack(claimID); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	// All events should be claimable again
	_, reclaimed, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim after nack: %v", err)
	}
	if len(reclaimed) != 3 {
		t.Fatalf("got %d events after nack, want 3", len(reclaimed))
	}
}

func TestSyncStore_AckAndRequeue(t *testing.T) {
	t.Parallel()
	store := openTestSyncStore(t)
	target := "raw.events"

	events := []SyncEvent{
		{ChangeType: "insert", RowID: 1, Snapshot: 100},
		{ChangeType: "insert", RowID: 2, Snapshot: 100},
		{ChangeType: "insert", RowID: 3, Snapshot: 100},
	}
	store.WriteBatch(target, events)

	claimID, _, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	// Only RowID=2 failed
	failed := []SyncEvent{{ChangeType: "insert", RowID: 2, Snapshot: 100}}
	if err := store.AckAndRequeue(claimID, target, failed, false); err != nil {
		t.Fatalf("AckAndRequeue: %v", err)
	}

	// Only the failed event should be claimable
	_, reclaimed, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim after AckAndRequeue: %v", err)
	}
	if len(reclaimed) != 1 {
		t.Fatalf("got %d events, want 1", len(reclaimed))
	}
	if reclaimed[0].RowID != 2 {
		t.Fatalf("got RowID %d, want 2", reclaimed[0].RowID)
	}
}

func TestSyncStore_AckAndRequeue_WithJobRef(t *testing.T) {
	t.Parallel()
	store := openTestSyncStore(t)
	target := "raw.events"

	store.WriteBatch(target, []SyncEvent{
		{ChangeType: "insert", RowID: 1, Snapshot: 100},
	})
	store.SaveJobRef(target, map[string]any{"id": "j1"}, "hash1")

	claimID, _, _ := store.Claim(target, 100)

	// AckAndRequeue with deleteJobRef=true
	failed := []SyncEvent{{ChangeType: "insert", RowID: 1, Snapshot: 100}}
	if err := store.AckAndRequeue(claimID, target, failed, true); err != nil {
		t.Fatalf("AckAndRequeue: %v", err)
	}

	// Job ref should be deleted
	ref, _, err := store.LoadJobRef(target)
	if err != nil {
		t.Fatalf("LoadJobRef: %v", err)
	}
	if ref != nil {
		t.Fatalf("expected nil job ref after AckAndRequeue with deleteJobRef, got %v", ref)
	}
}

func TestSyncStore_HasPendingEvents_ReturnsError(t *testing.T) {
	t.Parallel()
	store := openTestSyncStore(t)
	target := "raw.events"

	// Empty store: no pending
	has, err := store.HasPendingEvents(target)
	if err != nil {
		t.Fatalf("HasPendingEvents: %v", err)
	}
	if has {
		t.Fatal("expected no pending events on empty store")
	}

	// Write events: should have pending
	store.WriteBatch(target, []SyncEvent{
		{ChangeType: "insert", RowID: 1, Snapshot: 100},
	})

	has, err = store.HasPendingEvents(target)
	if err != nil {
		t.Fatalf("HasPendingEvents: %v", err)
	}
	if !has {
		t.Fatal("expected pending events after WriteBatch")
	}

	// After close, error should propagate (not be swallowed)
	store.Close()
	_, err = store.HasPendingEvents(target)
	if err == nil {
		t.Fatal("expected error after Close, got nil")
	}
}

func TestSyncStore_HasRecentInflight(t *testing.T) {
	t.Parallel()
	store := openTestSyncStore(t)
	target := "raw.events"

	// No inflight initially
	has, err := store.HasRecentInflight(target)
	if err != nil {
		t.Fatalf("HasRecentInflight: %v", err)
	}
	if has {
		t.Fatal("expected no inflight on empty store")
	}

	// Create inflight via Claim
	store.WriteBatch(target, []SyncEvent{
		{ChangeType: "insert", RowID: 1, Snapshot: 100},
	})
	_, _, err = store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	has, err = store.HasRecentInflight(target)
	if err != nil {
		t.Fatalf("HasRecentInflight: %v", err)
	}
	if !has {
		t.Fatal("expected recent inflight after Claim")
	}
}

func TestSyncStore_TouchClaim_Heartbeat(t *testing.T) {
	t.Parallel()
	store := openTestSyncStore(t)
	target := "raw.events"

	store.WriteBatch(target, []SyncEvent{
		{ChangeType: "insert", RowID: 1, Snapshot: 100},
	})
	claimID, _, _ := store.Claim(target, 100)

	// Touch the claim with a heartbeat
	if err := store.TouchClaim(claimID, 5*time.Minute); err != nil {
		t.Fatalf("TouchClaim: %v", err)
	}

	// Heartbeat should be detectable
	if !store.hasRecentHeartbeat(claimID) {
		t.Fatal("expected recent heartbeat after TouchClaim")
	}

	// HasRecentInflight should return true (heartbeat keeps it alive)
	has, err := store.HasRecentInflight(target)
	if err != nil {
		t.Fatalf("HasRecentInflight: %v", err)
	}
	if !has {
		t.Fatal("expected HasRecentInflight=true with active heartbeat")
	}
}

func TestSyncStore_RecoverOldInflight_UnparseableClaim(t *testing.T) {
	t.Parallel()
	store := openTestSyncStore(t)
	target := "raw.events"

	// Inject an inflight entry with an unparseable claim ID directly
	event := SyncEvent{ChangeType: "insert", RowID: 42, Snapshot: 100}
	val, _ := json.Marshal(event)
	inflightKey := fmt.Sprintf("%sbadclaim:%s:%d_%d", syncPrefixInflight, target, time.Now().UnixNano(), 1)
	err := store.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(inflightKey), val)
	})
	if err != nil {
		t.Fatalf("inject unparseable inflight: %v", err)
	}

	// RecoverOldInflight should recover (nack) it rather than leave it stuck
	if err := store.RecoverOldInflight(target); err != nil {
		t.Fatalf("RecoverOldInflight: %v", err)
	}

	// The event should be back in pending
	_, claimed, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim after recover: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("got %d events, want 1", len(claimed))
	}
	if claimed[0].RowID != 42 {
		t.Fatalf("got RowID %d, want 42", claimed[0].RowID)
	}
}

func TestSyncStore_WriteBatch_PreservesJobRef(t *testing.T) {
	t.Parallel()
	store := openTestSyncStore(t)
	target := "raw.events"

	store.SaveJobRef(target, map[string]any{"id": "j1"}, "hash1")

	// WriteBatch should not delete the job ref
	store.WriteBatch(target, []SyncEvent{
		{ChangeType: "insert", RowID: 1, Snapshot: 100},
	})

	ref, hash, err := store.LoadJobRef(target)
	if err != nil {
		t.Fatalf("LoadJobRef: %v", err)
	}
	if ref == nil {
		t.Fatal("expected job ref to survive WriteBatch")
	}
	if ref["id"] != "j1" {
		t.Fatalf("got job ref id %v, want j1", ref["id"])
	}
	if hash != "hash1" {
		t.Fatalf("got hash %q, want hash1", hash)
	}
}

func TestSyncStore_SaveLoadDeleteJobRef(t *testing.T) {
	t.Parallel()
	store := openTestSyncStore(t)
	target := "raw.events"

	// Save
	if err := store.SaveJobRef(target, map[string]any{"run_id": "abc123"}, "deadbeef"); err != nil {
		t.Fatalf("SaveJobRef: %v", err)
	}

	// Load
	ref, hash, err := store.LoadJobRef(target)
	if err != nil {
		t.Fatalf("LoadJobRef: %v", err)
	}
	if ref["run_id"] != "abc123" {
		t.Fatalf("got run_id %v, want abc123", ref["run_id"])
	}
	if hash != "deadbeef" {
		t.Fatalf("got hash %q, want deadbeef", hash)
	}

	// Delete
	if err := store.DeleteJobRef(target); err != nil {
		t.Fatalf("DeleteJobRef: %v", err)
	}

	// Load again should return nil
	ref, hash, err = store.LoadJobRef(target)
	if err != nil {
		t.Fatalf("LoadJobRef after delete: %v", err)
	}
	if ref != nil {
		t.Fatalf("expected nil job ref after delete, got %v", ref)
	}
	if hash != "" {
		t.Fatalf("expected empty hash after delete, got %q", hash)
	}
}

// Regression: Bug 2 — RecoverOldInflight + Claim must be atomic.
// Before the fix, RecoverOldInflight ran in a separate transaction,
// creating a window where recovered events could be double-claimed.
// This test verifies that Claim inlines recovery atomically.
func TestSyncStore_Claim_AtomicRecovery(t *testing.T) {
	t.Parallel()
	store := openTestSyncStore(t)
	target := "raw.events"

	// Write 3 events and claim them to create inflight state
	store.WriteBatch(target, []SyncEvent{
		{ChangeType: "insert", RowID: 1, Snapshot: 100},
		{ChangeType: "insert", RowID: 2, Snapshot: 100},
		{ChangeType: "insert", RowID: 3, Snapshot: 100},
	})
	oldClaimID, _, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("initial Claim: %v", err)
	}

	// Simulate crash: inject old timestamp on the inflight entries by
	// re-writing them with an old claim ID (> syncInflightMaxAge ago)
	oldTS := time.Now().Add(-15 * time.Minute).UnixNano()
	fakeClaim := fmt.Sprintf("%d_1", oldTS)

	// Move events from current claim to fake old claim
	err = store.db.Update(func(txn *badger.Txn) error {
		prefix := []byte(syncPrefixInflight + oldClaimID + ":")
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.Valid(); it.Next() {
			key := string(it.Item().Key())
			var val []byte
			it.Item().Value(func(v []byte) error {
				val = make([]byte, len(v))
				copy(val, v)
				return nil
			})
			// Create new key with fake old claim ID
			rest := key[len(syncPrefixInflight+oldClaimID+":"):]
			newKey := syncPrefixInflight + fakeClaim + ":" + target + ":" + rest
			if err := txn.Set([]byte(newKey), val); err != nil {
				return err
			}
			if err := txn.Delete([]byte(key)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("inject old inflight: %v", err)
	}

	// Now Claim should atomically: recover old inflight → claim them
	// Two concurrent Claims should not both get the same events
	claimID1, events1, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim 1: %v", err)
	}

	claimID2, events2, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim 2: %v", err)
	}

	// First claim should get all 3 events (recovered + claimed atomically)
	if len(events1) != 3 {
		t.Errorf("Claim 1 got %d events, want 3", len(events1))
	}
	// Second claim should get 0 (all already claimed by first)
	if len(events2) != 0 {
		t.Errorf("Claim 2 got %d events, want 0 (all claimed by first)", len(events2))
	}

	_ = claimID1
	_ = claimID2
}

// Regression: Verify that inflight entries from other targets are not
// recovered during Claim for a specific target.
func TestSyncStore_Claim_RecoveryIsolatedByTarget(t *testing.T) {
	t.Parallel()
	store := openTestSyncStore(t)

	// Write events for two targets
	store.WriteBatch("target_a", []SyncEvent{
		{ChangeType: "insert", RowID: 1, Snapshot: 100},
	})
	store.WriteBatch("target_b", []SyncEvent{
		{ChangeType: "insert", RowID: 2, Snapshot: 100},
	})

	// Claim both
	_, _, _ = store.Claim("target_a", 100)
	_, _, _ = store.Claim("target_b", 100)

	// Claiming target_a again should get 0 (not recover target_b's inflight)
	_, events, err := store.Claim("target_a", 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("got %d events, want 0 (should not recover other target's inflight)", len(events))
	}
}

// Regression: Bug 1 — WriteBatch must not destroy active inflight claims.
// Before the fix, the sink called ClearAllAndWrite even when another worker
// had active inflight entries, destroying their claim state.
// The fix is in queueDelta (uses WriteBatch when hasInflight=true), but we
// verify the store-level invariant here: WriteBatch preserves inflight.
func TestSyncStore_WriteBatch_PreservesInflight(t *testing.T) {
	t.Parallel()
	store := openTestSyncStore(t)
	target := "raw.events"

	// Write initial events and claim them (simulating active worker)
	store.WriteBatch(target, []SyncEvent{
		{ChangeType: "insert", RowID: 1, Snapshot: 100},
		{ChangeType: "insert", RowID: 2, Snapshot: 100},
	})
	claimID, claimed, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed %d, want 2", len(claimed))
	}

	// New delta arrives while inflight is active — WriteBatch should NOT
	// destroy the inflight entries (unlike ClearAllAndWrite which would)
	if err := store.WriteBatch(target, []SyncEvent{
		{ChangeType: "insert", RowID: 3, Snapshot: 200},
	}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	// Active worker acks — should succeed (inflight entries still exist)
	if err := store.Ack(claimID); err != nil {
		t.Fatalf("Ack after WriteBatch: %v", err)
	}

	// Only the new event should be pending
	_, remaining, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim remaining: %v", err)
	}
	if len(remaining) != 1 {
		t.Fatalf("got %d remaining events, want 1", len(remaining))
	}
	if remaining[0].RowID != 3 {
		t.Errorf("remaining RowID=%d, want 3", remaining[0].RowID)
	}
}

// Contrast: ClearAllAndWrite DOES destroy inflight (by design, for backfill).
// This test documents the expected behavior difference.
func TestSyncStore_ClearAllAndWrite_DestroysInflight(t *testing.T) {
	t.Parallel()
	store := openTestSyncStore(t)
	target := "raw.events"

	store.WriteBatch(target, []SyncEvent{
		{ChangeType: "insert", RowID: 1, Snapshot: 100},
	})
	_, _, _ = store.Claim(target, 100)

	// ClearAllAndWrite destroys inflight + pending + jobref
	if err := store.ClearAllAndWrite(target, []SyncEvent{
		{ChangeType: "insert", RowID: 99, Snapshot: 200},
	}); err != nil {
		t.Fatalf("ClearAllAndWrite: %v", err)
	}

	// Only the new event remains
	_, claimed, err := store.Claim(target, 100)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("got %d events, want 1", len(claimed))
	}
	if claimed[0].RowID != 99 {
		t.Errorf("RowID=%d, want 99", claimed[0].RowID)
	}
}

func TestSyncStore_Claim_CorruptEvent(t *testing.T) {
	t.Parallel()
	store := openTestSyncStore(t)
	target := "raw.events"

	// Inject a corrupt entry directly via Badger
	key := fmt.Sprintf("%s%s:%d_%d", syncPrefixEvt, target, time.Now().UnixNano(), 999)
	err := store.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), []byte("{invalid json"))
	})
	if err != nil {
		t.Fatalf("inject corrupt entry: %v", err)
	}

	// Claim should return a hard error, not silently skip
	_, _, err = store.Claim(target, 100)
	if err == nil {
		t.Fatal("expected error on corrupt event, got nil")
	}
}
