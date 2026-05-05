// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package state

import (
	"testing"
	"time"
)

// openTestStore returns a State + SyncStore in a temp dir, registered
// for cleanup via t.Cleanup. Avoids the per-process lock issue by
// scoping each test to its own dir.
func openTestStore(t *testing.T) (*State, *SyncStore) {
	t.Helper()
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	ss, err := NewSyncStore(st)
	if err != nil {
		t.Fatalf("NewSyncStore: %v", err)
	}
	return st, ss
}

func TestSyncStore_WriteThenReadAll(t *testing.T) {
	t.Parallel()
	_, ss := openTestStore(t)

	target := "sync.orders"
	events := []SyncEvent{
		{ChangeType: "insert", RowID: 1, Snapshot: 100},
		{ChangeType: "insert", RowID: 2, Snapshot: 100},
		{ChangeType: "update_postimage", RowID: 1, Snapshot: 101},
	}
	if err := ss.WriteBatch(target, events); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	got, err := ss.ReadAllEvents(target)
	if err != nil {
		t.Fatalf("ReadAllEvents: %v", err)
	}
	if len(got) != len(events) {
		t.Fatalf("got %d events, want %d", len(got), len(events))
	}

	// ReadAllEvents must return seq-ordered (regression: ORDER BY 1 used
	// to sort by payload bytes, not seq).
	for i, want := range events {
		if got[i].ChangeType != want.ChangeType || got[i].RowID != want.RowID || got[i].Snapshot != want.Snapshot {
			t.Errorf("events[%d] = %+v, want %+v", i, got[i], want)
		}
	}
}

func TestSyncStore_HasPendingEvents(t *testing.T) {
	t.Parallel()
	_, ss := openTestStore(t)

	target := "sync.orders"
	has, err := ss.HasPendingEvents(target)
	if err != nil {
		t.Fatalf("HasPendingEvents (empty): %v", err)
	}
	if has {
		t.Error("HasPendingEvents on empty queue = true, want false")
	}

	if err := ss.WriteBatch(target, []SyncEvent{{ChangeType: "insert", RowID: 1, Snapshot: 1}}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	has, err = ss.HasPendingEvents(target)
	if err != nil {
		t.Fatalf("HasPendingEvents (populated): %v", err)
	}
	if !has {
		t.Error("HasPendingEvents on populated queue = false, want true")
	}
}

func TestSyncStore_ClaimAck_RoundTrip(t *testing.T) {
	t.Parallel()
	_, ss := openTestStore(t)

	target := "sync.orders"
	events := []SyncEvent{
		{ChangeType: "insert", RowID: 1, Snapshot: 1},
		{ChangeType: "insert", RowID: 2, Snapshot: 1},
		{ChangeType: "insert", RowID: 3, Snapshot: 1},
	}
	if err := ss.WriteBatch(target, events); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	claimID, claimed, err := ss.Claim(target, 10)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if claimID == "" {
		t.Fatal("Claim returned empty claimID for non-empty queue")
	}
	if len(claimed) != 3 {
		t.Errorf("claimed %d events, want 3", len(claimed))
	}

	if err := ss.Ack(claimID); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// Post-ack: nothing pending, no inflight.
	has, _ := ss.HasPendingEvents(target)
	if has {
		t.Error("HasPendingEvents after Ack = true, want false")
	}
	hasInflight, _ := ss.HasRecentInflight(target)
	if hasInflight {
		t.Error("HasRecentInflight after Ack = true, want false")
	}
}

func TestSyncStore_Claim_EmptyQueueReturnsNoClaim(t *testing.T) {
	t.Parallel()
	_, ss := openTestStore(t)

	claimID, events, err := ss.Claim("sync.empty", 10)
	if err != nil {
		t.Fatalf("Claim on empty queue: %v", err)
	}
	if claimID != "" || len(events) != 0 {
		t.Errorf("Claim on empty queue returned (%q, %d), want empty", claimID, len(events))
	}

	// Critical regression: Claim must NOT leave a sync_claim "tombstone"
	// row when the queue was empty — that would block HasRecentInflight()
	// for the full staleness window.
	hasInflight, err := ss.HasRecentInflight("sync.empty")
	if err != nil {
		t.Fatalf("HasRecentInflight: %v", err)
	}
	if hasInflight {
		t.Error("empty Claim left a ghost sync_claim row (HasRecentInflight=true)")
	}
}

func TestSyncStore_Nack_RequeuesEvents(t *testing.T) {
	t.Parallel()
	_, ss := openTestStore(t)

	target := "sync.orders"
	if err := ss.WriteBatch(target, []SyncEvent{{ChangeType: "insert", RowID: 1, Snapshot: 1}}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	claimID, _, err := ss.Claim(target, 10)
	if err != nil || claimID == "" {
		t.Fatalf("Claim: %v, %q", err, claimID)
	}

	if err := ss.Nack(claimID); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	// Event should be reclaimable.
	has, _ := ss.HasPendingEvents(target)
	if !has {
		t.Error("HasPendingEvents after Nack = false, want true (event should be requeued)")
	}
}

func TestSyncStore_AckAndRequeue_PartialFailure(t *testing.T) {
	t.Parallel()
	_, ss := openTestStore(t)

	target := "sync.orders"
	all := []SyncEvent{
		{ChangeType: "insert", RowID: 1, Snapshot: 1},
		{ChangeType: "insert", RowID: 2, Snapshot: 1},
		{ChangeType: "insert", RowID: 3, Snapshot: 1},
	}
	if err := ss.WriteBatch(target, all); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	claimID, _, err := ss.Claim(target, 10)
	if err != nil || claimID == "" {
		t.Fatalf("Claim: %v, %q", err, claimID)
	}

	// Two events failed, one OK → requeue the two failed.
	failed := []SyncEvent{all[1], all[2]}
	if err := ss.AckAndRequeue(claimID, target, failed, false); err != nil {
		t.Fatalf("AckAndRequeue: %v", err)
	}

	got, err := ss.ReadAllEvents(target)
	if err != nil {
		t.Fatalf("ReadAllEvents: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("got %d requeued, want 2", len(got))
	}
}

func TestSyncStore_TouchClaim(t *testing.T) {
	t.Parallel()
	_, ss := openTestStore(t)

	target := "sync.orders"
	if err := ss.WriteBatch(target, []SyncEvent{{ChangeType: "insert", RowID: 1, Snapshot: 1}}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	claimID, _, err := ss.Claim(target, 10)
	if err != nil || claimID == "" {
		t.Fatalf("Claim: %v, %q", err, claimID)
	}

	if err := ss.TouchClaim(claimID, time.Minute); err != nil {
		t.Errorf("TouchClaim: %v", err)
	}

	hasInflight, _ := ss.HasRecentInflight(target)
	if !hasInflight {
		t.Error("HasRecentInflight after TouchClaim = false, want true")
	}
}

func TestSyncStore_JobRef_RoundTrip(t *testing.T) {
	t.Parallel()
	_, ss := openTestStore(t)

	target := "sync.orders"
	jobRef := map[string]any{"id": "job-123", "url": "https://example.test/job"}
	rowHash := "abc123"

	if err := ss.SaveJobRef(target, jobRef, rowHash); err != nil {
		t.Fatalf("SaveJobRef: %v", err)
	}
	gotRef, gotHash, err := ss.LoadJobRef(target)
	if err != nil {
		t.Fatalf("LoadJobRef: %v", err)
	}
	if gotHash != rowHash {
		t.Errorf("rowHash = %q, want %q", gotHash, rowHash)
	}
	if gotRef["id"] != "job-123" {
		t.Errorf("jobRef[id] = %v, want job-123", gotRef["id"])
	}

	if err := ss.DeleteJobRef(target); err != nil {
		t.Fatalf("DeleteJobRef: %v", err)
	}
	gotRef, _, _ = ss.LoadJobRef(target)
	if gotRef != nil {
		t.Errorf("LoadJobRef after Delete returned %v, want nil", gotRef)
	}
}

func TestSyncStore_RunGC_RecoversOrphanInflight(t *testing.T) {
	t.Parallel()
	st, ss := openTestStore(t)

	target := "sync.orders"
	if err := ss.WriteBatch(target, []SyncEvent{{ChangeType: "insert", RowID: 1, Snapshot: 1}}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	claimID, _, err := ss.Claim(target, 10)
	if err != nil || claimID == "" {
		t.Fatalf("Claim: %v, %q", err, claimID)
	}

	// Backdate the heartbeat past SyncInflightMaxAge.
	stale := time.Now().Add(-2 * SyncInflightMaxAge)
	if _, err := st.DB().Exec(
		`UPDATE sync_claim SET heartbeat = ? WHERE claim_id = ?`,
		stale, claimID); err != nil {
		t.Fatalf("backdate heartbeat: %v", err)
	}

	if err := ss.RunGC(); err != nil {
		t.Fatalf("RunGC: %v", err)
	}

	// Event should be back in sync_evt for retry.
	has, _ := ss.HasPendingEvents(target)
	if !has {
		t.Error("HasPendingEvents after orphan recovery = false, want true")
	}
	hasInflight, _ := ss.HasRecentInflight(target)
	if hasInflight {
		t.Error("HasRecentInflight after orphan recovery = true, want false (claim should be deleted)")
	}
}

func TestSyncStore_ReadAllEvents_OrderedBySeq(t *testing.T) {
	t.Parallel()
	_, ss := openTestStore(t)

	// Three events with payloads that would NOT sort the same way as
	// seq does — pin the regression where ORDER BY 1 sorted by payload
	// bytes (causing nondeterministic replay).
	target := "sync.orders"
	events := []SyncEvent{
		{ChangeType: "insert", RowID: 999, Snapshot: 1}, // seq=N
		{ChangeType: "insert", RowID: 1, Snapshot: 1},   // seq=N+1
		{ChangeType: "insert", RowID: 50, Snapshot: 1},  // seq=N+2
	}
	if err := ss.WriteBatch(target, events); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	got, err := ss.ReadAllEvents(target)
	if err != nil {
		t.Fatalf("ReadAllEvents: %v", err)
	}
	if len(got) != len(events) {
		t.Fatalf("got %d, want %d", len(got), len(events))
	}
	for i, want := range events {
		if got[i].RowID != want.RowID {
			t.Errorf("events[%d].RowID = %d, want %d (seq order broken)", i, got[i].RowID, want.RowID)
		}
	}
}
