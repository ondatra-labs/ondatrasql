// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package collect

import (
	"testing"
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
