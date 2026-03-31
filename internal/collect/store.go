// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package collect provides durable event buffering backed by Badger.
// Events are written via HTTP, stored in Badger with TTL, and flushed
// to DuckLake during pipeline runs via a claim/ack/nack protocol.
package collect

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
)

const (
	prefixEvt      = "evt:"
	prefixInflight = "inflight:"
	eventTTL       = 24 * time.Hour
	inflightMaxAge = 10 * time.Minute
	defaultLimit   = 1000
)

// Store wraps a Badger database for durable event buffering.
type Store struct {
	db      *badger.DB
	counter atomic.Uint64
}

// Open opens or creates a Badger store at the given directory.
func Open(dir string) (*Store, error) {
	opts := badger.DefaultOptions(dir).
		WithLogger(nil) // Suppress Badger's built-in logging
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open badger: %w", err)
	}
	return &Store{db: db}, nil
}

// Write stores an event for the given target (schema.table).
// Key format: "evt:{target}:{nanoTimestamp}_{counter}"
// Value: JSON-encoded event. TTL: 24h.
func (s *Store) Write(target string, event map[string]any) error {
	val, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	seq := s.counter.Add(1)
	key := fmt.Sprintf("%s%s:%d_%d", prefixEvt, target, time.Now().UnixNano(), seq)

	return s.db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry([]byte(key), val).WithTTL(eventTTL)
		return txn.SetEntry(e)
	})
}

// WriteBatch stores multiple events for the given target in a single transaction.
// Either all events are written or none (atomic).
func (s *Store) WriteBatch(target string, events []map[string]any) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, event := range events {
			val, err := json.Marshal(event)
			if err != nil {
				return fmt.Errorf("marshal event: %w", err)
			}
			seq := s.counter.Add(1)
			key := fmt.Sprintf("%s%s:%d_%d", prefixEvt, target, time.Now().UnixNano(), seq)
			e := badger.NewEntry([]byte(key), val).WithTTL(eventTTL)
			if err := txn.SetEntry(e); err != nil {
				return err
			}
		}
		return nil
	})
}

// Claim atomically moves up to `limit` events from "evt:{target}:" to
// "inflight:{claimID}:{target}:" and returns them.
//
// All three operations (read evt:, write inflight:, delete evt:) happen
// in a single Badger transaction for atomicity. Badger has no atomic
// rename — we do read+write+delete in the same txn.
//
// BADGER TXN LIMIT: ErrTxnTooBig triggers when too many operations occur.
// Each event = 3 ops (read + write inflight + delete evt). Default limit
// should be conservative (~1000 events = ~3000 ops) to fit in one txn.
// If ErrTxnTooBig occurs, the entire transaction is rolled back (nothing moved)
// and an error is returned. The caller should retry with a smaller limit.
func (s *Store) Claim(target string, limit int) (string, []map[string]any, error) {
	if limit <= 0 {
		limit = defaultLimit
	}

	// Step 1: Recover old inflight batches (older than 10 min, from crashed runs).
	// Do NOT recover recent inflight — they may belong to an active runner.
	if err := s.recoverOldInflight(target); err != nil {
		return "", nil, fmt.Errorf("recover old inflight: %w", err)
	}

	// Step 2: Claim events in a single atomic transaction.
	claimID := fmt.Sprintf("%d_%d", time.Now().UnixNano(), s.counter.Add(1))
	prefix := []byte(prefixEvt + target + ":")

	var events []map[string]any

	err := s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		var keys [][]byte
		for it.Seek(prefix); it.Valid(); it.Next() {
			if len(keys) >= limit {
				break
			}
			item := it.Item()
			// Copy key (iterator reuses buffer)
			key := make([]byte, len(item.Key()))
			copy(key, item.Key())

			var event map[string]any
			err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &event)
			})
			if err != nil {
				continue // Skip corrupt events
			}

			events = append(events, event)
			keys = append(keys, key)
		}

		if len(keys) == 0 {
			return nil
		}

		// Write to inflight and delete originals in the same txn.
		for i, key := range keys {
			val, err := json.Marshal(events[i])
			if err != nil {
				continue
			}
			inflightKey := fmt.Sprintf("%s%s:%s:%s", prefixInflight, claimID, target, string(key[len(prefix):]))
			// No TTL on inflight — we must not lose claimed events.
			if err := txn.Set([]byte(inflightKey), val); err != nil {
				return err
			}
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, badger.ErrTxnTooBig) && limit > 100 {
			// Txn too large — retry with half the limit.
			// The runner loops until 0 events, so smaller batches are fine.
			return s.Claim(target, limit/2)
		}
		return "", nil, fmt.Errorf("claim txn: %w", err)
	}

	return claimID, events, nil
}

// Ack deletes all inflight events for a claim (successful DuckLake commit).
func (s *Store) Ack(claimID string) error {
	prefix := []byte(prefixInflight + claimID + ":")

	return s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false // Only need keys
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.Valid(); it.Next() {
			key := make([]byte, len(it.Item().Key()))
			copy(key, it.Item().Key())
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

// Nack moves inflight events back to "evt:" in a single transaction
// (failed DuckLake insert — events need to be retried).
func (s *Store) Nack(claimID string) error {
	prefix := []byte(prefixInflight + claimID + ":")

	return s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.Valid(); it.Next() {
			item := it.Item()
			key := make([]byte, len(item.Key()))
			copy(key, item.Key())

			// Extract target and event suffix from inflight key.
			// Key format: "inflight:{claimID}:{target}:{suffix}"
			evtKey, err := inflightToEvtKey(string(key))
			if err != nil {
				continue
			}

			var val []byte
			err = item.Value(func(v []byte) error {
				val = make([]byte, len(v))
				copy(val, v)
				return nil
			})
			if err != nil {
				continue
			}

			e := badger.NewEntry([]byte(evtKey), val).WithTTL(eventTTL)
			if err := txn.SetEntry(e); err != nil {
				return err
			}
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

// FindInflightClaims returns unique claim IDs with inflight events for a target.
// Used by callers that need to selectively ack or nack individual claims
// (e.g., to skip already-committed claims after a crash).
func (s *Store) FindInflightClaims(target string) ([]string, error) {
	return s.findInflightClaims(target)
}

// RecoverInflight moves ALL inflight events for a target back to "evt:".
// Called at daemon startup (no runner is active) to recover from crashes.
func (s *Store) RecoverInflight(target string) error {
	// Find all claim IDs that have inflight events for this target.
	claimIDs, err := s.findInflightClaims(target)
	if err != nil {
		return err
	}

	for _, claimID := range claimIDs {
		if err := s.Nack(claimID); err != nil {
			return fmt.Errorf("recover claim %s: %w", claimID, err)
		}
	}
	return nil
}

// RecoverAllInflight recovers all inflight events regardless of target.
// Called at daemon startup.
func (s *Store) RecoverAllInflight() error {
	claimIDs, err := s.findAllInflightClaims()
	if err != nil {
		return err
	}
	for _, claimID := range claimIDs {
		if err := s.Nack(claimID); err != nil {
			return fmt.Errorf("recover claim %s: %w", claimID, err)
		}
	}
	return nil
}

// Close closes the Badger database.
func (s *Store) Close() error {
	return s.db.Close()
}

// RunGC triggers Badger's value log garbage collection.
// Should be called periodically (e.g. every 5 minutes).
func (s *Store) RunGC() {
	_ = s.db.RunValueLogGC(0.5)
}

// recoverOldInflight recovers inflight batches older than inflightMaxAge
// for a specific target. Recent inflight batches are left alone since
// they may belong to an active runner.
func (s *Store) recoverOldInflight(target string) error {
	claimIDs, err := s.findInflightClaims(target)
	if err != nil {
		return err
	}

	cutoff := time.Now().Add(-inflightMaxAge)
	for _, claimID := range claimIDs {
		ts, err := claimTimestamp(claimID)
		if err != nil {
			continue
		}
		if ts.Before(cutoff) {
			if err := s.Nack(claimID); err != nil {
				return err
			}
		}
	}
	return nil
}

// findInflightClaims returns unique claim IDs with inflight events for a target.
func (s *Store) findInflightClaims(target string) ([]string, error) {
	seen := make(map[string]bool)
	var claimIDs []string

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefixInflight)
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek([]byte(prefixInflight)); it.Valid(); it.Next() {
			key := string(it.Item().Key())
			// Key format: "inflight:{claimID}:{target}:{suffix}"
			parts := strings.SplitN(key[len(prefixInflight):], ":", 3)
			if len(parts) < 3 {
				continue
			}
			claimID := parts[0]
			evtTarget := parts[1]
			if evtTarget == target && !seen[claimID] {
				seen[claimID] = true
				claimIDs = append(claimIDs, claimID)
			}
		}
		return nil
	})
	return claimIDs, err
}

// findAllInflightClaims returns all unique claim IDs.
func (s *Store) findAllInflightClaims() ([]string, error) {
	seen := make(map[string]bool)
	var claimIDs []string

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefixInflight)
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek([]byte(prefixInflight)); it.Valid(); it.Next() {
			key := string(it.Item().Key())
			parts := strings.SplitN(key[len(prefixInflight):], ":", 3)
			if len(parts) < 1 {
				continue
			}
			claimID := parts[0]
			if !seen[claimID] {
				seen[claimID] = true
				claimIDs = append(claimIDs, claimID)
			}
		}
		return nil
	})
	return claimIDs, err
}

// inflightToEvtKey converts an inflight key back to an evt key.
// "inflight:{claimID}:{target}:{suffix}" → "evt:{target}:{suffix}"
func inflightToEvtKey(inflightKey string) (string, error) {
	rest := inflightKey[len(prefixInflight):]
	// rest = "{claimID}:{target}:{suffix}"
	parts := strings.SplitN(rest, ":", 3)
	if len(parts) < 3 {
		return "", fmt.Errorf("invalid inflight key: %s", inflightKey)
	}
	target := parts[1]
	suffix := parts[2]
	return prefixEvt + target + ":" + suffix, nil
}

// claimTimestamp extracts the timestamp from a claim ID.
// Claim ID format: "{nanoTimestamp}_{counter}"
func claimTimestamp(claimID string) (time.Time, error) {
	parts := strings.SplitN(claimID, "_", 2)
	if len(parts) == 0 {
		return time.Time{}, fmt.Errorf("invalid claim ID: %s", claimID)
	}
	nanos, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, nanos), nil
}
