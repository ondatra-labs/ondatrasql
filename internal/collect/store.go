// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
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
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
)

const (
	prefixEvt      = "evt:"
	prefixInflight = "inflight:"
	prefixJobRef   = "jobref:"
	eventTTL       = 24 * time.Hour
	inflightMaxAge = 10 * time.Minute
	defaultLimit   = 1000
)

// Store wraps a Badger database for durable event buffering.
type Store struct {
	db      *badger.DB
	counter atomic.Uint64
	wb      *badger.WriteBatch
	wbMu    sync.Mutex
}

// Open opens or creates a Badger store at the given directory.
func Open(dir string) (*Store, error) {
	opts := badger.DefaultOptions(dir).
		WithLogger(nil).        // Suppress Badger's built-in logging
		WithSyncWrites(false)   // Async writes — OS flushes to disk periodically
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open badger: %w", err)
	}
	s := &Store{db: db, wb: db.NewWriteBatch()}

	// Scan existing keys to find the max counter value, preventing
	// key collisions on restart (Bug S31-32).
	var maxSeq uint64
	db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := string(it.Item().Key())
			// Keys end with _{counter}
			if idx := strings.LastIndex(key, "_"); idx >= 0 {
				if n, err := strconv.ParseUint(key[idx+1:], 10, 64); err == nil && n > maxSeq {
					maxSeq = n
				}
			}
		}
		return nil
	})
	s.counter.Store(maxSeq)

	return s, nil
}

// Write stores an event for the given target (schema.table).
// Key format: "evt:{target}:{nanoTimestamp}_{counter}"
// Value: JSON-encoded event. TTL: 24h.
// Uses WriteBatch for high throughput — writes are non-blocking and
// batched automatically by Badger.
func (s *Store) Write(target string, event map[string]any) error {
	val, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	seq := s.counter.Add(1)
	key := fmt.Sprintf("%s%s:%d_%d", prefixEvt, target, time.Now().UnixNano(), seq)

	s.wbMu.Lock()
	defer s.wbMu.Unlock()
	e := badger.NewEntry([]byte(key), val).WithTTL(eventTTL)
	return s.wb.SetEntry(e)
}

// FlushWrites flushes pending WriteBatch entries to disk.
// Called periodically by the server and on shutdown.
func (s *Store) FlushWrites() error {
	s.wbMu.Lock()
	defer s.wbMu.Unlock()
	if err := s.wb.Flush(); err != nil {
		return err
	}
	s.wb = s.db.NewWriteBatch()
	return nil
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

// HasPendingEvents returns true if there are claimable events (evt: prefix) for the target.
// Does NOT count inflight batches -- those are from previous claims that haven't been acked.
func (s *Store) HasPendingEvents(target string) (bool, error) {
	if err := s.FlushWrites(); err != nil {
		return false, err
	}
	evtPrefix := []byte(prefixEvt + target + ":")
	found := false
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = evtPrefix
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Seek(evtPrefix)
		if it.Valid() {
			found = true
		}
		return nil
	})
	return found, err
}

// HasRecentInflight returns true if there are inflight batches for the target
// that are either young (< inflightMaxAge) or actively being processed (have a heartbeat).
// Young claims represent recently crashed processes. Heartbeats represent active async polling.
func (s *Store) HasRecentInflight(target string) (bool, error) {
	inflightPrefix := []byte(prefixInflight)
	targetPrefix := target + ":"
	cutoff := time.Now().Add(-inflightMaxAge)
	found := false

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = inflightPrefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(inflightPrefix); it.Valid(); it.Next() {
			key := string(it.Item().Key())
			rest := key[len(prefixInflight):]

			colonIdx := strings.Index(rest, ":")
			if colonIdx < 0 {
				continue
			}
			claimID := rest[:colonIdx]
			afterClaim := rest[colonIdx+1:]
			if !strings.HasPrefix(afterClaim, targetPrefix) {
				continue
			}

			// Check heartbeat first -- active polling keeps claim alive
			if s.hasRecentHeartbeat(claimID) {
				found = true
				return nil
			}

			// Fall back to claim age
			parts := strings.SplitN(claimID, "_", 2)
			if len(parts) < 1 {
				continue
			}
			var nanos int64
			if _, err := fmt.Sscanf(parts[0], "%d", &nanos); err != nil {
				continue
			}
			claimTime := time.Unix(0, nanos)
			if claimTime.After(cutoff) {
				found = true
				return nil
			}
		}
		return nil
	})
	return found, err
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
	// Flush buffered writes so they're visible to the claim scan
	if err := s.FlushWrites(); err != nil {
		return "", nil, fmt.Errorf("flush before claim: %w", err)
	}

	if limit <= 0 {
		limit = defaultLimit
	}

	// Recovery + claim in a single atomic transaction.
	// Before this fix, RecoverOldInflight ran as a separate transaction,
	// creating a window where recovered events could be double-claimed
	// by a concurrent Claim call.
	claimID := fmt.Sprintf("%d_%d", time.Now().UnixNano(), s.counter.Add(1))
	evtPrefix := []byte(prefixEvt + target + ":")
	inflightPrefix := []byte(prefixInflight)
	targetSuffix := target + ":"
	cutoff := time.Now().Add(-inflightMaxAge)

	var events []map[string]any

	err := s.db.Update(func(txn *badger.Txn) error {
		// Phase 1: Recover old inflight events back to evt: within this txn.
		// This is the inlined equivalent of RecoverOldInflight + Nack,
		// but done inside the same transaction as the claim.
		{
			opts := badger.DefaultIteratorOptions
			opts.Prefix = inflightPrefix
			it := txn.NewIterator(opts)

			type nackEntry struct {
				inflightKey []byte
				evtKey      string
				val         []byte
			}
			var toNack []nackEntry

			for it.Seek(inflightPrefix); it.Valid(); it.Next() {
				key := string(it.Item().Key())
				rest := key[len(prefixInflight):]
				cid, afterClaim, ok := splitClaimKey(rest)
				if !ok || !strings.HasPrefix(afterClaim, targetSuffix) {
					continue
				}
				// Skip recent claims (may still be active)
				ts, err := claimTimestamp(cid)
				if err == nil && !ts.Before(cutoff) {
					if s.hasRecentHeartbeat(cid) {
						continue
					}
					continue
				}
				// Old or unparseable claim — recover it
				evtKey, err := inflightToEvtKey(key)
				if err != nil {
					continue
				}
				var val []byte
				if err := it.Item().Value(func(v []byte) error {
					val = make([]byte, len(v))
					copy(val, v)
					return nil
				}); err != nil {
					continue
				}
				toNack = append(toNack, nackEntry{
					inflightKey: []byte(key),
					evtKey:      evtKey,
					val:         val,
				})
			}
			it.Close()

			for _, entry := range toNack {
				e := badger.NewEntry([]byte(entry.evtKey), entry.val).WithTTL(eventTTL)
				if err := txn.SetEntry(e); err != nil {
					return err
				}
				if err := txn.Delete(entry.inflightKey); err != nil {
					return err
				}
			}
		}

		// Phase 2: Claim pending events (evt: → inflight:)
		opts := badger.DefaultIteratorOptions
		opts.Prefix = evtPrefix
		it := txn.NewIterator(opts)
		defer it.Close()

		var keys [][]byte
		for it.Seek(evtPrefix); it.Valid(); it.Next() {
			if len(keys) >= limit {
				break
			}
			item := it.Item()
			key := make([]byte, len(item.Key()))
			copy(key, item.Key())

			var event map[string]any
			err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &event)
			})
			if err != nil {
				return fmt.Errorf("corrupt event at key %s: %w", string(key), err)
			}

			events = append(events, event)
			keys = append(keys, key)
		}

		if len(keys) == 0 {
			return nil
		}

		for i, key := range keys {
			val, err := json.Marshal(events[i])
			if err != nil {
				return fmt.Errorf("re-marshal event %d: %w", i, err)
			}
			inflightKey := fmt.Sprintf("%s%s:%s:%s", prefixInflight, claimID, target, string(key[len(evtPrefix):]))
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

// TouchClaim updates the heartbeat for an inflight claim so it won't be
// recovered by RecoverOldInflight while still actively being processed.
// ttl should be at least 3x the poll interval to survive between heartbeats.
func (s *Store) TouchClaim(claimID string, ttl time.Duration) error {
	key := []byte("heartbeat:" + claimID)
	return s.db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(key, []byte(fmt.Sprintf("%d", time.Now().UnixNano()))).
			WithTTL(ttl)
		return txn.SetEntry(e)
	})
}

// hasRecentHeartbeat checks if a claim has been touched recently.
func (s *Store) hasRecentHeartbeat(claimID string) bool {
	key := []byte("heartbeat:" + claimID)
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		return err
	})
	return err == nil // Key exists = heartbeat is recent (TTL handles expiry)
}

// ClearPendingAndWrite atomically clears all pending events for a target and writes new ones.
// Prevents the window where old backlog is cleared but new delta fails to write.
func (s *Store) ClearPendingAndWrite(target string, events []map[string]any) error {
	if err := s.FlushWrites(); err != nil {
		return err
	}
	prefix := []byte(prefixEvt + target + ":")
	return s.db.Update(func(txn *badger.Txn) error {
		// Delete all existing pending events
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.Valid(); it.Next() {
			key := make([]byte, len(it.Item().Key()))
			copy(key, it.Item().Key())
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		// Write new events in same transaction
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

// ClearAllAndWrite atomically clears ALL state (evt: + inflight: + jobref:) for a target
// and writes new events. Used by @kind: table sinks where a new full snapshot completely
// replaces everything, including any in-progress inflight claims that are now stale.
func (s *Store) ClearAllAndWrite(target string, events []map[string]any) error {
	if err := s.FlushWrites(); err != nil {
		return err
	}
	evtPrefix := []byte(prefixEvt + target + ":")
	inflightPrefix := []byte(prefixInflight)
	targetPrefix := target + ":"

	return s.db.Update(func(txn *badger.Txn) error {
		// Delete all evt: events
		opts := badger.DefaultIteratorOptions
		opts.Prefix = evtPrefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		for it.Seek(evtPrefix); it.Valid(); it.Next() {
			key := make([]byte, len(it.Item().Key()))
			copy(key, it.Item().Key())
			if err := txn.Delete(key); err != nil {
				it.Close()
				return err
			}
		}
		it.Close()

		// Delete all inflight: events for target
		opts2 := badger.DefaultIteratorOptions
		opts2.Prefix = inflightPrefix
		opts2.PrefetchValues = false
		it2 := txn.NewIterator(opts2)
		for it2.Seek(inflightPrefix); it2.Valid(); it2.Next() {
			key := string(it2.Item().Key())
			rest := key[len(prefixInflight):]
			colonIdx := strings.Index(rest, ":")
			if colonIdx < 0 {
				continue
			}
			afterClaim := rest[colonIdx+1:]
			if strings.HasPrefix(afterClaim, targetPrefix) {
				bkey := make([]byte, len(it2.Item().Key()))
				copy(bkey, it2.Item().Key())
				if err := txn.Delete(bkey); err != nil {
					it2.Close()
					return err
				}
			}
		}
		it2.Close()

		// Delete async job_ref
		jobRefKey := []byte(prefixJobRef + target)
		if err := txn.Delete(jobRefKey); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		// Write new events
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

// ReadPending returns all pending (evt:) events for a target without claiming them.
func (s *Store) ReadPending(target string) ([]map[string]any, error) {
	if err := s.FlushWrites(); err != nil {
		return nil, err
	}
	prefix := []byte(prefixEvt + target + ":")
	var events []map[string]any
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.Valid(); it.Next() {
			var event map[string]any
			if err := it.Item().Value(func(val []byte) error {
				return json.Unmarshal(val, &event)
			}); err != nil {
				return fmt.Errorf("corrupt pending event for %s: %w", target, err)
			}
			events = append(events, event)
		}
		return nil
	})
	return events, err
}

// ReadAllEvents returns all events for a target from both evt: (pending) and
// inflight: (claimed) prefixes. Used by mergeBacklogWithDelta to build the
// complete set of unsynced rows before ClearAllAndWrite replaces everything.
func (s *Store) ReadAllEvents(target string) ([]map[string]any, error) {
	if err := s.FlushWrites(); err != nil {
		return nil, err
	}
	var events []map[string]any

	err := s.db.View(func(txn *badger.Txn) error {
		// Read evt: events
		evtPrefix := []byte(prefixEvt + target + ":")
		opts := badger.DefaultIteratorOptions
		opts.Prefix = evtPrefix
		it := txn.NewIterator(opts)
		for it.Seek(evtPrefix); it.Valid(); it.Next() {
			var event map[string]any
			if err := it.Item().Value(func(val []byte) error {
				return json.Unmarshal(val, &event)
			}); err != nil {
				it.Close()
				return fmt.Errorf("corrupt evt entry for %s: %w", target, err)
			}
			events = append(events, event)
		}
		it.Close()

		// Read inflight: events for this target
		inflightPrefix := []byte(prefixInflight)
		targetPrefix := target + ":"
		opts2 := badger.DefaultIteratorOptions
		opts2.Prefix = inflightPrefix
		it2 := txn.NewIterator(opts2)
		for it2.Seek(inflightPrefix); it2.Valid(); it2.Next() {
			key := string(it2.Item().Key())
			rest := key[len(prefixInflight):]
			colonIdx := strings.Index(rest, ":")
			if colonIdx < 0 {
				continue
			}
			afterClaim := rest[colonIdx+1:]
			if !strings.HasPrefix(afterClaim, targetPrefix) {
				continue
			}
			var event map[string]any
			if err := it2.Item().Value(func(val []byte) error {
				return json.Unmarshal(val, &event)
			}); err != nil {
				it2.Close()
				return fmt.Errorf("corrupt inflight entry for %s: %w", target, err)
			}
			events = append(events, event)
		}
		it2.Close()

		return nil
	})
	return events, err
}

// ClearPending deletes all pending (evt:) events for a target without touching inflight.
func (s *Store) ClearPending(target string) error {
	if err := s.FlushWrites(); err != nil {
		return err
	}
	prefix := []byte(prefixEvt + target + ":")
	return s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
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

// ClearAll deletes all pending (evt:) AND inflight events for a target.
// Used by @kind: table sinks where a new full snapshot replaces everything.
// Also clears any async job_ref since old jobs are invalidated.
func (s *Store) ClearAll(target string) error {
	if err := s.FlushWrites(); err != nil {
		return err
	}
	evtPrefix := []byte(prefixEvt + target + ":")
	inflightPrefix := []byte(prefixInflight)
	targetPrefix := target + ":"

	return s.db.Update(func(txn *badger.Txn) error {
		// Delete all evt: events for target
		opts := badger.DefaultIteratorOptions
		opts.Prefix = evtPrefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		for it.Seek(evtPrefix); it.Valid(); it.Next() {
			key := make([]byte, len(it.Item().Key()))
			copy(key, it.Item().Key())
			if err := txn.Delete(key); err != nil {
				it.Close()
				return err
			}
		}
		it.Close()

		// Delete all inflight: events for target
		opts2 := badger.DefaultIteratorOptions
		opts2.Prefix = inflightPrefix
		opts2.PrefetchValues = false
		it2 := txn.NewIterator(opts2)
		for it2.Seek(inflightPrefix); it2.Valid(); it2.Next() {
			key := string(it2.Item().Key())
			rest := key[len(prefixInflight):]
			colonIdx := strings.Index(rest, ":")
			if colonIdx < 0 {
				continue
			}
			afterClaim := rest[colonIdx+1:]
			if strings.HasPrefix(afterClaim, targetPrefix) {
				bkey := make([]byte, len(it2.Item().Key()))
				copy(bkey, it2.Item().Key())
				if err := txn.Delete(bkey); err != nil {
					it2.Close()
					return err
				}
			}
		}
		it2.Close()

		// Delete async job_ref
		jobRefKey := []byte(prefixJobRef + target)
		if err := txn.Delete(jobRefKey); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		// Delete heartbeats for any claims belonging to this target
		// (heartbeat keys don't have target prefix, but they expire via TTL anyway)

		return nil
	})
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

// AckAndRequeue atomically deletes all inflight events for a claim AND writes
// new events to the queue in a single Badger transaction. Used for partial failure:
// the original claim is cleared, and only the failed rows are re-queued for retry.
// If deleteJobRef is true, also deletes the async job_ref for the target in the same txn.
func (s *Store) AckAndRequeue(claimID string, target string, failedRows []map[string]any, deleteJobRef bool) error {
	prefix := []byte(prefixInflight + claimID + ":")

	return s.db.Update(func(txn *badger.Txn) error {
		// Delete all inflight events for this claim
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.Valid(); it.Next() {
			key := make([]byte, len(it.Item().Key()))
			copy(key, it.Item().Key())
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		// Write failed rows back to evt: queue
		for _, row := range failedRows {
			val, err := json.Marshal(row)
			if err != nil {
				return fmt.Errorf("marshal failed row: %w", err)
			}
			seq := s.counter.Add(1)
			key := fmt.Sprintf("%s%s:%d_%d", prefixEvt, target, time.Now().UnixNano(), seq)
			e := badger.NewEntry([]byte(key), val).WithTTL(eventTTL)
			if err := txn.SetEntry(e); err != nil {
				return err
			}
		}

		// Delete async job_ref in same txn to prevent stale resume
		if deleteJobRef {
			jobRefKey := []byte(prefixJobRef + target)
			if err := txn.Delete(jobRefKey); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return fmt.Errorf("delete job_ref: %w", err)
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
				return fmt.Errorf("parse inflight key %s: %w", string(key), err)
			}

			var val []byte
			err = item.Value(func(v []byte) error {
				val = make([]byte, len(v))
				copy(val, v)
				return nil
			})
			if err != nil {
				return fmt.Errorf("read inflight value %s: %w", string(key), err)
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

// asyncJobState holds both the external job reference and a fingerprint
// of the batch rows, so resume can verify the saved state matches the current claim.
type asyncJobState struct {
	JobRef      map[string]any `json:"job_ref"`
	RowHash     string         `json:"row_hash"` // fingerprint of batch row IDs
}

// SaveJobRef persists an async job reference with a batch fingerprint.
func (s *Store) SaveJobRef(target string, jobRef map[string]any, rowHash string) error {
	state := asyncJobState{JobRef: jobRef, RowHash: rowHash}
	val, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal job state: %w", err)
	}
	key := prefixJobRef + target
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), val)
	})
}

// LoadJobRef loads a previously saved async job reference.
// Returns the job ref and row hash, or nil/empty if none exists.
func (s *Store) LoadJobRef(target string) (map[string]any, string, error) {
	key := prefixJobRef + target
	var state asyncJobState
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &state)
		})
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	return state.JobRef, state.RowHash, nil
}

// DeleteJobRef removes a saved async job reference after polling completes.
func (s *Store) DeleteJobRef(target string) error {
	key := prefixJobRef + target
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

// Close closes the Badger database.
func (s *Store) Close() error {
	// Flush pending writes before closing
	flushErr := s.FlushWrites()
	dbErr := s.db.Close()
	return errors.Join(flushErr, dbErr)
}

// RunGC triggers Badger's value log garbage collection.
// Should be called periodically (e.g. every 5 minutes).
func (s *Store) RunGC() {
	_ = s.db.RunValueLogGC(0.5)
}

// RecoverOldInflight nacks inflight claims older than inflightMaxAge back to evt:.
// Only recovers claims old enough that the original process is certainly dead.
func (s *Store) RecoverOldInflight(target string) error {
	claimIDs, err := s.findInflightClaims(target)
	if err != nil {
		return err
	}

	cutoff := time.Now().Add(-inflightMaxAge)
	for _, claimID := range claimIDs {
		ts, err := claimTimestamp(claimID)
		if err != nil {
			// Unparseable claim ID: recover it to avoid stuck state.
			if nackErr := s.Nack(claimID); nackErr != nil {
				return fmt.Errorf("recover unparseable claim %s: %w", claimID, nackErr)
			}
			continue
		}
		if ts.Before(cutoff) {
			// Skip if there's a recent heartbeat (async polling still active)
			if s.hasRecentHeartbeat(claimID) {
				continue
			}
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
			// Target may contain colons (e.g. "sync:schema.table"), so we can't
			// split on ":" to find the target. Instead, find the first ":" after
			// prefixInflight to get claimID, then check if the remainder starts
			// with the target followed by ":".
			rest := key[len(prefixInflight):]
			colonIdx := strings.Index(rest, ":")
			if colonIdx < 0 {
				continue
			}
			claimID := rest[:colonIdx]
			afterClaim := rest[colonIdx+1:] // "{target}:{suffix}"
			targetPrefix := target + ":"
			if strings.HasPrefix(afterClaim, targetPrefix) && !seen[claimID] {
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
// Target may contain colons (e.g. "sync:staging.orders"), so we find claimID
// by first colon, then reconstruct target:suffix as the remainder.
func inflightToEvtKey(inflightKey string) (string, error) {
	rest := inflightKey[len(prefixInflight):]
	// rest = "{claimID}:{target}:{suffix}"
	// Find first colon to extract claimID
	colonIdx := strings.Index(rest, ":")
	if colonIdx < 0 {
		return "", fmt.Errorf("invalid inflight key: %s", inflightKey)
	}
	// Everything after claimID: is "target:suffix" which maps directly to evt:target:suffix
	targetAndSuffix := rest[colonIdx+1:]
	if targetAndSuffix == "" {
		return "", fmt.Errorf("invalid inflight key (no target): %s", inflightKey)
	}
	return prefixEvt + targetAndSuffix, nil
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
