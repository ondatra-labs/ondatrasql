// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

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
	syncPrefixEvt      = "sevt:"
	syncPrefixInflight = "sinf:"
	syncPrefixJobRef   = "sjob:"
	syncEventTTL       = 7 * 24 * time.Hour // 7 days (longer than inbound 24h)
	syncInflightMaxAge = 10 * time.Minute
	syncDefaultLimit   = 1000
)

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

// SyncStore provides durable outbound sync tracking backed by Badger.
// Unlike Store (which stores full row data for inbound events), SyncStore
// stores only SyncEvent entries (rowid + operation + snapshot). Actual row
// data is read from DuckLake at push time.
type SyncStore struct {
	db      *badger.DB
	counter atomic.Uint64
}

// OpenSyncStore opens or creates a Badger-backed sync store at the given directory.
func OpenSyncStore(dir string) (*SyncStore, error) {
	opts := badger.DefaultOptions(dir).
		WithLogger(nil).
		WithSyncWrites(true)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open sync store: %w", err)
	}
	return &SyncStore{db: db}, nil
}

// Close closes the Badger database.
func (s *SyncStore) Close() error {
	return s.db.Close()
}

// RunGC triggers Badger's value log garbage collection.
func (s *SyncStore) RunGC() {
	_ = s.db.RunValueLogGC(0.5)
}

// --- Write ---

// WriteBatch stores multiple sync events for a target in a single transaction.
func (s *SyncStore) WriteBatch(target string, events []SyncEvent) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, event := range events {
			val, err := json.Marshal(event)
			if err != nil {
				return fmt.Errorf("marshal sync event: %w", err)
			}
			seq := s.counter.Add(1)
			key := fmt.Sprintf("%s%s:%d_%d", syncPrefixEvt, target, time.Now().UnixNano(), seq)
			e := badger.NewEntry([]byte(key), val).WithTTL(syncEventTTL)
			if err := txn.SetEntry(e); err != nil {
				return err
			}
		}
		return nil
	})
}

// ClearAllAndWrite atomically clears ALL state (evt + inflight + jobref) for
// a target and writes new events. Used by table/append-backfill (full replace)
// and merge/tracked (deduped merge replaces everything).
//
// NOTE: For very large event sets, this may hit Badger's ErrTxnTooBig limit.
// A retry-with-half-limit pattern (like Claim uses) would be needed to handle
// that case, but in practice event counts here are bounded by the pipeline batch.
func (s *SyncStore) ClearAllAndWrite(target string, events []SyncEvent) error {
	evtPrefix := []byte(syncPrefixEvt + target + ":")
	inflightPrefix := []byte(syncPrefixInflight)
	targetSuffix := target + ":"

	return s.db.Update(func(txn *badger.Txn) error {
		// Delete all evt: events
		if err := s.deleteByPrefix(txn, evtPrefix); err != nil {
			return err
		}

		// Delete all inflight: events for target
		if err := s.deleteInflightForTarget(txn, inflightPrefix, targetSuffix); err != nil {
			return err
		}

		// Delete async job_ref
		jobRefKey := []byte(syncPrefixJobRef + target)
		if err := txn.Delete(jobRefKey); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		// Write new events
		for _, event := range events {
			val, err := json.Marshal(event)
			if err != nil {
				return fmt.Errorf("marshal sync event: %w", err)
			}
			seq := s.counter.Add(1)
			key := fmt.Sprintf("%s%s:%d_%d", syncPrefixEvt, target, time.Now().UnixNano(), seq)
			e := badger.NewEntry([]byte(key), val).WithTTL(syncEventTTL)
			if err := txn.SetEntry(e); err != nil {
				return err
			}
		}
		return nil
	})
}

// ClearAll deletes all pending, inflight, and jobref state for a target.
func (s *SyncStore) ClearAll(target string) error {
	return s.ClearAllAndWrite(target, nil)
}

// --- Read ---

// HasPendingEvents returns true if there are claimable events for the target.
func (s *SyncStore) HasPendingEvents(target string) (bool, error) {
	prefix := []byte(syncPrefixEvt + target + ":")
	found := false
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Seek(prefix)
		if it.Valid() {
			found = true
		}
		return nil
	})
	return found, err
}

// HasRecentInflight returns true if there are inflight claims for the target
// that are young (< syncInflightMaxAge) or have an active heartbeat.
func (s *SyncStore) HasRecentInflight(target string) (bool, error) {
	inflightPrefix := []byte(syncPrefixInflight)
	targetSuffix := target + ":"
	cutoff := time.Now().Add(-syncInflightMaxAge)
	found := false

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = inflightPrefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(inflightPrefix); it.Valid(); it.Next() {
			key := string(it.Item().Key())
			rest := key[len(syncPrefixInflight):]

			claimID, afterClaim, ok := splitClaimKey(rest)
			if !ok || !strings.HasPrefix(afterClaim, targetSuffix) {
				continue
			}

			if s.hasRecentHeartbeat(claimID) {
				found = true
				return nil
			}

			ts, err := claimTimestamp(claimID)
			if err != nil {
				continue
			}
			if ts.After(cutoff) {
				found = true
				return nil
			}
		}
		return nil
	})
	return found, err
}

// ReadAllEvents returns all events (both pending and inflight) for a target.
// Used by mergeBacklogWithDelta to build the complete set before ClearAllAndWrite.
func (s *SyncStore) ReadAllEvents(target string) ([]SyncEvent, error) {
	var events []SyncEvent

	err := s.db.View(func(txn *badger.Txn) error {
		// Read pending events
		evtPrefix := []byte(syncPrefixEvt + target + ":")
		opts := badger.DefaultIteratorOptions
		opts.Prefix = evtPrefix
		it := txn.NewIterator(opts)
		for it.Seek(evtPrefix); it.Valid(); it.Next() {
			var event SyncEvent
			if err := it.Item().Value(func(val []byte) error {
				return json.Unmarshal(val, &event)
			}); err != nil {
				it.Close()
				return fmt.Errorf("corrupt sync event for %s: %w", target, err)
			}
			events = append(events, event)
		}
		it.Close()

		// Read inflight events
		inflightPrefix := []byte(syncPrefixInflight)
		targetSuffix := target + ":"
		opts2 := badger.DefaultIteratorOptions
		opts2.Prefix = inflightPrefix
		it2 := txn.NewIterator(opts2)
		for it2.Seek(inflightPrefix); it2.Valid(); it2.Next() {
			key := string(it2.Item().Key())
			rest := key[len(syncPrefixInflight):]
			_, afterClaim, ok := splitClaimKey(rest)
			if !ok || !strings.HasPrefix(afterClaim, targetSuffix) {
				continue
			}
			var event SyncEvent
			if err := it2.Item().Value(func(val []byte) error {
				return json.Unmarshal(val, &event)
			}); err != nil {
				it2.Close()
				return fmt.Errorf("corrupt sync inflight for %s: %w", target, err)
			}
			events = append(events, event)
		}
		it2.Close()

		return nil
	})
	return events, err
}

// --- Claim/Ack/Nack ---

// Claim atomically recovers old inflight events and moves up to limit
// pending events to inflight — all in a single Badger transaction.
// This prevents the race where two concurrent callers both recover the
// same old inflight events and then both claim them.
func (s *SyncStore) Claim(target string, limit int) (string, []SyncEvent, error) {
	if limit <= 0 {
		limit = syncDefaultLimit
	}

	claimID := fmt.Sprintf("%d_%d", time.Now().UnixNano(), s.counter.Add(1))
	evtPrefix := []byte(syncPrefixEvt + target + ":")
	inflightPrefix := []byte(syncPrefixInflight)
	targetSuffix := target + ":"
	cutoff := time.Now().Add(-syncInflightMaxAge)

	var events []SyncEvent

	err := s.db.Update(func(txn *badger.Txn) error {
		// Phase 1: Recover old inflight events back to evt: within this txn.
		// Inlined from RecoverOldInflight to make recover+claim atomic.
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
				rest := key[len(syncPrefixInflight):]
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
				evtKey, err := syncInflightToEvtKey(key)
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
				e := badger.NewEntry([]byte(entry.evtKey), entry.val).WithTTL(syncEventTTL)
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

			var event SyncEvent
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &event)
			}); err != nil {
				return fmt.Errorf("corrupt sync event at %s: %w", string(key), err)
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
				return fmt.Errorf("re-marshal sync event %d: %w", i, err)
			}
			inflightKey := fmt.Sprintf("%s%s:%s:%s", syncPrefixInflight, claimID, target, string(key[len(evtPrefix):]))
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
			return s.Claim(target, limit/2)
		}
		return "", nil, fmt.Errorf("claim txn: %w", err)
	}

	return claimID, events, nil
}

// Ack deletes all inflight events for a claim (successful push).
func (s *SyncStore) Ack(claimID string) error {
	prefix := []byte(syncPrefixInflight + claimID + ":")
	return s.db.Update(func(txn *badger.Txn) error {
		return s.deleteByPrefix(txn, prefix)
	})
}

// AckAndRequeue atomically deletes inflight events and re-queues failed ones.
func (s *SyncStore) AckAndRequeue(claimID string, target string, failedEvents []SyncEvent, deleteJobRef bool) error {
	prefix := []byte(syncPrefixInflight + claimID + ":")

	return s.db.Update(func(txn *badger.Txn) error {
		// Delete all inflight for this claim
		if err := s.deleteByPrefix(txn, prefix); err != nil {
			return err
		}

		// Re-queue failed events
		for _, event := range failedEvents {
			val, err := json.Marshal(event)
			if err != nil {
				return fmt.Errorf("marshal failed event: %w", err)
			}
			seq := s.counter.Add(1)
			key := fmt.Sprintf("%s%s:%d_%d", syncPrefixEvt, target, time.Now().UnixNano(), seq)
			e := badger.NewEntry([]byte(key), val).WithTTL(syncEventTTL)
			if err := txn.SetEntry(e); err != nil {
				return err
			}
		}

		if deleteJobRef {
			jobRefKey := []byte(syncPrefixJobRef + target)
			if err := txn.Delete(jobRefKey); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return fmt.Errorf("delete job_ref: %w", err)
			}
		}

		return nil
	})
}

// Nack moves all inflight events for a claim back to pending.
func (s *SyncStore) Nack(claimID string) error {
	prefix := []byte(syncPrefixInflight + claimID + ":")

	return s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.Valid(); it.Next() {
			item := it.Item()
			key := make([]byte, len(item.Key()))
			copy(key, item.Key())

			evtKey, err := syncInflightToEvtKey(string(key))
			if err != nil {
				return fmt.Errorf("parse inflight key %s: %w", string(key), err)
			}

			var val []byte
			if err := item.Value(func(v []byte) error {
				val = make([]byte, len(v))
				copy(val, v)
				return nil
			}); err != nil {
				return fmt.Errorf("read inflight value %s: %w", string(key), err)
			}

			e := badger.NewEntry([]byte(evtKey), val).WithTTL(syncEventTTL)
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

// --- Recovery ---

// RecoverOldInflight nacks inflight claims older than syncInflightMaxAge.
func (s *SyncStore) RecoverOldInflight(target string) error {
	claimIDs, err := s.findInflightClaims(target)
	if err != nil {
		return err
	}

	cutoff := time.Now().Add(-syncInflightMaxAge)
	for _, claimID := range claimIDs {
		ts, err := claimTimestamp(claimID)
		if err != nil {
			// Unparseable claim: recover to avoid stuck state
			if nackErr := s.Nack(claimID); nackErr != nil {
				return fmt.Errorf("recover unparseable claim %s: %w", claimID, nackErr)
			}
			continue
		}
		if ts.Before(cutoff) {
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

// --- Heartbeat ---

// TouchClaim updates the heartbeat for an inflight claim.
func (s *SyncStore) TouchClaim(claimID string, ttl time.Duration) error {
	key := []byte("heartbeat:" + claimID)
	return s.db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(key, []byte(fmt.Sprintf("%d", time.Now().UnixNano()))).
			WithTTL(ttl)
		return txn.SetEntry(e)
	})
}

func (s *SyncStore) hasRecentHeartbeat(claimID string) bool {
	key := []byte("heartbeat:" + claimID)
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		return err
	})
	return err == nil
}

// --- Async Job Ref ---

type syncAsyncJobState struct {
	JobRef  map[string]any `json:"job_ref"`
	RowHash string         `json:"row_hash"`
}

// SaveJobRef persists an async job reference with a batch fingerprint.
func (s *SyncStore) SaveJobRef(target string, jobRef map[string]any, rowHash string) error {
	state := syncAsyncJobState{JobRef: jobRef, RowHash: rowHash}
	val, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal job state: %w", err)
	}
	key := syncPrefixJobRef + target
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), val)
	})
}

// LoadJobRef loads a previously saved async job reference.
func (s *SyncStore) LoadJobRef(target string) (map[string]any, string, error) {
	key := syncPrefixJobRef + target
	var state syncAsyncJobState
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

// DeleteJobRef removes a saved async job reference.
func (s *SyncStore) DeleteJobRef(target string) error {
	key := syncPrefixJobRef + target
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

// --- Internal helpers ---

func (s *SyncStore) deleteByPrefix(txn *badger.Txn, prefix []byte) error {
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
}

func (s *SyncStore) deleteInflightForTarget(txn *badger.Txn, inflightPrefix []byte, targetSuffix string) error {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = inflightPrefix
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Seek(inflightPrefix); it.Valid(); it.Next() {
		key := string(it.Item().Key())
		rest := key[len(syncPrefixInflight):]
		_, afterClaim, ok := splitClaimKey(rest)
		if !ok {
			continue
		}
		if strings.HasPrefix(afterClaim, targetSuffix) {
			bkey := make([]byte, len(it.Item().Key()))
			copy(bkey, it.Item().Key())
			if err := txn.Delete(bkey); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *SyncStore) findInflightClaims(target string) ([]string, error) {
	seen := make(map[string]bool)
	var claimIDs []string

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(syncPrefixInflight)
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		targetSuffix := target + ":"
		for it.Seek([]byte(syncPrefixInflight)); it.Valid(); it.Next() {
			key := string(it.Item().Key())
			rest := key[len(syncPrefixInflight):]
			claimID, afterClaim, ok := splitClaimKey(rest)
			if !ok {
				continue
			}
			if strings.HasPrefix(afterClaim, targetSuffix) && !seen[claimID] {
				seen[claimID] = true
				claimIDs = append(claimIDs, claimID)
			}
		}
		return nil
	})
	return claimIDs, err
}

// splitClaimKey splits "{claimID}:{rest}" on the first colon.
func splitClaimKey(s string) (claimID, rest string, ok bool) {
	idx := strings.Index(s, ":")
	if idx < 0 {
		return "", "", false
	}
	return s[:idx], s[idx+1:], true
}

// syncInflightToEvtKey converts a sync inflight key to a sync evt key.
// "sinf:{claimID}:{target}:{suffix}" → "sevt:{target}:{suffix}"
func syncInflightToEvtKey(inflightKey string) (string, error) {
	rest := inflightKey[len(syncPrefixInflight):]
	idx := strings.Index(rest, ":")
	if idx < 0 {
		return "", fmt.Errorf("invalid sync inflight key: %s", inflightKey)
	}
	targetAndSuffix := rest[idx+1:]
	if targetAndSuffix == "" {
		return "", fmt.Errorf("invalid sync inflight key (no target): %s", inflightKey)
	}
	return syncPrefixEvt + targetAndSuffix, nil
}
