// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package state

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// SyncEventTTL is how long pending events are retained before periodic
	// GC removes them.
	SyncEventTTL = 7 * 24 * time.Hour

	// SyncInflightMaxAge is the heartbeat-staleness threshold beyond which
	// inflight claims are considered orphaned and recovered to evt:.
	SyncInflightMaxAge = 10 * time.Minute

	// SyncDefaultLimit is the default per-claim batch size.
	SyncDefaultLimit = 1000
)

// claimTimestamp extracts the timestamp from a claim ID in the format
// "{nanoTimestamp}_{counter}". Used by callers that need to age out
// inflight claims based on age.
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

// SyncStore tracks outbound sync state in state.duckdb. Replaces the
// previous implementation that lived in `internal/collect`. Same
// external API, same crash-recovery semantics.
type SyncStore struct {
	st        *State
	counter   atomic.Uint64
	heartbeat sync.Map // claim_id -> time.Time, in-process heartbeat cache
}

// NewSyncStore returns a SyncStore that uses the supplied State for
// storage. The four backing tables (sync_evt, sync_inflight, sync_claim,
// sync_jobref) are created if missing.
func NewSyncStore(st *State) (*SyncStore, error) {
	db := st.DB()

	createStmts := []string{
		`CREATE TABLE IF NOT EXISTS sync_evt (
			target VARCHAR NOT NULL,
			seq BIGINT NOT NULL,
			payload BLOB NOT NULL,
			created_at TIMESTAMP DEFAULT now(),
			PRIMARY KEY (target, seq)
		)`,
		`CREATE TABLE IF NOT EXISTS sync_inflight (
			claim_id VARCHAR NOT NULL,
			target VARCHAR NOT NULL,
			seq BIGINT NOT NULL,
			payload BLOB NOT NULL,
			PRIMARY KEY (claim_id, target, seq)
		)`,
		`CREATE TABLE IF NOT EXISTS sync_claim (
			claim_id VARCHAR PRIMARY KEY,
			target VARCHAR NOT NULL,
			claimed_at TIMESTAMP DEFAULT now(),
			heartbeat TIMESTAMP DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS sync_jobref (
			target VARCHAR PRIMARY KEY,
			job_ref BLOB,
			row_hash VARCHAR,
			updated_at TIMESTAMP DEFAULT now()
		)`,
	}
	for _, stmt := range createStmts {
		if _, err := db.Exec(stmt); err != nil {
			return nil, fmt.Errorf("create sync table: %w", err)
		}
	}

	return &SyncStore{st: st}, nil
}

// Close is a no-op; the State is owned by the caller and closed via
// State.Close().
func (s *SyncStore) Close() error {
	return nil
}

// RunGC removes events older than SyncEventTTL and returns to the unclaimed
// pool any inflight rows whose heartbeat is older than SyncInflightMaxAge.
// Returns the first error encountered; partial progress is rolled back
// via the transaction.
func (s *SyncStore) RunGC() error {
	db := s.st.DB()
	cutoff := time.Now().Add(-SyncEventTTL)
	if _, err := db.Exec(`DELETE FROM sync_evt WHERE created_at < ?`, cutoff); err != nil {
		return fmt.Errorf("expire sync_evt: %w", err)
	}

	hbCutoff := time.Now().Add(-SyncInflightMaxAge)
	// Pull events from inflight tables for orphan claims back to sync_evt.
	// DuckDB doesn't allow writing to two tables in one query — do it in
	// two statements within an explicit transaction so they commit atomically.
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin gc tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`
		INSERT INTO sync_evt(target, seq, payload, created_at)
		SELECT target, seq, payload, now()
		FROM sync_inflight
		WHERE claim_id IN (
			SELECT claim_id FROM sync_claim WHERE heartbeat < ?
		)`, hbCutoff); err != nil {
		return fmt.Errorf("requeue orphan inflight: %w", err)
	}
	if _, err := tx.Exec(`
		DELETE FROM sync_inflight
		WHERE claim_id IN (
			SELECT claim_id FROM sync_claim WHERE heartbeat < ?
		)`, hbCutoff); err != nil {
		return fmt.Errorf("delete orphan inflight: %w", err)
	}
	if _, err := tx.Exec(`DELETE FROM sync_claim WHERE heartbeat < ?`, hbCutoff); err != nil {
		return fmt.Errorf("delete orphan claims: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit gc: %w", err)
	}
	return nil
}

// WriteBatch stores multiple sync events for a target in one transaction.
func (s *SyncStore) WriteBatch(target string, events []SyncEvent) error {
	if len(events) == 0 {
		return nil
	}
	db := s.st.DB()
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin write tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, event := range events {
		val, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("marshal sync event: %w", err)
		}
		seq := s.nextSeq()
		if _, err := tx.Exec(
			`INSERT INTO sync_evt(target, seq, payload) VALUES (?, ?, ?)`,
			target, seq, val); err != nil {
			return fmt.Errorf("insert sync_evt: %w", err)
		}
	}
	return tx.Commit()
}

// ClearAllAndWrite atomically clears ALL state (evt + inflight + jobref)
// for a target and writes a fresh batch of events. Used when the caller
// wants to reset the queue (e.g. truncate-and-replace semantics).
func (s *SyncStore) ClearAllAndWrite(target string, events []SyncEvent) error {
	db := s.st.DB()
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin clear-and-write tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`DELETE FROM sync_evt WHERE target = ?`, target); err != nil {
		return fmt.Errorf("clear sync_evt: %w", err)
	}
	if _, err := tx.Exec(`
		DELETE FROM sync_claim
		WHERE claim_id IN (SELECT DISTINCT claim_id FROM sync_inflight WHERE target = ?)`,
		target); err != nil {
		return fmt.Errorf("clear sync_claim: %w", err)
	}
	if _, err := tx.Exec(`DELETE FROM sync_inflight WHERE target = ?`, target); err != nil {
		return fmt.Errorf("clear sync_inflight: %w", err)
	}
	if _, err := tx.Exec(`DELETE FROM sync_jobref WHERE target = ?`, target); err != nil {
		return fmt.Errorf("clear sync_jobref: %w", err)
	}

	for _, event := range events {
		val, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("marshal sync event: %w", err)
		}
		seq := s.nextSeq()
		if _, err := tx.Exec(
			`INSERT INTO sync_evt(target, seq, payload) VALUES (?, ?, ?)`,
			target, seq, val); err != nil {
			return fmt.Errorf("insert sync_evt: %w", err)
		}
	}
	return tx.Commit()
}

// ClearAll removes all state (evt + inflight + jobref) for a target.
func (s *SyncStore) ClearAll(target string) error {
	db := s.st.DB()
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin clear tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`DELETE FROM sync_evt WHERE target = ?`, target); err != nil {
		return fmt.Errorf("clear sync_evt: %w", err)
	}
	if _, err := tx.Exec(`
		DELETE FROM sync_claim
		WHERE claim_id IN (SELECT DISTINCT claim_id FROM sync_inflight WHERE target = ?)`,
		target); err != nil {
		return fmt.Errorf("clear sync_claim: %w", err)
	}
	if _, err := tx.Exec(`DELETE FROM sync_inflight WHERE target = ?`, target); err != nil {
		return fmt.Errorf("clear sync_inflight: %w", err)
	}
	if _, err := tx.Exec(`DELETE FROM sync_jobref WHERE target = ?`, target); err != nil {
		return fmt.Errorf("clear sync_jobref: %w", err)
	}
	return tx.Commit()
}

// HasPendingEvents reports whether sync_evt contains any rows for the
// given target.
func (s *SyncStore) HasPendingEvents(target string) (bool, error) {
	var n int
	if err := s.st.DB().QueryRow(
		`SELECT count(*) FROM sync_evt WHERE target = ? LIMIT 1`,
		target).Scan(&n); err != nil {
		return false, fmt.Errorf("count pending: %w", err)
	}
	return n > 0, nil
}

// HasRecentInflight reports whether any inflight claim for the target has
// a heartbeat newer than SyncInflightMaxAge — i.e. a worker is still
// actively processing.
func (s *SyncStore) HasRecentInflight(target string) (bool, error) {
	cutoff := time.Now().Add(-SyncInflightMaxAge)
	var n int
	if err := s.st.DB().QueryRow(`
		SELECT count(*)
		FROM sync_claim
		WHERE target = ? AND heartbeat >= ?
		LIMIT 1`,
		target, cutoff).Scan(&n); err != nil {
		return false, fmt.Errorf("count recent inflight: %w", err)
	}
	return n > 0, nil
}

// ReadAllEvents returns every event for the target — pending plus
// inflight — in seq order. Used by callers that want a full snapshot
// of outstanding work.
func (s *SyncStore) ReadAllEvents(target string) ([]SyncEvent, error) {
	rows, err := s.st.DB().Query(`
		WITH all_events AS (
			SELECT seq, payload FROM sync_evt WHERE target = ?
			UNION ALL
			SELECT seq, payload FROM sync_inflight WHERE target = ?
		)
		SELECT payload FROM all_events ORDER BY seq`,
		target, target)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []SyncEvent
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("scan payload: %w", err)
		}
		var ev SyncEvent
		if err := json.Unmarshal(payload, &ev); err != nil {
			return nil, fmt.Errorf("unmarshal event: %w", err)
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}
	return out, nil
}

// Claim moves up to `limit` pending events to a new inflight claim and
// returns the claim ID + payload list. Callers process the events and
// then call Ack/Nack/AckAndRequeue.
//
// Phase 1 of the state-store implementation also recovered orphaned inflight
// claims back to evt: in the same TX. We do that explicitly via RunGC
// here — the caller can choose when to reap.
func (s *SyncStore) Claim(target string, limit int) (string, []SyncEvent, error) {
	if limit <= 0 {
		limit = SyncDefaultLimit
	}

	db := s.st.DB()

	// Cheap up-front check so an empty queue doesn't write a sync_claim
	// row that would later need cleanup. Avoids the "ghost claim
	// tombstone" race where a transient cleanup-DELETE failure leaves a
	// claim-only row that HasRecentInflight() then treats as active work.
	var pending int
	if err := db.QueryRow(
		`SELECT count(*) FROM sync_evt WHERE target = ? LIMIT 1`,
		target).Scan(&pending); err != nil {
		return "", nil, fmt.Errorf("count pending: %w", err)
	}
	if pending == 0 {
		return "", nil, nil
	}

	claimID := s.newClaimID()

	tx, err := db.Begin()
	if err != nil {
		return "", nil, fmt.Errorf("begin claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(
		`INSERT INTO sync_claim(claim_id, target) VALUES (?, ?)`,
		claimID, target); err != nil {
		return "", nil, fmt.Errorf("insert sync_claim: %w", err)
	}

	if _, err := tx.Exec(`
		INSERT INTO sync_inflight(claim_id, target, seq, payload)
		SELECT ?, target, seq, payload
		FROM sync_evt
		WHERE target = ?
		ORDER BY seq
		LIMIT ?`,
		claimID, target, limit); err != nil {
		return "", nil, fmt.Errorf("move to inflight: %w", err)
	}

	if _, err := tx.Exec(`
		DELETE FROM sync_evt
		WHERE (target, seq) IN (
			SELECT target, seq FROM sync_inflight WHERE claim_id = ?
		)`, claimID); err != nil {
		return "", nil, fmt.Errorf("delete from sync_evt: %w", err)
	}

	rows, err := tx.Query(
		`SELECT payload FROM sync_inflight WHERE claim_id = ? ORDER BY seq`,
		claimID)
	if err != nil {
		return "", nil, fmt.Errorf("read claimed payloads: %w", err)
	}
	var events []SyncEvent
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			_ = rows.Close()
			return "", nil, fmt.Errorf("scan payload: %w", err)
		}
		var ev SyncEvent
		if err := json.Unmarshal(payload, &ev); err != nil {
			_ = rows.Close()
			return "", nil, fmt.Errorf("unmarshal event: %w", err)
		}
		events = append(events, ev)
	}
	if err := rows.Close(); err != nil {
		return "", nil, fmt.Errorf("close payload rows: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return "", nil, fmt.Errorf("commit claim: %w", err)
	}

	// The pending check above guarantees events is non-empty here under
	// normal operation. If a concurrent worker drained the queue between
	// the count and the INSERT-SELECT, fall through cleanly: the claim
	// row + zero inflight rows will be reaped by RunGC's heartbeat-stale
	// path on the next pipeline run rather than left as a tombstone.
	if len(events) == 0 {
		return "", nil, nil
	}
	s.heartbeat.Store(claimID, time.Now())
	return claimID, events, nil
}

// Ack permanently removes an inflight claim — its events were committed
// to the external system.
func (s *SyncStore) Ack(claimID string) error {
	return s.deleteClaim(claimID)
}

// AckAndRequeue completes a claim where some events succeeded and some
// failed. Failed events are written back to sync_evt for re-processing;
// the inflight rows and claim record are removed. If deleteJobRef is
// true, the per-target idempotency entry is also cleared.
func (s *SyncStore) AckAndRequeue(claimID string, target string, failedEvents []SyncEvent, deleteJobRef bool) error {
	db := s.st.DB()
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin requeue tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, event := range failedEvents {
		val, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("marshal failed event: %w", err)
		}
		seq := s.nextSeq()
		if _, err := tx.Exec(
			`INSERT INTO sync_evt(target, seq, payload) VALUES (?, ?, ?)`,
			target, seq, val); err != nil {
			return fmt.Errorf("requeue failed event: %w", err)
		}
	}

	if _, err := tx.Exec(`DELETE FROM sync_inflight WHERE claim_id = ?`, claimID); err != nil {
		return fmt.Errorf("delete inflight: %w", err)
	}
	if _, err := tx.Exec(`DELETE FROM sync_claim WHERE claim_id = ?`, claimID); err != nil {
		return fmt.Errorf("delete claim: %w", err)
	}
	if deleteJobRef {
		if _, err := tx.Exec(`DELETE FROM sync_jobref WHERE target = ?`, target); err != nil {
			return fmt.Errorf("delete jobref: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit requeue: %w", err)
	}
	s.heartbeat.Delete(claimID)
	return nil
}

// Nack returns all rows of an inflight claim back to sync_evt so they can
// be reclaimed. The claim record is removed.
func (s *SyncStore) Nack(claimID string) error {
	db := s.st.DB()
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin nack tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`
		INSERT INTO sync_evt(target, seq, payload, created_at)
		SELECT target, seq, payload, now()
		FROM sync_inflight
		WHERE claim_id = ?`, claimID); err != nil {
		return fmt.Errorf("requeue inflight: %w", err)
	}
	if _, err := tx.Exec(`DELETE FROM sync_inflight WHERE claim_id = ?`, claimID); err != nil {
		return fmt.Errorf("delete inflight: %w", err)
	}
	if _, err := tx.Exec(`DELETE FROM sync_claim WHERE claim_id = ?`, claimID); err != nil {
		return fmt.Errorf("delete claim: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit nack: %w", err)
	}
	s.heartbeat.Delete(claimID)
	return nil
}

// RecoverOldInflight returns any inflight claim for the target whose
// claim ID timestamp is older than SyncInflightMaxAge back to sync_evt.
// Distinct from RunGC, which is global; this is per-target.
func (s *SyncStore) RecoverOldInflight(target string) error {
	db := s.st.DB()
	rows, err := db.Query(`
		SELECT DISTINCT claim_id FROM sync_inflight WHERE target = ?`,
		target)
	if err != nil {
		return fmt.Errorf("scan inflight claims: %w", err)
	}
	var stale []string
	for rows.Next() {
		var cid string
		if err := rows.Scan(&cid); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan claim_id: %w", err)
		}
		ts, err := claimTimestamp(cid)
		if err != nil {
			continue
		}
		if time.Since(ts) >= SyncInflightMaxAge {
			stale = append(stale, cid)
		}
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close cursor: %w", err)
	}
	for _, cid := range stale {
		if err := s.Nack(cid); err != nil {
			return fmt.Errorf("recover claim %s: %w", cid, err)
		}
	}
	return nil
}

// TouchClaim refreshes a claim's heartbeat. Called by long-running
// workers so their inflight rows aren't reaped by RunGC.
//
// The ttl argument is ignored — heartbeat semantics now rely on the
// SyncInflightMaxAge constant rather than per-claim TTLs (state-store's
// model). Kept in the signature for API compatibility.
func (s *SyncStore) TouchClaim(claimID string, _ time.Duration) error {
	if _, err := s.st.DB().Exec(
		`UPDATE sync_claim SET heartbeat = now() WHERE claim_id = ?`,
		claimID); err != nil {
		return fmt.Errorf("touch claim: %w", err)
	}
	s.heartbeat.Store(claimID, time.Now())
	return nil
}

// SaveJobRef stores per-target idempotency state (job reference + row
// hash). Overwrites any previous entry for the target.
func (s *SyncStore) SaveJobRef(target string, jobRef map[string]any, rowHash string) error {
	val, err := json.Marshal(jobRef)
	if err != nil {
		return fmt.Errorf("marshal jobref: %w", err)
	}
	db := s.st.DB()
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin jobref tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`DELETE FROM sync_jobref WHERE target = ?`, target); err != nil {
		return fmt.Errorf("clear old jobref: %w", err)
	}
	if _, err := tx.Exec(
		`INSERT INTO sync_jobref(target, job_ref, row_hash) VALUES (?, ?, ?)`,
		target, val, rowHash); err != nil {
		return fmt.Errorf("insert jobref: %w", err)
	}
	return tx.Commit()
}

// LoadJobRef returns the saved job reference + row hash for a target,
// or zero values if none exists.
func (s *SyncStore) LoadJobRef(target string) (map[string]any, string, error) {
	var payload []byte
	var rowHash string
	err := s.st.DB().QueryRow(
		`SELECT job_ref, row_hash FROM sync_jobref WHERE target = ?`,
		target).Scan(&payload, &rowHash)
	if err == sql.ErrNoRows {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("query jobref: %w", err)
	}
	var jobRef map[string]any
	if err := json.Unmarshal(payload, &jobRef); err != nil {
		return nil, "", fmt.Errorf("unmarshal jobref: %w", err)
	}
	return jobRef, rowHash, nil
}

// DeleteJobRef removes the per-target idempotency entry.
func (s *SyncStore) DeleteJobRef(target string) error {
	if _, err := s.st.DB().Exec(`DELETE FROM sync_jobref WHERE target = ?`, target); err != nil {
		return fmt.Errorf("delete jobref: %w", err)
	}
	return nil
}

// deleteClaim removes inflight rows and the claim record for a claim_id
// in one transaction. Shared between Ack and other terminal paths.
func (s *SyncStore) deleteClaim(claimID string) error {
	db := s.st.DB()
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin delete tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`DELETE FROM sync_inflight WHERE claim_id = ?`, claimID); err != nil {
		return fmt.Errorf("delete inflight: %w", err)
	}
	if _, err := tx.Exec(`DELETE FROM sync_claim WHERE claim_id = ?`, claimID); err != nil {
		return fmt.Errorf("delete claim: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete: %w", err)
	}
	s.heartbeat.Delete(claimID)
	return nil
}

// nextSeq returns a per-process monotonic sequence value used as the
// secondary key inside (target, seq) primary keys.
func (s *SyncStore) nextSeq() int64 {
	return int64(time.Now().UnixNano()) + int64(s.counter.Add(1))
}

// newClaimID returns a claim ID in the format "{nanoTimestamp}_{counter}"
// matching the state-store implementation. The format is parsed by
// claimTimestamp for age-based recovery.
func (s *SyncStore) newClaimID() string {
	return fmt.Sprintf("%d_%d", time.Now().UnixNano(), s.counter.Add(1))
}
