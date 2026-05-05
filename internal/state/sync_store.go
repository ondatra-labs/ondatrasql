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
		// sync_apply_log persists per-row push() classifications BEFORE
		// AckAndRequeue mutates sync_inflight / sync_evt. This makes the
		// classify-then-apply step idempotent: if AckAndRequeue's TX
		// fails (transient DuckDB error, crash), the log survives and
		// the next attempt can replay the same per-row decisions
		// instead of falling back to a destructive nackAll that
		// resurrects ok/rejected events.
		//
		// The log is self-contained — payload is duplicated from
		// sync_inflight so apply doesn't need to JOIN back to inflight
		// (which may already be partially gone after a crash).
		//
		// `ord` is a per-claim ordinal assigned at record time. It's
		// the only thing that has to be unique within a claim — there's
		// no semantic relation to sync_inflight.seq or to SyncEvent.RowID
		// (a single batch can have insert + update_postimage + update_preimage
		// for the same RowID, which would collide on rid alone).
		//
		// Status values: 'ok' (delivered, ack), 'failed' (retry, requeue
		// to sync_evt), 'reject' (permanent, ack and drop).
		`CREATE TABLE IF NOT EXISTS sync_apply_log (
			claim_id VARCHAR NOT NULL,
			target VARCHAR NOT NULL,
			ord BIGINT NOT NULL,
			payload BLOB NOT NULL,
			status VARCHAR NOT NULL,
			delete_jobref BOOLEAN DEFAULT false,
			recorded_at TIMESTAMP DEFAULT now(),
			PRIMARY KEY (claim_id, ord)
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
// Returns the first error encountered.
//
// Ordering is load-bearing: RecoverOrphanApplyLogs runs FIRST so any claim
// that crashed between RecordPushOutcomes and ApplyLoggedOutcomes gets
// classified per-row (ok → drop, failed → requeue, reject → drop). If the
// stale-heartbeat reap ran first, it would blindly requeue every inflight
// row of those claims, resurrecting ok/rejected events the apply-log path
// was meant to retire.
//
// Defense in depth: the stale-heartbeat reap also excludes any claim_id
// that still has sync_apply_log rows, in case RecoverOrphanApplyLogs
// partially failed.
func (s *SyncStore) RunGC() error {
	db := s.st.DB()
	cutoff := time.Now().Add(-SyncEventTTL)
	hbCutoff := time.Now().Add(-SyncInflightMaxAge)

	// Step 1: consume any apply_log entries left over from a crash. This
	// must precede the stale-heartbeat reap below — see func-level comment.
	if err := s.RecoverOrphanApplyLogs(); err != nil {
		return fmt.Errorf("recover orphan apply logs: %w", err)
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin gc tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`DELETE FROM sync_evt WHERE created_at < ?`, cutoff); err != nil {
		return fmt.Errorf("expire sync_evt: %w", err)
	}
	if _, err := tx.Exec(`
		INSERT INTO sync_evt(target, seq, payload, created_at)
		SELECT target, seq, payload, now()
		FROM sync_inflight
		WHERE claim_id IN (
			SELECT claim_id FROM sync_claim WHERE heartbeat < ?
		)
		AND claim_id NOT IN (SELECT DISTINCT claim_id FROM sync_apply_log)`,
		hbCutoff); err != nil {
		return fmt.Errorf("requeue orphan inflight: %w", err)
	}
	if _, err := tx.Exec(`
		DELETE FROM sync_inflight
		WHERE claim_id IN (
			SELECT claim_id FROM sync_claim WHERE heartbeat < ?
		)
		AND claim_id NOT IN (SELECT DISTINCT claim_id FROM sync_apply_log)`,
		hbCutoff); err != nil {
		return fmt.Errorf("delete orphan inflight: %w", err)
	}
	if _, err := tx.Exec(`
		DELETE FROM sync_claim
		WHERE heartbeat < ?
		AND claim_id NOT IN (SELECT DISTINCT claim_id FROM sync_apply_log)`,
		hbCutoff); err != nil {
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
		DELETE FROM sync_apply_log
		WHERE target = ?`, target); err != nil {
		return fmt.Errorf("clear sync_apply_log: %w", err)
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
		DELETE FROM sync_apply_log
		WHERE target = ?`, target); err != nil {
		return fmt.Errorf("clear sync_apply_log: %w", err)
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

	// Count what actually moved. The pending check above is racy against
	// a concurrent drain, so we re-verify here BEFORE committing — if
	// zero rows ended up inflight we abandon the claim entirely (rollback)
	// rather than commit a sync_claim row with no inflight, which would
	// otherwise show up as a ghost tombstone for HasRecentInflight().
	var moved int
	if err := tx.QueryRow(
		`SELECT count(*) FROM sync_inflight WHERE claim_id = ?`,
		claimID).Scan(&moved); err != nil {
		return "", nil, fmt.Errorf("count moved rows: %w", err)
	}
	if moved == 0 {
		// Defer rollback runs and discards everything: the sync_claim
		// INSERT, the empty INSERT-SELECT, the DELETE — all undone.
		return "", nil, nil
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
	// rows.Err() must be checked AFTER the loop — without it a cursor
	// failure mid-iteration silently truncates the events slice and the
	// caller acks/nacks a partial batch (silent data drop).
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return "", nil, fmt.Errorf("iterate claimed rows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return "", nil, fmt.Errorf("close payload rows: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return "", nil, fmt.Errorf("commit claim: %w", err)
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

// Outcome status values for EventOutcome.Status. Defined as constants
// to keep the contract explicit — a typo in a caller would otherwise be
// silently treated as "ack and drop" by ApplyLoggedOutcomes' default
// branch, losing events without any error surfaced.
const (
	OutcomeOK     = "ok"     // delivered successfully → ack and drop
	OutcomeFailed = "failed" // transient failure → requeue to sync_evt
	OutcomeReject = "reject" // permanent failure → ack and drop
)

// EventOutcome pairs a SyncEvent with its push() outcome for durable
// classification before AckAndRequeue mutates inflight state.
//
// Status MUST be one of OutcomeOK/OutcomeFailed/OutcomeReject;
// RecordPushOutcomes rejects any other value.
type EventOutcome struct {
	Event  SyncEvent
	Status string
}

// RecordPushOutcomes durably persists per-row push() classifications
// for a claim into sync_apply_log. The full payload is duplicated into
// the log so the subsequent ApplyLoggedOutcomes pass is self-contained
// and doesn't depend on sync_inflight still being present.
//
// The point: if AckAndRequeue/Apply's TX fails (transient DuckDB error,
// crash), the classifications survive and the next attempt (either a
// retry on the same path or recovery on the next pipeline run via
// ApplyLoggedOutcomes / RecoverOrphanApplyLogs) replays the same per-row
// decisions instead of resurrecting ok/rejected events via a destructive
// nackAll.
func (s *SyncStore) RecordPushOutcomes(claimID, target string, outcomes []EventOutcome, deleteJobRef bool) error {
	if len(outcomes) == 0 {
		return nil
	}
	// Validate up front so a typo never lands in sync_apply_log. If we
	// only validated at apply time, the bad row would already be
	// committed — we'd still have to choose between dropping the entire
	// claim (data loss) or leaving the row stuck (apply log poisoned).
	for i, o := range outcomes {
		switch o.Status {
		case OutcomeOK, OutcomeFailed, OutcomeReject:
		default:
			return fmt.Errorf("outcome[%d]: invalid status %q (want %q|%q|%q)",
				i, o.Status, OutcomeOK, OutcomeFailed, OutcomeReject)
		}
	}
	db := s.st.DB()
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin record outcomes tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Idempotent: if a previous attempt persisted some rows, drop them
	// before re-inserting so a retry with the same outcomes succeeds
	// instead of hitting the (claim_id, ord) PK.
	if _, err := tx.Exec(
		`DELETE FROM sync_apply_log WHERE claim_id = ?`, claimID); err != nil {
		return fmt.Errorf("clear prior outcomes: %w", err)
	}

	for i, o := range outcomes {
		payload, err := json.Marshal(o.Event)
		if err != nil {
			return fmt.Errorf("marshal event for log: %w", err)
		}
		// `ord` is a per-claim ordinal assigned in caller order — its
		// only job is to make the (claim_id, ord) PK unique. It has no
		// semantic relation to sync_inflight.seq or SyncEvent.RowID, so
		// duplicate rids in the same batch (e.g. update_preimage +
		// update_postimage for the same row) don't collide.
		if _, err := tx.Exec(
			`INSERT INTO sync_apply_log(claim_id, target, ord, payload, status, delete_jobref)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			claimID, target, int64(i), payload, o.Status, deleteJobRef); err != nil {
			return fmt.Errorf("insert outcome: %w", err)
		}
	}
	return tx.Commit()
}

// ApplyLoggedOutcomes consumes the persisted classifications for a
// claim from sync_apply_log and applies them: failed events go to
// sync_evt, the inflight rows + claim record go away, and (if any
// row has delete_jobref=true) the jobref is cleared.
//
// Self-contained: target, payload, and status are all read from
// sync_apply_log (where RecordPushOutcomes copied them). The caller
// does NOT pass a target argument — using the per-row stored target
// is what makes recovery safe regardless of who triggered apply
// (worker path vs. RunGC orphan recovery).
//
// Idempotent: re-running on a partially-applied claim is safe because
// every step is bounded by the claim_id and the apply_log is the
// source of truth for what to requeue.
func (s *SyncStore) ApplyLoggedOutcomes(claimID string) error {
	db := s.st.DB()

	// Read the full apply log up-front. If it's empty there's nothing
	// to do — this also catches "log was already consumed" cases that
	// would otherwise silently delete sync_inflight/sync_claim rows
	// we have no business touching.
	rows, err := db.Query(`
		SELECT target, payload, status, delete_jobref
		FROM sync_apply_log
		WHERE claim_id = ?
		ORDER BY ord`, claimID)
	if err != nil {
		return fmt.Errorf("read apply log: %w", err)
	}
	type entry struct {
		target       string
		payload      []byte
		status       string
		deleteJobref bool
	}
	var entries []entry
	for rows.Next() {
		var e entry
		if err := rows.Scan(&e.target, &e.payload, &e.status, &e.deleteJobref); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan apply log row: %w", err)
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("iterate apply log: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close apply log cursor: %w", err)
	}
	if len(entries) == 0 {
		// Nothing logged for this claim → nothing to apply. Don't touch
		// sync_inflight or sync_claim — those decisions belong to
		// AckAndRequeue / Nack callers, not the recovery path.
		return nil
	}

	// A claim is bound to exactly one target (RecordPushOutcomes is
	// always called with one target per claim). If the log somehow
	// mixes targets we refuse to apply — silently routing requeued
	// rows to the wrong sync_evt target would be a worse failure than
	// stalling the claim.
	target := entries[0].target
	for _, e := range entries[1:] {
		if e.target != target {
			return fmt.Errorf("apply log for claim %q has mixed targets (%q vs %q)",
				claimID, target, e.target)
		}
	}
	deleteJobref := false
	for _, e := range entries {
		if e.deleteJobref {
			deleteJobref = true
			break
		}
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin apply tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, e := range entries {
		switch e.status {
		case OutcomeOK, OutcomeReject:
			// Delivered or permanently rejected — drop. The unconditional
			// DELETE FROM sync_inflight below removes the row.
		case OutcomeFailed:
			seq := s.nextSeq()
			if _, err := tx.Exec(
				`INSERT INTO sync_evt(target, seq, payload) VALUES (?, ?, ?)`,
				target, seq, e.payload); err != nil {
				return fmt.Errorf("requeue failed event: %w", err)
			}
		default:
			// RecordPushOutcomes validates Status, so this fires only on
			// schema corruption or a bypassed insert. Don't silently
			// ack-drop — surface so the operator sees it.
			return fmt.Errorf("apply log for claim %q has unknown status %q",
				claimID, e.status)
		}
	}

	if _, err := tx.Exec(`DELETE FROM sync_inflight WHERE claim_id = ?`, claimID); err != nil {
		return fmt.Errorf("delete inflight: %w", err)
	}
	if _, err := tx.Exec(`DELETE FROM sync_claim WHERE claim_id = ?`, claimID); err != nil {
		return fmt.Errorf("delete claim: %w", err)
	}
	if deleteJobref {
		if _, err := tx.Exec(`DELETE FROM sync_jobref WHERE target = ?`, target); err != nil {
			return fmt.Errorf("delete jobref: %w", err)
		}
	}
	if _, err := tx.Exec(`DELETE FROM sync_apply_log WHERE claim_id = ?`, claimID); err != nil {
		return fmt.Errorf("delete apply log: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit apply: %w", err)
	}
	s.heartbeat.Delete(claimID)
	return nil
}

// RecoverOrphanApplyLogs resumes any sync_apply_log entries left
// behind by a crash that happened between RecordPushOutcomes (which
// committed the log) and ApplyLoggedOutcomes (which would have
// consumed it). Called from RunGC.
//
// Heartbeat-gated: only claims whose sync_claim row is missing or
// has a stale heartbeat are recovered. A live worker that just
// committed RecordPushOutcomes is still racing toward
// ApplyLoggedOutcomes — preempting it from another goroutine would
// race the same TXs against each other and let one delete inflight
// rows the worker still expects to be there.
func (s *SyncStore) RecoverOrphanApplyLogs() error {
	hbCutoff := time.Now().Add(-SyncInflightMaxAge)
	rows, err := s.st.DB().Query(`
		SELECT DISTINCT al.claim_id
		FROM sync_apply_log al
		LEFT JOIN sync_claim c ON c.claim_id = al.claim_id
		WHERE c.claim_id IS NULL
		   OR c.heartbeat < ?`, hbCutoff)
	if err != nil {
		return fmt.Errorf("scan apply log: %w", err)
	}
	var orphans []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan apply-log row: %w", err)
		}
		orphans = append(orphans, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("iterate apply log: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close apply-log cursor: %w", err)
	}

	for _, id := range orphans {
		if err := s.ApplyLoggedOutcomes(id); err != nil {
			return fmt.Errorf("recover claim %s: %w", id, err)
		}
	}
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
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("iterate inflight claims: %w", err)
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
// SyncInflightMaxAge constant rather than per-claim TTLs. Kept in the
// signature for API compatibility.
//
// Returns ErrClaimNotFound when the claim no longer exists in
// sync_claim (because RunGC reaped it, the worker was nack'd, or
// somebody else acked the claim). The caller must treat that as
// "ownership lost" and stop processing this claim — silently ignoring
// it would mask split-brain conditions.
func (s *SyncStore) TouchClaim(claimID string, _ time.Duration) error {
	res, err := s.st.DB().Exec(
		`UPDATE sync_claim SET heartbeat = now() WHERE claim_id = ?`,
		claimID)
	if err != nil {
		return fmt.Errorf("touch claim: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("touch claim rows affected: %w", err)
	}
	if n == 0 {
		return ErrClaimNotFound
	}
	s.heartbeat.Store(claimID, time.Now())
	return nil
}

// ErrClaimNotFound is returned by TouchClaim when the claim no longer
// exists. Callers should stop processing the claim (ownership lost).
var ErrClaimNotFound = fmt.Errorf("claim not found")

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
