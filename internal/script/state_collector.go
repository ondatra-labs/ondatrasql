// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	dbsess "github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/state"
)

// stateCollector writes save() rows to a per-target staging table in
// state.duckdb, then claims and materializes them into a DuckDB temp
// table.
//
// Replaces the previous badger-backed implementation. Same external interface as
// the previous badgerCollector (add/count/createTempTable/ack/nack/close)
// — the runtime calls through a small adapter.
//
// Schema per target:
//
//	CREATE TABLE fetch_<target> (
//	    seq       BIGINT NOT NULL,
//	    claim_id  VARCHAR,        -- NULL = unclaimed, set on claim
//	    payload   BLOB NOT NULL,  -- JSON-serialized row
//	    PRIMARY KEY (seq)
//	)
type stateCollector struct {
	target    string
	tableName string // sanitized table name in state.duckdb
	st        *state.State
	sess      *dbsess.Session // main session, for IsAcked check + temp-table build
	mu        sync.Mutex      // guards seq counter increments
	seq       int64           // next seq to assign (monotonic per process)
	rowCount  atomic.Int64
}

// newStateCollector ensures the per-target staging table exists, recovers
// any inflight claims from a previous crashed run, and returns a collector
// ready for save() calls.
func newStateCollector(target string, st *state.State, sess *dbsess.Session) (*stateCollector, error) {
	// The raw target (e.g. "raw.orders") is used as a quoted DuckDB
	// identifier so distinct targets never collide. Earlier we sanitized
	// dots → underscores, which made `raw.orders` and `raw_orders` map to
	// the same table. ValidateIdentifier already rejects characters that
	// would break the quoted form (e.g. embedded `"`).
	tableName := "fetch:" + target
	db := st.DB()

	createSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (
		seq BIGINT NOT NULL,
		claim_id VARCHAR,
		payload BLOB NOT NULL,
		PRIMARY KEY (seq)
	)`, tableName)
	if _, err := db.Exec(createSQL); err != nil {
		return nil, fmt.Errorf("create staging table %q: %w", tableName, err)
	}

	// Recover inflight claims from previous runs. Same logic as the prior
	// badger-backed implementation:
	// already-committed claims (per _ondatra_acks in the main catalog) are
	// discarded; uncommitted claims are reset to unclaimed for retry.
	rows, err := db.Query(fmt.Sprintf(
		`SELECT DISTINCT claim_id FROM "%s" WHERE claim_id IS NOT NULL`, tableName))
	if err != nil {
		return nil, fmt.Errorf("scan inflight claims: %w", err)
	}
	var inflight []string
	for rows.Next() {
		var cid string
		if err := rows.Scan(&cid); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan claim_id: %w", err)
		}
		inflight = append(inflight, cid)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close claim cursor: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claims: %w", err)
	}

	for _, claimID := range inflight {
		alreadyAcked, ackErr := IsAcked(sess, claimID)
		if ackErr != nil {
			// Failing IsAcked means we cannot tell whether this claim's
			// rows were already committed. Resetting claim_id to NULL
			// would replay them; deleting would risk data loss. Surface
			// the error so the operator can fix the catalog and retry,
			// rather than silently choosing one side of the dilemma.
			return nil, fmt.Errorf("ack lookup for claim %s: %w", claimID, ackErr)
		}
		if alreadyAcked {
			if _, err := db.Exec(fmt.Sprintf(
				`DELETE FROM "%s" WHERE claim_id = ?`, tableName), claimID); err != nil {
				return nil, fmt.Errorf("delete already-committed claim %s: %w", claimID, err)
			}
		} else {
			if _, err := db.Exec(fmt.Sprintf(
				`UPDATE "%s" SET claim_id = NULL WHERE claim_id = ?`, tableName), claimID); err != nil {
				return nil, fmt.Errorf("reset uncommitted claim %s: %w", claimID, err)
			}
		}
	}

	// Initialize seq counter to MAX(seq)+1 so per-process sequences stay
	// monotonic across this run's writes.
	var maxSeq sql.NullInt64
	if err := db.QueryRow(fmt.Sprintf(`SELECT MAX(seq) FROM "%s"`, tableName)).Scan(&maxSeq); err != nil {
		return nil, fmt.Errorf("read max seq: %w", err)
	}
	startSeq := int64(0)
	if maxSeq.Valid {
		startSeq = maxSeq.Int64 + 1
	}

	// Seed rowCount from any rows that survived a prior crashed run so
	// save.count() reports the true outstanding work, not just rows
	// added in the current process. Without this, a resumed buffer
	// undercounts and downstream "fetch returned N rows" telemetry is
	// off by the carry-over amount.
	var existing int64
	if err := db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM "%s"`, tableName)).Scan(&existing); err != nil {
		return nil, fmt.Errorf("count existing rows: %w", err)
	}

	sc := &stateCollector{
		target:    target,
		tableName: tableName,
		st:        st,
		sess:      sess,
		seq:       startSeq,
	}
	sc.rowCount.Store(existing)
	return sc, nil
}

// add writes a single row to state.duckdb as JSON. Empty rows are
// rejected (matches saveCollector + badgerCollector contract).
func (sc *stateCollector) add(row map[string]interface{}) error {
	if len(row) == 0 {
		return fmt.Errorf("save.row: empty dict (no columns)")
	}
	payload, err := json.Marshal(row)
	if err != nil {
		return fmt.Errorf("marshal row: %w", err)
	}

	sc.mu.Lock()
	seq := sc.seq
	sc.seq++
	sc.mu.Unlock()

	if _, err := sc.st.DB().Exec(fmt.Sprintf(
		`INSERT INTO "%s" (seq, claim_id, payload) VALUES (?, NULL, ?)`,
		sc.tableName), seq, payload); err != nil {
		return fmt.Errorf("state insert: %w", err)
	}
	sc.rowCount.Add(1)
	return nil
}

func (sc *stateCollector) count() int {
	return int(sc.rowCount.Load())
}

// createTempTable claims all unclaimed rows for this target, decodes them
// from JSON, and materializes them into a DuckDB temp table via
// saveCollector. Returns the temp table name, row count, claim IDs, and
// any error.
func (sc *stateCollector) createTempTable() (string, int64, []string, error) {
	claimID, err := newClaimID()
	if err != nil {
		return "", 0, nil, fmt.Errorf("generate claim id: %w", err)
	}

	db := sc.st.DB()

	// resetClaim un-claims rows for this claim_id so a transient failure
	// after the UPDATE doesn't strand them as claimed-but-unprocessed.
	// Surface reset failures alongside the original error rather than
	// swallowing — operators need to know if the row state is now
	// inconsistent.
	resetClaim := func(orig error) error {
		if _, resetErr := db.Exec(fmt.Sprintf(
			`UPDATE "%s" SET claim_id = NULL WHERE claim_id = ?`,
			sc.tableName), claimID); resetErr != nil {
			return fmt.Errorf("%w (reset on failure also failed: %v)", orig, resetErr)
		}
		return orig
	}

	// Atomically mark all currently-unclaimed rows for this target as
	// belonging to this claim.
	if _, err := db.Exec(fmt.Sprintf(
		`UPDATE "%s" SET claim_id = ? WHERE claim_id IS NULL`,
		sc.tableName), claimID); err != nil {
		return "", 0, nil, fmt.Errorf("claim rows: %w", err)
	}

	rows, err := db.Query(fmt.Sprintf(
		`SELECT payload FROM "%s" WHERE claim_id = ? ORDER BY seq`,
		sc.tableName), claimID)
	if err != nil {
		return "", 0, nil, resetClaim(fmt.Errorf("query claimed rows: %w", err))
	}
	defer func() { _ = rows.Close() }()

	var events []map[string]any
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return "", 0, nil, resetClaim(fmt.Errorf("scan payload: %w", err))
		}
		var row map[string]any
		if err := json.Unmarshal(payload, &row); err != nil {
			return "", 0, nil, resetClaim(fmt.Errorf("unmarshal payload: %w", err))
		}
		events = append(events, row)
	}
	if err := rows.Err(); err != nil {
		return "", 0, nil, resetClaim(fmt.Errorf("iterate claimed rows: %w", err))
	}

	if len(events) == 0 {
		return "", 0, nil, nil
	}

	subSC := &saveCollector{target: sc.target, sess: sc.sess}
	for _, ev := range events {
		if err := subSC.add(ev); err != nil {
			// Reset claim on failure so a re-run can pick the rows up.
			if _, resetErr := db.Exec(fmt.Sprintf(
				`UPDATE "%s" SET claim_id = NULL WHERE claim_id = ?`,
				sc.tableName), claimID); resetErr != nil {
				return "", 0, nil, fmt.Errorf("collect rows: %w (reset failed: %v)", err, resetErr)
			}
			return "", 0, nil, fmt.Errorf("collect rows: %w", err)
		}
	}

	tmpTable, err := subSC.createTempTable()
	if err != nil {
		if _, resetErr := db.Exec(fmt.Sprintf(
			`UPDATE "%s" SET claim_id = NULL WHERE claim_id = ?`,
			sc.tableName), claimID); resetErr != nil {
			return "", 0, nil, fmt.Errorf("create temp table: %w (reset failed: %v)", err, resetErr)
		}
		return "", 0, nil, fmt.Errorf("create temp table: %w", err)
	}

	return tmpTable, int64(len(events)), []string{claimID}, nil
}

// ack confirms that the listed claim IDs were committed to DuckLake. The
// corresponding rows are deleted from the staging table.
func (sc *stateCollector) ack(claimIDs []string) error {
	for _, id := range claimIDs {
		if _, err := sc.st.DB().Exec(fmt.Sprintf(
			`DELETE FROM "%s" WHERE claim_id = ?`, sc.tableName), id); err != nil {
			return fmt.Errorf("ack claim %s: %w", id, err)
		}
	}
	return nil
}

// nack returns the listed claims to the unclaimed pool so they can be
// re-claimed on a subsequent run.
func (sc *stateCollector) nack(claimIDs []string) error {
	for _, id := range claimIDs {
		if _, err := sc.st.DB().Exec(fmt.Sprintf(
			`UPDATE "%s" SET claim_id = NULL WHERE claim_id = ?`,
			sc.tableName), id); err != nil {
			return fmt.Errorf("nack claim %s: %w", id, err)
		}
	}
	return nil
}

// close is a no-op — the underlying State is owned by the caller and
// closed by them. Provided for parity with the previous collector
// API so the runtime's defer plumbing stays unchanged.
func (sc *stateCollector) close() error {
	return nil
}

// newClaimID returns a 16-hex-byte random claim ID.
func newClaimID() (string, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}
