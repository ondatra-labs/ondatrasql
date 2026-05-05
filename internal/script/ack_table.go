// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"fmt"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
)

// EnsureAckTable creates the _ondatra_acks table if it doesn't exist.
// This table tracks which state-store claim IDs have been successfully committed to DuckDB.
func EnsureAckTable(sess *duckdb.Session) error {
	return sess.Exec(`CREATE TABLE IF NOT EXISTS _ondatra_acks (
		claim_id VARCHAR NOT NULL,
		target VARCHAR NOT NULL,
		created_at TIMESTAMPTZ DEFAULT current_timestamp,
		row_count BIGINT NOT NULL
	)`)
}

// AckSQL returns the SQL to insert an ack record for a claim.
func AckSQL(claimID, target string, rowCount int64) string {
	return fmt.Sprintf(
		"INSERT INTO _ondatra_acks (claim_id, target, row_count) VALUES ('%s', '%s', %d)",
		escapeAckSQL(claimID), escapeAckSQL(target), rowCount)
}

// IsAcked checks if a claim ID has already been committed.
//
// EnsureAckTable runs first because the resume path in
// internal/script/state_collector.go can call IsAcked BEFORE the first
// materialize has had a chance to create the table. On a fresh project
// where state.duckdb already has inflight claims from a prior crash but
// no successful run has populated _ondatra_acks yet, the SELECT would
// fail with a Catalog Error and crash-recovery would refuse to proceed.
// CREATE IF NOT EXISTS is cheap and idempotent.
func IsAcked(sess *duckdb.Session, claimID string) (bool, error) {
	if err := EnsureAckTable(sess); err != nil {
		return false, fmt.Errorf("ensure _ondatra_acks: %w", err)
	}
	result, err := sess.QueryValue(fmt.Sprintf(
		"SELECT COUNT(*) FROM _ondatra_acks WHERE claim_id = '%s'",
		escapeAckSQL(claimID)))
	if err != nil {
		return false, err
	}
	return result != "0", nil
}

// DeleteAck removes an ack record after state-store has confirmed the ack.
// At that point the crash-recovery window is closed and the record is
// no longer needed. Returns the DELETE error so the caller can decide
// whether to surface it as a warning — leaving stale rows in
// _ondatra_acks isn't fatal but observers should know cleanup didn't
// run (mirrors deleteSyncAck in internal/execute/sync_ack.go).
func DeleteAck(sess *duckdb.Session, claimID string) error {
	return sess.Exec(fmt.Sprintf(
		"DELETE FROM _ondatra_acks WHERE claim_id = '%s'",
		escapeAckSQL(claimID)))
}

// escapeAckSQL escapes single quotes in SQL string literals.
func escapeAckSQL(s string) string {
	result := make([]byte, 0, len(s))
	for i := range len(s) {
		if s[i] == '\'' {
			result = append(result, '\'', '\'')
		} else {
			result = append(result, s[i])
		}
	}
	return string(result)
}
