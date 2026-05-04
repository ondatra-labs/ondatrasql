// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"fmt"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
)

// ensureSyncAckTable creates _sync_acked if it doesn't exist.
// Tracks which outbound sync claims have been successfully pushed.
// If Badger ack fails after push, next run checks this table to
// avoid re-pushing already-delivered rows.
func ensureSyncAckTable(sess *duckdb.Session) error {
	return sess.Exec(`CREATE TABLE IF NOT EXISTS _sync_acked (
		claim_id VARCHAR NOT NULL,
		target VARCHAR NOT NULL,
		row_count BIGINT NOT NULL,
		created_at TIMESTAMPTZ DEFAULT current_timestamp
	)`)
}

// writeSyncAck records a successful push in DuckLake.
func writeSyncAck(sess *duckdb.Session, claimID, target string, rowCount int64) error {
	if err := ensureSyncAckTable(sess); err != nil {
		return fmt.Errorf("ensure _sync_acked table: %w", err)
	}
	return sess.Exec(fmt.Sprintf(
		"INSERT INTO _sync_acked (claim_id, target, row_count) VALUES ('%s', '%s', %d)",
		escSyncSQL(claimID), escSyncSQL(target), rowCount))
}

// isSyncAcked checks if a claim was already successfully pushed.
// On any error (table-create failure, query failure) returns false to
// signal "we don't know whether it was acked" — the caller will then
// re-push, which is at-least-once and matches the contract.
func isSyncAcked(sess *duckdb.Session, claimID string) bool {
	if err := ensureSyncAckTable(sess); err != nil {
		return false
	}
	result, err := sess.QueryValue(fmt.Sprintf(
		"SELECT COUNT(*) FROM _sync_acked WHERE claim_id = '%s'",
		escSyncSQL(claimID)))
	if err != nil {
		return false
	}
	return result != "0"
}

// deleteSyncAck removes the ack record after Badger confirms.
// Returns an error if the DELETE fails so the caller can surface it
// as a warning — leaving stale rows around isn't fatal but observers
// should know the cleanup didn't run.
func deleteSyncAck(sess *duckdb.Session, claimID string) error {
	return sess.Exec(fmt.Sprintf(
		"DELETE FROM _sync_acked WHERE claim_id = '%s'",
		escSyncSQL(claimID)))
}

// recordSyncAckCleanupWarning appends a non-fatal cleanup-failure
// warning to result when err != nil, no-op when err == nil. Extracted
// from ackAll so the warning wiring is testable without standing up
// the full pushExecutor + Badger store.
func recordSyncAckCleanupWarning(result *Result, err error) {
	if err == nil {
		return
	}
	result.Warnings = append(result.Warnings,
		fmt.Sprintf("_sync_acked cleanup failed (push and badger ack succeeded): %v", err))
}

func escSyncSQL(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
