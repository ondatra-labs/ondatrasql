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
func ensureSyncAckTable(sess *duckdb.Session) {
	sess.Exec(`CREATE TABLE IF NOT EXISTS _sync_acked (
		claim_id VARCHAR NOT NULL,
		target VARCHAR NOT NULL,
		row_count BIGINT NOT NULL,
		created_at TIMESTAMPTZ DEFAULT current_timestamp
	)`)
}

// writeSyncAck records a successful push in DuckLake.
func writeSyncAck(sess *duckdb.Session, claimID, target string, rowCount int64) error {
	ensureSyncAckTable(sess)
	return sess.Exec(fmt.Sprintf(
		"INSERT INTO _sync_acked (claim_id, target, row_count) VALUES ('%s', '%s', %d)",
		escSyncSQL(claimID), escSyncSQL(target), rowCount))
}

// isSyncAcked checks if a claim was already successfully pushed.
func isSyncAcked(sess *duckdb.Session, claimID string) bool {
	ensureSyncAckTable(sess)
	result, err := sess.QueryValue(fmt.Sprintf(
		"SELECT COUNT(*) FROM _sync_acked WHERE claim_id = '%s'",
		escSyncSQL(claimID)))
	if err != nil {
		return false
	}
	return result != "0"
}

// deleteSyncAck removes the ack record after Badger confirms.
func deleteSyncAck(sess *duckdb.Session, claimID string) {
	sess.Exec(fmt.Sprintf(
		"DELETE FROM _sync_acked WHERE claim_id = '%s'",
		escSyncSQL(claimID)))
}

func escSyncSQL(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
