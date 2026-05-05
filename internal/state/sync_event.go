// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package state

// SyncEvent represents an outbound sync tracking entry. It stores only
// the rowid, change type, and snapshot reference — the actual row data
// is read from DuckLake at push time.
//
// ChangeType is the raw DuckLake table_changes() change_type value,
// exposing full change semantics to the Starlark push() function.
type SyncEvent struct {
	ChangeType string `json:"ct"`   // raw DuckLake change_type: insert, update_preimage, update_postimage, delete
	RowID      int64  `json:"rid"`  // DuckLake stable row identifier (row lineage)
	Snapshot   int64  `json:"snap"` // snapshot where change occurred
}
