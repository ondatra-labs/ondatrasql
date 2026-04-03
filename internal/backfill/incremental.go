// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package backfill

import (
	"fmt"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
)

// IncrementalState contains cursor-based incremental loading state for a model.
// This state is derived from DuckLake metadata and the target table - no separate storage.
type IncrementalState struct {
	// IsBackfill is true if the target table doesn't exist (first run).
	IsBackfill bool

	// Cursor is the column name used for incremental tracking.
	Cursor string

	// LastValue is MAX(Cursor) from target table, or InitialValue if first run.
	LastValue string

	// LastRun is the commit_time from the most recent snapshot for this model.
	LastRun string

	// InitialValue is the starting value from @incremental_initial directive.
	InitialValue string
}

// DefaultInitialValue is used when @incremental_initial is not specified.
const DefaultInitialValue = "1970-01-01T00:00:00Z"

// GetIncrementalState retrieves cursor-based incremental state for a model.
// The state is derived from the target table and DuckLake snapshots.
// Uses a single batched query to minimize round-trips.
func GetIncrementalState(sess *duckdb.Session, target, cursor, initial string) (*IncrementalState, error) {
	state := &IncrementalState{
		Cursor:       cursor,
		InitialValue: initial,
	}

	// Set default initial value if not provided
	if state.InitialValue == "" {
		state.InitialValue = DefaultInitialValue
	}

	// Step 1: Check if target table exists
	parts := strings.Split(target, ".")
	var tableExistsExpr string
	switch len(parts) {
	case 1:
		tableExistsExpr = fmt.Sprintf("SELECT ondatra_table_exists_simple('%s')::VARCHAR", escapeSQL(parts[0]))
	case 2:
		tableExistsExpr = fmt.Sprintf("SELECT ondatra_table_exists('%s', '%s')::VARCHAR", escapeSQL(parts[0]), escapeSQL(parts[1]))
	default:
		tableName := strings.Join(parts[2:], ".")
		tableExistsExpr = fmt.Sprintf("SELECT ondatra_table_exists_full('%s', '%s', '%s')::VARCHAR", escapeSQL(parts[0]), escapeSQL(parts[1]), escapeSQL(tableName))
	}

	existsResult, err := sess.QueryValue(tableExistsExpr)
	if err != nil {
		state.IsBackfill = true
		state.LastValue = state.InitialValue
		return state, nil
	}
	state.IsBackfill = existsResult != "true"

	// Step 2: Get MAX(cursor) from target table (only if table exists)
	if !state.IsBackfill && cursor != "" {
		quotedTarget := quoteTarget(target)
		maxSQL := fmt.Sprintf("SELECT COALESCE(MAX(%s)::VARCHAR, '%s') FROM %s",
			duckdb.QuoteIdentifier(cursor), escapeSQL(state.InitialValue), quotedTarget)

		maxResult, err := sess.QueryValue(maxSQL)
		if err != nil {
			state.LastValue = state.InitialValue
		} else {
			state.LastValue = maxResult
		}
	} else {
		state.LastValue = state.InitialValue
	}

	// Step 3: Get last snapshot time for this model
	// Use prod catalog in sandbox mode (sandbox has no snapshot history)
	if !state.IsBackfill {
		snapshotCatalog := sess.CatalogAlias()
		if sess.ProdAlias() != "" {
			snapshotCatalog = sess.ProdAlias()
		}
		lastRunSQL := fmt.Sprintf(`SELECT snapshot_time::VARCHAR FROM %s.snapshots()
			WHERE commit_extra_info->>'model' = '%s'
			ORDER BY snapshot_id DESC LIMIT 1`,
			snapshotCatalog, escapeSQL(target))

		lastRunResult, err := sess.QueryValue(lastRunSQL)
		if err == nil {
			state.LastRun = lastRunResult
		}
	}

	return state, nil
}

// quoteTarget quotes each part of a qualified table name (schema.table).
// Handles reserved words by quoting each identifier separately.
func quoteTarget(target string) string {
	parts := strings.Split(target, ".")
	quoted := make([]string, len(parts))
	for i, p := range parts {
		quoted[i] = duckdb.QuoteIdentifier(p)
	}
	return strings.Join(quoted, ".")
}
