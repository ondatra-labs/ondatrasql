// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/collect"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
)

// createPushDelta computes the outbound sync delta AFTER materialization
// and returns a list of SyncEvents with raw DuckLake change_types.
// All kinds use the same table_changes() query — no filtering, no mapping.
// The Starlark push() function receives the raw change_type and decides
// what to do with each row.
func (r *Runner) createPushDelta(model *parser.Model, _ string, prevSnapshot, newSnapshot int64) ([]collect.SyncEvent, error) {
	if model.Push == "" {
		return nil, nil
	}
	if newSnapshot == 0 {
		return nil, nil
	}

	_, tableName := splitSchemaTable(model.Target)

	sql := fmt.Sprintf("SELECT rowid, snapshot_id, change_type FROM table_changes('%s', %d, %d)",
		strings.ReplaceAll(tableName, "'", "''"), prevSnapshot+1, newSnapshot)

	rows, err := r.sess.QueryRowsAny(sql)
	if err != nil {
		return nil, fmt.Errorf("table_changes for %s: %w", model.Target, err)
	}

	events := make([]collect.SyncEvent, 0, len(rows))
	for _, row := range rows {
		events = append(events, collect.SyncEvent{
			ChangeType: fmt.Sprintf("%v", row["change_type"]),
			RowID:      toInt64(row["rowid"]),
			Snapshot:   toInt64(row["snapshot_id"]),
		})
	}
	return events, nil
}

// toInt64 converts a DuckDB value to int64.
func toInt64(v any) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case int:
		return int64(val)
	case float64:
		return int64(val)
	case int32:
		return int64(val)
	default:
		// toInt64 is a coercion helper for DuckDB rowset values; the
		// documented contract is "return 0 for anything we can't read
		// as an integer" so unparseable strings fall back to zero.
		//strconvcheck:silent toInt64's contract is zero-on-non-numeric
		n, _ := strconv.ParseInt(fmt.Sprintf("%v", v), 10, 64)
		return n
	}
}

// splitSchemaTable splits "schema.table" into ("schema", "table").
func splitSchemaTable(target string) (string, string) {
	parts := strings.SplitN(target, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", target
}
