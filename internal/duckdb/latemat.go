// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package duckdb

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
)

// DuckDB 2.0.0's late_materialization optimizer writes past a heap buffer.
// src/storage/table/row_id_column_data.cpp:100 passes `count` instead of
// `max_count` as the scan count to ColumnSegment::FilterSelection, so on a
// native DuckDB table with deleted rows a small `ORDER BY ... LIMIT` makes the
// optimizer's bloom filter on rowid write past its hash buffer
// (heap-buffer-overflow under ASan). The state store (state.duckdb: push queue
// and fetch staging, both DELETEd from) is such a table, and the main session
// holds native temp tables too.
//
// Every DuckDB database ondatrasql opens therefore runs with
// late_materialization in disabled_optimizers, added to whatever the user's
// SQL disables. This stays until a DuckDB release fixes that line;
// TestLateMaterialization_* pins it.
const lateMaterialization = "late_materialization"

// lateMaterializationSQL returns the SET that adds late_materialization to
// the disabled optimizers, given the current value of the setting, or "" when
// it is already disabled. disabled_optimizers is a single global list, and a
// SET replaces it, so the user's entries are kept.
func lateMaterializationSQL(current string) string {
	var keep []string
	for _, name := range strings.Split(current, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if strings.EqualFold(name, lateMaterialization) {
			return ""
		}
		keep = append(keep, name)
	}
	keep = append(keep, lateMaterialization)
	return "SET disabled_optimizers = '" + strings.ReplaceAll(strings.Join(keep, ","), "'", "''") + "'"
}

// touchesDisabledOptimizers reports whether SQL may have replaced or reset
// disabled_optimizers, so the session has to add late_materialization again.
func touchesDisabledOptimizers(sqlStr string) bool {
	lower := strings.ToLower(sqlStr)
	return strings.Contains(lower, "disabled_optimizers") || strings.Contains(lower, "reset")
}

const currentDisabledOptimizers = "SELECT current_setting('disabled_optimizers')"

// disableLateMaterializationLocked applies the setting on the session's
// connection. The caller holds s.mu.
func (s *Session) disableLateMaterializationLocked(ctx context.Context) error {
	var current string
	if err := s.conn.QueryRowContext(ctx, currentDisabledOptimizers).Scan(&current); err != nil {
		return fmt.Errorf("read disabled_optimizers: %w", err)
	}
	if stmt := lateMaterializationSQL(current); stmt != "" {
		if _, err := s.conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("disable late_materialization: %w", err)
		}
	}
	return nil
}

// DisableLateMaterialization applies the setting through a raw driver
// connection, for connector init hooks such as the state store's.
func DisableLateMaterialization(ctx context.Context, conn driver.ExecerContext) error {
	current := ""
	if q, ok := conn.(driver.QueryerContext); ok {
		rows, err := q.QueryContext(ctx, currentDisabledOptimizers, nil)
		if err != nil {
			return fmt.Errorf("read disabled_optimizers: %w", err)
		}
		vals := make([]driver.Value, 1)
		err = rows.Next(vals)
		_ = rows.Close() // read-only iterator close
		if err != nil && err != io.EOF {
			return fmt.Errorf("read disabled_optimizers: %w", err)
		}
		if s, ok := vals[0].(string); ok {
			current = s
		}
	}
	if stmt := lateMaterializationSQL(current); stmt != "" {
		if _, err := conn.ExecContext(ctx, stmt, nil); err != nil {
			return fmt.Errorf("disable late_materialization: %w", err)
		}
	}
	return nil
}
