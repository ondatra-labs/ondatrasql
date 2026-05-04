// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"fmt"

	"github.com/ondatra-labs/ondatrasql/internal/backfill"
)

// stmtExecer is the narrow exec surface that applyIncrementalVars needs.
// *duckdb.Session satisfies it; tests inject a fake to verify the loop
// aborts on the first SET VARIABLE failure.
type stmtExecer interface {
	Exec(string) error
}

// applyIncrementalVars sets the four DuckDB session variables that
// incremental SQL models reference via getvariable(). Aborts on the
// first failure so a stale cursor can't silently slip into the model
// run — pre-fix, all four errors were discarded.
func applyIncrementalVars(exec stmtExecer, state *backfill.IncrementalState) error {
	setVars := []struct{ name, stmt string }{
		{"incr_last_value", fmt.Sprintf("SET VARIABLE incr_last_value = '%s'", escapeSQL(state.LastValue))},
		{"incr_last_run", fmt.Sprintf("SET VARIABLE incr_last_run = '%s'", escapeSQL(state.LastRun))},
		{"incr_is_backfill", fmt.Sprintf("SET VARIABLE incr_is_backfill = %t", state.IsBackfill)},
		{"incr_cursor", fmt.Sprintf("SET VARIABLE incr_cursor = '%s'", escapeSQL(state.Cursor))},
	}
	for _, v := range setVars {
		if err := exec.Exec(v.stmt); err != nil {
			return fmt.Errorf("set incremental variable %s: %w", v.name, err)
		}
	}
	return nil
}
