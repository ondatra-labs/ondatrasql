// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package duckdb

import "testing"

// late_materialization is added to whatever is disabled already, never
// replacing it, and a value that has it already needs no SET.
func TestLateMaterializationSQL(t *testing.T) {
	t.Parallel()
	for current, want := range map[string]string{
		"":                                "SET disabled_optimizers = 'late_materialization'",
		"join_order":                      "SET disabled_optimizers = 'join_order,late_materialization'",
		" join_order , filter_pushdown ":  "SET disabled_optimizers = 'join_order,filter_pushdown,late_materialization'",
		"late_materialization":            "",
		"join_order,LATE_MATERIALIZATION": "",
	} {
		if got := lateMaterializationSQL(current); got != want {
			t.Errorf("lateMaterializationSQL(%q) = %q, want %q", current, got, want)
		}
	}
}

func TestTouchesDisabledOptimizers(t *testing.T) {
	t.Parallel()
	for sql, want := range map[string]bool{
		"SET disabled_optimizers = 'join_order'": true,
		"SET GLOBAL Disabled_Optimizers TO ''":   true,
		"RESET disabled_optimizers":              true,
		"RESET ALL":                              true,
		"SET threads = 4":                        false,
		"CREATE TABLE t (a INT)":                 false,
		"SELECT password_reset_at FROM users":    false,
		"SELECT 1 AS reset":                      true, // a word match errs on the cheap side
		"reset   all":                            true,
	} {
		if got := touchesDisabledOptimizers(sql); got != want {
			t.Errorf("touchesDisabledOptimizers(%q) = %v, want %v", sql, got, want)
		}
	}
}
