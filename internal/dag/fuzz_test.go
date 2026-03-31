// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package dag

import (
	"testing"
)

// Fuzz extractQueryCalls: must never panic on arbitrary Starlark-like input.
func FuzzExtractQueryCalls(f *testing.F) {
	f.Add(`query("SELECT 1")`)
	f.Add(`query("SELECT * FROM staging.orders")`)
	f.Add(`rows = query("SELECT * FROM " + table)`)
	f.Add(`query(variable)`)
	f.Add(`other_func("SELECT 1")`)
	f.Add(`query()`)
	f.Add(`query(123)`)
	f.Add(``)
	f.Add(`{{{invalid syntax`)
	f.Add(`def f(): return query("SELECT 1")`)
	f.Add(`load("lib/x.star", "f")`)
	f.Add("query(\"SELECT * FROM a\")\nquery(\"SELECT * FROM b\")")
	f.Add(`query("WITH x AS (SELECT 1) SELECT * FROM x")`)

	f.Fuzz(func(t *testing.T, code string) {
		// Must never panic
		sqls := extractQueryCalls(code)

		// Property: every returned string came from a query() call with a string literal
		// (we can't verify exact correctness, but we can check structural properties)
		for _, sql := range sqls {
			if sql == "" {
				t.Error("extractQueryCalls returned empty string")
			}
		}
	})
}

// Fuzz extractCalls: must never panic, loads and queries must be non-empty strings.
func FuzzExtractCalls(f *testing.F) {
	f.Add(`load("lib/helpers.star", "fetch")`)
	f.Add(`query("SELECT 1")`)
	f.Add(`load("lib/a.star", "x"); query("SELECT * FROM t")`)
	f.Add(``)
	f.Add(`{{{`)
	f.Add(`def f(): load("lib/x.star", "y")`)

	f.Fuzz(func(t *testing.T, code string) {
		calls := extractCalls(code)

		for _, sql := range calls.querySQLs {
			if sql == "" {
				t.Error("empty query SQL")
			}
		}
		for _, mod := range calls.loads {
			if mod == "" {
				t.Error("empty load module path")
			}
		}
	})
}
