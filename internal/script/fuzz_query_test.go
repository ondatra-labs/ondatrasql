// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"strings"
	"testing"
)

// Fuzz validateReadOnly: must never panic, and must reject known mutating keywords.
func FuzzValidateReadOnly(f *testing.F) {
	f.Add("SELECT 1")
	f.Add("WITH cte AS (SELECT 1) SELECT * FROM cte")
	f.Add("INSERT INTO foo VALUES (1)")
	f.Add("DELETE FROM foo")
	f.Add("DROP TABLE foo")
	f.Add("UPDATE foo SET x = 1")
	f.Add("CREATE TABLE foo (id INT)")
	f.Add("")
	f.Add("  \t\n  ")
	f.Add("-- comment\nSELECT 1")
	f.Add("/* block */ SELECT 1")
	f.Add("/* unterminated block comment")
	f.Add("-- only comment")
	f.Add("select 1")           // lowercase
	f.Add("with x as (select 1) select 1")
	f.Add("  SELECT  1")        // leading whitespace
	f.Add("SELECTX")            // prefix but not keyword
	f.Add("SELEC")              // partial keyword
	f.Add("WITH")               // just the keyword
	f.Add("/* */ /* */ SELECT 1") // multiple block comments

	f.Fuzz(func(t *testing.T, sql string) {
		err := validateReadOnly(sql)

		// Property: known mutating prefixes must always be rejected
		trimmed := strings.TrimSpace(sql)
		upper := strings.ToUpper(trimmed)
		for _, prefix := range []string{"INSERT ", "DELETE ", "DROP ", "UPDATE ", "CREATE ", "ALTER ", "TRUNCATE "} {
			if strings.HasPrefix(upper, prefix) && err == nil {
				t.Errorf("validateReadOnly(%q) should reject %s", sql, prefix)
			}
		}

		// Property: empty/whitespace-only input must error
		if strings.TrimSpace(sql) == "" && err == nil {
			t.Errorf("validateReadOnly(%q) should reject empty input", sql)
		}
	})
}

// Fuzz escapeAckSQL: must double all single quotes and be idempotent for safe strings.
func FuzzEscapeAckSQL(f *testing.F) {
	f.Add("simple")
	f.Add("O'Brien")
	f.Add("it's")
	f.Add("''''")
	f.Add("")
	f.Add("no quotes")
	f.Add("a'b'c'd")

	f.Fuzz(func(t *testing.T, input string) {
		result := escapeAckSQL(input)

		// Oracle: must equal strings.ReplaceAll for single quotes
		want := strings.ReplaceAll(input, "'", "''")
		if result != want {
			t.Errorf("escapeAckSQL(%q) = %q, want %q", input, result, want)
		}
	})
}
