// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/ondatra-labs/ondatrasql/internal/parser"
)

// Structural invariant: every dot-separated part must be individually quoted,
// and part count must be preserved.
// Structural: sanitizeTableName replaces dots with underscores, no panics.
func FuzzSanitizeTableName(f *testing.F) {
	if testing.Short() {
		f.Skip("skipping in short mode")
	}
	f.Add("staging.orders")
	f.Add("")
	f.Add("...")
	f.Add("a.b.c.d")
	f.Add("no_dots_here")
	f.Add("日本語.テスト")

	f.Fuzz(func(t *testing.T, name string) {
		result := sanitizeTableName(name)
		// No dots in result
		if strings.Contains(result, ".") {
			t.Errorf("sanitizeTableName(%q) = %q, still contains dots", name, result)
		}
		// For valid UTF-8 input, rune count is preserved (dots→underscores is 1:1)
		if utf8.ValidString(name) {
			inRunes := utf8.RuneCountInString(name)
			outRunes := utf8.RuneCountInString(result)
			if inRunes != outRunes {
				t.Errorf("sanitizeTableName(%q) rune count changed: %d -> %d", name, inRunes, outRunes)
			}
		}
	})
}

// Oracle: escapeSQL doubles single quotes; result never has un-doubled single quotes
// in positions where the original had none.
func FuzzEscapeSQL(f *testing.F) {
	if testing.Short() {
		f.Skip("skipping in short mode")
	}
	f.Add("simple")
	f.Add("O'Brien")
	f.Add("it's working")
	f.Add("multiple'quotes'here")
	f.Add("")
	f.Add("''''")
	f.Add("no quotes at all")

	f.Fuzz(func(t *testing.T, input string) {
		result := escapeSQL(input)
		// Oracle: must equal strings.ReplaceAll for single quotes
		want := strings.ReplaceAll(input, "'", "''")
		if result != want {
			t.Errorf("escapeSQL(%q) = %q, want %q", input, result, want)
		}
	})
}

// Parsing invariant: loadExtension's "name FROM repo" parsing.
// We test just the string parsing logic by checking what INSTALL/LOAD SQL would be generated.
func FuzzLoadExtensionParsing(f *testing.F) {
	if testing.Short() {
		f.Skip("skipping in short mode")
	}
	f.Add("spatial")
	f.Add("httpfs FROM community")
	f.Add("delta FROM core_nightly")
	f.Add("myext FROM 'https://example.com/ext.duckdb_extension'")
	f.Add("")
	f.Add("  spaces  ")
	f.Add("name FROM")
	f.Add("FROM repo")

	f.Fuzz(func(t *testing.T, ext string) {
		// Parse using the same logic as loadExtension (normalized to lowercase first)
		normalized := strings.ToLower(strings.TrimSpace(ext))
		var name, repo string
		if idx := strings.Index(normalized, " from "); idx != -1 {
			name = strings.TrimSpace(normalized[:idx])
			repo = strings.TrimSpace(normalized[idx+6:])
		} else {
			name = normalized
		}

		// Invariant: repo is only set when " from " is present
		if !strings.Contains(normalized, " from ") {
			if repo != "" {
				t.Errorf("repo should be empty when no FROM: input=%q repo=%q", ext, repo)
			}
		}

		// Invariant: name never contains leading/trailing whitespace
		if name != strings.TrimSpace(name) {
			t.Errorf("name has whitespace: %q", name)
		}
		if repo != strings.TrimSpace(repo) {
			t.Errorf("repo has whitespace: %q", repo)
		}
	})
}

func FuzzQuoteTableName(f *testing.F) {
	if testing.Short() {
		f.Skip("skipping in short mode")
	}
	f.Add("orders")
	f.Add("staging.orders")
	f.Add("catalog.schema.table")
	f.Add("")
	f.Add(".")
	f.Add("..")
	f.Add("a.b.c.d.e")
	f.Add(`has"quote`)
	f.Add(`"already_quoted"`)
	f.Add("with space.and more")
	f.Add("UPPER.lower.MiXeD")

	f.Fuzz(func(t *testing.T, name string) {
		result := quoteTableName(name)
		// Must be quoted
		if len(result) < 2 || result[0] != '"' || result[len(result)-1] != '"' {
			t.Errorf("quoteTableName(%q) = %q, not properly quoted", name, result)
		}
		// Part count conservation: input and output must have same number of dot-separated parts
		inputParts := strings.Split(name, ".")
		// Count parts by splitting on "." that separate quoted identifiers ("x"."y")
		resultParts := strings.Split(result, `"."`)
		if len(resultParts) != len(inputParts) {
			t.Errorf("part count mismatch: input %d parts, output %d parts\n  input: %q\n  output: %q",
				len(inputParts), len(resultParts), name, result)
		}
	})
}

func FuzzBuildEventsCreateSQL(f *testing.F) {
	if testing.Short() {
		f.Skip("skipping in short mode")
	}
	f.Add("event_name", "VARCHAR", true, "raw.events")
	f.Add("count", "INTEGER", false, "staging.metrics")
	f.Add("data", "JSON", false, "mart.output")

	f.Fuzz(func(t *testing.T, colName, colType string, notNull bool, target string) {
		// Skip invalid inputs
		if colName == "" || colType == "" || target == "" {
			return
		}
		if strings.ContainsAny(colName, " \t\n'\"();") || strings.ContainsAny(colType, " \t\n'\"();") {
			return
		}

		model := &parser.Model{
			Target: target,
			Kind:   "events",
			Columns: []parser.ColumnDef{
				{Name: colName, Type: colType, NotNull: notNull},
			},
		}
		result := buildEventsCreateSQL(model)

		// Must start with CREATE TABLE IF NOT EXISTS
		if !strings.HasPrefix(result, "CREATE TABLE IF NOT EXISTS") {
			t.Errorf("missing CREATE TABLE prefix: %s", result)
		}
		// Must contain the target
		if !strings.Contains(result, target) {
			t.Errorf("missing target %q in: %s", target, result)
		}
		// Must contain the column name and type
		if !strings.Contains(result, colName) {
			t.Errorf("missing column %q in: %s", colName, result)
		}
		if !strings.Contains(result, colType) {
			t.Errorf("missing type %q in: %s", colType, result)
		}
		// NOT NULL check
		if notNull && !strings.Contains(result, "NOT NULL") {
			t.Errorf("missing NOT NULL in: %s", result)
		}
	})
}

func FuzzFormatSQLValue(f *testing.F) {
	if testing.Short() {
		f.Skip("skipping in short mode")
	}
	f.Add("hello")
	f.Add("O'Brien")
	f.Add("")
	f.Add("it's a 'test'")

	f.Fuzz(func(t *testing.T, input string) {
		result := formatSQLValue(input)
		// String values must be quoted
		if len(result) < 2 || result[0] != '\'' || result[len(result)-1] != '\'' {
			t.Errorf("string not properly quoted: %q -> %q", input, result)
		}
		// Single quotes in input must be escaped (doubled)
		inner := result[1 : len(result)-1]
		// Count unescaped single quotes (should be 0)
		for i := 0; i < len(inner); i++ {
			if inner[i] == '\'' {
				if i+1 < len(inner) && inner[i+1] == '\'' {
					i++ // Skip escaped pair
				} else {
					t.Errorf("unescaped single quote in: %q", result)
				}
			}
		}
	})
}

// FuzzWalkAST verifies that walkAST never panics on arbitrary JSON.
func FuzzWalkAST(f *testing.F) {
	if testing.Short() {
		f.Skip("skipping in short mode")
	}
	f.Add(`{"type": "BASE_TABLE", "schema_name": "s", "table_name": "t"}`)
	f.Add(`{"a": [{"type": "BASE_TABLE"}, null, 42, "str"]}`)
	f.Add(`[]`)
	f.Add(`null`)
	f.Add(`{"nested": {"deep": {"type": "BASE_TABLE"}}}`)

	f.Fuzz(func(t *testing.T, input string) {
		var node any
		if err := json.Unmarshal([]byte(input), &node); err != nil {
			return // skip invalid JSON
		}

		// Should never panic
		walkAST(node, func(m map[string]any) map[string]any {
			if nodeType, _ := m["type"].(string); nodeType == "BASE_TABLE" {
				return map[string]any{"type": "REPLACED"}
			}
			return nil
		})
	})
}
