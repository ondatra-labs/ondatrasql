// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"encoding/json"
	"strings"
	"testing"

	"pgregory.net/rapid"
)

// --- Generators ---

func genSchemaName() *rapid.Generator[string] {
	return rapid.SampledFrom([]string{"raw", "staging", "mart", "analytics", "public"})
}

func genTableName() *rapid.Generator[string] {
	return rapid.StringMatching(`^[a-z][a-z0-9_]{1,12}$`)
}

func genSchemaTable() *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		schema := genSchemaName().Draw(t, "schema")
		table := genTableName().Draw(t, "table")
		return schema + "." + table
	})
}

// --- walkAST Properties ---

// Property: walkAST with identity visitor preserves JSON structure.
func TestRapid_WalkAST_IdentityPreservesStructure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		schema := genSchemaName().Draw(t, "schema")
		table := genTableName().Draw(t, "table")
		alias := rapid.SampledFrom([]string{"", "a", "t1", "my_alias"}).Draw(t, "alias")

		root := map[string]any{
			"type":        "BASE_TABLE",
			"schema_name": schema,
			"table_name":  table,
			"alias":       alias,
		}

		// Marshal before walk
		before, _ := json.Marshal(root)

		// Identity walk (no replacements)
		walkAST(root, func(node map[string]any) map[string]any { return nil })

		// Marshal after walk
		after, _ := json.Marshal(root)

		if string(before) != string(after) {
			t.Fatalf("identity walk changed structure:\nbefore: %s\nafter:  %s", before, after)
		}
	})
}

// Property: walkAST replacement count equals number of matching nodes.
func TestRapid_WalkAST_ReplacementCount(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(1, 5).Draw(t, "n_tables")
		items := make([]any, n)
		for i := range items {
			items[i] = map[string]any{
				"type":        "BASE_TABLE",
				"schema_name": genSchemaName().Draw(t, "schema"),
				"table_name":  genTableName().Draw(t, "table"),
			}
		}
		root := map[string]any{"items": items}

		var count int
		walkAST(root, func(node map[string]any) map[string]any {
			if nodeType, _ := node["type"].(string); nodeType == "BASE_TABLE" {
				count++
				return map[string]any{"type": "REPLACED"}
			}
			return nil
		})

		if count != n {
			t.Fatalf("expected %d replacements, got %d", n, count)
		}
	})
}

// --- buildCDCSubquery Properties ---

// Property: CDC subquery always has EXCEPT and VERSION.
func TestRapid_BuildCDCSubquery_HasExceptAndVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		schema := genSchemaName().Draw(t, "schema")
		table := genTableName().Draw(t, "table")
		catalog := rapid.SampledFrom([]string{"", "lake", "prod"}).Draw(t, "catalog")
		alias := rapid.SampledFrom([]string{"", "a", "t1"}).Draw(t, "alias")
		snapshot := int64(rapid.IntRange(1, 10000).Draw(t, "snapshot"))

		node := buildCDCSubquery(schema, table, catalog, alias, snapshot)

		// Must be a SUBQUERY
		if node["type"] != "SUBQUERY" {
			t.Fatalf("expected SUBQUERY, got %v", node["type"])
		}

		// Alias must match
		if node["alias"] != alias {
			t.Fatalf("alias = %v, want %v", node["alias"], alias)
		}

		// Serialize to check structure
		bytes, _ := json.Marshal(node)
		s := string(bytes)

		if !strings.Contains(s, `"EXCEPT"`) {
			t.Fatalf("missing EXCEPT in: %s", s)
		}
		if !strings.Contains(s, `"VERSION"`) {
			t.Fatalf("missing VERSION in: %s", s)
		}
		if !strings.Contains(s, `"schema_name":"` + schema + `"`) {
			t.Fatalf("missing schema_name %q in: %s", schema, s)
		}
		if !strings.Contains(s, `"table_name":"` + table + `"`) {
			t.Fatalf("missing table_name %q in: %s", table, s)
		}
	})
}

// --- buildEmptyCDCSubquery Properties ---

// Property: Empty CDC subquery always has WHERE false (OPERATOR_CAST + BOOLEAN).
func TestRapid_BuildEmptyCDCSubquery_HasWhereFalse(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		schema := genSchemaName().Draw(t, "schema")
		table := genTableName().Draw(t, "table")
		catalog := rapid.SampledFrom([]string{"", "lake"}).Draw(t, "catalog")
		alias := rapid.SampledFrom([]string{"", "e"}).Draw(t, "alias")

		node := buildEmptyCDCSubquery(schema, table, catalog, alias)
		bytes, _ := json.Marshal(node)
		s := string(bytes)

		if !strings.Contains(s, `"OPERATOR_CAST"`) {
			t.Fatalf("missing OPERATOR_CAST in: %s", s)
		}
		if !strings.Contains(s, `"BOOLEAN"`) {
			t.Fatalf("missing BOOLEAN in: %s", s)
		}
	})
}

// --- qualifyTablesInAST Properties ---

// Property: qualifying tables sets catalog_name only on matching nodes.
func TestRapid_QualifyTablesInAST_SelectiveQualification(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(2, 5).Draw(t, "n_tables")
		seen := make(map[string]bool)
		var allTables []string
		var items []any

		for len(allTables) < n {
			tbl := genSchemaTable().Draw(t, "table")
			if !seen[tbl] {
				seen[tbl] = true
				allTables = append(allTables, tbl)
				parts := strings.SplitN(tbl, ".", 2)
				items = append(items, map[string]any{
					"type":         "BASE_TABLE",
					"schema_name":  parts[0],
					"table_name":   parts[1],
					"catalog_name": "",
				})
			}
		}

		root := map[string]any{"items": items}

		// Qualify only first half of tables
		half := n / 2
		tablesToQualify := make(map[string]bool)
		for i := 0; i < half; i++ {
			tablesToQualify[allTables[i]] = true
		}

		qualifyTablesInAST(root, tablesToQualify, "prod_lake")

		// Check: qualified tables have catalog, others don't
		resultItems := root["items"].([]any)
		for i, item := range resultItems {
			m := item.(map[string]any)
			catalogName, _ := m["catalog_name"].(string)
			if i < half {
				if catalogName != "prod_lake" {
					t.Fatalf("table %d (%s) should be qualified, got catalog=%q",
						i, allTables[i], catalogName)
				}
			} else {
				if catalogName != "" {
					t.Fatalf("table %d (%s) should NOT be qualified, got catalog=%q",
						i, allTables[i], catalogName)
				}
			}
		}
	})
}

// --- quoteTableName Properties ---

// Property: part count is preserved (dots become "." separators).
func TestRapid_QuoteTableName_PartCountPreserved(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		name := rapid.OneOf(
			genTableName(),
			genSchemaTable(),
			rapid.Custom(func(t *rapid.T) string {
				return genSchemaName().Draw(t, "cat") + "." +
					genSchemaName().Draw(t, "schema") + "." +
					genTableName().Draw(t, "table")
			}),
		).Draw(t, "name")

		result := quoteTableName(name)
		inputParts := strings.Split(name, ".")
		resultParts := strings.Split(result, `"."`)
		if len(resultParts) != len(inputParts) {
			t.Fatalf("part count mismatch: input %d, output %d\n  input: %q\n  output: %q",
				len(inputParts), len(resultParts), name, result)
		}
	})
}

// Property: quoteTableName output is always wrapped in double quotes.
func TestRapid_QuoteTableName_Wrapped(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		name := rapid.OneOf(
			genTableName(),
			genSchemaTable(),
		).Draw(t, "name")

		result := quoteTableName(name)
		if result[0] != '"' || result[len(result)-1] != '"' {
			t.Fatalf("quoteTableName(%q) = %q, not properly quoted", name, result)
		}
	})
}

// Property: quoteTableName is recoverable — splitting and unquoting recovers parts.
func TestRapid_QuoteTableName_Recoverable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		// Use identifiers without dots or quotes to test clean roundtrip
		parts := make([]string, rapid.IntRange(1, 3).Draw(t, "n"))
		for i := range parts {
			parts[i] = rapid.StringMatching(`^[a-z][a-z0-9_]{0,10}$`).Draw(t, "part")
		}
		name := strings.Join(parts, ".")

		result := quoteTableName(name)

		// Recover: split on ".", strip outer quotes from each part
		quotedParts := strings.Split(result, ".")
		recovered := make([]string, len(quotedParts))
		for i, qp := range quotedParts {
			if len(qp) < 2 || qp[0] != '"' || qp[len(qp)-1] != '"' {
				t.Fatalf("part %d not quoted: %q", i, qp)
			}
			inner := qp[1 : len(qp)-1]
			recovered[i] = strings.ReplaceAll(inner, `""`, `"`)
		}

		recoveredName := strings.Join(recovered, ".")
		if recoveredName != name {
			t.Fatalf("not recoverable: %q -> %q -> %q", name, result, recoveredName)
		}
	})
}

