// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package lineage

import (
	"encoding/json"
	"testing"
)

// Structural: if input is valid JSON, ExtractFromAST must not return
// columns with empty names.
func FuzzExtractFromAST(f *testing.F) {
	// Valid minimal AST
	f.Add(`{"statements":[{"node":{"type":"SELECT_NODE","select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["id"],"table_name":"orders"}],"from_table":{"type":"BASE_TABLE","alias":"","table_name":"orders","schema_name":""}}}]}`)

	// Edge cases
	f.Add(`{}`)
	f.Add(`{"statements":[]}`)
	f.Add(`{"statements":[{"node":{}}]}`)
	f.Add(`null`)
	f.Add(`[]`)
	f.Add(`""`)
	f.Add(`{"statements":[{"node":{"type":"SELECT_NODE","select_list":[]}}]}`)
	f.Add(`not json at all`)
	f.Add(`{"statements":[{"node":{"type":"UNKNOWN_TYPE"}}]}`)

	f.Fuzz(func(t *testing.T, astJSON string) {
		cols, err := ExtractFromAST(astJSON)
		if err != nil {
			return
		}
		// Structural: returned columns must have non-empty names
		for _, col := range cols {
			if col.Column == "" {
				t.Errorf("empty column name in lineage result for input: %.100s", astJSON)
			}
		}
	})
}

// Structural: returned table names must be non-empty.
func FuzzExtractTablesFromAST(f *testing.F) {
	f.Add(`{"statements":[{"node":{"type":"SELECT_NODE","from_table":{"type":"BASE_TABLE","table_name":"orders","schema_name":"raw"}}}]}`)
	f.Add(`{}`)
	f.Add(`{"statements":[]}`)
	f.Add(`not json`)
	f.Add(`null`)

	f.Fuzz(func(t *testing.T, astJSON string) {
		tables, err := ExtractTablesFromAST(astJSON)
		if err != nil {
			return
		}
		// Structural: table names must be non-empty
		for _, table := range tables {
			if table.Table == "" {
				t.Error("empty table name in result")
			}
		}
		// Structural: if input is valid JSON with statements, tables should be deterministic
		var raw json.RawMessage
		if json.Unmarshal([]byte(astJSON), &raw) == nil {
			tables2, err2 := ExtractTablesFromAST(astJSON)
			if err2 != nil {
				t.Error("non-deterministic: succeeded then failed on same input")
			}
			if len(tables) != len(tables2) {
				t.Errorf("non-deterministic: got %d then %d tables", len(tables), len(tables2))
			}
		}
	})
}

// Structural: renames + added + dropped must account for all column changes.
func FuzzDetectRenames(f *testing.F) {
	f.Add("id", "order_id", "orders", "id")
	f.Add("name", "customer_name", "customers", "name")
	f.Add("", "", "", "")
	f.Add("col", "col", "t", "col") // same name, no rename

	f.Fuzz(func(t *testing.T, oldCol, newCol, table, srcCol string) {
		old := []ColumnLineage{
			{Column: oldCol, Sources: []SourceColumn{{Table: table, Column: srcCol, Transformation: TransformIdentity}}},
		}
		new := []ColumnLineage{
			{Column: newCol, Sources: []SourceColumn{{Table: table, Column: srcCol, Transformation: TransformIdentity}}},
		}
		renames, added, dropped := DetectRenames(old, new)

		if oldCol == newCol {
			// Same column name: no renames, no added, no dropped
			if len(renames) != 0 {
				t.Errorf("same column %q but got %d renames", oldCol, len(renames))
			}
			if len(added) != 0 {
				t.Errorf("same column %q but got %d added", oldCol, len(added))
			}
			if len(dropped) != 0 {
				t.Errorf("same column %q but got %d dropped", oldCol, len(dropped))
			}
		} else {
			// Different names with same source: should detect as rename OR as add+drop
			totalChanges := len(renames) + len(added) + len(dropped)
			if totalChanges == 0 {
				t.Errorf("different columns %q→%q but no changes detected", oldCol, newCol)
			}
		}
	})
}
