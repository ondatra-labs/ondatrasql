// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package backfill

import (
	"os"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/sql"
)

var shared *duckdb.Session

func TestMain(m *testing.M) {
	sess, err := duckdb.NewSession(":memory:?threads=4&memory_limit=2GB")
	if err != nil {
		panic("create shared session: " + err.Error())
	}

	// Load schema macros (same as production)
	macros, err := sql.Load("macros/schema.sql")
	if err != nil {
		sess.Close()
		panic("load schema macros: " + err.Error())
	}
	if err := sess.Exec(macros); err != nil {
		sess.Close()
		panic("exec schema macros: " + err.Error())
	}

	shared = sess
	code := m.Run()
	shared.Close()
	os.Exit(code)
}

func TestComputeSchemaHash_Pairs(t *testing.T) {
	tests := []struct {
		name  string
		cols1 []Column
		cols2 []Column
		equal bool
	}{
		{
			name:  "identical columns",
			cols1: []Column{{Name: "id", Type: "INTEGER"}, {Name: "name", Type: "VARCHAR"}},
			cols2: []Column{{Name: "id", Type: "INTEGER"}, {Name: "name", Type: "VARCHAR"}},
			equal: true,
		},
		{
			name:  "different order - same hash (sorted)",
			cols1: []Column{{Name: "id", Type: "INTEGER"}, {Name: "name", Type: "VARCHAR"}},
			cols2: []Column{{Name: "name", Type: "VARCHAR"}, {Name: "id", Type: "INTEGER"}},
			equal: true,
		},
		{
			name:  "different column name",
			cols1: []Column{{Name: "id", Type: "INTEGER"}},
			cols2: []Column{{Name: "user_id", Type: "INTEGER"}},
			equal: false,
		},
		{
			name:  "different column type",
			cols1: []Column{{Name: "id", Type: "INTEGER"}},
			cols2: []Column{{Name: "id", Type: "BIGINT"}},
			equal: false,
		},
		{
			name:  "extra column",
			cols1: []Column{{Name: "id", Type: "INTEGER"}},
			cols2: []Column{{Name: "id", Type: "INTEGER"}, {Name: "name", Type: "VARCHAR"}},
			equal: false,
		},
		{
			name:  "empty columns",
			cols1: []Column{},
			cols2: []Column{},
			equal: true,
		},
		{
			name:  "nil vs empty",
			cols1: nil,
			cols2: []Column{},
			equal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h1 := ComputeSchemaHash(tt.cols1)
			h2 := ComputeSchemaHash(tt.cols2)

			if tt.equal && h1 != h2 {
				t.Errorf("expected equal hashes:\n  cols1: %v -> %s\n  cols2: %v -> %s", tt.cols1, h1, tt.cols2, h2)
			}
			if !tt.equal && h1 == h2 {
				t.Errorf("expected different hashes:\n  cols1: %v -> %s\n  cols2: %v -> %s", tt.cols1, h1, tt.cols2, h2)
			}
		})
	}
}

func TestComputeSchemaHash_Deterministic(t *testing.T) {
	cols := []Column{
		{Name: "id", Type: "INTEGER"},
		{Name: "name", Type: "VARCHAR"},
		{Name: "created_at", Type: "TIMESTAMP"},
	}

	// Hash same columns multiple times - should be identical
	h1 := ComputeSchemaHash(cols)
	h2 := ComputeSchemaHash(cols)
	h3 := ComputeSchemaHash(cols)

	if h1 != h2 || h2 != h3 {
		t.Errorf("hash not deterministic: %s, %s, %s", h1, h2, h3)
	}

	// Verify hash is non-empty
	if h1 == "" {
		t.Error("expected non-empty hash")
	}

	// Verify hash has expected length (32 hex chars = 16 bytes)
	if len(h1) != 32 {
		t.Errorf("expected hash length 32, got %d", len(h1))
	}
}

func TestClassifySchemaChange(t *testing.T) {
	tests := []struct {
		name         string
		old          []Column
		new          []Column
		expectedType SchemaChangeType
		addedCount   int
		droppedCount int
		changedCount int
	}{
		{
			name:         "no changes",
			old:          []Column{{Name: "id", Type: "INTEGER"}, {Name: "name", Type: "VARCHAR"}},
			new:          []Column{{Name: "id", Type: "INTEGER"}, {Name: "name", Type: "VARCHAR"}},
			expectedType: SchemaChangeNone,
		},
		{
			name:         "add column",
			old:          []Column{{Name: "id", Type: "INTEGER"}},
			new:          []Column{{Name: "id", Type: "INTEGER"}, {Name: "name", Type: "VARCHAR"}},
			expectedType: SchemaChangeAdditive,
			addedCount:   1,
		},
		{
			name:         "add multiple columns",
			old:          []Column{{Name: "id", Type: "INTEGER"}},
			new:          []Column{{Name: "id", Type: "INTEGER"}, {Name: "name", Type: "VARCHAR"}, {Name: "age", Type: "INTEGER"}},
			expectedType: SchemaChangeAdditive,
			addedCount:   2,
		},
		{
			name:         "drop column - destructive",
			old:          []Column{{Name: "id", Type: "INTEGER"}, {Name: "name", Type: "VARCHAR"}},
			new:          []Column{{Name: "id", Type: "INTEGER"}},
			expectedType: SchemaChangeDestructive,
			droppedCount: 1,
		},
		{
			name:         "type promotion INT to BIGINT",
			old:          []Column{{Name: "id", Type: "INTEGER"}},
			new:          []Column{{Name: "id", Type: "BIGINT"}},
			expectedType: SchemaChangeTypeChange,
			changedCount: 1,
		},
		{
			name:         "type narrowing BIGINT to INT - destructive",
			old:          []Column{{Name: "id", Type: "BIGINT"}},
			new:          []Column{{Name: "id", Type: "INTEGER"}},
			expectedType: SchemaChangeDestructive,
			changedCount: 1,
		},
		{
			name:         "add column and promote type",
			old:          []Column{{Name: "id", Type: "INTEGER"}},
			new:          []Column{{Name: "id", Type: "BIGINT"}, {Name: "name", Type: "VARCHAR"}},
			expectedType: SchemaChangeAdditive,
			addedCount:   1,
			changedCount: 1,
		},
		{
			name:         "drop and add - destructive (because of drop)",
			old:          []Column{{Name: "id", Type: "INTEGER"}, {Name: "old_col", Type: "VARCHAR"}},
			new:          []Column{{Name: "id", Type: "INTEGER"}, {Name: "new_col", Type: "VARCHAR"}},
			expectedType: SchemaChangeDestructive,
			addedCount:   1,
			droppedCount: 1,
		},
		{
			name:         "empty to columns",
			old:          []Column{},
			new:          []Column{{Name: "id", Type: "INTEGER"}},
			expectedType: SchemaChangeAdditive,
			addedCount:   1,
		},
		{
			name:         "columns to empty - destructive",
			old:          []Column{{Name: "id", Type: "INTEGER"}},
			new:          []Column{},
			expectedType: SchemaChangeDestructive,
			droppedCount: 1,
		},
		{
			name:         "FLOAT to DOUBLE promotion",
			old:          []Column{{Name: "price", Type: "FLOAT"}},
			new:          []Column{{Name: "price", Type: "DOUBLE"}},
			expectedType: SchemaChangeTypeChange,
			changedCount: 1,
		},
		{
			name:         "TINYINT to INTEGER promotion",
			old:          []Column{{Name: "age", Type: "TINYINT"}},
			new:          []Column{{Name: "age", Type: "INTEGER"}},
			expectedType: SchemaChangeTypeChange,
			changedCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ClassifySchemaChange(tt.old, tt.new, shared)

			if result.Type != tt.expectedType {
				t.Errorf("Type = %q, want %q", result.Type, tt.expectedType)
			}
			if len(result.Added) != tt.addedCount {
				t.Errorf("Added count = %d, want %d", len(result.Added), tt.addedCount)
			}
			if len(result.Dropped) != tt.droppedCount {
				t.Errorf("Dropped count = %d, want %d", len(result.Dropped), tt.droppedCount)
			}
			if len(result.TypeChanged) != tt.changedCount {
				t.Errorf("TypeChanged count = %d, want %d", len(result.TypeChanged), tt.changedCount)
			}
		})
	}
}

func TestTypeNormalization(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		// Integer aliases
		{"INT", "INTEGER"},
		{"INT4", "INTEGER"},
		{"SIGNED", "INTEGER"},

		// Bigint aliases
		{"INT8", "BIGINT"},
		{"LONG", "BIGINT"},

		// Smallint aliases
		{"INT2", "SMALLINT"},
		{"SHORT", "SMALLINT"},

		// Tinyint
		{"INT1", "TINYINT"},

		// Float aliases
		{"FLOAT4", "FLOAT"},
		{"REAL", "FLOAT"},

		// Double aliases
		{"FLOAT8", "DOUBLE"},
		{"NUMERIC", "DOUBLE"},

		// String aliases
		{"STRING", "VARCHAR"},
		{"CHAR", "VARCHAR"},
		{"BPCHAR", "VARCHAR"},

		// Boolean aliases
		{"BOOL", "BOOLEAN"},

		// Already canonical
		{"INTEGER", "INTEGER"},
		{"BIGINT", "BIGINT"},
		{"VARCHAR", "VARCHAR"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			query := "SELECT ondatra_normalize_type('" + tt.input + "')"
			result, err := shared.QueryValue(query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("ondatra_normalize_type(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}
