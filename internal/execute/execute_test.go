// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"errors"
	"fmt"
	"testing"

	duckdbdriver "github.com/duckdb/duckdb-go/v2"
	"github.com/ondatra-labs/ondatrasql/internal/backfill"
)

func TestSanitizeTableName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{"staging.orders", "staging_orders"},
		{"main.customers", "main_customers"},
		{"simple_table", "simple_table"},
		{"schema.table.extra", "schema_table_extra"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			got := sanitizeTableName(tt.input)
			if got != tt.want {
				t.Errorf("sanitizeTableName(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestQuoteTableName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{"orders", `"orders"`},
		{"staging.orders", `"staging"."orders"`},
		{"catalog.schema.table", `"catalog"."schema"."table"`},
		{`weird"name`, `"weird""name"`}, // quotes are escaped
		{"", `""`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			got := quoteTableName(tt.input)
			if got != tt.want {
				t.Errorf("quoteTableName(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestNewRunner_DefaultState(t *testing.T) {
	t.Parallel()
	runner := NewRunner(nil, ModeRun, "test-run")
	if runner.mode != ModeRun {
		t.Errorf("mode = %v, want ModeRun", runner.mode)
	}
	if runner.dagRunID != "test-run" {
		t.Errorf("dagRunID = %q, want %q", runner.dagRunID, "test-run")
	}
}

func TestSetGitInfo_Roundtrip(t *testing.T) {
	t.Parallel()
	runner := NewRunner(nil, ModeRun, "test-run")
	runner.SetGitInfo("abc123", "main", "https://github.com/user/repo")

	if runner.gitInfo.Commit != "abc123" || runner.gitInfo.Branch != "main" || runner.gitInfo.RepoURL != "https://github.com/user/repo" {
		t.Errorf("git info not stored correctly: %+v", runner.gitInfo)
	}
}

func TestFilterColumnsByName(t *testing.T) {
	t.Parallel()
	columns := []backfill.Column{
		{Name: "id", Type: "INTEGER"},
		{Name: "name", Type: "VARCHAR"},
		{Name: "age", Type: "INTEGER"},
		{Name: "email", Type: "VARCHAR"},
	}

	filtered := filterColumnsByName(columns, []string{"id", "age"})
	if len(filtered) != 2 {
		t.Fatalf("got %d columns, want 2", len(filtered))
	}
	names := make(map[string]bool)
	for _, c := range filtered {
		names[c.Name] = true
	}
	if !names["id"] || !names["age"] {
		t.Errorf("expected id and age, got %v", names)
	}
}

func TestFilterColumnsByName_Empty(t *testing.T) {
	t.Parallel()
	columns := []backfill.Column{
		{Name: "id", Type: "INTEGER"},
	}
	filtered := filterColumnsByName(columns, nil)
	if filtered != nil {
		t.Errorf("expected nil for empty names, got %v", filtered)
	}
}

func TestFilterColumnsByName_NoMatch(t *testing.T) {
	t.Parallel()
	columns := []backfill.Column{
		{Name: "id", Type: "INTEGER"},
	}
	filtered := filterColumnsByName(columns, []string{"nonexistent"})
	if len(filtered) != 0 {
		t.Errorf("expected 0 matches, got %d", len(filtered))
	}
}

func TestSetRunTypeDecisions(t *testing.T) {
	t.Parallel()
	runner := NewRunner(nil, ModeRun, "test-run")
	decisions := RunTypeDecisions{
		"staging.a": {RunType: "full"},
		"staging.b": {RunType: "incremental"},
	}
	runner.SetRunTypeDecisions(decisions)

	if runner.runTypeDecisions == nil {
		t.Fatal("decisions not set")
	}
	if len(runner.runTypeDecisions) != 2 {
		t.Errorf("decisions len = %d, want 2", len(runner.runTypeDecisions))
	}
}

func TestIsTableNotExistError(t *testing.T) {
	t.Parallel()

	duckErr := func(typ duckdbdriver.ErrorType, msg string) error {
		return &duckdbdriver.Error{Type: typ, Msg: msg}
	}

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "DuckLake catalog error",
			err:  duckErr(duckdbdriver.ErrorTypeCatalog, `Table with name "staging.orders" does not exist! Did you mean "pg_class"?`),
			want: true,
		},
		{
			name: "catalog error does not exist lowercase",
			err:  duckErr(duckdbdriver.ErrorTypeCatalog, `table does not exist`),
			want: true,
		},
		{
			name: "catalog error unrelated",
			err:  duckErr(duckdbdriver.ErrorTypeCatalog, `Schema "staging" already exists`),
			want: false,
		},
		{
			name: "invalid input partition corruption",
			err:  duckErr(duckdbdriver.ErrorTypeInvalidInput, `Could not find matching table for partition entry`),
			want: false,
		},
		{
			name: "IO error with not found",
			err:  duckErr(duckdbdriver.ErrorTypeIO, `file not found: /path/to/data.parquet`),
			want: false,
		},
		{
			name: "IO error lock",
			err:  duckErr(duckdbdriver.ErrorTypeIO, `Could not set lock on file`),
			want: false,
		},
		{
			name: "invalid input snapshot",
			err:  duckErr(duckdbdriver.ErrorTypeInvalidInput, `No snapshot found at timestamp 2026-01-01`),
			want: false,
		},
		{
			name: "internal error with does not exist",
			err:  duckErr(duckdbdriver.ErrorTypeInternal, `table does not exist in catalog`),
			want: false,
		},
		{
			name: "plain error not duckdb typed",
			err:  errors.New(`Catalog Error: Table does not exist`),
			want: false,
		},
		{
			name: "wrapped duckdb error",
			err:  fmt.Errorf("query failed: %w", duckErr(duckdbdriver.ErrorTypeCatalog, `Table with name "orders" does not exist!`)),
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := isTableNotExistError(tt.err)
			if got != tt.want {
				t.Errorf("isTableNotExistError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestEscapeSQL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{"simple", "simple"},
		{"O'Brien", "O''Brien"},
		{"it's working", "it''s working"},
		{"multiple'quotes'here", "multiple''quotes''here"},
		{"no quotes", "no quotes"},
		{"", ""},
		{"'; DROP TABLE users; --", "''; DROP TABLE users; --"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			got := escapeSQL(tt.input)
			if got != tt.want {
				t.Errorf("escapeSQL(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
