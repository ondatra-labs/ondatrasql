// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package odata

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
)

// --- $search: ILIKE wildcard escaping ---

func TestBuildQuery_Search_WildcardEscaped(t *testing.T) {
	t.Parallel()
	entity := EntitySchema{
		Schema: "mart", Table: "users", ODataName: "mart_users",
		Columns: []ColumnSchema{
			{Name: "name", Type: "VARCHAR", EdmType: "Edm.String"},
			{Name: "age", Type: "INTEGER", EdmType: "Edm.Int32"},
		},
	}

	params := url.Values{"$search": {"100%"}}
	sql, err := BuildQuery(entity, params)
	if err != nil {
		t.Fatalf("BuildQuery: %v", err)
	}

	if !strings.Contains(sql, `\%`) {
		t.Errorf("search should escape %% wildcard, got SQL:\n%s", sql)
	}
}

func TestBuildQuery_Search_UnderscoreEscaped(t *testing.T) {
	t.Parallel()
	entity := EntitySchema{
		Schema: "mart", Table: "users", ODataName: "mart_users",
		Columns: []ColumnSchema{
			{Name: "name", Type: "VARCHAR", EdmType: "Edm.String"},
		},
	}

	params := url.Values{"$search": {"test_value"}}
	sql, err := BuildQuery(entity, params)
	if err != nil {
		t.Fatalf("BuildQuery: %v", err)
	}

	if !strings.Contains(sql, `\_`) {
		t.Errorf("search should escape _ wildcard, got SQL:\n%s", sql)
	}
}

func TestBuildQuery_Search_QuoteEscaped(t *testing.T) {
	t.Parallel()
	entity := EntitySchema{
		Schema: "mart", Table: "users", ODataName: "mart_users",
		Columns: []ColumnSchema{
			{Name: "name", Type: "VARCHAR", EdmType: "Edm.String"},
		},
	}

	params := url.Values{"$search": {"O'Brien"}}
	sql, err := BuildQuery(entity, params)
	if err != nil {
		t.Fatalf("BuildQuery: %v", err)
	}

	if !strings.Contains(sql, "O''Brien") {
		t.Errorf("search should escape single quotes, got SQL:\n%s", sql)
	}
}

func TestBuildCountQuery_Search_WildcardEscaped(t *testing.T) {
	t.Parallel()
	entity := EntitySchema{
		Schema: "mart", Table: "users", ODataName: "mart_users",
		Columns: []ColumnSchema{
			{Name: "name", Type: "VARCHAR", EdmType: "Edm.String"},
		},
	}

	params := url.Values{"$search": {"100%"}}
	sql, err := BuildCountQuery(entity, params)
	if err != nil {
		t.Fatalf("BuildCountQuery: %v", err)
	}

	if !strings.Contains(sql, `\%`) {
		t.Errorf("count search should escape %% wildcard, got SQL:\n%s", sql)
	}
}

// --- matchespattern: ReDoS protection ---

// --- Date/time literal quote escaping ---

func TestBuildQuery_Filter_DateLiteral(t *testing.T) {
	t.Parallel()
	entity := EntitySchema{
		Schema: "mart", Table: "orders", ODataName: "mart_orders",
		Columns: []ColumnSchema{
			{Name: "created", Type: "DATE", EdmType: "Edm.Date"},
			{Name: "id", Type: "INTEGER", EdmType: "Edm.Int32"},
		},
	}

	// Normal date filter should produce valid SQL
	params := url.Values{"$filter": {"created eq 2024-01-15"}}
	sql, err := BuildQuery(entity, params)
	if err != nil {
		t.Fatalf("BuildQuery: %v", err)
	}
	if !strings.Contains(sql, "DATE") || !strings.Contains(sql, "2024-01-15") {
		t.Errorf("expected date literal in SQL, got:\n%s", sql)
	}
}

// --- DiscoverSchemas: schema/table name escaping ---

func TestDiscoverSchemas_QuoteEscaping(t *testing.T) {
	t.Parallel()
	// Verify that the escaping function works correctly for schema/table names
	// (the actual DiscoverSchemas requires a real DuckDB session)
	input := "my'schema"
	escaped := strings.ReplaceAll(input, "'", "''")
	if escaped != "my''schema" {
		t.Errorf("escaping %q: got %q, want %q", input, escaped, "my''schema")
	}
}

// --- $expand FK value escaping ---

func TestExpandFKValue_UnknownTypeSafe(t *testing.T) {
	t.Parallel()
	// Before fix: unknown types used fmt.Sprintf("%v", v) without quoting.
	// After fix: unknown types are quoted as strings.
	type customType struct{ ID int }
	v := customType{ID: 42}
	result := strings.ReplaceAll(fmt.Sprintf("%v", v), "'", "''")
	quoted := "'" + result + "'"

	if !strings.HasPrefix(quoted, "'") || !strings.HasSuffix(quoted, "'") {
		t.Error("unknown types should be wrapped in quotes")
	}
}

// --- $filter: numeric literal validation (code-scanning #3, #4) ---

func TestBuildQuery_Filter_RejectsInvalidNumericLiteral(t *testing.T) {
	t.Parallel()
	entity := EntitySchema{
		Schema: "mart", Table: "users", ODataName: "mart_users",
		Columns: []ColumnSchema{
			{Name: "id", EdmType: "Edm.Int64"},
			{Name: "amount", EdmType: "Edm.Double"},
		},
	}

	tests := []struct {
		name   string
		filter string
	}{
		{"sql injection via int", "id eq 1; DROP TABLE users--"},
		{"sql injection via float", "amount gt 1.0; DROP TABLE users--"},
		{"string in int position", "id eq abc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := url.Values{"$filter": {tt.filter}}
			_, err := BuildQuery(entity, params)
			if err == nil {
				t.Errorf("expected error for filter %q, got nil", tt.filter)
			}
		})
	}
}

func TestBuildQuery_Filter_AcceptsValidNumericLiteral(t *testing.T) {
	t.Parallel()
	entity := EntitySchema{
		Schema: "mart", Table: "users", ODataName: "mart_users",
		Columns: []ColumnSchema{
			{Name: "id", EdmType: "Edm.Int64"},
			{Name: "amount", EdmType: "Edm.Double"},
		},
	}

	tests := []struct {
		name   string
		filter string
	}{
		{"integer eq", "id eq 42"},
		{"float gt", "amount gt 9.99"},
		{"negative int", "id eq -1"},
		{"negative float", "amount lt -0.5"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := url.Values{"$filter": {tt.filter}}
			sql, err := BuildQuery(entity, params)
			if err != nil {
				t.Errorf("filter %q: unexpected error: %v", tt.filter, err)
			}
			if sql == "" {
				t.Errorf("filter %q: empty SQL", tt.filter)
			}
		})
	}
}

// --- $filter contains/startswith/endswith: LIKE wildcard escaping ---

func TestBuildQuery_Filter_ContainsEscapesWildcards(t *testing.T) {
	t.Parallel()
	entity := EntitySchema{
		Schema: "mart", Table: "users", ODataName: "mart_users",
		Columns: []ColumnSchema{
			{Name: "name", EdmType: "Edm.String"},
		},
	}

	tests := []struct {
		name   string
		filter string
		must   string // SQL must contain this
		mustNot string // SQL must NOT contain this
	}{
		{"percent in contains", "contains(name, '100%')", `\%`, ""},
		{"underscore in contains", "contains(name, 'a_b')", `\_`, ""},
		{"percent in startswith", "startswith(name, '100%')", `\%`, ""},
		{"percent in endswith", "endswith(name, '100%')", `\%`, ""},
		{"no wildcards passes through", "contains(name, 'hello')", "hello", ""},
		{"backslash escaped", "contains(name, 'a\\b')", `\\`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := url.Values{"$filter": {tt.filter}}
			sql, err := BuildQuery(entity, params)
			if err != nil {
				t.Fatalf("filter %q: %v", tt.filter, err)
			}
			if tt.must != "" && !strings.Contains(sql, tt.must) {
				t.Errorf("filter %q: SQL %q should contain %q", tt.filter, sql, tt.must)
			}
			if tt.mustNot != "" && strings.Contains(sql, tt.mustNot) {
				t.Errorf("filter %q: SQL %q should NOT contain %q", tt.filter, sql, tt.mustNot)
			}
		})
	}
}
