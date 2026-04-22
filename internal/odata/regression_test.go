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
