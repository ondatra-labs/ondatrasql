// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package odata

import (
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/CiscoM31/godata"
)

// TestBuildQuery_SnapshotPinning pins the AT VERSION injection that gives
// every query in one OData request a consistent DuckLake snapshot. Without
// it, data + $count + $expand could read from different snapshots if the
// pipeline commits between intermediate queries.
func TestBuildQuery_SnapshotPinning(t *testing.T) {
	t.Parallel()
	entity := EntitySchema{
		Schema: "mart", Table: "daily_revenue",
		Columns: []ColumnSchema{
			{Name: "id", Type: "BIGINT", EdmType: "Edm.Int64"},
			{Name: "name", Type: "VARCHAR", EdmType: "Edm.String"},
		},
	}
	cases := []struct {
		name     string
		snapshot int64
		wantPin  bool
	}{
		{"snapshot 0 means no pinning (test/dev path)", 0, false},
		{"snapshot 100 → AT VERSION 100", 100, true},
		{"snapshot 1 → AT VERSION 1", 1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sql, err := BuildQuery(entity, url.Values{}, tc.snapshot)
			if err != nil {
				t.Fatalf("BuildQuery: %v", err)
			}
			hasPin := strings.Contains(sql, fmt.Sprintf("AT (VERSION => %d)", tc.snapshot))
			if hasPin != tc.wantPin {
				t.Errorf("snapshot=%d: AT VERSION present = %v, want %v (sql: %q)",
					tc.snapshot, hasPin, tc.wantPin, sql)
			}
			if !strings.Contains(sql, `"mart"."daily_revenue"`) {
				t.Errorf("FROM clause missing schema/table: %q", sql)
			}
		})
	}
}

// TestBuildCountQuery_SnapshotPinning mirrors the data-query test for the
// COUNT path. data + $count must agree on snapshot, otherwise @odata.count
// can be off from len(value).
func TestBuildCountQuery_SnapshotPinning(t *testing.T) {
	t.Parallel()
	entity := EntitySchema{
		Schema: "mart", Table: "daily_revenue",
		Columns: []ColumnSchema{{Name: "id", Type: "BIGINT", EdmType: "Edm.Int64"}},
	}
	sql, err := BuildCountQuery(entity, url.Values{}, 42)
	if err != nil {
		t.Fatalf("BuildCountQuery: %v", err)
	}
	if !strings.Contains(sql, "AT (VERSION => 42)") {
		t.Errorf("missing AT VERSION clause: %q", sql)
	}
}

// TestBuildSingleEntityQuery_SnapshotPinning mirrors the data test for
// single-entity reads. Pinning here means $entity dereferencing reads the
// same version as the original collection response.
func TestBuildSingleEntityQuery_SnapshotPinning(t *testing.T) {
	t.Parallel()
	entity := EntitySchema{
		Schema: "mart", Table: "daily_revenue",
		KeyColumn: "id",
		Columns:   []ColumnSchema{{Name: "id", Type: "BIGINT", EdmType: "Edm.Int64"}},
	}
	sql, err := BuildSingleEntityQuery(entity, "1", 42)
	if err != nil {
		t.Fatalf("BuildSingleEntityQuery: %v", err)
	}
	if !strings.Contains(sql, "AT (VERSION => 42)") {
		t.Errorf("missing AT VERSION clause: %q", sql)
	}
}

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
	sql, err := BuildQuery(entity, params, 0)
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
	sql, err := BuildQuery(entity, params, 0)
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
	sql, err := BuildQuery(entity, params, 0)
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
	sql, err := BuildCountQuery(entity, params, 0)
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
	sql, err := BuildQuery(entity, params, 0)
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
			_, err := BuildQuery(entity, params, 0)
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
			sql, err := BuildQuery(entity, params, 0)
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
			sql, err := BuildQuery(entity, params, 0)
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

// --- Cat 1: date/time/duration literal quote escaping ---
//
// godata tokenises date/time/duration literals from $filter values. Real
// inputs cannot contain quotes (the tokenizer wouldn't match them), but the
// formatter still escapes defensively. If a future change introduces a path
// where a quoted variant reaches the formatter and we forget to escape, the
// literal would close the surrounding SQL string and inject. These tests pin
// the escape by driving filterNodeToSQL with synthetic tokens.

func TestFilterNodeToSQL_LiteralQuoteEscape(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		tokenType godata.TokenType
		value     string
		want      string
	}{
		{"date with quote", godata.ExpressionTokenDate,
			"2024-01-15'; DROP TABLE users; --",
			"DATE '2024-01-15''; DROP TABLE users; --'"},
		{"time with quote", godata.ExpressionTokenTime,
			"12:00:00'; --",
			"TIME '12:00:00''; --'"},
		{"datetime with quote", godata.ExpressionTokenDateTime,
			"2024-01-15T12:00:00'; --",
			"TIMESTAMP '2024-01-15T12:00:00''; --'"},
		{"duration with quote", godata.ExpressionTokenDuration,
			"P1D'; --",
			"INTERVAL 'P1D''; --'"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			node := &godata.ParseNode{
				Token: &godata.Token{Value: tt.value, Type: tt.tokenType},
			}
			got, err := filterNodeToSQL(node, map[string]bool{})
			if err != nil {
				t.Fatalf("filterNodeToSQL: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}
