// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package odata

import (
	"math/big"
	"net/url"
	"strings"
	"testing"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

var testEntity = EntitySchema{
	Target:    "mart.daily_revenue",
	ODataName: "mart_daily_revenue",
	Schema:    "mart",
	Table:     "daily_revenue",
	KeyColumn: "order_date",
	Columns: []ColumnSchema{
		{Name: "order_date", Type: "DATE", EdmType: "Edm.Date"},
		{Name: "orders", Type: "BIGINT", EdmType: "Edm.Int64"},
		{Name: "revenue", Type: "DOUBLE", EdmType: "Edm.Double"},
		{Name: "name", Type: "VARCHAR", EdmType: "Edm.String"},
		{Name: "active", Type: "BOOLEAN", EdmType: "Edm.Boolean"},
	},
}

func TestBuildQuery_NoParams(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{})
	if err != nil {
		t.Fatalf("BuildQuery: %v", err)
	}
	if !strings.Contains(sql, `SELECT * FROM "mart"."daily_revenue"`) {
		t.Errorf("sql = %q", sql)
	}
}

func TestBuildQuery_Select(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$select": {"name,revenue"}})
	if err != nil {
		t.Fatalf("BuildQuery: %v", err)
	}
	if !strings.Contains(sql, `"name"`) || !strings.Contains(sql, `"revenue"`) {
		t.Errorf("sql = %q", sql)
	}
	if strings.Contains(sql, "*") {
		t.Errorf("should not contain * with $select")
	}
}

func TestBuildQuery_SelectUnknownColumn(t *testing.T) {
	t.Parallel()
	_, err := BuildQuery(testEntity, url.Values{"$select": {"nonexistent"}})
	if err == nil {
		t.Fatal("expected error for unknown column")
	}
}

func TestBuildQuery_FilterEq(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"name eq 'Alice'"}})
	if err != nil {
		t.Fatalf("BuildQuery: %v", err)
	}
	if !strings.Contains(sql, "WHERE") || !strings.Contains(sql, "= 'Alice'") {
		t.Errorf("sql = %q", sql)
	}
}

func TestBuildQuery_FilterGt(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"revenue gt 1000"}})
	if err != nil {
		t.Fatalf("BuildQuery: %v", err)
	}
	if !strings.Contains(sql, "> 1000") {
		t.Errorf("sql = %q", sql)
	}
}

func TestBuildQuery_FilterAnd(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"revenue gt 100 and name eq 'Bob'"}})
	if err != nil {
		t.Fatalf("BuildQuery: %v", err)
	}
	if !strings.Contains(sql, "AND") {
		t.Errorf("sql = %q", sql)
	}
}

func TestBuildQuery_FilterOr(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"name eq 'Alice' or name eq 'Bob'"}})
	if err != nil {
		t.Fatalf("BuildQuery: %v", err)
	}
	if !strings.Contains(sql, "OR") {
		t.Errorf("sql = %q", sql)
	}
}

func TestBuildQuery_FilterBoolean(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"active eq true"}})
	if err != nil {
		t.Fatalf("BuildQuery: %v", err)
	}
	if !strings.Contains(sql, "TRUE") {
		t.Errorf("sql = %q", sql)
	}
}

func TestBuildQuery_FilterNull(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"name eq null"}})
	if err != nil {
		t.Fatalf("BuildQuery: %v", err)
	}
	if !strings.Contains(sql, "IS NULL") {
		t.Errorf("expected IS NULL, sql = %q", sql)
	}
}

func TestBuildQuery_FilterNotNull(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"name ne null"}})
	if err != nil {
		t.Fatalf("BuildQuery: %v", err)
	}
	if !strings.Contains(sql, "IS NOT NULL") {
		t.Errorf("expected IS NOT NULL, sql = %q", sql)
	}
}

func TestBuildQuery_FilterUnknownColumn(t *testing.T) {
	t.Parallel()
	_, err := BuildQuery(testEntity, url.Values{"$filter": {"fake gt 1"}})
	if err == nil {
		t.Fatal("expected error for unknown column")
	}
}

func TestBuildQuery_FilterSQLInjection(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"name eq 'Alice''; DROP TABLE--'"}})
	if err != nil {
		t.Fatalf("BuildQuery: %v", err)
	}
	// Escaped quotes should prevent injection
	if strings.Contains(sql, "DROP TABLE") && !strings.Contains(sql, "''") {
		t.Errorf("possible SQL injection: %q", sql)
	}
}

func TestBuildQuery_OrderByAsc(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$orderby": {"revenue asc"}})
	if err != nil {
		t.Fatalf("BuildQuery: %v", err)
	}
	if !strings.Contains(sql, `ORDER BY "revenue" ASC`) {
		t.Errorf("sql = %q", sql)
	}
}

func TestBuildQuery_OrderByDesc(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$orderby": {"revenue desc"}})
	if err != nil {
		t.Fatalf("BuildQuery: %v", err)
	}
	if !strings.Contains(sql, `ORDER BY "revenue" DESC`) {
		t.Errorf("sql = %q", sql)
	}
}

func TestBuildQuery_OrderByUnknownColumn(t *testing.T) {
	t.Parallel()
	_, err := BuildQuery(testEntity, url.Values{"$orderby": {"fake desc"}})
	if err == nil {
		t.Fatal("expected error for unknown column")
	}
}

func TestBuildQuery_Top(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$top": {"10"}})
	if err != nil {
		t.Fatalf("BuildQuery: %v", err)
	}
	if !strings.Contains(sql, "LIMIT 10") {
		t.Errorf("sql = %q", sql)
	}
}

func TestBuildQuery_Skip(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$skip": {"20"}})
	if err != nil {
		t.Fatalf("BuildQuery: %v", err)
	}
	if !strings.Contains(sql, "OFFSET 20") {
		t.Errorf("sql = %q", sql)
	}
}

func TestBuildQuery_TopAndSkip(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$top": {"5"}, "$skip": {"10"}})
	if err != nil {
		t.Fatalf("BuildQuery: %v", err)
	}
	if !strings.Contains(sql, "LIMIT 5") || !strings.Contains(sql, "OFFSET 10") {
		t.Errorf("sql = %q", sql)
	}
}

func TestBuildQuery_Combined(t *testing.T) {
	t.Parallel()
	params := url.Values{
		"$select":  {"name,revenue"},
		"$filter":  {"revenue gt 100"},
		"$orderby": {"revenue desc"},
		"$top":     {"10"},
		"$skip":    {"5"},
	}
	sql, err := BuildQuery(testEntity, params)
	if err != nil {
		t.Fatalf("BuildQuery: %v", err)
	}
	if !strings.Contains(sql, `"name"`) {
		t.Errorf("missing select: %q", sql)
	}
	if !strings.Contains(sql, "WHERE") {
		t.Errorf("missing where: %q", sql)
	}
	if !strings.Contains(sql, "ORDER BY") {
		t.Errorf("missing orderby: %q", sql)
	}
	if !strings.Contains(sql, "LIMIT 10") {
		t.Errorf("missing limit: %q", sql)
	}
	if !strings.Contains(sql, "OFFSET 5") {
		t.Errorf("missing offset: %q", sql)
	}
}

func TestBuildCountQuery(t *testing.T) {
	t.Parallel()
	sql, err := BuildCountQuery(testEntity, url.Values{})
	if err != nil {
		t.Fatalf("BuildCountQuery: %v", err)
	}
	if !strings.Contains(sql, "SELECT COUNT(*)") {
		t.Errorf("sql = %q", sql)
	}
}

func TestBuildCountQuery_WithFilter(t *testing.T) {
	t.Parallel()
	sql, err := BuildCountQuery(testEntity, url.Values{"$filter": {"revenue gt 100"}})
	if err != nil {
		t.Fatalf("BuildCountQuery: %v", err)
	}
	if !strings.Contains(sql, "COUNT(*)") || !strings.Contains(sql, "WHERE") {
		t.Errorf("sql = %q", sql)
	}
}

func TestBuildQuery_InvalidFilter(t *testing.T) {
	t.Parallel()
	_, err := BuildQuery(testEntity, url.Values{"$filter": {")))invalid"}})
	if err == nil {
		t.Fatal("expected error for invalid filter")
	}
}

func TestBuildQuery_InvalidTop(t *testing.T) {
	t.Parallel()
	_, err := BuildQuery(testEntity, url.Values{"$top": {"abc"}})
	if err == nil {
		t.Fatal("expected error for invalid top")
	}
}

func TestDuckDBToEdm(t *testing.T) {
	t.Parallel()
	tests := []struct {
		duckType string
		want     string
	}{
		{"VARCHAR", "Edm.String"},
		{"INTEGER", "Edm.Int32"},
		{"BIGINT", "Edm.Int64"},
		{"DOUBLE", "Edm.Double"},
		{"FLOAT", "Edm.Single"},
		{"BOOLEAN", "Edm.Boolean"},
		{"DATE", "Edm.Date"},
		{"TIMESTAMP", "Edm.DateTimeOffset"},
		{"DECIMAL(18,2)", "Edm.Decimal"},
		{"UNKNOWN_TYPE", "Edm.String"},
	}
	for _, tt := range tests {
		got := duckDBToEdm(tt.duckType)
		if got != tt.want {
			t.Errorf("duckDBToEdm(%q) = %q, want %q", tt.duckType, got, tt.want)
		}
	}
}

func TestGenerateMetadata(t *testing.T) {
	t.Parallel()
	schemas := []EntitySchema{testEntity}
	data, err := GenerateMetadata(schemas)
	if err != nil {
		t.Fatalf("GenerateMetadata: %v", err)
	}
	xml := string(data)
	if !strings.Contains(xml, "edmx:Edmx") {
		t.Error("missing edmx root")
	}
	if !strings.Contains(xml, "mart_daily_revenue") {
		t.Error("missing entity name (should use underscore)")
	}
	if strings.Contains(xml, `Name="mart.daily_revenue"`) {
		t.Error("entity name should not contain dots")
	}
	if !strings.Contains(xml, "Edm.Double") {
		t.Error("missing Edm.Double type")
	}
	// Should have Key with explicit key column
	if !strings.Contains(xml, "<Key>") {
		t.Error("missing Key element")
	}
	if !strings.Contains(xml, `<PropertyRef Name="order_date"`) {
		t.Error("missing PropertyRef for key column")
	}
}

func TestGenerateMetadata_WithKey(t *testing.T) {
	t.Parallel()
	schema := EntitySchema{
		Target:    "mart.orders",
		ODataName: "mart_orders",
		Schema:    "mart",
		Table:     "orders",
		KeyColumn: "id",
		Columns: []ColumnSchema{
			{Name: "id", Type: "INTEGER", EdmType: "Edm.Int32"},
			{Name: "name", Type: "VARCHAR", EdmType: "Edm.String"},
		},
	}
	data, err := GenerateMetadata([]EntitySchema{schema})
	if err != nil {
		t.Fatalf("GenerateMetadata: %v", err)
	}
	xml := string(data)
	if !strings.Contains(xml, `<PropertyRef Name="id"`) {
		t.Error("missing explicit key PropertyRef")
	}
	// Should only have one PropertyRef (not composite)
	if strings.Contains(xml, `<PropertyRef Name="name"`) {
		t.Error("should not include non-key column in Key")
	}
	// Key property must be Nullable="false"
	if !strings.Contains(xml, `Name="id" Type="Edm.Int32" Nullable="false"`) {
		t.Errorf("key property missing Nullable=false: %s", xml)
	}
	// Non-key property should not have Nullable attribute
	if strings.Contains(xml, `Name="name" Type="Edm.String" Nullable`) {
		t.Error("non-key property should not have Nullable attribute")
	}
}

func TestFormatResponse(t *testing.T) {
	t.Parallel()
	rows := []map[string]any{
		{"name": "Alice", "revenue": int64(100)},
	}
	schema := EntitySchema{
		Target: "mart.test",
		Schema: "mart",
		Table:  "test",
		Columns: []ColumnSchema{
			{Name: "name", Type: "VARCHAR", EdmType: "Edm.String"},
			{Name: "revenue", Type: "BIGINT", EdmType: "Edm.Int64"},
		},
	}
	data, err := FormatResponse("http://localhost:8090", "mart.test", rows, nil, schema)
	if err != nil {
		t.Fatalf("FormatResponse: %v", err)
	}
	s := string(data)
	if !strings.Contains(s, "@odata.context") {
		t.Error("missing @odata.context")
	}
	if !strings.Contains(s, "Alice") {
		t.Error("missing data")
	}
	if !strings.Contains(s, `"revenue":100`) {
		t.Errorf("revenue should be number: %s", s)
	}
}

func TestFormatResponse_WithCount(t *testing.T) {
	t.Parallel()
	count := 42
	data, err := FormatResponse("http://localhost:8090", "mart.test", nil, &count, testEntity)
	if err != nil {
		t.Fatalf("FormatResponse: %v", err)
	}
	if !strings.Contains(string(data), `"@odata.count":42`) {
		t.Errorf("missing count: %s", data)
	}
}

func TestFormatResponse_EmptyStringPreserved(t *testing.T) {
	t.Parallel()
	schema := EntitySchema{
		Columns: []ColumnSchema{{Name: "name", Type: "VARCHAR"}},
	}
	rows := []map[string]any{{"name": ""}}
	data, err := FormatResponse("http://localhost:8090", "test", rows, nil, schema)
	if err != nil {
		t.Fatalf("FormatResponse: %v", err)
	}
	if !strings.Contains(string(data), `"name":""`) {
		t.Errorf("empty string lost: %s", data)
	}
}

func TestFormatResponse_NullPreserved(t *testing.T) {
	t.Parallel()
	schema := EntitySchema{
		Columns: []ColumnSchema{{Name: "name", Type: "VARCHAR"}},
	}
	rows := []map[string]any{{"name": nil}}
	data, err := FormatResponse("http://localhost:8090", "test", rows, nil, schema)
	if err != nil {
		t.Fatalf("FormatResponse: %v", err)
	}
	if !strings.Contains(string(data), `"name":null`) {
		t.Errorf("NULL not converted to null: %s", data)
	}
}

func TestFormatResponse_TypedValues(t *testing.T) {
	t.Parallel()
	rows := []map[string]any{
		{"count": int32(42), "amount": float64(3.14), "flag": true, "label": "test"},
	}
	schema := EntitySchema{}
	data, err := FormatResponse("http://localhost:8090", "test", rows, nil, schema)
	if err != nil {
		t.Fatalf("FormatResponse: %v", err)
	}
	s := string(data)
	if !strings.Contains(s, `"count":42`) {
		t.Errorf("integer not typed: %s", s)
	}
	if !strings.Contains(s, `"amount":3.14`) {
		t.Errorf("double not typed: %s", s)
	}
	if !strings.Contains(s, `"flag":true`) {
		t.Errorf("boolean not typed: %s", s)
	}
	if !strings.Contains(s, `"label":"test"`) {
		t.Errorf("string not typed: %s", s)
	}
}

func TestFormatResponse_Decimal(t *testing.T) {
	t.Parallel()
	schema := EntitySchema{
		Columns: []ColumnSchema{{Name: "amount", Type: "DECIMAL(18,2)"}},
	}
	rows := []map[string]any{
		{"amount": duckdb.Decimal{Width: 18, Scale: 2, Value: big.NewInt(15099)}},
	}
	data, err := FormatResponse("http://localhost:8090", "test", rows, nil, schema)
	if err != nil {
		t.Fatalf("FormatResponse: %v", err)
	}
	// 15099 with scale 2 = 150.99
	if !strings.Contains(string(data), `"amount":150.99`) {
		t.Errorf("decimal not converted to number: %s", data)
	}
}

func TestFormatResponse_DecimalHighPrecision(t *testing.T) {
	t.Parallel()
	schema := EntitySchema{
		Columns: []ColumnSchema{{Name: "val", Type: "DECIMAL(38,10)"}},
	}
	// 12345678901234567890 with scale 10 = 1234567890.1234567890
	v := new(big.Int)
	v.SetString("12345678901234567890", 10)
	rows := []map[string]any{
		{"val": duckdb.Decimal{Width: 38, Scale: 10, Value: v}},
	}
	data, err := FormatResponse("http://localhost:8090", "test", rows, nil, schema)
	if err != nil {
		t.Fatalf("FormatResponse: %v", err)
	}
	// Must preserve exact digits, not round to float64
	// Trailing zero trimmed by json.Number: .1234567890 → .123456789
	if !strings.Contains(string(data), `1234567890.123456789`) {
		t.Errorf("decimal precision lost: %s", data)
	}
	// float64 would round this to 1234567890.1234567 (loses last 2 digits)
	if strings.Contains(string(data), `1234567890.1234567}`) {
		t.Errorf("decimal was rounded to float64: %s", data)
	}
}

func TestFormatResponse_MidnightTimestamp(t *testing.T) {
	t.Parallel()
	schema := EntitySchema{
		Columns: []ColumnSchema{
			{Name: "ts", Type: "TIMESTAMP"},
			{Name: "dt", Type: "DATE"},
		},
	}
	midnight := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
	rows := []map[string]any{
		{"ts": midnight, "dt": midnight},
	}
	data, err := FormatResponse("http://localhost:8090", "test", rows, nil, schema)
	if err != nil {
		t.Fatalf("FormatResponse: %v", err)
	}
	s := string(data)
	// TIMESTAMP at midnight must still be RFC3339
	if !strings.Contains(s, `"ts":"2026-01-03T00:00:00Z"`) {
		t.Errorf("midnight timestamp wrong: %s", s)
	}
	// DATE should be date-only
	if !strings.Contains(s, `"dt":"2026-01-03"`) {
		t.Errorf("date wrong: %s", s)
	}
}

func TestFormatServiceDocument(t *testing.T) {
	t.Parallel()
	schemas := []EntitySchema{testEntity}
	data, err := FormatServiceDocument("http://localhost:8090", schemas)
	if err != nil {
		t.Fatalf("FormatServiceDocument: %v", err)
	}
	s := string(data)
	if !strings.Contains(s, "mart_daily_revenue") {
		t.Error("missing entity set (should use underscore)")
	}
	if !strings.Contains(s, "EntitySet") {
		t.Error("missing kind")
	}
}

func TestFormatServiceDocument_MultipleEntities(t *testing.T) {
	t.Parallel()
	schemas := []EntitySchema{
		{Target: "mart.revenue", ODataName: "mart_revenue"},
		{Target: "staging.orders", ODataName: "staging_orders"},
	}
	data, err := FormatServiceDocument("http://localhost:8090", schemas)
	if err != nil {
		t.Fatalf("FormatServiceDocument: %v", err)
	}
	s := string(data)
	if !strings.Contains(s, "mart_revenue") {
		t.Error("missing mart_revenue")
	}
	if !strings.Contains(s, "staging_orders") {
		t.Error("missing staging_orders")
	}
}

func TestGenerateMetadata_MultipleEntities(t *testing.T) {
	t.Parallel()
	schemas := []EntitySchema{
		{
			Target: "mart.revenue", ODataName: "mart_revenue",
			KeyColumn: "id",
			Columns: []ColumnSchema{
				{Name: "id", Type: "INTEGER", EdmType: "Edm.Int32"},
				{Name: "amount", Type: "DOUBLE", EdmType: "Edm.Double"},
			},
		},
		{
			Target: "staging.orders", ODataName: "staging_orders",
			KeyColumn: "order_id",
			Columns: []ColumnSchema{
				{Name: "order_id", Type: "BIGINT", EdmType: "Edm.Int64"},
				{Name: "customer", Type: "VARCHAR", EdmType: "Edm.String"},
			},
		},
	}
	data, err := GenerateMetadata(schemas)
	if err != nil {
		t.Fatalf("GenerateMetadata: %v", err)
	}
	xml := string(data)
	if !strings.Contains(xml, `Name="mart_revenue"`) {
		t.Error("missing mart_revenue EntityType")
	}
	if !strings.Contains(xml, `Name="staging_orders"`) {
		t.Error("missing staging_orders EntityType")
	}
	// mart_revenue has explicit key
	if !strings.Contains(xml, `<PropertyRef Name="id"`) {
		t.Error("missing explicit key for mart_revenue")
	}
	// staging_orders has explicit key
	if !strings.Contains(xml, `<PropertyRef Name="order_id"`) {
		t.Error("missing key for staging_orders")
	}
}

func TestQuoteIdent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{"name", `"name"`},
		{`has"quote`, `"has""quote"`},
		{"normal_col", `"normal_col"`},
	}
	for _, tt := range tests {
		got := quoteIdent(tt.input)
		if got != tt.want {
			t.Errorf("quoteIdent(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// --- Phase 1: in operator + string functions ---

func TestBuildQuery_FilterIn(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"name in ('Alice','Bob')"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "IN") || !strings.Contains(sql, "'Alice'") || !strings.Contains(sql, "'Bob'") {
		t.Errorf("expected IN clause, got: %q", sql)
	}
}

func TestBuildQuery_FilterInSingle(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"name in ('Alice')"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "IN") || !strings.Contains(sql, "'Alice'") {
		t.Errorf("expected IN clause, got: %q", sql)
	}
}

func TestBuildQuery_FilterInUnknownCol(t *testing.T) {
	t.Parallel()
	_, err := BuildQuery(testEntity, url.Values{"$filter": {"fake in ('x')"}})
	if err == nil {
		t.Fatal("expected error for unknown column in 'in'")
	}
}

func TestBuildQuery_FilterContains(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"contains(name,'Ali')"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "ILIKE") {
		t.Errorf("expected ILIKE for contains, got: %q", sql)
	}
}

func TestBuildQuery_FilterStartsWith(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"startswith(name,'Ali')"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "LIKE") {
		t.Errorf("expected LIKE for startswith, got: %q", sql)
	}
}

func TestBuildQuery_FilterEndsWith(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"endswith(name,'ce')"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "LIKE") && !strings.Contains(sql, "'ce'") {
		t.Errorf("expected LIKE for endswith, got: %q", sql)
	}
}

func TestBuildQuery_FilterContainsAndEq(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"contains(name,'A') and revenue gt 100"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "ILIKE") || !strings.Contains(sql, "AND") || !strings.Contains(sql, "> 100") {
		t.Errorf("expected compound filter, got: %q", sql)
	}
}

func TestBuildQuery_FilterFuncUnknownCol(t *testing.T) {
	t.Parallel()
	_, err := BuildQuery(testEntity, url.Values{"$filter": {"contains(fake,'x')"}})
	if err == nil {
		t.Fatal("expected error for unknown column in function")
	}
}

func TestBuildQuery_FilterUnsupportedFunc(t *testing.T) {
	t.Parallel()
	_, err := BuildQuery(testEntity, url.Values{"$filter": {"substringof('x',name)"}})
	if err == nil {
		t.Fatal("expected error for unsupported function")
	}
}

// --- Phase 2: remaining functions + arithmetic ---

func TestBuildQuery_FilterToLower(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"tolower(name) eq 'alice'"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "LOWER") {
		t.Errorf("expected LOWER, got: %q", sql)
	}
}

func TestBuildQuery_FilterToUpper(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"toupper(name) eq 'ALICE'"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "UPPER") {
		t.Errorf("expected UPPER, got: %q", sql)
	}
}

func TestBuildQuery_FilterLength(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"length(name) gt 3"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "LENGTH") {
		t.Errorf("expected LENGTH, got: %q", sql)
	}
}

func TestBuildQuery_FilterTrim(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"trim(name) eq 'Alice'"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "TRIM") {
		t.Errorf("expected TRIM, got: %q", sql)
	}
}

func TestBuildQuery_FilterConcat(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"concat(name,name) eq 'AliceAlice'"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "CONCAT") {
		t.Errorf("expected CONCAT, got: %q", sql)
	}
}

func TestBuildQuery_FilterIndexOf(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"indexof(name,'li') eq 1"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "STRPOS") && !strings.Contains(sql, "- 1") {
		t.Errorf("expected STRPOS - 1, got: %q", sql)
	}
}

func TestBuildQuery_FilterSubstring2(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"substring(name,1) eq 'lice'"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "SUBSTR") {
		t.Errorf("expected SUBSTR, got: %q", sql)
	}
}

func TestBuildQuery_FilterSubstring3(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"substring(name,1,3) eq 'lic'"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "SUBSTR") {
		t.Errorf("expected SUBSTR, got: %q", sql)
	}
}

func TestBuildQuery_FilterYear(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"year(order_date) eq 2026"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "EXTRACT") || !strings.Contains(sql, "YEAR") {
		t.Errorf("expected EXTRACT(YEAR), got: %q", sql)
	}
}

func TestBuildQuery_FilterMonth(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"month(order_date) eq 1"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "EXTRACT") || !strings.Contains(sql, "MONTH") {
		t.Errorf("expected EXTRACT(MONTH), got: %q", sql)
	}
}

func TestBuildQuery_FilterRound(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"round(revenue) eq 100"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "ROUND") {
		t.Errorf("expected ROUND, got: %q", sql)
	}
}

func TestBuildQuery_FilterFloor(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"floor(revenue) eq 99"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "FLOOR") {
		t.Errorf("expected FLOOR, got: %q", sql)
	}
}

func TestBuildQuery_FilterCeiling(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"ceiling(revenue) eq 100"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "CEIL") {
		t.Errorf("expected CEIL, got: %q", sql)
	}
}

func TestBuildQuery_FilterArithAdd(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"revenue add 10 gt 100"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "+") {
		t.Errorf("expected + operator, got: %q", sql)
	}
}

func TestBuildQuery_FilterArithSub(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"revenue sub 10 gt 100"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "-") {
		t.Errorf("expected - operator, got: %q", sql)
	}
}

func TestBuildQuery_FilterArithMul(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"revenue mul 2 gt 100"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "*") {
		t.Errorf("expected * operator, got: %q", sql)
	}
}

func TestBuildQuery_FilterArithMod(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"revenue mod 10 eq 0"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "%") {
		t.Errorf("expected %% operator, got: %q", sql)
	}
}

func TestBuildQuery_FilterNestedFunc(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"contains(tolower(name),'alice')"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "LOWER") || !strings.Contains(sql, "ILIKE") {
		t.Errorf("expected nested LOWER + ILIKE, got: %q", sql)
	}
}

func TestBuildQuery_FilterNow(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$filter": {"order_date lt now()"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "NOW()") {
		t.Errorf("expected NOW(), got: %q", sql)
	}
}

// --- Phase 3: single entity by key ---

func TestBuildSingleEntityQuery_Numeric(t *testing.T) {
	t.Parallel()
	entity := EntitySchema{
		Schema: "raw", Table: "orders", KeyColumn: "id",
		Columns: []ColumnSchema{{Name: "id", Type: "INTEGER", EdmType: "Edm.Int32"}, {Name: "name", Type: "VARCHAR", EdmType: "Edm.String"}},
	}
	sql, err := BuildSingleEntityQuery(entity, "42")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "42") || !strings.Contains(sql, "LIMIT 1") || !strings.Contains(sql, `"id"`) {
		t.Errorf("unexpected sql: %q", sql)
	}
}

func TestBuildSingleEntityQuery_String(t *testing.T) {
	t.Parallel()
	entity := EntitySchema{
		Schema: "raw", Table: "orders", KeyColumn: "name",
		Columns: []ColumnSchema{{Name: "name", Type: "VARCHAR", EdmType: "Edm.String"}},
	}
	sql, err := BuildSingleEntityQuery(entity, "'Alice'")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "'Alice'") || !strings.Contains(sql, "LIMIT 1") {
		t.Errorf("unexpected sql: %q", sql)
	}
}

func TestBuildSingleEntityQuery_InvalidNumeric(t *testing.T) {
	t.Parallel()
	entity := EntitySchema{
		Schema: "raw", Table: "orders", KeyColumn: "id",
		Columns: []ColumnSchema{{Name: "id", Type: "INTEGER", EdmType: "Edm.Int32"}},
	}
	_, err := BuildSingleEntityQuery(entity, "abc")
	if err == nil {
		t.Fatal("expected error for non-numeric key")
	}
}

func TestBuildSingleEntityQuery_SQLInjection(t *testing.T) {
	t.Parallel()
	entity := EntitySchema{
		Schema: "raw", Table: "orders", KeyColumn: "name",
		Columns: []ColumnSchema{{Name: "name", Type: "VARCHAR", EdmType: "Edm.String"}},
	}
	sql, err := BuildSingleEntityQuery(entity, "'Alice'; DROP TABLE--'")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(sql, "DROP TABLE") && !strings.Contains(sql, "''") {
		t.Error("SQL injection not escaped")
	}
}

// --- Additional Phase 4 features ---

func TestBuildQuery_Search(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$search": {"alice"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "ILIKE") || !strings.Contains(sql, "alice") {
		t.Errorf("expected search across string columns, got: %q", sql)
	}
}

func TestBuildQuery_Apply_Aggregate(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$apply": {"aggregate(revenue with sum as total)"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "SUM") || !strings.Contains(sql, "total") {
		t.Errorf("expected SUM aggregate, got: %q", sql)
	}
}

func TestBuildQuery_Apply_GroupBy(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$apply": {"groupby((name),aggregate(revenue with sum as total))"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "GROUP BY") || !strings.Contains(sql, "SUM") {
		t.Errorf("expected GROUP BY + SUM, got: %q", sql)
	}
}

func TestBuildQuery_Apply_Count(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{"$apply": {"aggregate($count as total)"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "COUNT(*)") {
		t.Errorf("expected COUNT(*), got: %q", sql)
	}
}

func TestBuildQuery_Apply_InvalidCol(t *testing.T) {
	t.Parallel()
	_, err := BuildQuery(testEntity, url.Values{"$apply": {"aggregate(fake with sum as total)"}})
	if err == nil {
		t.Fatal("expected error for unknown column in aggregate")
	}
}

func TestBuildQuery_Apply_OrderByAlias(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{
		"$apply":   {"aggregate(revenue with sum as total)"},
		"$orderby": {"total desc"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "ORDER BY") || !strings.Contains(sql, "total") {
		t.Errorf("expected ORDER BY total, got: %q", sql)
	}
}

func TestBuildQuery_Apply_OrderByUnknownAlias(t *testing.T) {
	t.Parallel()
	_, err := BuildQuery(testEntity, url.Values{
		"$apply":   {"aggregate(revenue with sum as total)"},
		"$orderby": {"fake desc"},
	})
	if err == nil {
		t.Fatal("expected error for unknown alias in $orderby after $apply")
	}
	if !strings.Contains(err.Error(), "unknown column") {
		t.Errorf("expected 'unknown column' error, got: %v", err)
	}
}

func TestBuildQuery_Apply_OrderByInvalidDirection(t *testing.T) {
	t.Parallel()
	_, err := BuildQuery(testEntity, url.Values{
		"$apply":   {"aggregate(revenue with sum as total)"},
		"$orderby": {"total sideways"},
	})
	if err == nil {
		t.Fatal("expected error for invalid direction after $apply")
	}
}

func TestBuildQuery_Apply_EmptyAggregate(t *testing.T) {
	t.Parallel()
	_, err := BuildQuery(testEntity, url.Values{"$apply": {"aggregate()"}})
	if err == nil {
		t.Fatal("expected error for empty aggregate")
	}
}

func TestBuildQuery_Apply_EmptyGroupBy(t *testing.T) {
	t.Parallel()
	_, err := BuildQuery(testEntity, url.Values{"$apply": {"groupby(())"}})
	if err == nil {
		t.Fatal("expected error for empty groupby")
	}
}

func TestBuildQuery_Apply_MalformedAggregate(t *testing.T) {
	t.Parallel()
	_, err := BuildQuery(testEntity, url.Values{"$apply": {"aggregate("}})
	if err == nil {
		t.Fatal("expected error for malformed aggregate")
	}
}

func TestBuildQuery_Apply_UnknownTransformation(t *testing.T) {
	t.Parallel()
	_, err := BuildQuery(testEntity, url.Values{"$apply": {"groupby((name),bogus(revenue))"}})
	if err == nil {
		t.Fatal("expected error for unknown transformation after groupby")
	}
}

func TestBuildQuery_Compute_WithSelect(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{
		"$compute": {"revenue mul 2 as doubled"},
		"$select":  {"name,doubled"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, `"doubled"`) || !strings.Contains(sql, `"name"`) {
		t.Errorf("expected name and doubled in select, got: %q", sql)
	}
}

func TestBuildQuery_Compute_WithOrderBy(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{
		"$compute": {"revenue mul 2 as doubled"},
		"$orderby": {"doubled desc"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "ORDER BY") || !strings.Contains(sql, `"doubled"`) {
		t.Errorf("expected ORDER BY doubled, got: %q", sql)
	}
}

func TestBuildQuery_Compute_UnknownAlias(t *testing.T) {
	t.Parallel()
	_, err := BuildQuery(testEntity, url.Values{
		"$compute": {"revenue mul 2 as doubled"},
		"$select":  {"fake"},
	})
	if err == nil {
		t.Fatal("expected error for unknown column in $select")
	}
}

func TestBuildQuery_Apply_AggregateAliasCollision(t *testing.T) {
	t.Parallel()
	_, err := BuildQuery(testEntity, url.Values{
		"$apply": {"groupby((name),aggregate(revenue with sum as name))"},
	})
	if err == nil {
		t.Fatal("expected error for aggregate alias colliding with groupby column")
	}
}

func TestBuildQuery_Apply_DuplicateAggregateAlias(t *testing.T) {
	t.Parallel()
	_, err := BuildQuery(testEntity, url.Values{
		"$apply": {"aggregate(revenue with sum as total,revenue with avg as total)"},
	})
	if err == nil {
		t.Fatal("expected error for duplicate aggregate alias")
	}
}

func TestBuildQuery_Compute_FilterAndSelect(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{
		"$compute": {"revenue mul 2 as doubled"},
		"$filter":  {"doubled gt 100"},
		"$select":  {"name"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should be subquery with doubled available for filter, outer selects name only
	if !strings.Contains(sql, "_compute") {
		t.Errorf("expected subquery pattern, got: %q", sql)
	}
	if !strings.Contains(sql, `"doubled"`) {
		t.Errorf("expected doubled in subquery, got: %q", sql)
	}
}

func TestBuildQuery_Compute_MultipleAliases(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{
		"$compute": {"revenue mul 2 as doubled,revenue add 10 as plus_ten"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "doubled") || !strings.Contains(sql, "plus_ten") {
		t.Errorf("expected both aliases, got: %q", sql)
	}
}

func TestBuildQuery_Compute_FilterOnly(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{
		"$compute": {"revenue mul 2 as doubled"},
		"$filter":  {"doubled gt 500"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "doubled") && !strings.Contains(sql, "500") {
		t.Errorf("expected filter on doubled, got: %q", sql)
	}
}

func TestBuildQuery_Compute_OrderByOnly(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{
		"$compute": {"revenue mul 2 as doubled"},
		"$orderby": {"doubled desc"},
		"$select":  {"name"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "ORDER BY") {
		t.Errorf("expected ORDER BY, got: %q", sql)
	}
}

func TestBuildQuery_Compute_AliasCollision(t *testing.T) {
	t.Parallel()
	_, err := BuildQuery(testEntity, url.Values{"$compute": {"revenue mul 2 as name"}})
	if err == nil {
		t.Fatal("expected error for alias colliding with existing column")
	}
	if !strings.Contains(err.Error(), "conflicts") {
		t.Errorf("expected 'conflicts' error, got: %v", err)
	}
}

func TestBuildQuery_Apply_WithFilter(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{
		"$filter": {"name eq 'Alice'"},
		"$apply":  {"aggregate(revenue with sum as total)"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "WHERE") || !strings.Contains(sql, "Alice") {
		t.Errorf("expected WHERE clause in apply query, got: %q", sql)
	}
}

func TestBuildQuery_Apply_WithSelect(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{
		"$apply":  {"aggregate(revenue with sum as total)"},
		"$select": {"total"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, `"total"`) && !strings.Contains(sql, "total") {
		t.Errorf("expected total in select, got: %q", sql)
	}
}

func TestBuildQuery_Apply_WithSelectUnknown(t *testing.T) {
	t.Parallel()
	_, err := BuildQuery(testEntity, url.Values{
		"$apply":  {"aggregate(revenue with sum as total)"},
		"$select": {"fake"},
	})
	if err == nil {
		t.Fatal("expected error for unknown column in $select after $apply")
	}
}

func TestBuildQuery_Apply_WithTopSkip(t *testing.T) {
	t.Parallel()
	sql, err := BuildQuery(testEntity, url.Values{
		"$apply": {"groupby((name),aggregate(revenue with sum as total))"},
		"$top":   {"2"},
		"$skip":  {"1"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "LIMIT 2") || !strings.Contains(sql, "OFFSET 1") {
		t.Errorf("expected LIMIT/OFFSET, got: %q", sql)
	}
}

func TestBuildSingleEntityQuery_EscapedQuote(t *testing.T) {
	t.Parallel()
	entity := EntitySchema{
		Schema: "raw", Table: "orders", KeyColumn: "name",
		Columns: []ColumnSchema{{Name: "name", Type: "VARCHAR", EdmType: "Edm.String"}},
	}
	sql, err := BuildSingleEntityQuery(entity, "'O''Reilly'")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should produce WHERE "name" = 'O''Reilly' (single quote in SQL)
	if !strings.Contains(sql, "O''Reilly") {
		t.Errorf("expected properly escaped quote, got: %q", sql)
	}
	// Should NOT contain O''''Reilly (double-escaped)
	if strings.Contains(sql, "''''") {
		t.Errorf("double-escaped quote detected: %q", sql)
	}
}

func TestBuildSingleEntityQuery_Empty(t *testing.T) {
	t.Parallel()
	entity := EntitySchema{
		Schema: "raw", Table: "orders", KeyColumn: "id",
		Columns: []ColumnSchema{{Name: "id", Type: "INTEGER", EdmType: "Edm.Int32"}},
	}
	_, err := BuildSingleEntityQuery(entity, "")
	if err == nil {
		t.Fatal("expected error for empty key")
	}
}
