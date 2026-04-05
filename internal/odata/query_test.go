// OndatraSQL - You don't need a data stack anymore
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
	// Should have Key with all columns (no explicit key set)
	if !strings.Contains(xml, "<Key>") {
		t.Error("missing Key element")
	}
	if !strings.Contains(xml, `<PropertyRef Name="order_date"`) {
		t.Error("missing PropertyRef for composite key")
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
	// staging_orders has composite key (all columns)
	if !strings.Contains(xml, `<PropertyRef Name="order_id"`) {
		t.Error("missing composite key for staging_orders")
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
