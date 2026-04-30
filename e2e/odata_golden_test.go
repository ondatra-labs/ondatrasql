// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/odata"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// odataSetup creates a project with two @expose models and starts an OData server.
// Returns the server URL and a cleanup function.
func odataSetup(t *testing.T) (string, *testutil.Project) {
	t.Helper()
	p := testutil.NewProject(t)

	// Model 1: revenue — all common types, explicit key
	p.AddModel("mart/revenue.sql", `-- @kind: table
-- @expose order_id
SELECT * FROM (VALUES
    (1,  'Alice',   150.00,   '2026-01-01'::DATE, TIMESTAMP '2026-01-01 08:30:00', true,  CAST(100 AS BIGINT),  CAST(3.14 AS FLOAT), CAST(2.71 AS DOUBLE)),
    (2,  'Bob',     250.50,   '2026-01-02'::DATE, TIMESTAMP '2026-01-02 14:15:30', false, CAST(200 AS BIGINT),  CAST(1.5 AS FLOAT),  CAST(9.99 AS DOUBLE)),
    (3,  'Carol',   0.00,     '2026-01-03'::DATE, TIMESTAMP '2026-01-03 00:00:00', true,  CAST(0 AS BIGINT),    CAST(0.0 AS FLOAT),  CAST(0.0 AS DOUBLE)),
    (4,  'Dave',    999.99,   NULL::DATE,          NULL::TIMESTAMP,                 true,  NULL::BIGINT,         NULL::FLOAT,         NULL::DOUBLE),
    (5,  '',        50.00,    '2026-01-05'::DATE, TIMESTAMP '2026-01-05 23:59:59', false, CAST(-1 AS BIGINT),   CAST(-0.5 AS FLOAT), CAST(-100.123 AS DOUBLE)),
    (6,  'Frank',   -75.25,   '2026-01-06'::DATE, TIMESTAMP '2026-01-06 12:00:00', NULL::BOOLEAN, CAST(9999999999 AS BIGINT), CAST(3.4e38 AS FLOAT), CAST(1.7e308 AS DOUBLE))
) AS t(order_id, customer, amount, order_date, created_at, active, big_num, float_val, double_val)
`)

	// Model 2: customers — explicit key, NULL vs empty string
	p.AddModel("mart/customers.sql", `-- @kind: table
-- @expose id
SELECT * FROM (VALUES
    (1, 'Alice', 'Premium', 42),
    (2, 'Bob',   NULL::VARCHAR, NULL::INTEGER),
    (3, 'Carol', '', 0),
    (4, 'Dave',  'Basic', -1)
) AS t(id, name, tier, score)
`)

	// Model 3: not exposed
	p.AddModel("staging/internal.sql", `-- @kind: table
SELECT 1 AS secret_id, 'hidden' AS data
`)

	runModel(t, p, "mart/revenue.sql")
	runModel(t, p, "mart/customers.sql")
	runModel(t, p, "staging/internal.sql")

	schemas, err := odata.DiscoverSchemas(p.Sess, []odata.ExposeTarget{
		{Target: "mart.revenue", KeyColumn: "order_id"},
		{Target: "mart.customers", KeyColumn: "id"},
	})
	if err != nil {
		t.Fatalf("DiscoverSchemas: %v", err)
	}

	handler, err := odata.NewServer(p.Sess, schemas, "http://testhost")
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv := newTestServer(t, handler)
	t.Cleanup(func() { srv.Close() })

	return srv.URL, p
}

// odataGet fetches a URL and returns the body.
func odataGet(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body)
}

// odataGetStatus fetches a URL and returns the status code.
func odataGetStatus(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

// odataGetHeader fetches a URL and returns a specific header value.
func odataGetHeader(t *testing.T, url, header string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	resp.Body.Close()
	return resp.Header.Get(header)
}

// formatJSON pretty-prints JSON with sorted keys for deterministic golden output.
func formatJSON(raw string) string {
	var obj map[string]any
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return raw
	}
	data, _ := json.MarshalIndent(obj, "", "  ")
	return string(data)
}

// formatJSONRows extracts and formats the "value" array from an OData response,
// sorting map keys for deterministic output.
func formatJSONRows(raw string) string {
	var resp odata.ODataResponse
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		return raw
	}
	var lines []string
	for i, row := range resp.Value {
		// Sort keys
		keys := make([]string, 0, len(row))
		for k := range row {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var parts []string
		for _, k := range keys {
			v := row[k]
			data, _ := json.Marshal(v)
			parts = append(parts, fmt.Sprintf("%s=%s", k, string(data)))
		}
		lines = append(lines, fmt.Sprintf("  [%d] %s", i, strings.Join(parts, ", ")))
	}
	return strings.Join(lines, "\n")
}

// --- Golden Tests ---

func TestE2E_OData_ServiceDocument(t *testing.T) {
	url, _ := odataSetup(t)
	snap := newSnapshot()

	body := odataGet(t, url+"/odata")
	var doc odata.ServiceDocument
	json.Unmarshal([]byte(body), &doc)

	snap.addLine(fmt.Sprintf("entity_count: %d", len(doc.Value)))
	for _, e := range doc.Value {
		snap.addLine(fmt.Sprintf("entity: %s (kind=%s, url=%s)", e.Name, e.Kind, e.URL))
	}

	// Verify non-exposed model is absent
	if strings.Contains(body, "staging") {
		snap.addLine("ERROR: staging_internal exposed")
	} else {
		snap.addLine("staging_internal: not exposed")
	}

	// Headers
	snap.addLine(fmt.Sprintf("odata-version: %s", odataGetHeader(t, url+"/odata", "OData-Version")))
	snap.addLine(fmt.Sprintf("content-type: %s", odataGetHeader(t, url+"/odata", "Content-Type")))

	assertGolden(t, "odata_service_document", snap)
}

func TestE2E_OData_Metadata(t *testing.T) {
	url, _ := odataSetup(t)
	snap := newSnapshot()

	body := odataGet(t, url+"/odata/$metadata")

	// Check key structural elements
	checks := []struct{ label, expect string }{
		{"edmx_root", "edmx:Edmx"},
		{"version_4.0", `Version="4.0"`},
		{"namespace", `Namespace="ondatra"`},
		{"entity_mart_customers", `Name="mart_customers"`},
		{"entity_mart_revenue", `Name="mart_revenue"`},
		{"key_element", "<Key>"},
		{"explicit_key_order_id", `<PropertyRef Name="order_id"`},
		{"composite_key_id", `<PropertyRef Name="id"`},
		{"edm_decimal", `Name="amount" Type="Edm.Decimal"`},
		{"edm_date", `Name="order_date" Type="Edm.Date"`},
		{"edm_datetimeoffset", `Name="created_at" Type="Edm.DateTimeOffset"`},
		{"edm_boolean", `Name="active" Type="Edm.Boolean"`},
		{"edm_int64", `Name="big_num" Type="Edm.Int64"`},
		{"edm_single", `Name="float_val" Type="Edm.Single"`},
		{"edm_double", `Name="double_val" Type="Edm.Double"`},
		{"edm_int32", `Name="order_id" Type="Edm.Int32"`},
		{"edm_string", `Name="customer" Type="Edm.String"`},
		{"entity_container", `Name="Default"`},
		{"entity_set_revenue", `EntityType="ondatra.mart_revenue"`},
	}
	for _, c := range checks {
		if strings.Contains(body, c.expect) {
			snap.addLine(fmt.Sprintf("%s: ok", c.label))
		} else {
			snap.addLine(fmt.Sprintf("%s: MISSING (%s)", c.label, c.expect))
		}
	}

	snap.addLine(fmt.Sprintf("content-type: %s", odataGetHeader(t, url+"/odata/$metadata", "Content-Type")))
	snap.addLine(fmt.Sprintf("odata-version: %s", odataGetHeader(t, url+"/odata/$metadata", "OData-Version")))

	assertGolden(t, "odata_metadata", snap)
}

func TestE2E_OData_DataTypes(t *testing.T) {
	url, _ := odataSetup(t)
	snap := newSnapshot()

	// All rows — check every type
	body := odataGet(t, url+"/odata/mart_revenue")
	snap.addLine("--- all rows ---")
	snap.addLine(formatJSONRows(body))

	assertGolden(t, "odata_data_types", snap)
}

func TestE2E_OData_NullHandling(t *testing.T) {
	url, _ := odataSetup(t)
	snap := newSnapshot()

	// Row 4 (Dave) has NULL date, timestamp, bigint, float, double
	body := odataGet(t, url+"/odata/mart_revenue?$filter=order_id%20eq%204")
	snap.addLine("--- row with NULLs (Dave) ---")
	snap.addLine(formatJSONRows(body))

	// Row 6 (Frank) has NULL boolean
	body = odataGet(t, url+"/odata/mart_revenue?$filter=order_id%20eq%206")
	snap.addLine("--- row with NULL boolean (Frank) ---")
	snap.addLine(formatJSONRows(body))

	// Customers: Bob has NULL tier and score
	body = odataGet(t, url+"/odata/mart_customers?$filter=id%20eq%202")
	snap.addLine("--- NULL varchar and integer (Bob) ---")
	snap.addLine(formatJSONRows(body))

	assertGolden(t, "odata_null_handling", snap)
}

func TestE2E_OData_EmptyStringVsNull(t *testing.T) {
	url, _ := odataSetup(t)
	snap := newSnapshot()

	// Carol: tier = '' (empty string), score = 0
	body := odataGet(t, url+"/odata/mart_customers?$filter=id%20eq%203&$select=name,tier,score")
	snap.addLine("--- empty string (Carol) ---")
	snap.addLine(formatJSONRows(body))

	// Bob: tier = null, score = null
	body = odataGet(t, url+"/odata/mart_customers?$filter=id%20eq%202&$select=name,tier,score")
	snap.addLine("--- null (Bob) ---")
	snap.addLine(formatJSONRows(body))

	// Row 5: customer = '' (empty string in revenue)
	body = odataGet(t, url+"/odata/mart_revenue?$filter=order_id%20eq%205&$select=order_id,customer")
	snap.addLine("--- empty string customer ---")
	snap.addLine(formatJSONRows(body))

	assertGolden(t, "odata_empty_vs_null", snap)
}

func TestE2E_OData_Select(t *testing.T) {
	url, _ := odataSetup(t)
	snap := newSnapshot()

	body := odataGet(t, url+"/odata/mart_revenue?$select=customer,amount&$top=3")
	snap.addLine("--- $select=customer,amount&$top=3 ---")
	snap.addLine(formatJSONRows(body))

	body = odataGet(t, url+"/odata/mart_revenue?$select=order_id&$top=2")
	snap.addLine("--- $select=order_id&$top=2 ---")
	snap.addLine(formatJSONRows(body))

	assertGolden(t, "odata_select", snap)
}

func TestE2E_OData_Filter(t *testing.T) {
	url, _ := odataSetup(t)
	snap := newSnapshot()

	queries := []struct{ label, qs string }{
		{"eq_string", "$filter=customer%20eq%20'Alice'&$select=order_id,customer"},
		{"gt_number", "$filter=amount%20gt%20100&$select=order_id,amount"},
		{"lt_number", "$filter=amount%20lt%200&$select=order_id,amount"},
		{"le_number", "$filter=amount%20le%200&$select=order_id,amount"},
		{"ne_string", "$filter=customer%20ne%20'Alice'&$select=order_id,customer"},
		{"eq_bool_true", "$filter=active%20eq%20true&$select=order_id,active"},
		{"eq_bool_false", "$filter=active%20eq%20false&$select=order_id,active"},
		{"eq_null", "$filter=order_date%20eq%20null&$select=order_id,order_date"},
		{"ne_null", "$filter=order_date%20ne%20null&$select=order_id,order_date"},
		{"and", "$filter=amount%20gt%20100%20and%20active%20eq%20true&$select=order_id"},
		{"or", "$filter=customer%20eq%20'Alice'%20or%20customer%20eq%20'Bob'&$select=order_id,customer"},
		{"no_match", "$filter=order_id%20eq%20999&$select=order_id"},
	}

	for _, q := range queries {
		body := odataGet(t, url+"/odata/mart_revenue?"+q.qs)
		var resp odata.ODataResponse
		json.Unmarshal([]byte(body), &resp)
		snap.addLine(fmt.Sprintf("--- %s → %d rows ---", q.label, len(resp.Value)))
		snap.addLine(formatJSONRows(body))
	}

	assertGolden(t, "odata_filter", snap)
}

func TestE2E_OData_OrderBy(t *testing.T) {
	url, _ := odataSetup(t)
	snap := newSnapshot()

	body := odataGet(t, url+"/odata/mart_revenue?$orderby=amount%20asc&$select=order_id,customer,amount")
	snap.addLine("--- orderby amount asc ---")
	snap.addLine(formatJSONRows(body))

	body = odataGet(t, url+"/odata/mart_revenue?$orderby=amount%20desc&$top=3&$select=order_id,customer,amount")
	snap.addLine("--- orderby amount desc, top 3 ---")
	snap.addLine(formatJSONRows(body))

	assertGolden(t, "odata_orderby", snap)
}

func TestE2E_OData_TopSkip(t *testing.T) {
	url, _ := odataSetup(t)
	snap := newSnapshot()

	queries := []struct{ label, qs string }{
		{"top_3", "$top=3&$select=order_id"},
		{"top_0", "$top=0&$select=order_id"},
		{"top_exceeds", "$top=100&$select=order_id"},
		{"skip_4", "$skip=4&$select=order_id"},
		{"skip_all", "$skip=100&$select=order_id"},
		{"top2_skip1", "$top=2&$skip=1&$select=order_id"},
	}

	for _, q := range queries {
		body := odataGet(t, url+"/odata/mart_revenue?"+q.qs)
		var resp odata.ODataResponse
		json.Unmarshal([]byte(body), &resp)
		snap.addLine(fmt.Sprintf("--- %s → %d rows ---", q.label, len(resp.Value)))
		snap.addLine(formatJSONRows(body))
	}

	assertGolden(t, "odata_top_skip", snap)
}

func TestE2E_OData_Count(t *testing.T) {
	url, _ := odataSetup(t)
	snap := newSnapshot()

	// Inline $count=true
	body := odataGet(t, url+"/odata/mart_revenue?$count=true&$top=1&$select=order_id")
	var resp odata.ODataResponse
	json.Unmarshal([]byte(body), &resp)
	countVal := 0
	if resp.Count != nil {
		countVal = *resp.Count
	}
	snap.addLine(fmt.Sprintf("inline_count: %d", countVal))
	snap.addLine(fmt.Sprintf("rows_returned: %d", len(resp.Value)))

	// Inline $count with filter
	body = odataGet(t, url+"/odata/mart_revenue?$count=true&$filter=active%20eq%20true")
	json.Unmarshal([]byte(body), &resp)
	countVal = 0
	if resp.Count != nil {
		countVal = *resp.Count
	}
	snap.addLine(fmt.Sprintf("inline_count_filtered: %d", countVal))

	// /$count endpoint
	body = odataGet(t, url+"/odata/mart_revenue/$count")
	snap.addLine(fmt.Sprintf("path_count: %s", strings.TrimSpace(body)))

	// /$count with filter
	body = odataGet(t, url+"/odata/mart_revenue/$count?$filter=amount%20gt%20100")
	snap.addLine(fmt.Sprintf("path_count_filtered: %s", strings.TrimSpace(body)))

	// /$count content-type
	snap.addLine(fmt.Sprintf("path_count_content_type: %s", odataGetHeader(t, url+"/odata/mart_revenue/$count", "Content-Type")))

	assertGolden(t, "odata_count", snap)
}

func TestE2E_OData_Combined(t *testing.T) {
	url, _ := odataSetup(t)
	snap := newSnapshot()

	body := odataGet(t, url+"/odata/mart_revenue?$filter=active%20eq%20true&$select=order_id,customer,amount&$orderby=amount%20desc&$top=2&$skip=1&$count=true")
	var resp odata.ODataResponse
	json.Unmarshal([]byte(body), &resp)
	countVal := 0
	if resp.Count != nil {
		countVal = *resp.Count
	}
	snap.addLine(fmt.Sprintf("total_count: %d", countVal))
	snap.addLine(fmt.Sprintf("rows_returned: %d", len(resp.Value)))
	snap.addLine(formatJSONRows(body))

	assertGolden(t, "odata_combined", snap)
}

func TestE2E_OData_Errors(t *testing.T) {
	url, _ := odataSetup(t)
	snap := newSnapshot()

	// Unknown entity
	snap.addLine(fmt.Sprintf("unknown_entity: %d", odataGetStatus(t, url+"/odata/nonexistent")))

	// Invalid filter
	snap.addLine(fmt.Sprintf("invalid_filter: %d", odataGetStatus(t, url+"/odata/mart_revenue?$filter=)))bad")))

	// Unknown column in filter
	snap.addLine(fmt.Sprintf("unknown_filter_col: %d", odataGetStatus(t, url+"/odata/mart_revenue?$filter=fake%20eq%201")))

	// Unknown column in select
	snap.addLine(fmt.Sprintf("unknown_select_col: %d", odataGetStatus(t, url+"/odata/mart_revenue?$select=fake")))

	// Unknown column in orderby
	snap.addLine(fmt.Sprintf("unknown_orderby_col: %d", odataGetStatus(t, url+"/odata/mart_revenue?$orderby=fake")))

	// Invalid top
	snap.addLine(fmt.Sprintf("invalid_top: %d", odataGetStatus(t, url+"/odata/mart_revenue?$top=abc")))

	// Entity by key → 501
	snap.addLine(fmt.Sprintf("entity_by_key: %d", odataGetStatus(t, url+"/odata/mart_revenue(1)")))

	// Error response format
	body := odataGet(t, url+"/odata/nonexistent")
	var errResp map[string]any
	json.Unmarshal([]byte(body), &errResp)
	if e, ok := errResp["error"].(map[string]any); ok {
		snap.addLine(fmt.Sprintf("error_code: %s", e["code"]))
		snap.addLine("error_has_message: true")
	}

	// Error content-type is JSON
	snap.addLine(fmt.Sprintf("error_content_type: %s", odataGetHeader(t, url+"/odata/nonexistent", "Content-Type")))
	snap.addLine(fmt.Sprintf("error_odata_version: %s", odataGetHeader(t, url+"/odata/nonexistent", "OData-Version")))

	assertGolden(t, "odata_errors", snap)
}

func TestE2E_OData_Headers(t *testing.T) {
	url, _ := odataSetup(t)
	snap := newSnapshot()

	endpoints := []struct{ label, path string }{
		{"service_doc", "/odata"},
		{"metadata", "/odata/$metadata"},
		{"collection", "/odata/mart_revenue?$top=1"},
		{"count_path", "/odata/mart_revenue/$count"},
	}

	for _, ep := range endpoints {
		snap.addLine(fmt.Sprintf("--- %s ---", ep.label))
		snap.addLine(fmt.Sprintf("odata-version: %s", odataGetHeader(t, url+ep.path, "OData-Version")))
		snap.addLine(fmt.Sprintf("content-type: %s", odataGetHeader(t, url+ep.path, "Content-Type")))
	}

	assertGolden(t, "odata_headers", snap)
}

// TestE2E_OData_AdvancedFilter tests string/date/math functions, arithmetic, in operator.
func TestE2E_OData_AdvancedFilter(t *testing.T) {
	url, _ := odataSetup(t)
	snap := newSnapshot()

	tests := []struct {
		label string
		query string
	}{
		// String functions
		{"contains", "$filter=contains(customer,'Ali')"},
		{"startswith", "$filter=startswith(customer,'Bo')"},
		{"endswith", "$filter=endswith(customer,'ol')"},
		{"tolower", "$filter=tolower(customer) eq 'alice'"},
		{"toupper", "$filter=toupper(customer) eq 'ALICE'"},
		{"length", "$filter=length(customer) gt 4"},
		{"trim", "$filter=trim(customer) eq 'Carol'"},
		{"indexof", "$filter=indexof(customer,'ob') gt 0"},
		{"substring", "$filter=substring(customer,0,3) eq 'Ali'"},
		{"concat", "$filter=concat(customer,'!') eq 'Carol!'"},
		{"nested_contains_tolower", "$filter=contains(tolower(customer),'alice')"},

		// Date functions
		{"year", "$filter=year(order_date) eq 2026"},
		{"month", "$filter=month(order_date) eq 1"},
		{"day", "$filter=day(order_date) eq 1"},

		// Math functions
		{"round", "$filter=round(amount) eq 150"},
		{"floor", "$filter=floor(amount) eq 250"},
		{"ceiling", "$filter=ceiling(amount) eq 251"},

		// Arithmetic
		{"add", "$filter=amount add 50 gt 1000"},
		{"sub", "$filter=amount sub 100 gt 100"},
		{"mul", "$filter=amount mul 2 gt 500"},

		// In operator
		{"in", "$filter=customer in ('Alice','Carol')"},

		// Combined
		{"func_and_arith", "$filter=length(customer) add 1 gt 5"},
	}

	for _, tt := range tests {
		body := odataGet(t, url+"/odata/mart_revenue?"+tt.query)
		var resp odata.ODataResponse
		json.Unmarshal([]byte(body), &resp)
		snap.addLine(fmt.Sprintf("--- %s → %d rows ---", tt.label, len(resp.Value)))
		snap.addLine(formatJSONRows(body))
	}

	assertGolden(t, "odata_advanced_filter", snap)
}

// TestE2E_OData_Compute tests $compute combinations.
func TestE2E_OData_Compute(t *testing.T) {
	url, _ := odataSetup(t)
	snap := newSnapshot()

	// Basic compute
	body := odataGet(t, url+"/odata/mart_revenue?$compute=amount%20mul%202%20as%20doubled&$top=2")
	var resp odata.ODataResponse
	json.Unmarshal([]byte(body), &resp)
	snap.addLine(fmt.Sprintf("--- basic → %d rows ---", len(resp.Value)))
	if len(resp.Value) > 0 {
		snap.addLine(fmt.Sprintf("  has_doubled: %v", resp.Value[0]["doubled"] != nil))
	}

	// Compute + $select (only customer, doubled excluded from output)
	body = odataGet(t, url+"/odata/mart_revenue?$compute=amount%20mul%202%20as%20doubled&$select=customer")
	var selectResp2 odata.ODataResponse
	json.Unmarshal([]byte(body), &selectResp2)
	resp = selectResp2
	snap.addLine(fmt.Sprintf("--- select_without_alias → %d rows ---", len(resp.Value)))
	if len(resp.Value) > 0 {
		_, hasDoubled := resp.Value[0]["doubled"]
		snap.addLine(fmt.Sprintf("  has_doubled: %v", hasDoubled))
	}

	// Compute + $filter on alias + $select without alias
	body = odataGet(t, url+"/odata/mart_revenue?$compute=amount%20mul%202%20as%20doubled&$filter=doubled%20gt%20400&$select=customer")
	var filterResp odata.ODataResponse
	json.Unmarshal([]byte(body), &filterResp)
	snap.addLine(fmt.Sprintf("--- filter_alias_select_other → %d rows ---", len(filterResp.Value)))
	snap.addLine(formatJSONRows(body))

	// Compute + $select including alias
	body = odataGet(t, url+"/odata/mart_revenue?$compute=amount%20mul%202%20as%20doubled&$select=customer,doubled")
	var withAliasResp odata.ODataResponse
	json.Unmarshal([]byte(body), &withAliasResp)
	snap.addLine(fmt.Sprintf("--- select_with_alias → %d rows ---", len(withAliasResp.Value)))
	if len(withAliasResp.Value) > 0 {
		_, hasDoubled := withAliasResp.Value[0]["doubled"]
		snap.addLine(fmt.Sprintf("  has_doubled: %v", hasDoubled))
	}

	// Multiple compute aliases
	body = odataGet(t, url+"/odata/mart_revenue?$compute=amount%20mul%202%20as%20doubled,amount%20add%2010%20as%20plus_ten&$top=1")
	var multiResp odata.ODataResponse
	json.Unmarshal([]byte(body), &multiResp)
	snap.addLine(fmt.Sprintf("--- multiple_aliases → %d rows ---", len(multiResp.Value)))
	if len(multiResp.Value) > 0 {
		_, d := multiResp.Value[0]["doubled"]
		_, p := multiResp.Value[0]["plus_ten"]
		snap.addLine(fmt.Sprintf("  has_doubled: %v has_plus_ten: %v", d, p))
	}

	// /$count with compute + filter on alias
	countBody := odataGet(t, url+"/odata/mart_revenue/$count?$compute=amount%20mul%202%20as%20doubled&$filter=doubled%20gt%20400")
	snap.addLine(fmt.Sprintf("--- count_with_compute: %s ---", strings.TrimSpace(countBody)))

	// Compute alias collision
	body = odataGet(t, url+"/odata/mart_revenue?$compute=amount%20mul%202%20as%20customer")
	snap.addLine(fmt.Sprintf("--- alias_collision: %v ---", strings.Contains(body, "error")))

	assertGolden(t, "odata_compute", snap)
}

// TestE2E_OData_Search tests $search full-text search.
func TestE2E_OData_Search(t *testing.T) {
	url, _ := odataSetup(t)
	snap := newSnapshot()

	tests := []struct {
		label  string
		search string
	}{
		{"match_name", "Alice"},
		{"match_tier", "Premium"},
		{"no_match", "zzzzz"},
		{"empty_search", ""},
	}

	for _, tt := range tests {
		q := ""
		if tt.search != "" {
			q = "?$search=" + tt.search
		}
		body := odataGet(t, url+"/odata/mart_customers"+q)
		var resp odata.ODataResponse
		json.Unmarshal([]byte(body), &resp)
		snap.addLine(fmt.Sprintf("--- %s → %d rows ---", tt.label, len(resp.Value)))
	}

	assertGolden(t, "odata_search", snap)
}

// TestE2E_OData_Apply tests $apply aggregation.
func TestE2E_OData_Apply(t *testing.T) {
	url, _ := odataSetup(t)
	snap := newSnapshot()

	tests := []struct {
		label string
		apply string
	}{
		{"sum", "aggregate(amount with sum as total)"},
		{"avg", "aggregate(amount with avg as avg_amount)"},
		{"min", "aggregate(amount with min as min_amount)"},
		{"max", "aggregate(amount with max as max_amount)"},
		{"count", "aggregate($count as total)"},
		{"groupby_sum", "groupby((customer),aggregate(amount with sum as total))&$orderby=customer asc"},
	}

	for _, tt := range tests {
		body := odataGet(t, url+"/odata/mart_revenue?$apply="+strings.ReplaceAll(tt.apply, " ", "%20"))
		var resp odata.ODataResponse
		json.Unmarshal([]byte(body), &resp)
		snap.addLine(fmt.Sprintf("--- %s → %d rows ---", tt.label, len(resp.Value)))
		snap.addLine(formatJSONRows(body))
	}

	// $apply + $count=true
	countBody := odataGet(t, url+"/odata/mart_revenue?$apply="+strings.ReplaceAll("groupby((customer),aggregate(amount with sum as total))", " ", "%20")+"&$count=true")
	var countResp odata.ODataResponse
	json.Unmarshal([]byte(countBody), &countResp)
	countVal := 0
	if countResp.Count != nil {
		countVal = *countResp.Count
	}
	snap.addLine(fmt.Sprintf("--- apply_count → rows=%d count=%d ---", len(countResp.Value), countVal))

	// $apply + $select
	selectBody := odataGet(t, url+"/odata/mart_revenue?$apply="+strings.ReplaceAll("aggregate(amount with sum as total)", " ", "%20")+"&$select=total")
	var selectResp odata.ODataResponse
	json.Unmarshal([]byte(selectBody), &selectResp)
	snap.addLine(fmt.Sprintf("--- apply_select → %d rows ---", len(selectResp.Value)))
	snap.addLine(formatJSONRows(selectBody))

	assertGolden(t, "odata_apply", snap)
}

// TestE2E_OData_Batch tests $batch JSON batch requests.
func TestE2E_OData_Batch(t *testing.T) {
	url, _ := odataSetup(t)
	snap := newSnapshot()

	batchBody := `{"requests":[
		{"id":"1","method":"GET","url":"mart_revenue?$top=2"},
		{"id":"2","method":"GET","url":"mart_customers/$count"},
		{"id":"3","method":"GET","url":"nonexistent"}
	]}`

	resp, err := http.Post(url+"/odata/$batch", "application/json", strings.NewReader(batchBody))
	if err != nil {
		t.Fatalf("POST $batch: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var batch struct {
		Responses []struct {
			ID     string          `json:"id"`
			Status int             `json:"status"`
			Body   json.RawMessage `json:"body"`
		} `json:"responses"`
	}
	json.Unmarshal(body, &batch)

	snap.addLine(fmt.Sprintf("batch_responses: %d", len(batch.Responses)))
	for _, r := range batch.Responses {
		snap.addLine(fmt.Sprintf("  id=%s status=%d", r.ID, r.Status))
	}

	assertGolden(t, "odata_batch", snap)
}

// TestE2E_OData_Expand tests $expand navigation properties.
func TestE2E_OData_Expand(t *testing.T) {
	url, _ := odataExpandSetup(t)
	snap := newSnapshot()

	// Basic expand
	body := odataGet(t, url+"/odata/raw_parents?$expand=raw_children")
	var resp odata.ODataResponse
	json.Unmarshal([]byte(body), &resp)
	snap.addLine(fmt.Sprintf("expand_rows: %d", len(resp.Value)))
	for i, row := range resp.Value {
		children, _ := json.Marshal(row["raw_children"])
		snap.addLine(fmt.Sprintf("  [%d] name=%v children=%s", i, row["name"], string(children)))
	}

	// Expand with $select (FK not in select — should auto-inject)
	body = odataGet(t, url+"/odata/raw_parents?$select=name&$expand=raw_children")
	json.Unmarshal([]byte(body), &resp)
	snap.addLine(fmt.Sprintf("expand_select_rows: %d", len(resp.Value)))
	for i, row := range resp.Value {
		children, _ := json.Marshal(row["raw_children"])
		hasParentID := row["parent_id"] != nil
		snap.addLine(fmt.Sprintf("  [%d] name=%v has_fk=%v children=%s", i, row["name"], hasParentID, string(children)))
	}

	// Many-to-one: child → parent
	body = odataGet(t, url+"/odata/raw_children?$expand=raw_parents")
	json.Unmarshal([]byte(body), &resp)
	snap.addLine(fmt.Sprintf("expand_many_to_one: %d", len(resp.Value)))
	for i, row := range resp.Value {
		parent, _ := json.Marshal(row["raw_parents"])
		snap.addLine(fmt.Sprintf("  [%d] child_id=%v parent=%s", i, row["child_id"], string(parent)))
	}

	// Expand on unknown nav
	status := odataGetStatus(t, url+"/odata/raw_parents?$expand=fake")
	snap.addLine(fmt.Sprintf("expand_unknown: %d", status))

	assertGolden(t, "odata_expand", snap)
}

// odataExpandSetup creates two related @expose models for $expand testing.
func odataExpandSetup(t *testing.T) (string, *testutil.Project) {
	t.Helper()
	p := testutil.NewProject(t)

	p.AddModel("raw/parents.sql", `-- @kind: table
-- @expose parent_id
SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS t(parent_id, name)
`)
	p.AddModel("raw/children.sql", `-- @kind: table
-- @expose child_id
SELECT * FROM (VALUES (101, 1, 'X'), (102, 1, 'Y'), (103, 2, 'Z')) AS t(child_id, parent_id, name)
`)

	runModel(t, p, "raw/parents.sql")
	runModel(t, p, "raw/children.sql")

	schemas, err := odata.DiscoverSchemas(p.Sess, []odata.ExposeTarget{
		{Target: "raw.parents", KeyColumn: "parent_id"},
		{Target: "raw.children", KeyColumn: "child_id"},
	})
	if err != nil {
		t.Fatalf("DiscoverSchemas: %v", err)
	}

	odata.DiscoverNavigationProperties(schemas)

	handler, err := odata.NewServer(p.Sess, schemas, "http://testhost")
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv := newTestServer(t, handler)
	t.Cleanup(func() { srv.Close() })

	return srv.URL, p
}

// TestE2E_OData_SingleEntity tests entity-by-key access.
func TestE2E_OData_SingleEntity(t *testing.T) {
	url, _ := odataSetup(t)
	snap := newSnapshot()

	// Valid key
	body := odataGet(t, url+"/odata/mart_revenue(1)")
	var entity map[string]any
	json.Unmarshal([]byte(body), &entity)
	snap.addLine(fmt.Sprintf("entity_1: customer=%v", entity["customer"]))

	// Not found
	status := odataGetStatus(t, url+"/odata/mart_revenue(99)")
	snap.addLine(fmt.Sprintf("entity_99: %d", status))

	// Key on customers entity
	status = odataGetStatus(t, url+"/odata/mart_customers(1)")
	snap.addLine(fmt.Sprintf("entity_customers_1: %d", status))

	assertGolden(t, "odata_single_entity", snap)
}
