// OndatraSQL - You don't need a data stack anymore
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

	// Model 2: customers — composite key (no explicit), NULL vs empty string
	p.AddModel("mart/customers.sql", `-- @kind: table
-- @expose
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
		{Target: "mart.customers"},
	})
	if err != nil {
		t.Fatalf("DiscoverSchemas: %v", err)
	}

	handler := odata.NewServer(p.Sess, schemas, "http://testhost")
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
