// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ondatra-labs/ondatrasql/internal/odata"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// TestE2E_Grafana_OData starts a real Grafana container with the OData plugin,
// creates a datasource pointing to our OData server, runs a query, and verifies results.
func TestE2E_Grafana_OData(t *testing.T) {
	ctx := context.Background()

	// --- 1. Set up OData server ---
	p := testutil.NewProject(t)
	p.AddModel("mart/sales.sql", `-- @kind: table
-- @expose order_id
SELECT * FROM (VALUES
    (1, 'Alice', 150.00, '2026-01-01'::DATE),
    (2, 'Bob',   250.50, '2026-01-02'::DATE),
    (3, 'Carol', 75.00,  '2026-01-03'::DATE)
) AS t(order_id, customer, amount, order_date)
`)
	runModel(t, p, "mart/sales.sql")

	schemas, err := odata.DiscoverSchemas(p.Sess, []odata.ExposeTarget{
		{Target: "mart.sales", KeyColumn: "order_id"},
	})
	if err != nil {
		t.Fatalf("DiscoverSchemas: %v", err)
	}

	// Listen on all interfaces so the container can reach us
	listener, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	odataPort := listener.Addr().(*net.TCPAddr).Port

	hostIP := "host.docker.internal"
	handler := odata.NewServer(p.Sess, schemas, fmt.Sprintf("http://%s:%d", hostIP, odataPort))
	odataSrv := &http.Server{Handler: handler}
	go odataSrv.Serve(listener)
	t.Cleanup(func() { odataSrv.Close() })

	// --- 2. Start Grafana container ---
	grafanaReq := testcontainers.ContainerRequest{
		Image:        "grafana/grafana-oss:12.0.1",
		ExposedPorts: []string{"3000/tcp"},
		// Map host.docker.internal to host-gateway for Linux Docker (not just Docker Desktop)
		ExtraHosts: []string{"host.docker.internal:host-gateway"},
		Env: map[string]string{
			"GF_INSTALL_PLUGINS":                        "dvelop-odata-datasource",
			"GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS":  "dvelop-odata-datasource",
			"GF_SECURITY_ADMIN_PASSWORD":                 "admin",
			"GF_LOG_LEVEL":                               "warn",
		},
		WaitingFor: wait.ForHTTP("/api/health").
			WithPort("3000").
			WithStartupTimeout(120 * time.Second),
	}

	grafana, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: grafanaReq,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start grafana: %v", err)
	}
	t.Cleanup(func() { grafana.Terminate(ctx) })

	grafanaHost, _ := grafana.Host(ctx)
	grafanaPort, _ := grafana.MappedPort(ctx, "3000")
	grafanaURL := fmt.Sprintf("http://%s:%s", grafanaHost, grafanaPort.Port())

	t.Logf("Grafana: %s", grafanaURL)
	t.Logf("OData:   http://%s:%d/odata", hostIP, odataPort)

	// --- 3. Create OData datasource ---
	dsPayload := fmt.Sprintf(`{
		"name": "OData Test",
		"type": "dvelop-odata-datasource",
		"access": "proxy",
		"url": "http://%s:%d/odata",
		"jsonData": {"urlSpaceEncoding": "%%20"}
	}`, hostIP, odataPort)

	dsResp := grafanaAPI(t, grafanaURL, "POST", "/api/datasources", dsPayload)
	var dsResult struct {
		Datasource struct {
			UID string `json:"uid"`
		} `json:"datasource"`
	}
	json.Unmarshal([]byte(dsResp), &dsResult)
	dsUID := dsResult.Datasource.UID
	if dsUID == "" {
		t.Fatalf("failed to create datasource: %s", dsResp)
	}
	t.Logf("Datasource UID: %s", dsUID)

	// --- 4. Query via Grafana ---
	queryPayload := fmt.Sprintf(`{
		"queries": [{
			"refId": "A",
			"datasource": {
				"type": "dvelop-odata-datasource",
				"uid": "%s"
			},
			"entitySet": {
				"name": "mart_sales",
				"entityType": "ondatra.mart_sales"
			},
			"properties": [
				{"name": "order_id", "type": "Edm.Int32"},
				{"name": "customer", "type": "Edm.String"},
				{"name": "amount", "type": "Edm.Decimal"}
			],
			"timeProperty": null,
			"filterConditions": []
		}],
		"from": "now-1h",
		"to": "now"
	}`, dsUID)

	queryResp := grafanaAPI(t, grafanaURL, "POST", "/api/ds/query", queryPayload)

	// --- 5. Verify results ---
	snap := newSnapshot()
	snap.addLine(fmt.Sprintf("datasource_created: %v", dsUID != ""))

	// Parse Grafana query response
	var qr grafanaQueryResponse
	if err := json.Unmarshal([]byte(queryResp), &qr); err != nil {
		snap.addLine(fmt.Sprintf("query_parse_error: %v", err))
		snap.addLine(fmt.Sprintf("raw_response: %s", truncate(queryResp, 500)))
		assertGolden(t, "grafana_odata", snap)
		return
	}

	frameA, ok := qr.Results["A"]
	if !ok {
		snap.addLine("query_error: no result for refId A")
		snap.addLine(fmt.Sprintf("raw_response: %s", truncate(queryResp, 500)))
		assertGolden(t, "grafana_odata", snap)
		return
	}

	if frameA.Error != "" {
		snap.addLine(fmt.Sprintf("query_error: %s", frameA.Error))
		assertGolden(t, "grafana_odata", snap)
		return
	}

	snap.addLine(fmt.Sprintf("frames: %d", len(frameA.Frames)))
	if len(frameA.Frames) > 0 {
		frame := frameA.Frames[0]
		snap.addLine(fmt.Sprintf("fields: %d", len(frame.Schema.Fields)))
		for _, f := range frame.Schema.Fields {
			snap.addLine(fmt.Sprintf("  field: %s (type=%s)", f.Name, f.Type))
		}

		// Count rows from first field's values
		if len(frame.Data.Values) > 0 {
			snap.addLine(fmt.Sprintf("rows: %d", len(frame.Data.Values[0])))
		}

		// Print values
		for i := 0; i < len(frame.Data.Values[0]); i++ {
			var parts []string
			for fi, f := range frame.Schema.Fields {
				val := frame.Data.Values[fi][i]
				data, _ := json.Marshal(val)
				parts = append(parts, fmt.Sprintf("%s=%s", f.Name, string(data)))
			}
			snap.addLine(fmt.Sprintf("  [%d] %s", i, strings.Join(parts, ", ")))
		}
	}

	assertGolden(t, "grafana_odata", snap)
}

// grafanaQueryResponse models the /api/ds/query response.
type grafanaQueryResponse struct {
	Results map[string]grafanaResult `json:"results"`
}

type grafanaResult struct {
	Error  string          `json:"error"`
	Frames []grafanaFrame  `json:"frames"`
}

type grafanaFrame struct {
	Schema struct {
		Fields []grafanaField `json:"fields"`
	} `json:"schema"`
	Data struct {
		Values [][]any `json:"values"`
	} `json:"data"`
}

type grafanaField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// grafanaAPI makes an authenticated Grafana API call.
func grafanaAPI(t *testing.T, baseURL, method, path, body string) string {
	t.Helper()
	req, err := http.NewRequest(method, baseURL+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("admin", "admin")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 400 {
		t.Logf("%s %s → %d: %s", method, path, resp.StatusCode, string(data))
	}
	return string(data)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
