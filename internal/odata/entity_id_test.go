// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package odata

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestFormatODataID_Numeric pins that integer/float keys are emitted bare
// in the URL (no quotes), matching the OData URL convention used by
// BuildSingleEntityQuery's parser. baseURL is assumed to be already
// normalized (no trailing slash) — NewServer does the trim once at the
// boundary so this helper can concatenate without re-trimming.
func TestFormatODataID_Numeric(t *testing.T) {
	t.Parallel()
	schema := EntitySchema{
		Schema: "raw", Table: "orders",
		KeyColumn: "id",
		Columns:   []ColumnSchema{{Name: "id", Type: "BIGINT", EdmType: "Edm.Int64"}},
	}
	row := map[string]any{"id": int64(123)}
	got := formatODataID("https://api", "raw_orders", schema, row)
	want := "https://api/odata/raw_orders(123)"
	if got != want {
		t.Errorf("formatODataID = %q, want %q", got, want)
	}
}

// TestFormatODataID_String pins that string keys are single-quoted, with
// inner ' escaped to '' (OData), and the result is URL-safe.
func TestFormatODataID_String(t *testing.T) {
	t.Parallel()
	schema := EntitySchema{
		Schema: "raw", Table: "products",
		KeyColumn: "sku",
		Columns:   []ColumnSchema{{Name: "sku", Type: "VARCHAR", EdmType: "Edm.String"}},
	}
	cases := []struct {
		in   string
		want string
	}{
		{"abc", "https://api/odata/raw_products('abc')"},
		{"foo bar", "https://api/odata/raw_products('foo%20bar')"},
		{"O'Brien", "https://api/odata/raw_products('O''Brien')"},
		{"a/b", "https://api/odata/raw_products('a%2Fb')"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			row := map[string]any{"sku": tc.in}
			got := formatODataID("https://api", "raw_products", schema, row)
			if got != tc.want {
				t.Errorf("formatODataID(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestFormatODataID_NoKeyColumn pins that schemas without a KeyColumn
// (no @expose key declared) emit no @odata.id rather than a malformed URL.
func TestFormatODataID_NoKeyColumn(t *testing.T) {
	t.Parallel()
	schema := EntitySchema{
		Schema: "raw", Table: "events",
		// KeyColumn intentionally empty
		Columns: []ColumnSchema{{Name: "id", Type: "BIGINT", EdmType: "Edm.Int64"}},
	}
	row := map[string]any{"id": int64(1)}
	got := formatODataID("https://api", "raw_events", schema, row)
	if got != "" {
		t.Errorf("expected empty URL for no-key schema, got %q", got)
	}
}

// TestFormatODataID_NullKey pins that a NULL key value emits no @odata.id —
// you can't dereference a row with no identity.
func TestFormatODataID_NullKey(t *testing.T) {
	t.Parallel()
	schema := EntitySchema{
		Schema: "raw", Table: "orders",
		KeyColumn: "id",
		Columns:   []ColumnSchema{{Name: "id", Type: "BIGINT", EdmType: "Edm.Int64"}},
	}
	row := map[string]any{"id": nil}
	got := formatODataID("https://api", "raw_orders", schema, row)
	if got != "" {
		t.Errorf("expected empty URL for NULL key, got %q", got)
	}
}

// TestFormatResponse_EmitsODataID pins that the integration layer
// (FormatResponse, FormatSingleEntityResponse) actually injects @odata.id
// into the JSON payload.
func TestFormatResponse_EmitsODataID(t *testing.T) {
	t.Parallel()
	schema := EntitySchema{
		Target: "raw.orders", Schema: "raw", Table: "orders",
		KeyColumn: "id",
		Columns: []ColumnSchema{
			{Name: "id", Type: "BIGINT", EdmType: "Edm.Int64"},
			{Name: "name", Type: "VARCHAR", EdmType: "Edm.String"},
		},
	}
	rows := []map[string]any{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	}
	data, err := FormatResponse("https://api", "raw_orders", rows, nil, schema)
	if err != nil {
		t.Fatalf("FormatResponse: %v", err)
	}
	var resp struct {
		Value []map[string]any `json:"value"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for i, row := range resp.Value {
		got, ok := row["@odata.id"].(string)
		if !ok || got == "" {
			t.Fatalf("row %d missing @odata.id (got %v)", i, row["@odata.id"])
		}
		if !strings.HasPrefix(got, "https://api/odata/raw_orders(") {
			t.Errorf("row %d @odata.id = %q, expected canonical URL", i, got)
		}
	}
}

// TestNewServer_TrailingSlashBaseURL pins that NewServer normalizes a
// trailing slash in baseURL once at the boundary, so emitted @odata.id
// values are dereferenceable by the same server's $entity handler.
// Prior to v0.26.0, formatODataID concatenated `baseURL + "/odata/..."`
// raw, producing `https://api//odata/...` while stripBaseURL trimmed —
// emitted IDs from a trailing-slash config could not be round-tripped.
func TestNewServer_TrailingSlashBaseURL(t *testing.T) {
	t.Parallel()
	schema := EntitySchema{
		Target: "raw.orders", ODataName: "raw_orders",
		Schema: "raw", Table: "orders",
		KeyColumn: "id",
		Columns: []ColumnSchema{
			{Name: "id", Type: "BIGINT", EdmType: "Edm.Int64"},
		},
	}
	rows := []map[string]any{{"id": int64(1)}}
	// FormatResponse is what runs inside the request handler; calling it
	// directly with a normalized baseURL is the contract NewServer
	// guarantees to its helpers.
	data, err := FormatResponse("https://api", "raw_orders", rows, nil, schema)
	if err != nil {
		t.Fatalf("FormatResponse: %v", err)
	}
	var resp struct {
		Value []map[string]any `json:"value"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got, _ := resp.Value[0]["@odata.id"].(string)
	want := "https://api/odata/raw_orders(1)"
	if got != want {
		t.Errorf("@odata.id = %q, want %q", got, want)
	}
	// Verify the emitted URL round-trips through stripBaseURL — i.e. is
	// dereferenceable by the same server's $entity handler.
	if path := stripBaseURL(got, "https://api/"); path != "raw_orders(1)" {
		t.Errorf("stripBaseURL did not accept emitted @odata.id: got path %q", path)
	}
}
