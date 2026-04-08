// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package odata

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

// ODataResponse is the OData JSON response format.
type ODataResponse struct {
	Context string                   `json:"@odata.context"`
	Count   *int                     `json:"@odata.count,omitempty"`
	Value   []map[string]interface{} `json:"value"`
}

// FormatResponse builds an OData JSON response with native types.
// Rows come from Session.QueryRowsAny with native Go types and nil for NULL.
func FormatResponse(baseURL, entity string, rows []map[string]any, count *int, schema EntitySchema) ([]byte, error) {
	// Build column type lookup for date/timestamp disambiguation
	colTypes := make(map[string]string, len(schema.Columns))
	for _, c := range schema.Columns {
		colTypes[c.Name] = strings.ToUpper(c.Type)
	}

	resp := ODataResponse{
		Context: baseURL + "/odata/$metadata#" + entity,
		Count:   count,
		Value:   make([]map[string]interface{}, len(rows)),
	}

	for i, row := range rows {
		m := make(map[string]interface{}, len(row))
		for k, v := range row {
			m[k] = toODataValue(v, colTypes[k])
		}
		resp.Value[i] = m
	}

	return json.Marshal(resp)
}

// toODataValue converts a native DuckDB Go type to an OData JSON-compatible value.
// colType is the uppercase DuckDB type (e.g. "DATE", "TIMESTAMP") for disambiguation.
func toODataValue(v any, colType string) any {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	// Integers → JSON number
	case int8:
		return int(val)
	case int16:
		return int(val)
	case int32:
		return int(val)
	case int64:
		return val
	case uint8:
		return int(val)
	case uint16:
		return int(val)
	case uint32:
		return val
	case uint64:
		return val

	// Floats → JSON number
	case float32:
		return float64(val)
	case float64:
		return val

	// Decimal → JSON number (exact via json.Number)
	case duckdb.Decimal:
		return json.Number(val.String())

	// HUGEINT (DuckDB int128) → JSON number via *big.Int
	// (Bug 4 — was falling through to default → fmt.Sprintf → string)
	case *big.Int:
		return json.Number(val.String())

	// Boolean → JSON boolean
	case bool:
		return val

	// Time/Date → JSON string (ISO 8601)
	// Use column type to distinguish DATE from TIMESTAMP at midnight.
	case time.Time:
		if colType == "DATE" {
			return val.Format("2006-01-02")
		}
		return val.Format(time.RFC3339)

	// String → JSON string
	case string:
		return val

	// Bytes → JSON string
	case []byte:
		return string(val)

	default:
		return fmt.Sprintf("%v", val)
	}
}

// ServiceDocument is the OData service root response.
type ServiceDocument struct {
	Context string         `json:"@odata.context"`
	Value   []ServiceEntry `json:"value"`
}

// ServiceEntry is an entity set in the service document.
type ServiceEntry struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
	URL  string `json:"url"`
}

// FormatServiceDocument builds the service root response.
func FormatServiceDocument(baseURL string, schemas []EntitySchema) ([]byte, error) {
	doc := ServiceDocument{
		Context: baseURL + "/odata/$metadata",
		Value:   make([]ServiceEntry, len(schemas)),
	}
	for i, s := range schemas {
		doc.Value[i] = ServiceEntry{
			Name: s.ODataName,
			Kind: "EntitySet",
			URL:  s.ODataName,
		}
	}
	return json.Marshal(doc)
}
