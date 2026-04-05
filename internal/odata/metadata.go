// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package odata

import (
	"encoding/xml"
	"fmt"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
)

// EntitySchema describes an exposed OData entity.
type EntitySchema struct {
	Target  string         // "mart.daily_revenue" (internal DuckDB name)
	ODataName string       // "mart_daily_revenue" (OData-safe, dots replaced)
	Schema  string         // "mart"
	Table   string         // "daily_revenue"
	Columns []ColumnSchema // from INFORMATION_SCHEMA
	KeyColumn string       // optional primary key column for OData Key
}

// ColumnSchema describes a column in an entity.
type ColumnSchema struct {
	Name    string // "revenue"
	Type    string // "DOUBLE"
	EdmType string // "Edm.Double"
}

// ExposeTarget pairs a target with its optional key column.
type ExposeTarget struct {
	Target    string // "mart.daily_revenue"
	KeyColumn string // optional: "id"
}

// DiscoverSchemas reads column metadata from INFORMATION_SCHEMA for exposed models.
func DiscoverSchemas(sess *duckdb.Session, targets []ExposeTarget) ([]EntitySchema, error) {
	var schemas []EntitySchema
	seen := make(map[string]string) // ODataName → Target (for collision detection)
	for _, et := range targets {
		parts := strings.SplitN(et.Target, ".", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid target: %s", et.Target)
		}
		schema, table := parts[0], parts[1]

		rows, err := sess.QueryRowsMap(fmt.Sprintf(
			"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' ORDER BY ordinal_position",
			schema, table))
		if err != nil {
			return nil, fmt.Errorf("discover schema for %s: %w", et.Target, err)
		}

		if len(rows) == 0 {
			return nil, fmt.Errorf("no columns found for %s — table may not exist or has not been materialized", et.Target)
		}

		odataName := strings.ReplaceAll(et.Target, ".", "_")
		if prev, ok := seen[odataName]; ok {
			return nil, fmt.Errorf("OData name collision: %s and %s both map to %s", prev, et.Target, odataName)
		}
		seen[odataName] = et.Target

		es := EntitySchema{
			Target:    et.Target,
			ODataName: odataName,
			Schema:    schema,
			Table:     table,
			KeyColumn: et.KeyColumn,
		}
		for _, row := range rows {
			es.Columns = append(es.Columns, ColumnSchema{
				Name:    row["column_name"],
				Type:    row["data_type"],
				EdmType: duckDBToEdm(row["data_type"]),
			})
		}

		// Validate key column exists in schema
		if es.KeyColumn != "" {
			found := false
			for _, c := range es.Columns {
				if c.Name == es.KeyColumn {
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("@expose key column %q not found in %s", es.KeyColumn, et.Target)
			}
		}

		schemas = append(schemas, es)
	}
	return schemas, nil
}

func duckDBToEdm(duckType string) string {
	upper := strings.ToUpper(duckType)
	switch {
	case upper == "VARCHAR" || strings.HasPrefix(upper, "VARCHAR"):
		return "Edm.String"
	case upper == "INTEGER" || upper == "INT" || upper == "INT4":
		return "Edm.Int32"
	case upper == "BIGINT" || upper == "INT8":
		return "Edm.Int64"
	case upper == "SMALLINT" || upper == "INT2":
		return "Edm.Int16"
	case upper == "TINYINT" || upper == "INT1":
		return "Edm.Byte"
	case upper == "DOUBLE" || upper == "FLOAT8":
		return "Edm.Double"
	case upper == "FLOAT" || upper == "FLOAT4" || upper == "REAL":
		return "Edm.Single"
	case upper == "BOOLEAN" || upper == "BOOL":
		return "Edm.Boolean"
	case upper == "DATE":
		return "Edm.Date"
	case upper == "TIMESTAMP" || upper == "TIMESTAMP WITH TIME ZONE" || upper == "TIMESTAMPTZ":
		return "Edm.DateTimeOffset"
	case strings.HasPrefix(upper, "DECIMAL") || strings.HasPrefix(upper, "NUMERIC"):
		return "Edm.Decimal"
	default:
		return "Edm.String"
	}
}

// CSDL XML types for $metadata

type edmx struct {
	XMLName      xml.Name     `xml:"edmx:Edmx"`
	Version      string       `xml:"Version,attr"`
	XmlnsEdmx    string       `xml:"xmlns:edmx,attr"`
	DataServices dataServices `xml:"edmx:DataServices"`
}

type dataServices struct {
	Schema schemaElement `xml:"Schema"`
}

type schemaElement struct {
	Namespace string           `xml:"Namespace,attr"`
	Xmlns     string           `xml:"xmlns,attr"`
	Types     []entityType     `xml:"EntityType"`
	Container entityContainer  `xml:"EntityContainer"`
}

type entityType struct {
	Name       string        `xml:"Name,attr"`
	Key        *entityKey    `xml:"Key,omitempty"`
	Properties []property    `xml:"Property"`
}

type entityKey struct {
	PropertyRefs []propertyRef `xml:"PropertyRef"`
}

type propertyRef struct {
	Name string `xml:"Name,attr"`
}

type property struct {
	Name     string `xml:"Name,attr"`
	Type     string `xml:"Type,attr"`
	Nullable *bool  `xml:"Nullable,attr,omitempty"`
}

type entityContainer struct {
	Name string      `xml:"Name,attr"`
	Sets []entitySet `xml:"EntitySet"`
}

type entitySet struct {
	Name       string `xml:"Name,attr"`
	EntityType string `xml:"EntityType,attr"`
}

// GenerateMetadata produces CSDL XML for all exposed entities.
func GenerateMetadata(schemas []EntitySchema) ([]byte, error) {
	doc := edmx{
		Version:   "4.0",
		XmlnsEdmx: "http://docs.oasis-open.org/odata/ns/edmx",
		DataServices: dataServices{
			Schema: schemaElement{
				Namespace: "ondatra",
				Xmlns:     "http://docs.oasis-open.org/odata/ns/edm",
			},
		},
	}

	for _, es := range schemas {
		et := entityType{Name: es.ODataName}

		// Key element: use explicit key column or all columns as composite key
		key := &entityKey{}
		keyColumns := make(map[string]bool)
		if es.KeyColumn != "" {
			key.PropertyRefs = []propertyRef{{Name: es.KeyColumn}}
			keyColumns[es.KeyColumn] = true
		} else {
			for _, col := range es.Columns {
				key.PropertyRefs = append(key.PropertyRefs, propertyRef{Name: col.Name})
				keyColumns[col.Name] = true
			}
		}
		et.Key = key

		notNullable := false
		for _, col := range es.Columns {
			p := property{
				Name: col.Name,
				Type: col.EdmType,
			}
			if keyColumns[col.Name] {
				p.Nullable = &notNullable
			}
			et.Properties = append(et.Properties, p)
		}
		doc.DataServices.Schema.Types = append(doc.DataServices.Schema.Types, et)
		doc.DataServices.Schema.Container.Sets = append(doc.DataServices.Schema.Container.Sets, entitySet{
			Name:       es.ODataName,
			EntityType: "ondatra." + es.ODataName,
		})
	}

	doc.DataServices.Schema.Container.Name = "Default"

	output, err := xml.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, err
	}
	return append([]byte(xml.Header), output...), nil
}
