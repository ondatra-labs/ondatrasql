// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
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
	Columns       []ColumnSchema       // from INFORMATION_SCHEMA
	KeyColumn     string               // optional primary key column for OData Key
	NavProperties []NavigationProperty // relationships to other entities
}

// ColumnSchema describes a column in an entity.
type ColumnSchema struct {
	Name    string // "revenue"
	Type    string // "DOUBLE"
	EdmType string // "Edm.Double"
}

// NavigationProperty describes a relationship between two OData entities.
type NavigationProperty struct {
	Name         string // "orders" (navigation property name)
	TargetEntity string // "raw_orders" (OData entity name)
	SourceColumn string // "order_id" (FK in this entity)
	TargetColumn string // "order_id" (PK/column in target entity)
	IsCollection bool   // true = one-to-many, false = many-to-one
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
			strings.ReplaceAll(schema, "'", "''"), strings.ReplaceAll(table, "'", "''")))
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

// DiscoverNavigationProperties finds relationships between exposed entities
// based on key columns. Two patterns are detected:
//
//   - FK→PK: source has a column matching target's KeyColumn (many-to-one)
//     Example: orders.customer_id → customers.customer_id (key)
//
//   - PK→FK: target has a column matching source's KeyColumn (one-to-many)
//     Example: customers.customer_id (key) ← orders.customer_id
func DiscoverNavigationProperties(schemas []EntitySchema) {
	for i := range schemas {
		colsI := make(map[string]bool)
		for _, c := range schemas[i].Columns {
			colsI[c.Name] = true
		}

		for j := range schemas {
			if i == j {
				continue
			}

			colsJ := make(map[string]bool)
			for _, c := range schemas[j].Columns {
				colsJ[c.Name] = true
			}

			var nav *NavigationProperty

			// Pattern 1: FK→PK — source has target's key column (many-to-one)
			// Example: orders has customer_id, customers has key customer_id
			if schemas[j].KeyColumn != "" && colsI[schemas[j].KeyColumn] &&
				schemas[i].KeyColumn != schemas[j].KeyColumn {
				nav = &NavigationProperty{
					Name:         schemas[j].ODataName,
					TargetEntity: schemas[j].ODataName,
					SourceColumn: schemas[j].KeyColumn,
					TargetColumn: schemas[j].KeyColumn,
					IsCollection: false, // many-to-one
				}
			}

			// Pattern 2: PK→FK — target has source's key column (one-to-many)
			// Example: customers has key customer_id, orders has customer_id
			if nav == nil && schemas[i].KeyColumn != "" && colsJ[schemas[i].KeyColumn] &&
				schemas[j].KeyColumn != schemas[i].KeyColumn {
				nav = &NavigationProperty{
					Name:         schemas[j].ODataName,
					TargetEntity: schemas[j].ODataName,
					SourceColumn: schemas[i].KeyColumn,
					TargetColumn: schemas[i].KeyColumn,
					IsCollection: true, // one-to-many
				}
			}

			if nav == nil {
				continue
			}

			// Avoid duplicates
			exists := false
			for _, existing := range schemas[i].NavProperties {
				if existing.Name == nav.Name {
					exists = true
					break
				}
			}
			if !exists {
				schemas[i].NavProperties = append(schemas[i].NavProperties, *nav)
			}
		}
	}
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
	Name           string           `xml:"Name,attr"`
	Key            *entityKey       `xml:"Key,omitempty"`
	Properties     []property       `xml:"Property"`
	NavProperties  []navProperty    `xml:"NavigationProperty,omitempty"`
}

type navProperty struct {
	Name string `xml:"Name,attr"`
	Type string `xml:"Type,attr"`
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
		// CSDL/EDMX version matches the OData protocol version we
		// advertise via the OData-Version response header. v4.01 because
		// the implementation includes v4.01-only features.
		Version:   "4.01",
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

		// Key element: explicit key column (required by @expose <column>)
		key := &entityKey{}
		keyColumns := make(map[string]bool)
		key.PropertyRefs = []propertyRef{{Name: es.KeyColumn}}
		keyColumns[es.KeyColumn] = true
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
		// Navigation properties
		for _, nav := range es.NavProperties {
			npType := "ondatra." + nav.TargetEntity
			if nav.IsCollection {
				npType = "Collection(" + npType + ")"
			}
			et.NavProperties = append(et.NavProperties, navProperty{
				Name: nav.Name,
				Type: npType,
			})
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
