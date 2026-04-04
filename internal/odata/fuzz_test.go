// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package odata

import (
	"net/url"
	"testing"
)

var fuzzEntity = EntitySchema{
	Target: "test.table",
	Schema: "test",
	Table:  "table",
	Columns: []ColumnSchema{
		{Name: "id", Type: "INTEGER", EdmType: "Edm.Int32"},
		{Name: "name", Type: "VARCHAR", EdmType: "Edm.String"},
		{Name: "value", Type: "DOUBLE", EdmType: "Edm.Double"},
	},
}

func FuzzBuildQuery_Filter(f *testing.F) {
	f.Add("name eq 'Alice'")
	f.Add("value gt 100")
	f.Add("id eq 1 and name eq 'Bob'")
	f.Add("value ge 0 or value le -1")
	f.Add("name eq null")
	f.Add("name eq 'has''quote'")
	f.Add("")
	f.Add(")))invalid")
	f.Add("'; DROP TABLE--")
	f.Add("name eq 'Alice' and value gt 100 or id lt 5")

	f.Fuzz(func(t *testing.T, filter string) {
		params := url.Values{}
		if filter != "" {
			params.Set("$filter", filter)
		}
		// Should not panic
		BuildQuery(fuzzEntity, params)
	})
}

func FuzzBuildQuery_Select(f *testing.F) {
	f.Add("name")
	f.Add("name,value")
	f.Add("id,name,value")
	f.Add("")
	f.Add("nonexistent")
	f.Add("*")
	f.Add("name,,value")

	f.Fuzz(func(t *testing.T, sel string) {
		params := url.Values{}
		if sel != "" {
			params.Set("$select", sel)
		}
		BuildQuery(fuzzEntity, params)
	})
}

func FuzzBuildQuery_OrderBy(f *testing.F) {
	f.Add("name asc")
	f.Add("value desc")
	f.Add("name asc,value desc")
	f.Add("")
	f.Add("nonexistent desc")

	f.Fuzz(func(t *testing.T, orderby string) {
		params := url.Values{}
		if orderby != "" {
			params.Set("$orderby", orderby)
		}
		BuildQuery(fuzzEntity, params)
	})
}

func FuzzBuildQuery_TopSkip(f *testing.F) {
	f.Add("10", "0")
	f.Add("1", "100")
	f.Add("0", "0")
	f.Add("-1", "0")
	f.Add("abc", "0")
	f.Add("10", "abc")

	f.Fuzz(func(t *testing.T, top, skip string) {
		params := url.Values{}
		if top != "" {
			params.Set("$top", top)
		}
		if skip != "" {
			params.Set("$skip", skip)
		}
		BuildQuery(fuzzEntity, params)
	})
}

func FuzzDuckDBToEdm(f *testing.F) {
	f.Add("VARCHAR")
	f.Add("INTEGER")
	f.Add("BIGINT")
	f.Add("DOUBLE")
	f.Add("BOOLEAN")
	f.Add("DATE")
	f.Add("TIMESTAMP")
	f.Add("DECIMAL(18,2)")
	f.Add("UNKNOWN")
	f.Add("")

	f.Fuzz(func(t *testing.T, duckType string) {
		result := duckDBToEdm(duckType)
		if result == "" {
			t.Error("duckDBToEdm returned empty string")
		}
	})
}
