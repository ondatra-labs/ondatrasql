// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute_test

import (
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// A blueprint concatenates its args as strings, and named arguments bind by
// name, in any order.
func TestLibArgs_BlueprintGetsStringsBoundByName(t *testing.T) {
	p := testutil.NewProject(t)
	writeLib(t, p, "argsapi", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource", "page_size", "flag"],
    },
}

def fetch(resource, page_size, flag="", page=None):
    return {
        "rows": [{"q": resource + "?limit=" + page_size + "&flag=" + flag + "&type=" + type(page_size)}],
        "next": None,
    }
`)
	p.AddModel("raw/args.sql", `-- @kind: table
-- @fetch
SELECT q::VARCHAR AS q FROM argsapi('items', flag => true, page_size => 5)
`)
	res := runModelWithLib(t, p, "raw/args.sql")
	for _, w := range res.Warnings {
		if strings.Contains(w, "AST") {
			t.Fatalf("the model must take the AST path: %s", w)
		}
	}
	got, err := p.Sess.QueryValue("SELECT q FROM raw.args")
	if err != nil {
		t.Fatal(err)
	}
	if want := "items?limit=5&flag=true&type=string"; got != want {
		t.Errorf("blueprint built %q, want %q", got, want)
	}
}

// An argument that cannot be bound fails the run with the reason instead of
// shifting onto another parameter.
func TestLibArgs_UnbindableArgumentFails(t *testing.T) {
	p := testutil.NewProject(t)
	writeLib(t, p, "argsapi", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource", "page_size"],
    },
}

def fetch(resource, page_size="", page=None):
    return {"rows": [{"q": resource}], "next": None}
`)
	for sql, want := range map[string]string{
		"argsapi('items', size => 5)":           `no parameter named "size"`,
		"argsapi('items', resource => 'other')": `"resource" is given more than once`,
		"argsapi(page_size => 5, 'items')":      "positional argument 2 follows a named argument",
		"argsapi('items', 5, 6)":                "passes 3 arguments but the blueprint declares only 2",
	} {
		p.AddModel("raw/args.sql", "-- @kind: table\n-- @fetch\nSELECT q::VARCHAR AS q FROM "+sql+"\n")
		_, err := runModelWithLibErr(t, p, "raw/args.sql")
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want it to mention %q", sql, err, want)
		}
	}
}
