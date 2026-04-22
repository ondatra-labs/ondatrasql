// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package libregistry

import (
	"strings"
	"testing"
)

// --- buildMacroSQL: column and macro name quoting ---

func TestBuildMacroSQL_ReservedWordColumnName(t *testing.T) {
	t.Parallel()
	// Before fix: DATE as column name produced unquoted SQL that failed in DuckDB.
	lf := &LibFunc{
		Name: "gam_fetch",
		Args: []string{"key"},
		Columns: []Column{
			{Name: "DATE", Type: "VARCHAR"},
			{Name: "ORDER", Type: "BIGINT"},
		},
	}
	sql := buildMacroSQL(lf)

	// Column names must be quoted
	if !strings.Contains(sql, `"DATE"`) {
		t.Errorf("DATE column should be quoted, got: %s", sql)
	}
	if !strings.Contains(sql, `"ORDER"`) {
		t.Errorf("ORDER column should be quoted, got: %s", sql)
	}
}

func TestBuildMacroSQL_ReservedWordMacroName(t *testing.T) {
	t.Parallel()
	// Before fix: macro name like "order" was unquoted → DuckDB parse error.
	lf := &LibFunc{
		Name:    "order",
		Args:    []string{},
		Columns: []Column{{Name: "id", Type: "BIGINT"}},
	}
	sql := buildMacroSQL(lf)

	if !strings.HasPrefix(sql, `CREATE OR REPLACE TEMP MACRO "order"(`) {
		t.Errorf("macro name should be quoted, got: %s", sql)
	}
}

func TestBuildMacroSQL_AllPartsQuoted(t *testing.T) {
	t.Parallel()
	lf := &LibFunc{
		Name: "fetch_data",
		Args: []string{"schema", "table"},
		Columns: []Column{
			{Name: "value", Type: "DOUBLE"},
		},
	}
	sql := buildMacroSQL(lf)

	// Macro name, args, and columns should all be quoted
	if !strings.Contains(sql, `"fetch_data"`) {
		t.Error("macro name not quoted")
	}
	if !strings.Contains(sql, `"schema"`) {
		t.Error("arg 'schema' not quoted")
	}
	if !strings.Contains(sql, `"table"`) {
		t.Error("arg 'table' not quoted")
	}
	if !strings.Contains(sql, `"value"`) {
		t.Error("column 'value' not quoted")
	}
}

// --- extractAnyDict: falsy values preserved ---

func TestScan_APIDict_AuthFalsyValues(t *testing.T) {
	dir := t.TempDir()
	writeLib(t, dir, "falsy_api.star", `
API = {
    "base_url": "https://api.example.com",
    "auth": {"env": "KEY", "verify": False, "retries": 0, "prefix": ""},
    "fetch": {
        "args": [],
        "dynamic_columns": True,
    },
}

def fetch(page):
    pass
`)
	reg, err := Scan(dir)
	if err != nil {
		t.Fatal(err)
	}
	lf := reg.Get("falsy_api")
	if lf == nil {
		t.Fatal("falsy_api not found")
	}
	auth := lf.APIConfig.Auth
	if auth == nil {
		t.Fatal("auth config should not be nil")
	}

	// Before fix: False, 0, and "" were silently dropped
	if _, ok := auth["verify"]; !ok {
		t.Error("auth['verify'] = False should be preserved (was dropped before fix)")
	} else if auth["verify"] != false {
		t.Errorf("auth['verify'] = %v, want false", auth["verify"])
	}

	if _, ok := auth["retries"]; !ok {
		t.Error("auth['retries'] = 0 should be preserved (was dropped before fix)")
	} else if auth["retries"] != 0 {
		t.Errorf("auth['retries'] = %v, want 0", auth["retries"])
	}

	if _, ok := auth["prefix"]; !ok {
		t.Error("auth['prefix'] = '' should be preserved (was dropped before fix)")
	} else if auth["prefix"] != "" {
		t.Errorf("auth['prefix'] = %v, want empty string", auth["prefix"])
	}

	// Non-falsy values should still work
	if auth["env"] != "KEY" {
		t.Errorf("auth['env'] = %v, want 'KEY'", auth["env"])
	}
}

// --- validateFunc: extra undeclared params ---

func TestScan_ExtraUndeclaredParam(t *testing.T) {
	dir := t.TempDir()
	writeLib(t, dir, "extra_param.star", `
API = {
    "base_url": "https://api.example.com",
    "fetch": {
        "args": ["key"],
        "dynamic_columns": True,
    },
}

def fetch(key, undeclared_extra, page):
    pass
`)
	_, err := Scan(dir)
	if err == nil {
		t.Fatal("expected error for undeclared extra parameter")
	}
	if !strings.Contains(err.Error(), "undeclared parameter") {
		t.Errorf("error should mention undeclared parameter, got: %v", err)
	}
}

// --- API dict: auth config parsing ---

func TestScan_APIDict_AuthConfig(t *testing.T) {
	dir := t.TempDir()
	writeLib(t, dir, "authed_api.star", `
API = {
    "base_url": "https://api.example.com",
    "auth": {"env": "API_KEY", "header": "X-Auth"},
    "fetch": {
        "args": ["query"],
        "dynamic_columns": True,
    },
}

def fetch(query, page):
    pass
`)
	reg, err := Scan(dir)
	if err != nil {
		t.Fatal(err)
	}
	lf := reg.Get("authed_api")
	if lf == nil {
		t.Fatal("authed_api not found")
	}
	if lf.APIConfig == nil {
		t.Fatal("APIConfig should not be nil")
	}
	if lf.APIConfig.Auth == nil {
		t.Fatal("Auth config should not be nil")
	}
	if lf.APIConfig.Auth["env"] != "API_KEY" {
		t.Errorf("auth.env = %v, want API_KEY", lf.APIConfig.Auth["env"])
	}
	if lf.APIConfig.Auth["header"] != "X-Auth" {
		t.Errorf("auth.header = %v, want X-Auth", lf.APIConfig.Auth["header"])
	}
}
