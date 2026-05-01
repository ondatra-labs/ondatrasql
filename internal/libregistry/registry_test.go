// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package libregistry

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeLib(t *testing.T, dir, name, code string) {
	t.Helper()
	libDir := filepath.Join(dir, "lib")
	os.MkdirAll(libDir, 0o755)
	err := os.WriteFile(filepath.Join(libDir, name), []byte(code), 0o644)
	if err != nil {
		t.Fatal(err)
	}
}

func TestScan_FetchLib(t *testing.T) {
	dir := t.TempDir()
	writeLib(t, dir, "gam_fetch.star", `
API = {
    "base_url": "https://api.google.com",
    "fetch": {
        "args": ["network_code", "key_file"],
    },
}

def fetch(network_code, key_file, page):
    pass
`)

	reg, err := Scan(dir)
	if err != nil {
		t.Fatal(err)
	}

	lf := reg.Get("gam_fetch")
	if lf == nil {
		t.Fatal("gam_fetch not found")
	}

	if lf.IsSink {
		t.Error("expected fetch (IsSink=false), got sink")
	}
	if lf.FuncName != "fetch" {
		t.Errorf("expected func fetch, got %s", lf.FuncName)
	}
	if len(lf.Args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(lf.Args))
	}
	if lf.Args[0] != "network_code" || lf.Args[1] != "key_file" {
		t.Errorf("unexpected args: %v", lf.Args)
	}
	if !lf.DynamicColumns {
		t.Error("expected DynamicColumns=true for API dict")
	}

	if !reg.IsLibFunction("gam_fetch") {
		t.Error("IsLibFunction should return true")
	}
	if reg.IsLibFunction("nonexistent") {
		t.Error("IsLibFunction should return false for unknown")
	}
}

func TestScan_PushLib(t *testing.T) {
	dir := t.TempDir()
	writeLib(t, dir, "hubspot_push.star", `
API = {
    "base_url": "https://api.hubapi.com",
    "push": {
        "batch_size": 100,
    },
}

def push(rows):
    pass
`)

	reg, err := Scan(dir)
	if err != nil {
		t.Fatal(err)
	}

	lf := reg.Get("hubspot_push")
	if lf == nil {
		t.Fatal("hubspot_push not found")
	}

	if !lf.IsSink {
		t.Error("expected push (IsSink=true), got fetch")
	}
	if lf.FuncName != "push" {
		t.Errorf("expected func push, got %s", lf.FuncName)
	}
	if lf.PushConfig == nil {
		t.Fatal("expected PushConfig")
	}
	if lf.PushConfig.BatchSize != 100 {
		t.Errorf("batch_size = %d, want 100", lf.PushConfig.BatchSize)
	}
}

func TestScan_DynamicColumns(t *testing.T) {
	dir := t.TempDir()
	writeLib(t, dir, "mistral_ocr.star", `
API = {
    "base_url": "https://api.mistral.ai",
    "fetch": {
        "args": ["api_key", "files"],
    },
}

def fetch(api_key, files, page):
    pass
`)

	reg, err := Scan(dir)
	if err != nil {
		t.Fatal(err)
	}

	lf := reg.Get("mistral_ocr")
	if lf == nil {
		t.Fatal("mistral_ocr not found")
	}
	if !lf.DynamicColumns {
		t.Error("expected dynamic_columns=true")
	}
	if len(lf.Columns) != 0 {
		t.Errorf("expected 0 columns for dynamic, got %d", len(lf.Columns))
	}
}

func TestScan_SkipsNonLibFiles(t *testing.T) {
	dir := t.TempDir()
	// File without API dict
	writeLib(t, dir, "helper.star", `
def paginate(url):
    pass
`)

	reg, err := Scan(dir)
	if err != nil {
		t.Fatal(err)
	}

	if !reg.Empty() {
		t.Error("expected empty registry for helper-only lib")
	}
}

func TestScan_MissingFetchFunction(t *testing.T) {
	dir := t.TempDir()
	writeLib(t, dir, "broken.star", `
API = {
    "base_url": "https://api.example.com",
    "fetch": {
        "args": ["key"],
    },
}
# No fetch() function defined
`)

	_, err := Scan(dir)
	if err == nil {
		t.Fatal("expected error for missing fetch()")
	}
}

func TestScan_ArgMismatch(t *testing.T) {
	dir := t.TempDir()
	writeLib(t, dir, "bad_args.star", `
API = {
    "base_url": "https://api.example.com",
    "fetch": {
        "args": ["network_code", "key_file"],
    },
}

def fetch(net_code, key_file, page):
    pass
`)

	_, err := Scan(dir)
	if err == nil {
		t.Fatal("expected error for arg mismatch (network_code vs net_code)")
	}
}

func TestScan_EmptyLibDir(t *testing.T) {
	dir := t.TempDir()
	// No lib/ directory at all

	reg, err := Scan(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !reg.Empty() {
		t.Error("expected empty registry")
	}
}

func TestBuildMacroSQL_TypedColumns(t *testing.T) {
	lf := &LibFunc{
		Name: "gam_fetch",
		Args: []string{"network_code", "key_file"},
		Columns: []Column{
			{Name: "AD_UNIT_NAME", Type: "VARCHAR"},
			{Name: "AD_SERVER_IMPRESSIONS", Type: "BIGINT"},
		},
	}
	sql := buildMacroSQL(lf)
	expected := `CREATE OR REPLACE TEMP MACRO "gam_fetch"("network_code", "key_file") AS TABLE SELECT NULL::VARCHAR AS "AD_UNIT_NAME", NULL::BIGINT AS "AD_SERVER_IMPRESSIONS" WHERE false`
	if sql != expected {
		t.Errorf("got:\n  %s\nexpected:\n  %s", sql, expected)
	}
}

func TestBuildMacroSQL_DynamicColumns(t *testing.T) {
	lf := &LibFunc{
		Name:           "mistral_ocr",
		Args:           []string{"api_key", "files"},
		DynamicColumns: true,
	}
	sql := buildMacroSQL(lf)
	expected := `CREATE OR REPLACE TEMP MACRO "mistral_ocr"("api_key", "files") AS TABLE SELECT NULL AS _dynamic WHERE false`
	if sql != expected {
		t.Errorf("got:\n  %s\nexpected:\n  %s", sql, expected)
	}
}

func TestBuildMacroSQL_NoArgs(t *testing.T) {
	lf := &LibFunc{
		Name:    "simple_fetch",
		Args:    []string{},
		Columns: []Column{{Name: "name", Type: "VARCHAR"}},
	}
	sql := buildMacroSQL(lf)
	expected := `CREATE OR REPLACE TEMP MACRO "simple_fetch"() AS TABLE SELECT NULL::VARCHAR AS "name" WHERE false`
	if sql != expected {
		t.Errorf("got:\n  %s\nexpected:\n  %s", sql, expected)
	}
}

// mockExec records all SQL executed against it.
type mockExec struct {
	executed []string
	failOn   string
}

func (m *mockExec) Exec(sql string) error {
	m.executed = append(m.executed, sql)
	if m.failOn != "" && strings.Contains(sql, m.failOn) {
		return fmt.Errorf("mock error on %s", m.failOn)
	}
	return nil
}

func TestRegisterMacros(t *testing.T) {
	dir := t.TempDir()
	writeLib(t, dir, "api_fetch.star", `
API = {
    "base_url": "https://api.example.com",
    "fetch": {"args": ["url"]},
}
def fetch(url, page):
    pass
`)
	writeLib(t, dir, "api_push.star", `
API = {
    "base_url": "https://api.example.com",
    "push": {"batch_size": 50},
}
def push(rows):
    pass
`)

	reg, err := Scan(dir)
	if err != nil {
		t.Fatal(err)
	}

	mock := &mockExec{}
	err = reg.RegisterMacros(mock)
	if err != nil {
		t.Fatal(err)
	}

	// Only TABLE libs registered, not SINK libs
	if len(mock.executed) != 1 {
		t.Fatalf("expected 1 macro registration, got %d: %v", len(mock.executed), mock.executed)
	}
	if !strings.Contains(mock.executed[0], "api_fetch") {
		t.Errorf("expected api_fetch macro, got: %s", mock.executed[0])
	}
}

func TestRegisterMacros_NilRegistry(t *testing.T) {
	var reg *Registry
	err := reg.RegisterMacros(&mockExec{})
	if err != nil {
		t.Fatal("nil registry should not error")
	}
}

func TestScan_FetchAndPushFuncs(t *testing.T) {
	dir := t.TempDir()
	writeLib(t, dir, "fetch_api.star", `
API = {
    "base_url": "https://api.example.com",
    "fetch": {"args": ["url"]},
}
def fetch(url, page):
    pass
`)
	writeLib(t, dir, "push_api.star", `
API = {
    "base_url": "https://api.example.com",
    "push": {"batch_size": 100},
}
def push(rows):
    pass
`)

	reg, err := Scan(dir)
	if err != nil {
		t.Fatal(err)
	}

	tables := reg.TableFuncs()
	sinks := reg.PushFuncs()
	if len(tables) != 1 {
		t.Errorf("expected 1 table func, got %d", len(tables))
	}
	if len(sinks) != 1 {
		t.Errorf("expected 1 sink func, got %d", len(sinks))
	}
}
