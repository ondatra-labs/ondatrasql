// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package duckdb

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInitSandbox(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	dir := t.TempDir()
	configDir := filepath.Join(dir, "config")
	os.MkdirAll(configDir, 0o755)

	// Create a prod catalog
	prodCatalogPath := filepath.Join(dir, "prod_ducklake.sqlite")
	prodDataPath := filepath.Join(dir, "prod_data")
	prodConnStr := "ducklake:sqlite:" + prodCatalogPath

	// First create the prod catalog by attaching it in a setup session
	setupSess, err := NewSession(":memory:")
	if err != nil {
		t.Fatalf("create setup session: %v", err)
	}
	if err := setupSess.Exec("ATTACH 'ducklake:sqlite:" + prodCatalogPath + "' AS lake (DATA_PATH '" + prodDataPath + "')"); err != nil {
		setupSess.Close()
		t.Fatalf("create prod catalog: %v", err)
	}
	setupSess.Close()

	// Create sandbox catalog path
	sandboxCatalog := filepath.Join(dir, "sandbox_ducklake.sqlite")

	// Now test InitSandbox
	sess, err := NewSession(":memory:")
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	err = sess.InitSandbox(configDir, prodConnStr, prodDataPath, sandboxCatalog, "lake")
	if err != nil {
		t.Fatalf("InitSandbox: %v", err)
	}

	if sess.CatalogAlias() != "sandbox" {
		t.Errorf("CatalogAlias = %q, want sandbox", sess.CatalogAlias())
	}
	if sess.ProdAlias() != "lake" {
		t.Errorf("ProdAlias = %q, want lake", sess.ProdAlias())
	}

	// ondatra_run_time should be set
	val, err := sess.QueryValue("SELECT getvariable('ondatra_run_time')")
	if err != nil {
		t.Fatalf("query run_time: %v", err)
	}
	if val == "" {
		t.Error("ondatra_run_time not set")
	}

	// Registry should exist in sandbox
	val, err = sess.QueryValue("SELECT COUNT(*) FROM sandbox._ondatra_registry")
	if err != nil {
		t.Fatalf("query registry: %v", err)
	}
	if val != "0" {
		t.Errorf("registry count = %s, want 0", val)
	}
}

func TestQueryPrint_Markdown(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	sess, err := NewSession(":memory:")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer sess.Close()

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err = sess.QueryPrint("SELECT 1 AS id, 'hello' AS msg", "markdown")

	w.Close()
	os.Stdout = old

	buf := make([]byte, 4096)
	n, _ := r.Read(buf)
	output := string(buf[:n])

	if err != nil {
		t.Fatalf("QueryPrint markdown: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 markdown lines (header+sep+data), got %d:\n%s", len(lines), output)
	}
	if !strings.Contains(lines[0], "| id") || !strings.Contains(lines[0], "| msg") {
		t.Errorf("header missing columns: %q", lines[0])
	}
	if !strings.Contains(lines[2], "| 1") || !strings.Contains(lines[2], "| hello") {
		t.Errorf("data row missing values: %q", lines[2])
	}
}

func TestQueryPrint_JSON(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	sess, err := NewSession(":memory:")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer sess.Close()

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err = sess.QueryPrint("SELECT 1 AS id, 'test' AS val", "json")

	w.Close()
	os.Stdout = old

	buf := make([]byte, 4096)
	n, _ := r.Read(buf)
	output := string(buf[:n])

	if err != nil {
		t.Fatalf("QueryPrint json: %v", err)
	}
	var rows []map[string]string
	if jsonErr := json.Unmarshal(buf[:n], &rows); jsonErr != nil {
		t.Fatalf("invalid JSON: %v\noutput:\n%s", jsonErr, output)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0]["id"] != "1" {
		t.Errorf("id = %q, want %q", rows[0]["id"], "1")
	}
	if rows[0]["val"] != "test" {
		t.Errorf("val = %q, want %q", rows[0]["val"], "test")
	}
}

func TestQueryPrint_CSV(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	sess, err := NewSession(":memory:")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer sess.Close()

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err = sess.QueryPrint("SELECT 1 AS id, 'hello' AS msg", "csv")

	w.Close()
	os.Stdout = old

	buf := make([]byte, 4096)
	n, _ := r.Read(buf)
	output := string(buf[:n])

	if err != nil {
		t.Fatalf("QueryPrint csv: %v", err)
	}
	if !strings.Contains(output, "id,msg") {
		t.Error("csv output should contain header 'id,msg'")
	}
	if !strings.Contains(output, "1,hello") {
		t.Error("csv output should contain '1,hello'")
	}
}

func TestRawConn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	sess, err := NewSession(":memory:")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer sess.Close()

	err = sess.RawConn(func(rawConn any) error {
		if rawConn == nil {
			t.Error("rawConn is nil")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RawConn: %v", err)
	}
}

func TestInitWithCatalog_UserMacros(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	dir := t.TempDir()
	configDir := filepath.Join(dir, "config")
	os.MkdirAll(configDir, 0o755)

	catalogPath := filepath.Join(dir, "ducklake.sqlite")
	dataPath := filepath.Join(dir, "data")
	os.WriteFile(filepath.Join(configDir, "catalog.sql"),
		[]byte("ATTACH 'ducklake:sqlite:"+catalogPath+"' AS lake (DATA_PATH '"+dataPath+"');\n"), 0o644)

	// Create a macros.sql with a user-defined macro
	os.WriteFile(filepath.Join(configDir, "macros.sql"),
		[]byte("CREATE OR REPLACE MACRO my_double(x) AS x * 2;\n"), 0o644)

	sess, err := NewSession(":memory:")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	if err := sess.InitWithCatalog(configDir); err != nil {
		t.Fatalf("init: %v", err)
	}

	val, err := sess.QueryValue("SELECT my_double(21)")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "42" {
		t.Errorf("my_double(21) = %q, want 42", val)
	}
}

func TestIsOnlyComments(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	tests := []struct {
		input string
		want  bool
	}{
		{"-- comment\n-- another", true},
		{"", true},
		{"SELECT 1", false},
		{"-- comment\nSELECT 1", false},
		{"  \n  \n  ", true},
	}
	for _, tt := range tests {
		got := isOnlyComments(tt.input)
		if got != tt.want {
			t.Errorf("isOnlyComments(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestAnyToString(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	tests := []struct {
		input any
		want  string
	}{
		{nil, ""},
		{"hello", "hello"},
		{[]byte("bytes"), "bytes"},
		{int64(42), "42"},
		{float64(3.14), "3.14"},
		{true, "true"},
		{false, "false"},
		{42, "42"}, // default case
	}
	for _, tt := range tests {
		got := anyToString(tt.input)
		if got != tt.want {
			t.Errorf("anyToString(%v) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestEscapeCSV(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	tests := []struct {
		input string
		want  string
	}{
		{"simple", "simple"},
		{"has,comma", `"has,comma"`},
		{`has"quote`, `"has""quote"`},
		{"has\nnewline", "\"has\nnewline\""},
	}
	for _, tt := range tests {
		got := escapeCSV(tt.input)
		if got != tt.want {
			t.Errorf("escapeCSV(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
