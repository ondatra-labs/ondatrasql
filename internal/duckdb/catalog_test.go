// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package duckdb

import (
	"os"
	"path/filepath"
	"testing"
)

// setupCatalog creates a temp DuckLake project and initializes the session.
func setupCatalog(t *testing.T) (*Session, string) {
	t.Helper()
	dir := t.TempDir()

	// Create config directory
	configDir := filepath.Join(dir, "config")
	os.MkdirAll(configDir, 0o755)

	// Create catalog.sql
	catalogPath := filepath.Join(dir, "ducklake.sqlite")
	dataPath := filepath.Join(dir, "data")
	catalogSQL := "ATTACH 'ducklake:sqlite:" + catalogPath + "' AS lake (DATA_PATH '" + dataPath + "');\n"
	os.WriteFile(filepath.Join(configDir, "catalog.sql"), []byte(catalogSQL), 0o644)

	sess, err := NewSession(":memory:?threads=2&memory_limit=1GB")
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	if err := sess.InitWithCatalog(configDir); err != nil {
		t.Fatalf("init catalog: %v", err)
	}

	return sess, dir
}

func TestInitWithCatalog(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	sess, _ := setupCatalog(t)

	if sess.CatalogAlias() != "lake" {
		t.Errorf("alias = %q, want lake", sess.CatalogAlias())
	}
	if sess.ProdAlias() != "" {
		t.Errorf("prod alias = %q, want empty (not sandbox)", sess.ProdAlias())
	}
}

func TestInitWithCatalog_RuntimeVariables(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	sess, _ := setupCatalog(t)

	// ondatra_run_time should be set
	val, err := sess.QueryValue("SELECT getvariable('ondatra_run_time')")
	if err != nil {
		t.Fatalf("query run_time: %v", err)
	}
	if val == "" {
		t.Error("ondatra_run_time not set")
	}

	// ondatra_load_id should be set
	val, err = sess.QueryValue("SELECT getvariable('ondatra_load_id')")
	if err != nil {
		t.Fatalf("query load_id: %v", err)
	}
	if val == "" {
		t.Error("ondatra_load_id not set")
	}
}

func TestInitWithCatalog_CDCVariables(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	sess, _ := setupCatalog(t)

	// curr_snapshot should be set (0 for new catalog)
	val, err := sess.QueryValue("SELECT getvariable('curr_snapshot')::BIGINT")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "0" {
		t.Errorf("curr_snapshot = %s, want 0", val)
	}

	val, err = sess.QueryValue("SELECT getvariable('dag_start_snapshot')::BIGINT")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "0" {
		t.Errorf("dag_start_snapshot = %s, want 0", val)
	}
}

func TestInitWithCatalog_RegistryTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	sess, _ := setupCatalog(t)

	// _ondatra_registry should exist
	val, err := sess.QueryValue("SELECT COUNT(*) FROM lake._ondatra_registry")
	if err != nil {
		t.Fatalf("query registry: %v", err)
	}
	if val != "0" {
		t.Errorf("registry count = %s, want 0", val)
	}
}

func TestInitWithCatalog_SchemaMacros(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	sess, _ := setupCatalog(t)

	// ondatra_normalize_type macro should be available
	val, err := sess.QueryValue("SELECT ondatra_normalize_type('BIGINT')")
	if err != nil {
		t.Fatalf("macro: %v", err)
	}
	if val != "BIGINT" {
		t.Errorf("normalize = %q, want BIGINT", val)
	}
}

func TestInitWithCatalog_CustomSettings(t *testing.T) {
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
	os.WriteFile(filepath.Join(configDir, "settings.sql"),
		[]byte("SET threads = 1;\n"), 0o644)
	os.WriteFile(filepath.Join(configDir, "variables.sql"),
		[]byte("SET VARIABLE my_var = 'hello';\n"), 0o644)

	sess, err := NewSession(":memory:")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	if err := sess.InitWithCatalog(configDir); err != nil {
		t.Fatalf("init: %v", err)
	}

	val, err := sess.QueryValue("SELECT getvariable('my_var')")
	if err != nil {
		t.Fatalf("query var: %v", err)
	}
	if val != "hello" {
		t.Errorf("my_var = %q, want hello", val)
	}
}

func TestRefreshSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	sess, _ := setupCatalog(t)

	if err := sess.RefreshSnapshot(); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	val, err := sess.QueryValue("SELECT getvariable('curr_snapshot')::BIGINT")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val == "" {
		t.Error("snapshot not refreshed")
	}
}

func TestHasCDCChanges_NoChanges(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	sess, _ := setupCatalog(t)

	has, err := sess.HasCDCChanges()
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if has {
		t.Error("expected no changes on fresh catalog")
	}
}

func TestSetHighWaterMark(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	sess, _ := setupCatalog(t)

	if err := sess.SetHighWaterMark("staging.test"); err != nil {
		t.Fatalf("set hwm: %v", err)
	}

	val, err := sess.QueryValue("SELECT getvariable('dag_start_snapshot')::BIGINT")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	// Should be 0 since no commits exist for staging.test
	if val != "0" {
		t.Errorf("hwm = %s, want 0", val)
	}
}

func TestGetDagStartSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	sess, _ := setupCatalog(t)

	snap, err := sess.GetDagStartSnapshot()
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if snap != 0 {
		t.Errorf("snap = %d, want 0", snap)
	}
}

func TestQueryRowsMap(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	rows, err := shared.QueryRowsMap("SELECT 1 AS id, 'hello' AS msg UNION ALL SELECT 2, 'world'")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	if rows[0]["id"] != "1" {
		t.Errorf("row0.id = %q, want 1", rows[0]["id"])
	}
	if rows[0]["msg"] != "hello" {
		t.Errorf("row0.msg = %q, want hello", rows[0]["msg"])
	}
	if rows[1]["id"] != "2" {
		t.Errorf("row1.id = %q, want 2", rows[1]["id"])
	}
}

func TestQueryRows(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	rows, err := shared.QueryRows("SELECT 'a' UNION ALL SELECT 'b' UNION ALL SELECT 'c'")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(rows))
	}
	if rows[0] != "a" || rows[1] != "b" || rows[2] != "c" {
		t.Errorf("rows = %v, want [a b c]", rows)
	}
}

func TestGetVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	v := shared.GetVersion()
	if v == "" || v == "unknown" {
		t.Errorf("version = %q", v)
	}
}

func TestQuoteIdentifier(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	tests := []struct {
		input, want string
	}{
		{"simple", `"simple"`},
		{`has"quote`, `"has""quote"`},
		{"", `""`},
	}
	for _, tt := range tests {
		got := QuoteIdentifier(tt.input)
		if got != tt.want {
			t.Errorf("QuoteIdentifier(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestInitWithCatalog_MissingCatalogSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	dir := t.TempDir()
	configDir := filepath.Join(dir, "config")
	os.MkdirAll(configDir, 0o755)
	// No catalog.sql

	sess, err := NewSession(":memory:")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer sess.Close()

	err = sess.InitWithCatalog(configDir)
	if err == nil {
		t.Fatal("expected error for missing catalog.sql")
	}
}

func TestInitWithCatalog_WithSchemas(t *testing.T) {
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
	os.WriteFile(filepath.Join(configDir, "schemas.sql"),
		[]byte("CREATE SCHEMA IF NOT EXISTS custom;\n"), 0o644)

	sess, err := NewSession(":memory:")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	if err := sess.InitWithCatalog(configDir); err != nil {
		t.Fatalf("init: %v", err)
	}

	// Custom schema should exist
	err = sess.Exec("CREATE TABLE custom.test_table (id INT)")
	if err != nil {
		t.Errorf("custom schema should exist: %v", err)
	}
}

func TestInitWithCatalog_WithSources(t *testing.T) {
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
	os.WriteFile(filepath.Join(configDir, "sources.sql"),
		[]byte("SET VARIABLE my_source = 'test_source';\n"), 0o644)

	sess, err := NewSession(":memory:")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	if err := sess.InitWithCatalog(configDir); err != nil {
		t.Fatalf("init: %v", err)
	}

	val, err := sess.QueryValue("SELECT getvariable('my_source')")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "test_source" {
		t.Errorf("my_source = %q, want test_source", val)
	}
}

func TestInitWithCatalog_WithExtensions(t *testing.T) {
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
	// extensions.sql loads json extension
	os.WriteFile(filepath.Join(configDir, "extensions.sql"),
		[]byte("INSTALL json; LOAD json;\n"), 0o644)

	sess, err := NewSession(":memory:")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	if err := sess.InitWithCatalog(configDir); err != nil {
		t.Fatalf("init: %v", err)
	}
}

func TestInitWithCatalog_WithSecrets(t *testing.T) {
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
	// secrets.sql with a variable (no real secrets in tests)
	os.WriteFile(filepath.Join(configDir, "secrets.sql"),
		[]byte("SET VARIABLE secret_loaded = 'yes';\n"), 0o644)

	sess, err := NewSession(":memory:")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	if err := sess.InitWithCatalog(configDir); err != nil {
		t.Fatalf("init: %v", err)
	}

	val, err := sess.QueryValue("SELECT getvariable('secret_loaded')")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "yes" {
		t.Errorf("secret_loaded = %q, want yes", val)
	}
}

func TestInitSandbox_WithCustomSettings(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	dir := t.TempDir()
	configDir := filepath.Join(dir, "config")
	os.MkdirAll(configDir, 0o755)

	// Create prod catalog
	prodCatalogPath := filepath.Join(dir, "prod_ducklake.sqlite")
	prodDataPath := filepath.Join(dir, "prod_data")
	prodConnStr := "ducklake:sqlite:" + prodCatalogPath

	setupSess, err := NewSession(":memory:")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := setupSess.Exec("ATTACH 'ducklake:sqlite:" + prodCatalogPath + "' AS lake (DATA_PATH '" + prodDataPath + "')"); err != nil {
		setupSess.Close()
		t.Fatalf("create prod: %v", err)
	}
	setupSess.Close()

	sandboxCatalog := filepath.Join(dir, "sandbox_ducklake.sqlite")

	// Add settings.sql and variables.sql
	os.WriteFile(filepath.Join(configDir, "settings.sql"),
		[]byte("SET threads = 1;\n"), 0o644)
	os.WriteFile(filepath.Join(configDir, "variables.sql"),
		[]byte("SET VARIABLE sandbox_var = 'sandbox_val';\n"), 0o644)

	sess, err := NewSession(":memory:")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	err = sess.InitSandbox(configDir, prodConnStr, prodDataPath, sandboxCatalog, "lake")
	if err != nil {
		t.Fatalf("InitSandbox: %v", err)
	}

	val, err := sess.QueryValue("SELECT getvariable('sandbox_var')")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "sandbox_val" {
		t.Errorf("sandbox_var = %q, want sandbox_val", val)
	}
}

func TestHasCDCChanges_AfterInsert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	sess, _ := setupCatalog(t)

	// Create a table and insert data to create a snapshot
	sess.Exec("CREATE TABLE staging.test_cdc (id INT)")
	sess.Exec("INSERT INTO staging.test_cdc VALUES (1)")

	// Refresh snapshot
	if err := sess.RefreshSnapshot(); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	has, err := sess.HasCDCChanges()
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	// After insert and refresh, there should be changes
	if !has {
		t.Error("expected changes after insert")
	}
}

func TestGetDagStartSnapshot_AfterInsert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	sess, _ := setupCatalog(t)

	// Create a table to generate a snapshot
	sess.Exec("CREATE TABLE staging.snap_test (id INT)")
	sess.Exec("INSERT INTO staging.snap_test VALUES (1)")

	snap, err := sess.GetDagStartSnapshot()
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	// dag_start_snapshot is set at init time (before inserts), so it's 0
	if snap != 0 {
		t.Errorf("snap = %d, want 0 (set before insert)", snap)
	}
}

func TestInitWithCatalog_MacroWithCatalogTemplate(t *testing.T) {
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
	// Macro using {{catalog}} template
	os.WriteFile(filepath.Join(configDir, "macros.sql"),
		[]byte("CREATE OR REPLACE MACRO my_catalog_macro() AS '{{catalog}}';\n"), 0o644)

	sess, err := NewSession(":memory:")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	if err := sess.InitWithCatalog(configDir); err != nil {
		t.Fatalf("init: %v", err)
	}

	val, err := sess.QueryValue("SELECT my_catalog_macro()")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "lake" {
		t.Errorf("my_catalog_macro() = %q, want lake", val)
	}
}
