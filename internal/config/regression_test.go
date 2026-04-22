// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package config

import (
	"os"
	"path/filepath"
	"testing"
)

// --- LoadEnvFile: no overwrite existing vars ---

func TestLoadEnvFile_NoOverwrite(t *testing.T) {
	dir := t.TempDir()
	envFile := filepath.Join(dir, ".env")
	os.WriteFile(envFile, []byte("TEST_EXISTING_VAR=from_file\n"), 0o644)

	t.Setenv("TEST_EXISTING_VAR", "from_env")
	LoadEnvFile(envFile)

	if os.Getenv("TEST_EXISTING_VAR") != "from_env" {
		t.Error("LoadEnvFile should not overwrite existing env vars")
	}
}

func TestLoadEnvFile_SetsUnset(t *testing.T) {
	dir := t.TempDir()
	envFile := filepath.Join(dir, ".env")
	os.WriteFile(envFile, []byte("TEST_NEW_LOADENV_VAR=hello\n"), 0o644)

	os.Unsetenv("TEST_NEW_LOADENV_VAR")
	LoadEnvFile(envFile)

	if os.Getenv("TEST_NEW_LOADENV_VAR") != "hello" {
		t.Errorf("expected 'hello', got %q", os.Getenv("TEST_NEW_LOADENV_VAR"))
	}
	os.Unsetenv("TEST_NEW_LOADENV_VAR")
}

// --- LoadEnvFile: escaped quotes ---

func TestLoadEnvFile_EscapedQuotes(t *testing.T) {
	dir := t.TempDir()
	envFile := filepath.Join(dir, ".env")
	os.WriteFile(envFile, []byte(`TEST_ESCAPED_Q="it\'s a \"test\""`+"\n"), 0o644)

	os.Unsetenv("TEST_ESCAPED_Q")
	LoadEnvFile(envFile)

	val := os.Getenv("TEST_ESCAPED_Q")
	if val != `it's a "test"` {
		t.Errorf("escaped quotes: got %q, want %q", val, `it's a "test"`)
	}
	os.Unsetenv("TEST_ESCAPED_Q")
}

// --- parseCatalogSQL: multi-line ATTACH ---

func TestParseCatalogSQL_MultiLine(t *testing.T) {
	dir := t.TempDir()
	configDir := filepath.Join(dir, "config")
	os.MkdirAll(configDir, 0o755)
	os.WriteFile(filepath.Join(configDir, "catalog.sql"), []byte(
		"INSTALL ducklake;\nLOAD ducklake;\nATTACH 'ducklake:sqlite:my.db'\n  AS lake (DATA_PATH 's3://bucket/');"), 0o644)

	info := parseCatalogSQL(configDir, dir)
	if info.Alias != "lake" {
		t.Errorf("alias = %q, want lake", info.Alias)
	}
}

func TestParseCatalogSQL_SingleLine(t *testing.T) {
	dir := t.TempDir()
	configDir := filepath.Join(dir, "config")
	os.MkdirAll(configDir, 0o755)
	os.WriteFile(filepath.Join(configDir, "catalog.sql"), []byte(
		"ATTACH 'ducklake:sqlite:my.db' AS mycat (DATA_PATH 'local/');"), 0o644)

	info := parseCatalogSQL(configDir, dir)
	if info.Alias != "mycat" {
		t.Errorf("alias = %q, want mycat", info.Alias)
	}
}
