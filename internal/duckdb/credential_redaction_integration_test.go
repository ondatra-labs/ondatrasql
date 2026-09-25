// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package duckdb

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A DuckLake ATTACH over Postgres that fails echoes its whole keyword/value
// string, password included, twice: once from DuckLake and once from the
// postgres extension. The unset host here reproduces the incident that found
// it — `host=${PG_HOST}` with PG_HOST unset becomes `host= port=5432`, and
// libpq tries to resolve "port=5432". The error must keep that diagnosis,
// name the unset variable, and carry no password.
func TestInitWithCatalog_FailedPostgresAttachHidesPassword(t *testing.T) {
	const pw = "S3ntinel-pw_42"
	t.Setenv("ODX_TEST_PG_PASSWORD", pw)
	os.Unsetenv("ODX_TEST_PG_HOST")

	dir := t.TempDir()
	configDir := filepath.Join(dir, "config")
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		t.Fatal(err)
	}
	catalogSQL := "ATTACH 'ducklake:postgres:dbname=ducklake_catalog host=${ODX_TEST_PG_HOST} port=5432 user=postgres password=${ODX_TEST_PG_PASSWORD} sslmode=disable' AS lake (DATA_PATH '" + filepath.Join(dir, "data") + "');\n"
	if err := os.WriteFile(filepath.Join(configDir, "catalog.sql"), []byte(catalogSQL), 0o644); err != nil {
		t.Fatal(err)
	}

	sess, err := NewSession(":memory:?threads=2&memory_limit=1GB")
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	err = sess.InitWithCatalog(configDir)
	if err == nil {
		t.Fatal("expected the attach to fail with no host")
	}
	msg := err.Error()
	if strings.Contains(msg, pw) {
		t.Fatalf("password leaked into the error:\n%s", msg)
	}
	if !strings.Contains(msg, "password=[REDACTED]") {
		t.Fatalf("expected the redacted connection string to stay readable:\n%s", msg)
	}
	if !strings.Contains(msg, "not set in the environment: ODX_TEST_PG_HOST") {
		t.Fatalf("expected the unset variable to be named:\n%s", msg)
	}
}
