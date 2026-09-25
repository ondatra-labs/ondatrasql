// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package state

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// state.sql attaches its own catalog in its own session, so a failed
// Postgres ATTACH there echoes the password just like one in catalog.sql —
// and returns through Open, not through the duckdb session.
func TestOpen_FailedPostgresAttachHidesPassword(t *testing.T) {
	const pw = "S3ntinel-pw_42"
	t.Setenv("ODX_TEST_STATE_PASSWORD", pw)
	os.Unsetenv("ODX_TEST_STATE_HOST")

	dir := t.TempDir()
	stateSQL := "INSTALL postgres; LOAD postgres;\n" +
		"ATTACH 'dbname=ondatra_state host=${ODX_TEST_STATE_HOST} port=5432 user=postgres password=${ODX_TEST_STATE_PASSWORD} sslmode=disable' AS state (TYPE postgres);\n"
	if err := os.WriteFile(filepath.Join(dir, "state.sql"), []byte(stateSQL), 0o644); err != nil {
		t.Fatal(err)
	}

	st, err := Open(dir)
	if err == nil {
		_ = st.Close()
		t.Fatal("expected the attach to fail with no host")
	}
	msg := err.Error()
	if strings.Contains(msg, pw) {
		t.Fatalf("password leaked into the error:\n%s", msg)
	}
	if !strings.Contains(msg, "not set in the environment: ODX_TEST_STATE_HOST") {
		t.Fatalf("expected the unset variable to be named:\n%s", msg)
	}
}
