// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package state

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// setupTestConfig writes a minimal config/state.sql that attaches an
// unencrypted local file. Returns the configPath to pass to Open.
func setupTestConfig(t *testing.T) string {
	t.Helper()
	configPath := t.TempDir()
	stateFile := filepath.Join(t.TempDir(), "state.duckdb")
	//escapesqlcheck:trusted-input stateFile is t.TempDir() — test-controlled, no user input
	stateSQL := fmt.Sprintf("ATTACH '%s' AS state;\n", stateFile)
	if err := os.WriteFile(filepath.Join(configPath, "state.sql"), []byte(stateSQL), 0o644); err != nil {
		t.Fatalf("write state.sql: %v", err)
	}
	return configPath
}

func TestOpen_RunsStateSQL(t *testing.T) {
	t.Parallel()
	configPath := setupTestConfig(t)
	st, err := Open(configPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	if st.DB() == nil {
		t.Fatal("DB() returned nil")
	}

	// init.sql should have created the state tables. A trivial query
	// against any of them validates both the ATTACH and the init.
	var n int
	if err := st.DB().QueryRow("SELECT count(*) FROM sync_evt").Scan(&n); err != nil {
		t.Fatalf("query sync_evt: %v", err)
	}
	if n != 0 {
		t.Errorf("fresh sync_evt has %d rows, want 0", n)
	}
}

// TestOpen_USEStateSurvivesConnRecreation pins the robustness fix: the
// connection-init hook re-runs `USE state` on every connection, so a
// query that lands on a pool-recreated connection still resolves
// unqualified names into the state catalog rather than memory.main.
func TestOpen_USEStateSurvivesConnRecreation(t *testing.T) {
	t.Parallel()
	configPath := setupTestConfig(t)
	st, err := Open(configPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()
	db := st.DB()

	currentDB := func() string {
		var name string
		if err := db.QueryRow("SELECT current_database()").Scan(&name); err != nil {
			t.Fatalf("current_database: %v", err)
		}
		return name
	}

	if got := currentDB(); got != "state" {
		t.Fatalf("bootstrap connection current_database = %q, want state", got)
	}

	// Expire the pooled connection so the next query is forced onto a
	// freshly-created one — the same thing database/sql does on a
	// driver.ErrBadConn. Without the per-connection USE state in the
	// init hook, this connection would default to memory.main.
	db.SetConnMaxLifetime(time.Nanosecond)
	time.Sleep(5 * time.Millisecond)

	if got := currentDB(); got != "state" {
		t.Errorf("recreated connection current_database = %q, want state", got)
	}
}

// TestOpen_EncryptedStateFile exercises the default init path: an ATTACH
// with ENCRYPTION_KEY from ONDATRA_STATE_KEY. It verifies the refresh
// token is unreadable in the raw file bytes and that a wrong key fails.
func TestOpen_EncryptedStateFile(t *testing.T) {
	key := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef")) // 32 bytes
	t.Setenv("ONDATRA_STATE_KEY", key)

	configPath := t.TempDir()
	stateFile := filepath.Join(t.TempDir(), "state.duckdb")
	//escapesqlcheck:trusted-input stateFile is t.TempDir() — test-controlled, no user input
	stateSQL := fmt.Sprintf("ATTACH '%s' AS state (ENCRYPTION_KEY '${ONDATRA_STATE_KEY}');\n", stateFile)
	if err := os.WriteFile(filepath.Join(configPath, "state.sql"), []byte(stateSQL), 0o644); err != nil {
		t.Fatalf("write state.sql: %v", err)
	}

	const secret = "rt_SUPERSECRET_TOKEN_DO_NOT_LEAK"
	st, err := Open(configPath)
	if err != nil {
		t.Fatalf("Open (encrypted): %v", err)
	}
	if _, err := st.DB().Exec(
		`INSERT INTO tokens (provider, refresh_token) VALUES ('test', ?)`, secret); err != nil {
		t.Fatalf("insert token: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// No on-disk artifact (main file or WAL) may contain the plaintext.
	artifacts, _ := filepath.Glob(stateFile + "*")
	if len(artifacts) == 0 {
		t.Fatalf("no state file written at %s", stateFile)
	}
	for _, f := range artifacts {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		if bytes.Contains(raw, []byte(secret)) {
			t.Errorf("plaintext token found in %s — file not encrypted", filepath.Base(f))
		}
	}

	// Reopening with the correct key reads the token back.
	st2, err := Open(configPath)
	if err != nil {
		t.Fatalf("reopen (correct key): %v", err)
	}
	var got string
	if err := st2.DB().QueryRow(
		`SELECT refresh_token FROM tokens WHERE provider = 'test'`).Scan(&got); err != nil {
		t.Fatalf("read token back: %v", err)
	}
	_ = st2.Close()
	if got != secret {
		t.Errorf("read token = %q, want %q", got, secret)
	}

	// The wrong key must fail to open the encrypted file.
	t.Setenv("ONDATRA_STATE_KEY", base64.StdEncoding.EncodeToString([]byte("ffffffffffffffffffffffffffffffff")))
	if st3, err := Open(configPath); err == nil {
		_ = st3.Close()
		t.Error("Open with wrong ENCRYPTION_KEY succeeded, want failure")
	}
}

func TestOpen_MissingStateSQL(t *testing.T) {
	t.Parallel()
	configPath := t.TempDir() // no state.sql created
	_, err := Open(configPath)
	if err == nil {
		t.Fatal("expected error for missing state.sql, got nil")
	}
}

func TestGC_NoOpOnEmptyStore(t *testing.T) {
	t.Parallel()
	configPath := setupTestConfig(t)
	st, err := Open(configPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	if err := GC(st); err != nil {
		t.Errorf("GC on empty store: %v", err)
	}
	// Idempotent — second call should also succeed.
	if err := GC(st); err != nil {
		t.Errorf("second GC: %v", err)
	}
}

func TestGC_NilStateOK(t *testing.T) {
	t.Parallel()
	if err := GC(nil); err != nil {
		t.Errorf("GC(nil) = %v, want nil", err)
	}
}
