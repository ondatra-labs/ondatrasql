// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/ondatra-labs/ondatrasql/internal/config"
	"github.com/ondatra-labs/ondatrasql/internal/oauth2host"
)

// newTestServer creates an httptest server bound to IPv4 localhost to avoid
// failures in environments where IPv6 loopback is unavailable.
func newTestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("skipping: cannot bind IPv4 loopback: %v", err)
	}
	srv := &httptest.Server{
		Listener: l,
		Config:   &http.Server{Handler: handler},
	}
	srv.Start()
	return srv
}

func TestRunAuth_InvalidProvider(t *testing.T) {
	dir := t.TempDir()

	cfg := &config.Config{ProjectDir: dir}
	err := runAuth(context.Background(), cfg, "../evil")
	if err == nil {
		t.Fatal("expected error for invalid provider")
	}
}

func TestRunAuthList_Local(t *testing.T) {
	t.Setenv("MYAPI_CLIENT_ID", "id")
	t.Setenv("MYAPI_CLIENT_SECRET", "secret")
	t.Setenv("MYAPI_AUTH_URL", "https://example.com/auth")
	t.Setenv("MYAPI_TOKEN_URL", "https://example.com/token")
	t.Setenv("MYAPI_SCOPE", "read")

	if err := runAuthList(context.Background()); err != nil {
		t.Fatalf("runAuthList local: %v", err)
	}
}

func TestRunAuthList_LocalEmpty(t *testing.T) {
	if err := runAuthList(context.Background()); err != nil {
		t.Fatalf("runAuthList local empty: %v", err)
	}
}

func TestRunAuthLocal_MissingEnv(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "config"), 0755)

	t.Setenv("FORTNOX_CLIENT_ID", "")
	t.Setenv("FORTNOX_CLIENT_SECRET", "")
	t.Setenv("FORTNOX_AUTH_URL", "")
	t.Setenv("FORTNOX_TOKEN_URL", "")

	cfg := &config.Config{ProjectDir: dir}
	err := runAuthLocal(context.Background(), cfg, "fortnox")
	if err == nil {
		t.Fatal("expected error for missing env variables")
	}
}

// TestRunAuthLocal_Success tests the local auth flow by directly exercising its
// components: ExchangeCode + WriteLocalToken. The full localhost callback flow
// with a browser is tested manually and in e2e.
func TestRunAuthLocal_Success(t *testing.T) {
	tokenSrv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		if r.FormValue("grant_type") != "authorization_code" {
			t.Errorf("grant_type = %q", r.FormValue("grant_type"))
		}
		if r.FormValue("code") != "TEST_CODE" {
			t.Errorf("code = %q", r.FormValue("code"))
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token":  "AT_local",
			"refresh_token": "RT_local",
			"expires_in":    3600,
		})
	}))
	defer tokenSrv.Close()

	result, err := oauth2host.ExchangeCode(context.Background(), tokenSrv.URL, "cid", "csecret", "TEST_CODE", "http://127.0.0.1:8888/callback")
	if err != nil {
		t.Fatalf("ExchangeCode: %v", err)
	}
	if result.RefreshToken != "RT_local" {
		t.Errorf("refresh_token = %q", result.RefreshToken)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE tokens (
		provider VARCHAR PRIMARY KEY,
		refresh_token VARCHAR NOT NULL,
		local BOOLEAN DEFAULT false,
		token_url VARCHAR,
		updated_at TIMESTAMP DEFAULT now()
	)`); err != nil {
		t.Fatalf("create tokens: %v", err)
	}

	if err := oauth2host.WriteLocalToken(db, "testlocal", result.RefreshToken, tokenSrv.URL); err != nil {
		t.Fatalf("WriteLocalToken: %v", err)
	}

	tf, err := oauth2host.ReadToken(db, "testlocal")
	if err != nil {
		t.Fatalf("ReadToken: %v", err)
	}
	if !tf.Local {
		t.Error("expected Local = true")
	}
	if tf.RefreshToken != "RT_local" {
		t.Errorf("refresh_token = %q", tf.RefreshToken)
	}
}

func TestRunAuth_DispatchLocal(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "config"), 0755)

	t.Setenv("FORTNOX_CLIENT_ID", "")

	cfg := &config.Config{ProjectDir: dir}
	err := runAuth(context.Background(), cfg, "fortnox")
	if err == nil {
		t.Fatal("expected error (missing env)")
	}
	// The local path's error names the .env variables.
	if !strings.Contains(err.Error(), "FORTNOX_CLIENT_ID") {
		t.Errorf("expected local path error, got: %v", err)
	}
}
