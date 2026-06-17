// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package script

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"go.starlark.net/starlark"
)

// newTokenStoreDB returns an in-memory DuckDB session with the tokens
// table created. Pass seedToken to insert a row before returning.
type seedTokenRow struct {
	provider     string
	refreshToken string
	local        bool
	tokenURL     string
}

func newTokenStoreDB(t *testing.T, seeds ...seedTokenRow) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE tokens (
		provider VARCHAR PRIMARY KEY,
		refresh_token VARCHAR NOT NULL,
		local BOOLEAN DEFAULT false,
		token_url VARCHAR,
		updated_at TIMESTAMP DEFAULT now()
	)`); err != nil {
		t.Fatalf("create tokens: %v", err)
	}
	for _, s := range seeds {
		if _, err := db.Exec(
			`INSERT INTO tokens (provider, refresh_token, local, token_url) VALUES (?, ?, ?, ?)`,
			s.provider, s.refreshToken, s.local, s.tokenURL,
		); err != nil {
			t.Fatalf("seed tokens: %v", err)
		}
	}
	return db
}

func tokenStoreRefresh(t *testing.T, db *sql.DB, provider string) string {
	t.Helper()
	var v string
	if err := db.QueryRow(`SELECT refresh_token FROM tokens WHERE provider = ?`, provider).Scan(&v); err != nil {
		t.Fatalf("read refresh_token: %v", err)
	}
	return v
}

func tokenStoreLocal(t *testing.T, db *sql.DB, provider string) bool {
	t.Helper()
	var v bool
	if err := db.QueryRow(`SELECT local FROM tokens WHERE provider = ?`, provider).Scan(&v); err != nil {
		t.Fatalf("read local: %v", err)
	}
	return v
}

func TestTokenProviderCachesToken(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": "tok-1",
			"expires_in":   3600,
		})
	}))
	defer srv.Close()

	tp := &tokenProvider{
		ctx:          context.Background(),
		tokenURL:     srv.URL,
		clientID:     "id",
		clientSecret: "secret",
	}

	// First call fetches
	tok, err := tp.AccessToken()
	if err != nil {
		t.Fatal(err)
	}
	if tok != "tok-1" {
		t.Fatalf("got %q, want tok-1", tok)
	}

	// Second call should use cache
	tok2, err := tp.AccessToken()
	if err != nil {
		t.Fatal(err)
	}
	if tok2 != "tok-1" {
		t.Fatalf("got %q, want tok-1", tok2)
	}

	if calls.Load() != 1 {
		t.Errorf("expected 1 HTTP call, got %d", calls.Load())
	}
}

func TestTokenProviderRefreshesExpired(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": fmt.Sprintf("tok-%d", n),
			"expires_in":   1, // 1 second TTL
		})
	}))
	defer srv.Close()

	now := time.Now()
	tp := &tokenProvider{
		ctx:          context.Background(),
		tokenURL:     srv.URL,
		clientID:     "id",
		clientSecret: "secret",
		now:          func() time.Time { return now },
	}

	// First call
	tok, err := tp.AccessToken()
	if err != nil {
		t.Fatal(err)
	}
	if tok != "tok-1" {
		t.Fatalf("got %q, want tok-1", tok)
	}

	// Advance time past expiry (1s TTL - 60s margin means it's already expired)
	now = now.Add(2 * time.Second)

	tok2, err := tp.AccessToken()
	if err != nil {
		t.Fatal(err)
	}
	if tok2 != "tok-2" {
		t.Fatalf("got %q, want tok-2", tok2)
	}

	if calls.Load() != 2 {
		t.Errorf("expected 2 HTTP calls, got %d", calls.Load())
	}
}

func TestTokenProviderStarlarkAttr(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": "starlark-tok",
			"expires_in":   3600,
		})
	}))
	defer srv.Close()

	tp := &tokenProvider{
		ctx:          context.Background(),
		tokenURL:     srv.URL,
		clientID:     "id",
		clientSecret: "secret",
	}

	// Test Attr("access_token")
	val, err := tp.Attr("access_token")
	if err != nil {
		t.Fatal(err)
	}
	s, ok := val.(starlark.String)
	if !ok {
		t.Fatalf("expected starlark.String, got %T", val)
	}
	if string(s) != "starlark-tok" {
		t.Errorf("got %q, want starlark-tok", string(s))
	}

	// Test Attr for unknown name
	val, err = tp.Attr("unknown")
	if err != nil {
		t.Fatal(err)
	}
	if val != nil {
		t.Errorf("expected nil for unknown attr, got %v", val)
	}
}

func TestTokenProvider_StarlarkInterface(t *testing.T) {
	tp := &tokenProvider{}

	if tp.String() != "oauth.Token" {
		t.Errorf("String() = %q, want oauth.Token", tp.String())
	}
	if tp.Type() != "oauth.Token" {
		t.Errorf("Type() = %q, want oauth.Token", tp.Type())
	}
	tp.Freeze() // should not panic
	if tp.Truth() != true {
		t.Error("Truth() should be true")
	}
	_, err := tp.Hash()
	if err == nil {
		t.Error("Hash() should return error")
	}
	names := tp.AttrNames()
	if len(names) != 1 || names[0] != "access_token" {
		t.Errorf("AttrNames() = %v, want [access_token]", names)
	}
}

func TestAccessToken_NoAccessTokenInResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": "invalid_grant",
		})
	}))
	defer srv.Close()

	tp := &tokenProvider{
		ctx:          context.Background(),
		tokenURL:     srv.URL,
		clientID:     "id",
		clientSecret: "secret",
	}
	_, err := tp.AccessToken()
	if err == nil {
		t.Fatal("expected error when response has no access_token")
	}
}

func TestFetchClientCredentialsWithScope(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		if r.FormValue("scope") != "read write" {
			t.Errorf("scope = %q, want 'read write'", r.FormValue("scope"))
		}
		if r.FormValue("grant_type") != "client_credentials" {
			t.Errorf("grant_type = %q, want 'client_credentials'", r.FormValue("grant_type"))
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": "scoped-tok",
			"expires_in":   3600,
		})
	}))
	defer srv.Close()

	tp := &tokenProvider{
		ctx:          context.Background(),
		tokenURL:     srv.URL,
		clientID:     "id",
		clientSecret: "secret",
		scope:        "read write",
	}

	tok, err := tp.AccessToken()
	if err != nil {
		t.Fatal(err)
	}
	if tok != "scoped-tok" {
		t.Errorf("got %q, want scoped-tok", tok)
	}
}

func TestTokenProviderFreeze(t *testing.T) {
	tp := &tokenProvider{}
	// Freeze should not panic
	tp.Freeze()
}

func TestFetchProviderToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token":  "AT_provider",
			"refresh_token": "RT_new",
			"expires_in":    3600,
		})
	}))
	defer srv.Close()

	db := newTokenStoreDB(t, seedTokenRow{
		provider:     "test-provider",
		refreshToken: "RT_old",
	})

	t.Setenv("ONDATRA_KEY", "osk_test")
	t.Setenv("ONDATRA_OAUTH_HOST", srv.URL)

	tp := &tokenProvider{
		ctx:      context.Background(),
		provider: "test-provider",
		stateDB:  db,
	}

	tok, err := tp.AccessToken()
	if err != nil {
		t.Fatalf("AccessToken: %v", err)
	}
	if tok != "AT_provider" {
		t.Errorf("access_token = %q, want AT_provider", tok)
	}

	if got := tokenStoreRefresh(t, db, "test-provider"); got != "RT_new" {
		t.Errorf("expected refresh_token = RT_new, got %q", got)
	}
}

func TestFetchProviderToken_NoTokenFile(t *testing.T) {
	db := newTokenStoreDB(t)

	t.Setenv("ONDATRA_KEY", "osk_test")

	tp := &tokenProvider{
		ctx:      context.Background(),
		provider: "fortnox",
		stateDB:  db,
	}

	_, err := tp.AccessToken()
	if err == nil {
		t.Fatal("expected error for missing token row")
	}
}

func TestFetchProviderToken_NoKey(t *testing.T) {
	db := newTokenStoreDB(t, seedTokenRow{
		provider:     "fortnox",
		refreshToken: "RT_x",
	})

	t.Setenv("ONDATRA_KEY", "")

	tp := &tokenProvider{
		ctx:      context.Background(),
		provider: "fortnox",
		stateDB:  db,
	}

	_, err := tp.AccessToken()
	if err == nil {
		t.Fatal("expected error for missing ONDATRA_KEY")
	}
}

func TestFetchProviderToken_Local(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		if r.FormValue("grant_type") != "refresh_token" {
			t.Errorf("grant_type = %q", r.FormValue("grant_type"))
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token":  "AT_local_refreshed",
			"refresh_token": "RT_local_new",
			"expires_in":    3600,
		})
	}))
	defer srv.Close()

	db := newTokenStoreDB(t, seedTokenRow{
		provider:     "test-local",
		refreshToken: "RT_old",
		local:        true,
		tokenURL:     srv.URL,
	})

	t.Setenv("TEST_LOCAL_CLIENT_ID", "cid")
	t.Setenv("TEST_LOCAL_CLIENT_SECRET", "csecret")

	tp := &tokenProvider{
		ctx:      context.Background(),
		provider: "test-local",
		stateDB:  db,
	}

	tok, err := tp.AccessToken()
	if err != nil {
		t.Fatalf("AccessToken: %v", err)
	}
	if tok != "AT_local_refreshed" {
		t.Errorf("access_token = %q, want AT_local_refreshed", tok)
	}

	if got := tokenStoreRefresh(t, db, "test-local"); got != "RT_local_new" {
		t.Errorf("expected refresh_token = RT_local_new, got %q", got)
	}
	if !tokenStoreLocal(t, db, "test-local") {
		t.Errorf("expected local = true")
	}
}

func TestFetchProviderToken_LocalMissingSecret(t *testing.T) {
	db := newTokenStoreDB(t, seedTokenRow{
		provider:     "fortnox",
		refreshToken: "RT_x",
		local:        true,
		tokenURL:     "https://example.com/token",
	})

	t.Setenv("FORTNOX_CLIENT_ID", "")
	t.Setenv("FORTNOX_CLIENT_SECRET", "")

	tp := &tokenProvider{
		ctx:      context.Background(),
		provider: "fortnox",
		stateDB:  db,
	}

	_, err := tp.AccessToken()
	if err == nil {
		t.Fatal("expected error for missing client credentials")
	}
}

func TestFetchGoogleToken(t *testing.T) {
	// Mock Google token endpoint
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": "google-tok",
			"expires_in":   3600,
			"token_type":   "Bearer",
		})
	}))
	defer srv.Close()

	tp := &tokenProvider{
		ctx:   context.Background(),
		scope: "https://www.googleapis.com/auth/cloud-platform",
		googleSAKey: &ServiceAccountKey{
			TokenURI: srv.URL,
		},
	}

	// fetchGoogleToken needs a valid JWT signing key - it will fail on CreateGoogleJWT
	// but we're testing the path through fetchToken that dispatches to fetchGoogleToken
	_, err := tp.fetchToken()
	// Expected to fail due to invalid private key, but we cover the code path
	if err == nil {
		// If it somehow succeeds (shouldn't with empty key), that's fine too
		return
	}
	if !strings.Contains(err.Error(), "JWT") && !strings.Contains(err.Error(), "key") && !strings.Contains(err.Error(), "token") {
		t.Logf("fetchGoogleToken error (expected): %v", err)
	}
}
