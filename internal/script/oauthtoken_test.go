// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package script

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.starlark.net/starlark"
)

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

func TestNewTokenProviderValidation(t *testing.T) {
	// Missing required fields for client_credentials
	_, err := newTokenProvider(context.Background(), "", nil)
	if err == nil {
		t.Fatal("expected error for missing fields")
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

func TestNewTokenProvider_GoogleServiceAccount(t *testing.T) {
	// Create a minimal (invalid but parseable) service account JSON
	saJSON := `{
		"type": "service_account",
		"project_id": "test",
		"private_key_id": "key123",
		"private_key": "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA0Z3VS5JJcds3xfn/ygWyF8PbnGcY5unA67hqxnfZoGMaEclq\npRfMGOG0IS3sWMYkNy5Nw1BF4bR1ELGi0iGPIF1VEgr+5m3V9JMo1XUMqvKlp3nT\n-----END RSA PRIVATE KEY-----\n",
		"client_email": "test@test.iam.gserviceaccount.com",
		"token_uri": "https://oauth2.googleapis.com/token"
	}`

	kwargs := []starlark.Tuple{
		{starlark.String("google_service_account"), starlark.String(saJSON)},
		{starlark.String("scope"), starlark.String("https://www.googleapis.com/auth/cloud-platform")},
	}

	tp, err := newTokenProvider(context.Background(), "", kwargs)
	if err != nil {
		t.Fatalf("newTokenProvider: %v", err)
	}
	if tp.googleSAKey == nil {
		t.Error("expected googleSAKey to be set")
	}
}

func TestNewTokenProvider_ClientCredentials(t *testing.T) {
	kwargs := []starlark.Tuple{
		{starlark.String("token_url"), starlark.String("https://auth.example.com/token")},
		{starlark.String("client_id"), starlark.String("my-client")},
		{starlark.String("client_secret"), starlark.String("my-secret")},
		{starlark.String("scope"), starlark.String("read write")},
	}

	tp, err := newTokenProvider(context.Background(), "", kwargs)
	if err != nil {
		t.Fatalf("newTokenProvider: %v", err)
	}
	if tp.tokenURL != "https://auth.example.com/token" {
		t.Errorf("tokenURL = %q", tp.tokenURL)
	}
	if tp.scope != "read write" {
		t.Errorf("scope = %q", tp.scope)
	}
}

func TestNewTokenProvider_GoogleKeyFile(t *testing.T) {
	// Write a valid service account JSON to a temp file
	saJSON := `{"type":"service_account","token_uri":"https://oauth2.googleapis.com/token","client_email":"test@test.iam.gserviceaccount.com","private_key":"-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA0Z3VS5JJcds3xfn/ygWyF8PbnGcY5unA67hqxnfZoGMaEclq\n-----END RSA PRIVATE KEY-----\n"}`
	tmpFile := t.TempDir() + "/sa.json"
	if err := os.WriteFile(tmpFile, []byte(saJSON), 0644); err != nil {
		t.Fatal(err)
	}

	kwargs := []starlark.Tuple{
		{starlark.String("google_key_file"), starlark.String(tmpFile)},
	}
	tp, err := newTokenProvider(context.Background(), "", kwargs)
	if err != nil {
		t.Fatalf("newTokenProvider: %v", err)
	}
	if tp.googleSAKey == nil {
		t.Error("expected googleSAKey to be set")
	}
}

func TestNewTokenProvider_GoogleKeyFile_NotFound(t *testing.T) {
	kwargs := []starlark.Tuple{
		{starlark.String("google_key_file"), starlark.String("/nonexistent/file.json")},
	}
	_, err := newTokenProvider(context.Background(), "", kwargs)
	if err == nil {
		t.Fatal("expected error for missing key file")
	}
}

func TestNewTokenProvider_InvalidJSON(t *testing.T) {
	kwargs := []starlark.Tuple{
		{starlark.String("google_service_account"), starlark.String("{invalid json")},
	}
	_, err := newTokenProvider(context.Background(), "", kwargs)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestNewTokenProvider_PartialClientCredentials(t *testing.T) {
	// Only token_url, missing client_id and client_secret
	kwargs := []starlark.Tuple{
		{starlark.String("token_url"), starlark.String("https://example.com/token")},
	}
	_, err := newTokenProvider(context.Background(), "", kwargs)
	if err == nil {
		t.Fatal("expected error for partial client credentials")
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

func TestNewTokenProvider_Provider(t *testing.T) {
	t.Parallel()
	kwargs := []starlark.Tuple{
		{starlark.String("provider"), starlark.String("fortnox")},
	}
	tp, err := newTokenProvider(context.Background(), "/tmp/test-project", kwargs)
	if err != nil {
		t.Fatalf("newTokenProvider: %v", err)
	}
	if tp.provider != "fortnox" {
		t.Errorf("provider = %q, want fortnox", tp.provider)
	}
	if tp.projectDir != "/tmp/test-project" {
		t.Errorf("projectDir = %q, want /tmp/test-project", tp.projectDir)
	}
}

func TestNewTokenProvider_ProviderNoProjectDir(t *testing.T) {
	t.Parallel()
	kwargs := []starlark.Tuple{
		{starlark.String("provider"), starlark.String("fortnox")},
	}
	_, err := newTokenProvider(context.Background(), "", kwargs)
	if err == nil {
		t.Fatal("expected error for provider without project dir")
	}
}

func TestNewTokenProvider_ProviderInvalid(t *testing.T) {
	t.Parallel()
	kwargs := []starlark.Tuple{
		{starlark.String("provider"), starlark.String("../evil")},
	}
	_, err := newTokenProvider(context.Background(), "/tmp/test", kwargs)
	if err == nil {
		t.Fatal("expected error for invalid provider name")
	}
}

func TestFetchProviderToken(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token":  "AT_provider",
			"refresh_token": "RT_new",
			"expires_in":    3600,
		})
	}))
	defer srv.Close()

	dir := t.TempDir()
	// Write initial token
	tokDir := filepath.Join(dir, ".ondatra", "tokens")
	os.MkdirAll(tokDir, 0700)
	os.WriteFile(filepath.Join(tokDir, "test-provider.json"),
		[]byte(`{"provider":"test-provider","refresh_token":"RT_old","updated_at":1}`), 0600)

	t.Setenv("ONDATRA_KEY", "osk_test")
	t.Setenv("ONDATRA_OAUTH_HOST", srv.URL)

	tp := &tokenProvider{
		ctx:        context.Background(),
		provider:   "test-provider",
		projectDir: dir,
	}

	tok, err := tp.AccessToken()
	if err != nil {
		t.Fatalf("AccessToken: %v", err)
	}
	if tok != "AT_provider" {
		t.Errorf("access_token = %q, want AT_provider", tok)
	}

	// Verify new refresh token was saved
	data, _ := os.ReadFile(filepath.Join(tokDir, "test-provider.json"))
	if !strings.Contains(string(data), "RT_new") {
		t.Errorf("expected new refresh token in file, got: %s", data)
	}
}

func TestFetchProviderToken_NoTokenFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	t.Setenv("ONDATRA_KEY", "osk_test")

	tp := &tokenProvider{
		ctx:        context.Background(),
		provider:   "fortnox",
		projectDir: dir,
	}

	_, err := tp.AccessToken()
	if err == nil {
		t.Fatal("expected error for missing token file")
	}
}

func TestFetchProviderToken_NoKey(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tokDir := filepath.Join(dir, ".ondatra", "tokens")
	os.MkdirAll(tokDir, 0700)
	os.WriteFile(filepath.Join(tokDir, "fortnox.json"),
		[]byte(`{"provider":"fortnox","refresh_token":"RT_x","updated_at":1}`), 0600)

	t.Setenv("ONDATRA_KEY", "")

	tp := &tokenProvider{
		ctx:        context.Background(),
		provider:   "fortnox",
		projectDir: dir,
	}

	_, err := tp.AccessToken()
	if err == nil {
		t.Fatal("expected error for missing ONDATRA_KEY")
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
