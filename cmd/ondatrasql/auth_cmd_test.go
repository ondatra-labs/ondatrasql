// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/config"
	"github.com/ondatra-labs/ondatrasql/internal/oauth2host"
)

// newTestServer creates an httptest server bound to IPv4 localhost
// to avoid failures in environments where IPv6 loopback is unavailable.
func encryptTestToken(plaintext, keyHex string) (string, error) {
	keyBytes, _ := hex.DecodeString(keyHex)
	block, err := aes.NewCipher(keyBytes)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	iv := make([]byte, 12)
	ciphertext := gcm.Seal(nil, iv, []byte(plaintext), nil)
	return hex.EncodeToString(iv) + ":" + hex.EncodeToString(ciphertext), nil
}

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

func TestRunAuth_Success(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "config"), 0755)

	var pollCount atomic.Int32
	var capturedEphemeralKey string

	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET" && r.URL.Path == "/providers/test-provider.json":
			json.NewEncoder(w).Encode(map[string]string{
				"name":         "test-provider",
				"auth_url":     "https://example.com/auth",
				"token_url":    "https://example.com/token",
				"client_id":    "cid",
				"scope":        "read",
				"redirect_uri": "https://oauth2.ondatra.sh/oauth/callback",
			})
		case r.Method == "POST" && r.URL.Path == "/oauth/register":
			var body map[string]string
			json.NewDecoder(r.Body).Decode(&body)
			capturedEphemeralKey = body["ephemeral_key"]
			json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		case r.Method == "POST" && r.URL.Path == "/oauth/poll":
			if pollCount.Add(1) < 2 {
				w.WriteHeader(404)
				return
			}
			// Encrypt with captured ephemeral key
			encrypted, _ := encryptTestToken("RT_success", capturedEphemeralKey)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"provider":      "test-provider",
				"refresh_token": encrypted,
				"encrypted":     true,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	t.Setenv("ONDATRA_KEY", "osk_testkey")
	t.Setenv("ONDATRA_OAUTH_HOST", srv.URL)

	cfg := &config.Config{ProjectDir: dir}
	err := runAuth(context.Background(), cfg, "test-provider")
	if err != nil {
		t.Fatalf("runAuth: %v", err)
	}

	// Verify token was saved
	data, err := os.ReadFile(filepath.Join(dir, ".ondatra", "tokens", "test-provider.json"))
	if err != nil {
		t.Fatalf("token file not found: %v", err)
	}
	var tf map[string]interface{}
	json.Unmarshal(data, &tf)
	if tf["refresh_token"] != "RT_success" {
		t.Errorf("refresh_token = %q, want RT_success", tf["refresh_token"])
	}
}

func TestRunAuth_MissingKey(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "config"), 0755)

	t.Setenv("ONDATRA_KEY", "")

	cfg := &config.Config{ProjectDir: dir}
	err := runAuth(context.Background(), cfg, "fortnox")
	if err == nil {
		t.Fatal("expected error for missing ONDATRA_KEY")
	}
}

func TestRunAuth_InvalidProvider(t *testing.T) {
	dir := t.TempDir()

	t.Setenv("ONDATRA_KEY", "osk_testkey")

	cfg := &config.Config{ProjectDir: dir}
	err := runAuth(context.Background(), cfg, "../evil")
	if err == nil {
		t.Fatal("expected error for invalid provider")
	}
}

func TestRunAuth_ProviderMismatch(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "config"), 0755)

	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET" && r.URL.Path == "/providers/fortnox.json":
			json.NewEncoder(w).Encode(map[string]string{
				"name": "fortnox", "auth_url": "https://example.com/auth",
				"token_url": "https://example.com/token", "client_id": "cid",
				"scope": "read", "redirect_uri": "https://example.com/callback",
			})
		case r.Method == "POST" && r.URL.Path == "/oauth/register":
			json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		case r.Method == "POST" && r.URL.Path == "/oauth/poll":
			// Return wrong provider
			json.NewEncoder(w).Encode(map[string]string{
				"provider":      "wrong-provider",
				"refresh_token": "RT_bad",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	t.Setenv("ONDATRA_KEY", "osk_testkey")
	t.Setenv("ONDATRA_OAUTH_HOST", srv.URL)

	cfg := &config.Config{ProjectDir: dir}
	err := runAuth(context.Background(), cfg, "fortnox")
	if err == nil {
		t.Fatal("expected error for provider mismatch")
	}
}

func TestRunAuth_Timeout(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "config"), 0755)

	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET" && r.URL.Path == "/providers/fortnox.json":
			json.NewEncoder(w).Encode(map[string]string{
				"name": "fortnox", "auth_url": "https://example.com/auth",
				"token_url": "https://example.com/token", "client_id": "cid",
				"scope": "read", "redirect_uri": "https://example.com/callback",
			})
		case r.Method == "POST" && r.URL.Path == "/oauth/register":
			json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		case r.Method == "POST" && r.URL.Path == "/oauth/poll":
			w.WriteHeader(404) // Always pending
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	t.Setenv("ONDATRA_KEY", "osk_testkey")
	t.Setenv("ONDATRA_OAUTH_HOST", srv.URL)

	// Use cancelled context to simulate timeout quickly
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := &config.Config{ProjectDir: dir}
	err := runAuth(ctx, cfg, "fortnox")
	if err == nil {
		t.Fatal("expected error for timeout/cancellation")
	}
}

func TestRunAuth_BadLicenseKey(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "config"), 0755)

	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET" && r.URL.Path == "/providers/fortnox.json":
			json.NewEncoder(w).Encode(map[string]string{
				"name": "fortnox", "auth_url": "https://example.com/auth",
				"token_url": "https://example.com/token", "client_id": "cid",
				"scope": "read", "redirect_uri": "https://example.com/callback",
			})
		case r.Method == "POST" && r.URL.Path == "/oauth/register":
			w.WriteHeader(403)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	t.Setenv("ONDATRA_KEY", "osk_badkey")
	t.Setenv("ONDATRA_OAUTH_HOST", srv.URL)

	cfg := &config.Config{ProjectDir: dir}
	err := runAuth(context.Background(), cfg, "fortnox")
	if err == nil {
		t.Fatal("expected error for bad license key")
	}
}

func TestRunAuthList_Success(t *testing.T) {
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/providers" || r.URL.Path == "/providers/" {
			json.NewEncoder(w).Encode([]string{"fortnox", "google-sheets"})
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	t.Setenv("ONDATRA_KEY", "osk_testkey")
	t.Setenv("ONDATRA_OAUTH_HOST", srv.URL)

	err := runAuthList(context.Background())
	if err != nil {
		t.Fatalf("runAuthList: %v", err)
	}
}

func TestRunAuthList_Empty(t *testing.T) {
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]string{})
	}))
	defer srv.Close()

	t.Setenv("ONDATRA_KEY", "osk_testkey")
	t.Setenv("ONDATRA_OAUTH_HOST", srv.URL)

	err := runAuthList(context.Background())
	if err != nil {
		t.Fatalf("runAuthList: %v", err)
	}
}

func TestRunAuthList_Error(t *testing.T) {
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	t.Setenv("ONDATRA_KEY", "osk_testkey")
	t.Setenv("ONDATRA_OAUTH_HOST", srv.URL)

	err := runAuthList(context.Background())
	if err == nil {
		t.Fatal("expected error for server error")
	}
}

func TestRunAuthList_Local(t *testing.T) {
	t.Setenv("ONDATRA_KEY", "")
	t.Setenv("MYAPI_CLIENT_ID", "id")
	t.Setenv("MYAPI_CLIENT_SECRET", "secret")
	t.Setenv("MYAPI_AUTH_URL", "https://example.com/auth")
	t.Setenv("MYAPI_TOKEN_URL", "https://example.com/token")
	t.Setenv("MYAPI_SCOPE", "read")

	err := runAuthList(context.Background())
	if err != nil {
		t.Fatalf("runAuthList local: %v", err)
	}
}

func TestRunAuthList_LocalEmpty(t *testing.T) {
	t.Setenv("ONDATRA_KEY", "")

	err := runAuthList(context.Background())
	if err != nil {
		t.Fatalf("runAuthList local empty: %v", err)
	}
}

func TestRunAuthLocal_MissingEnv(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "config"), 0755)

	t.Setenv("ONDATRA_KEY", "")
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

// TestRunAuthLocal_Success tests the local auth flow by directly testing the components:
// ExchangeCode + WriteLocalToken. The full localhost callback flow with browser
// is tested manually and in e2e.
func TestRunAuthLocal_Success(t *testing.T) {
	dir := t.TempDir()

	// Mock token endpoint
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

	// Exchange code directly
	result, err := oauth2host.ExchangeCode(context.Background(), tokenSrv.URL, "cid", "csecret", "TEST_CODE", "http://127.0.0.1:8888/callback")
	if err != nil {
		t.Fatalf("ExchangeCode: %v", err)
	}
	if result.RefreshToken != "RT_local" {
		t.Errorf("refresh_token = %q", result.RefreshToken)
	}

	// Save as local token
	err = oauth2host.WriteLocalToken(dir, "testlocal", result.RefreshToken, tokenSrv.URL)
	if err != nil {
		t.Fatalf("WriteLocalToken: %v", err)
	}

	// Verify
	tf, err := oauth2host.ReadToken(dir, "testlocal")
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

	t.Setenv("ONDATRA_KEY", "")
	t.Setenv("FORTNOX_CLIENT_ID", "")

	cfg := &config.Config{ProjectDir: dir}
	err := runAuth(context.Background(), cfg, "fortnox")
	if err == nil {
		t.Fatal("expected error (missing env)")
	}
	// Verify it went local path (error mentions .env variables, not ONDATRA_KEY)
	if !strings.Contains(err.Error(), "FORTNOX_CLIENT_ID") {
		t.Errorf("expected local path error, got: %v", err)
	}
}

func TestRunAuth_DispatchManaged(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "config"), 0755)

	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()

	t.Setenv("ONDATRA_KEY", "osk_testkey")
	t.Setenv("ONDATRA_OAUTH_HOST", srv.URL)

	cfg := &config.Config{ProjectDir: dir}
	err := runAuth(context.Background(), cfg, "fortnox")
	if err == nil {
		t.Fatal("expected error")
	}
	// Verify it went managed path (error mentions provider, not .env variables)
	if strings.Contains(err.Error(), "FORTNOX_CLIENT_ID") {
		t.Errorf("expected managed path error, got: %v", err)
	}
}
