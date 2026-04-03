// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/config"
)

// newTestServer creates an httptest server bound to IPv4 localhost
// to avoid failures in environments where IPv6 loopback is unavailable.
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
			json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		case r.Method == "GET" && r.URL.Path[:12] == "/oauth/poll/":
			if pollCount.Add(1) < 2 {
				w.WriteHeader(404)
				return
			}
			json.NewEncoder(w).Encode(map[string]string{
				"provider":      "test-provider",
				"refresh_token": "RT_success",
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
		case r.Method == "GET" && r.URL.Path[:12] == "/oauth/poll/":
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
		case r.Method == "GET" && r.URL.Path[:12] == "/oauth/poll/":
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

	t.Setenv("ONDATRA_OAUTH_HOST", srv.URL)

	err := runAuthList(context.Background())
	if err == nil {
		t.Fatal("expected error for server error")
	}
}
