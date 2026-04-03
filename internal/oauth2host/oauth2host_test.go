// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package oauth2host

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

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

func TestFetchProviderConfig(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/providers/fortnox.json" {
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(ProviderConfig{
			Name:        "fortnox",
			AuthURL:     "https://apps.fortnox.se/oauth-v1/auth",
			TokenURL:    "https://apps.fortnox.se/oauth-v1/token",
			ClientID:    "test-client-id",
			Scope:       "companyinformation",
			RedirectURI: "https://oauth2.ondatra.sh/oauth/callback",
		})
	}))
	defer srv.Close()

	cfg, err := FetchProviderConfig(context.Background(), srv.URL, "fortnox")
	if err != nil {
		t.Fatalf("FetchProviderConfig: %v", err)
	}
	if cfg.Name != "fortnox" {
		t.Errorf("name = %q, want fortnox", cfg.Name)
	}
	if cfg.ClientID != "test-client-id" {
		t.Errorf("client_id = %q, want test-client-id", cfg.ClientID)
	}
}

func TestFetchProviderConfig_NotFound(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, http.NotFoundHandler())
	defer srv.Close()

	_, err := FetchProviderConfig(context.Background(), srv.URL, "unknown")
	if err == nil {
		t.Fatal("expected error for unknown provider")
	}
}

func TestRegister(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/oauth/register" {
			http.NotFound(w, r)
			return
		}
		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body)
		if body["license_key"] == "bad" {
			w.WriteHeader(403)
			return
		}
		json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}))
	defer srv.Close()

	err := Register(context.Background(), srv.URL, "fortnox", "abc123", "good-key")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	err = Register(context.Background(), srv.URL, "fortnox", "abc123", "bad")
	if err == nil {
		t.Fatal("expected error for bad key")
	}
}

func TestPoll_Pending(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, http.NotFoundHandler())
	defer srv.Close()

	_, err := Poll(context.Background(), srv.URL, "abc123")
	if err != ErrPending {
		t.Fatalf("expected ErrPending, got %v", err)
	}
}

func TestPoll_Success(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(PollResult{
			Provider:     "fortnox",
			RefreshToken: "RT_test",
		})
	}))
	defer srv.Close()

	result, err := Poll(context.Background(), srv.URL, "abc123")
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if result.RefreshToken != "RT_test" {
		t.Errorf("refresh_token = %q, want RT_test", result.RefreshToken)
	}
}

func TestRefresh(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/oauth/refresh" {
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(RefreshResult{
			AccessToken:  "AT_new",
			RefreshToken: "RT_new",
			ExpiresIn:    3600,
		})
	}))
	defer srv.Close()

	result, err := Refresh(context.Background(), srv.URL, "fortnox", "RT_old", "key")
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if result.AccessToken != "AT_new" {
		t.Errorf("access_token = %q, want AT_new", result.AccessToken)
	}
	if result.RefreshToken != "RT_new" {
		t.Errorf("refresh_token = %q, want RT_new", result.RefreshToken)
	}
}

func TestListProviders(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]string{"fortnox", "google-sheets"})
	}))
	defer srv.Close()

	providers, err := ListProviders(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("ListProviders: %v", err)
	}
	if len(providers) != 2 {
		t.Errorf("len = %d, want 2", len(providers))
	}
}

func TestListProviders_Error(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	_, err := ListProviders(context.Background(), srv.URL)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestHost_Default(t *testing.T) {
	t.Setenv("ONDATRA_OAUTH_HOST", "")
	if got := Host(); got != DefaultHost {
		t.Errorf("Host() = %q, want %q", got, DefaultHost)
	}
}

func TestHost_Override(t *testing.T) {
	t.Setenv("ONDATRA_OAUTH_HOST", "https://custom.example.com")
	if got := Host(); got != "https://custom.example.com" {
		t.Errorf("Host() = %q, want custom", got)
	}
}
