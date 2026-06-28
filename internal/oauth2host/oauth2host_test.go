// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
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

func TestExchangeCode(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		if r.FormValue("grant_type") != "authorization_code" {
			t.Errorf("grant_type = %q", r.FormValue("grant_type"))
		}
		if r.FormValue("code") != "AUTH_CODE" {
			t.Errorf("code = %q", r.FormValue("code"))
		}
		if r.FormValue("client_secret") != "secret" {
			t.Errorf("client_secret = %q", r.FormValue("client_secret"))
		}
		json.NewEncoder(w).Encode(RefreshResult{
			AccessToken:  "AT_new",
			RefreshToken: "RT_new",
			ExpiresIn:    3600,
		})
	}))
	defer srv.Close()

	result, err := ExchangeCode(context.Background(), srv.URL, "client-id", "secret", "AUTH_CODE", "http://localhost/callback")
	if err != nil {
		t.Fatalf("ExchangeCode: %v", err)
	}
	if result.AccessToken != "AT_new" {
		t.Errorf("access_token = %q", result.AccessToken)
	}
	if result.RefreshToken != "RT_new" {
		t.Errorf("refresh_token = %q", result.RefreshToken)
	}
}

func TestExchangeCode_Error(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		w.Write([]byte(`{"error":"invalid_grant"}`))
	}))
	defer srv.Close()

	_, err := ExchangeCode(context.Background(), srv.URL, "id", "secret", "bad", "http://localhost/callback")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRefreshLocal(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		if r.FormValue("grant_type") != "refresh_token" {
			t.Errorf("grant_type = %q", r.FormValue("grant_type"))
		}
		if r.FormValue("refresh_token") != "RT_old" {
			t.Errorf("refresh_token = %q", r.FormValue("refresh_token"))
		}
		json.NewEncoder(w).Encode(RefreshResult{
			AccessToken:  "AT_refreshed",
			RefreshToken: "RT_rotated",
			ExpiresIn:    3600,
		})
	}))
	defer srv.Close()

	result, err := RefreshLocal(context.Background(), srv.URL, "client-id", "secret", "RT_old")
	if err != nil {
		t.Fatalf("RefreshLocal: %v", err)
	}
	if result.AccessToken != "AT_refreshed" {
		t.Errorf("access_token = %q", result.AccessToken)
	}
	if result.RefreshToken != "RT_rotated" {
		t.Errorf("refresh_token = %q", result.RefreshToken)
	}
}

func TestRefreshLocal_Error(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		w.Write([]byte(`{"error":"invalid_grant"}`))
	}))
	defer srv.Close()

	_, err := RefreshLocal(context.Background(), srv.URL, "id", "secret", "bad")
	if err == nil {
		t.Fatal("expected error")
	}
}
