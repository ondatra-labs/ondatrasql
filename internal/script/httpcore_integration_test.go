// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
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
	"strings"
	"testing"
	"time"
)

func TestDoHTTPGet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("method = %s, want GET", r.Method)
		}
		w.Header().Set("X-Custom", "hello")
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]string{"msg": "ok"})
	}))
	defer srv.Close()

	resp, err := DoHTTP(context.Background(), "GET", srv.URL, nil, nil, httpOptions{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if resp.Headers.Get("X-Custom") != "hello" {
		t.Errorf("X-Custom = %q, want 'hello'", resp.Headers.Get("X-Custom"))
	}
	if resp.JSON == nil {
		t.Fatal("expected JSON to be parsed")
	}
}

func TestDoHTTPPost(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", r.Header.Get("Content-Type"))
		}
		w.WriteHeader(201)
		w.Write([]byte("created"))
	}))
	defer srv.Close()

	resp, err := DoHTTP(context.Background(), "POST", srv.URL, []byte(`{"key":"val"}`), nil, httpOptions{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 201 {
		t.Errorf("status = %d, want 201", resp.StatusCode)
	}
	if string(resp.Body) != "created" {
		t.Errorf("body = %q, want 'created'", string(resp.Body))
	}
}

func TestDoHTTPCustomHeaders(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok123" {
			t.Errorf("Authorization = %q", r.Header.Get("Authorization"))
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	headers := map[string]string{"Authorization": "Bearer tok123"}
	resp, err := DoHTTP(context.Background(), "GET", srv.URL, nil, headers, httpOptions{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status = %d", resp.StatusCode)
	}
}

func TestDoHTTPLinkHeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Link", `<https://api.example.com/page2>; rel="next"`)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	resp, err := DoHTTP(context.Background(), "GET", srv.URL, nil, nil, httpOptions{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Links["next"] != "https://api.example.com/page2" {
		t.Errorf("links = %v", resp.Links)
	}
}

func TestDoHTTPNonJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("plain text"))
	}))
	defer srv.Close()

	resp, err := DoHTTP(context.Background(), "GET", srv.URL, nil, nil, httpOptions{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if resp.JSON != nil {
		t.Errorf("expected nil JSON for plain text, got %v", resp.JSON)
	}
	if string(resp.Body) != "plain text" {
		t.Errorf("body = %q", string(resp.Body))
	}
}

func TestDoHTTPContextCanceled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := DoHTTP(ctx, "GET", srv.URL, nil, nil, httpOptions{Timeout: 5 * time.Second})
	if err == nil {
		t.Fatal("expected error from canceled context")
	}
}

func TestDoHTTPWithRetrySuccess(t *testing.T) {
	attempt := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt++
		if attempt < 3 {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	opts := httpOptions{Timeout: 5 * time.Second, Retry: 3, Backoff: 10}
	resp, err := DoHTTPWithRetry(context.Background(), "GET", srv.URL, nil, nil, opts)
	if err != nil {
		t.Fatalf("expected success after retries, got: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if attempt != 3 {
		t.Errorf("attempts = %d, want 3", attempt)
	}
}

func TestDoHTTPWithRetry429(t *testing.T) {
	attempt := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt++
		if attempt == 1 {
			w.WriteHeader(429)
			return
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	opts := httpOptions{Timeout: 5 * time.Second, Retry: 1, Backoff: 10}
	resp, err := DoHTTPWithRetry(context.Background(), "GET", srv.URL, nil, nil, opts)
	if err != nil {
		t.Fatalf("expected success after 429 retry, got: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestDoHTTPWithRetryExhausted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	opts := httpOptions{Timeout: 5 * time.Second, Retry: 2, Backoff: 10}
	_, err := DoHTTPWithRetry(context.Background(), "GET", srv.URL, nil, nil, opts)
	if err == nil {
		t.Fatal("expected error after exhausted retries")
	}
}

func TestDoHTTPWithRetryNoRetryOn4xx(t *testing.T) {
	attempt := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt++
		w.WriteHeader(400)
	}))
	defer srv.Close()

	opts := httpOptions{Timeout: 5 * time.Second, Retry: 3, Backoff: 10}
	resp, err := DoHTTPWithRetry(context.Background(), "GET", srv.URL, nil, nil, opts)
	if err != nil {
		t.Fatalf("4xx should not cause retry error: %v", err)
	}
	if resp.StatusCode != 400 {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
	if attempt != 1 {
		t.Errorf("attempts = %d, want 1 (no retry on 4xx)", attempt)
	}
}

func TestDigestAuthIntegration(t *testing.T) {
	const (
		testUser  = "admin"
		testPass  = "secret"
		testNonce = "test-nonce-12345"
		testRealm = "test-realm"
	)

	requestCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		authHeader := r.Header.Get("Authorization")

		if !strings.HasPrefix(authHeader, "Digest ") {
			// First request: send challenge
			w.Header().Set("WWW-Authenticate",
				fmt.Sprintf(`Digest realm="%s", nonce="%s", qop="auth", algorithm=MD5`, testRealm, testNonce))
			w.WriteHeader(401)
			w.Write([]byte("Unauthorized"))
			return
		}

		// Second request: verify digest auth
		if !strings.Contains(authHeader, fmt.Sprintf(`username="%s"`, testUser)) {
			t.Errorf("missing username in auth header: %s", authHeader)
		}
		if !strings.Contains(authHeader, fmt.Sprintf(`realm="%s"`, testRealm)) {
			t.Errorf("missing realm in auth header: %s", authHeader)
		}
		if !strings.Contains(authHeader, fmt.Sprintf(`nonce="%s"`, testNonce)) {
			t.Errorf("missing nonce in auth header: %s", authHeader)
		}
		if !strings.Contains(authHeader, `qop=auth`) {
			t.Errorf("missing qop in auth header: %s", authHeader)
		}
		if !strings.Contains(authHeader, `response="`) {
			t.Errorf("missing response in auth header: %s", authHeader)
		}

		w.WriteHeader(200)
		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer srv.Close()

	opts := httpOptions{
		Timeout:    5 * time.Second,
		Retry:      1,
		Backoff:    10,
		DigestAuth: &digestCredentials{Username: testUser, Password: testPass},
	}

	resp, err := DoHTTPWithRetry(context.Background(), "GET", srv.URL+"/protected", nil, map[string]string{}, opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if requestCount != 2 {
		t.Errorf("requestCount = %d, want 2 (challenge + auth)", requestCount)
	}
}

func TestBasicAuthKwarg(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	headers := map[string]string{
		"Authorization": BasicAuth("user", "pass"),
	}
	opts := httpOptions{Timeout: 5 * time.Second}

	resp, err := DoHTTPWithRetry(context.Background(), "GET", srv.URL, nil, headers, opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if !strings.HasPrefix(gotAuth, "Basic ") {
		t.Errorf("Authorization = %q, want Basic prefix", gotAuth)
	}
}

func TestDigestAuthNotRetryAfterSuccess(t *testing.T) {
	// Verify digest auth is only attempted once (DigestAuth set to nil after use)
	requestCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		// Always return 401 with Digest challenge
		w.Header().Set("WWW-Authenticate", `Digest realm="test", nonce="n1", qop="auth"`)
		w.WriteHeader(401)
	}))
	defer srv.Close()

	opts := httpOptions{
		Timeout:    5 * time.Second,
		Retry:      3,
		Backoff:    10,
		DigestAuth: &digestCredentials{Username: "user", Password: "pass"},
	}

	resp, err := DoHTTPWithRetry(context.Background(), "GET", srv.URL, nil, map[string]string{}, opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// After digest retry, DigestAuth is nil so subsequent 401s are returned as-is
	if resp.StatusCode != 401 {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
	// Should be exactly 2: initial + one digest retry
	if requestCount != 2 {
		t.Errorf("requestCount = %d, want 2", requestCount)
	}
}

func TestDoHTTPWithRetry429RetryAfter(t *testing.T) {
	var attempts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(429)
			return
		}
		w.WriteHeader(200)
		w.Write([]byte(`"ok"`))
	}))
	defer srv.Close()

	opts := httpOptions{
		Timeout: 10 * time.Second,
		Retry:   2,
		Backoff: 10,
	}

	resp, err := DoHTTPWithRetry(context.Background(), "GET", srv.URL, nil, nil, opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if attempts != 2 {
		t.Errorf("attempts = %d, want 2", attempts)
	}
}

func TestDoHTTPWithRetryNetworkError(t *testing.T) {
	// Use a server that immediately closes
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close() // Close immediately to cause connection errors

	opts := httpOptions{
		Timeout: 1 * time.Second,
		Retry:   1,
		Backoff: 10,
	}

	_, err := DoHTTPWithRetry(context.Background(), "GET", srv.URL, nil, nil, opts)
	if err == nil {
		t.Fatal("expected error for closed server")
	}
}

func TestDoHTTPInvalidMethod(t *testing.T) {
	// Invalid HTTP method causes NewRequestWithContext to fail
	opts := httpOptions{Timeout: 1 * time.Second}
	_, err := DoHTTP(context.Background(), "INVALID METHOD", "http://localhost", nil, nil, opts)
	if err == nil {
		t.Fatal("expected error for invalid method")
	}
}
