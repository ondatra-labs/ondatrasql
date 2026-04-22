// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.starlark.net/starlark"
)

func TestHttpUpload_InheritsBaseURL(t *testing.T) {
	t.Parallel()

	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok": true}`))
	}))
	defer srv.Close()

	dir := t.TempDir()
	testFile := filepath.Join(dir, "test.txt")
	os.WriteFile(testFile, []byte("hello"), 0644)

	cfg := &APIHTTPConfig{BaseURL: srv.URL}
	mod := httpModule(context.Background(), cfg)
	uploadFn := mod.Members["upload"]

	thread := &starlark.Thread{Name: "test"}
	_, err := starlark.Call(thread, uploadFn, nil, []starlark.Tuple{
		{starlark.String("url"), starlark.String("/v1/files")},
		{starlark.String("file"), starlark.String(testFile)},
	})
	if err != nil {
		t.Fatalf("upload: %v", err)
	}

	if gotPath != "/v1/files" {
		t.Errorf("path = %q, want /v1/files (base_url should prepend)", gotPath)
	}
}

func TestHttpUpload_InheritsAuth(t *testing.T) {
	t.Parallel()

	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok": true}`))
	}))
	defer srv.Close()

	dir := t.TempDir()
	testFile := filepath.Join(dir, "test.txt")
	os.WriteFile(testFile, []byte("hello"), 0644)

	os.Setenv("TEST_UPLOAD_AUTH_KEY", "test_secret_789")
	defer os.Unsetenv("TEST_UPLOAD_AUTH_KEY")

	cfg := &APIHTTPConfig{
		BaseURL: srv.URL,
		Auth:    map[string]any{"env": "TEST_UPLOAD_AUTH_KEY"},
	}
	mod := httpModule(context.Background(), cfg)
	uploadFn := mod.Members["upload"]

	thread := &starlark.Thread{Name: "test"}
	_, err := starlark.Call(thread, uploadFn, nil, []starlark.Tuple{
		{starlark.String("url"), starlark.String("/v1/files")},
		{starlark.String("file"), starlark.String(testFile)},
	})
	if err != nil {
		t.Fatalf("upload: %v", err)
	}

	if !strings.Contains(gotAuth, "test_secret_789") {
		t.Errorf("auth = %q, want to contain test_secret_789", gotAuth)
	}
}

func TestHttpUpload_WithoutConfig_NoBaseURL(t *testing.T) {
	t.Parallel()

	// Without API config, relative URLs should fail (no base_url to prepend)
	dir := t.TempDir()
	testFile := filepath.Join(dir, "test.txt")
	os.WriteFile(testFile, []byte("hello"), 0644)

	mod := httpModule(context.Background()) // no config
	uploadFn := mod.Members["upload"]

	thread := &starlark.Thread{Name: "test"}
	_, err := starlark.Call(thread, uploadFn, nil, []starlark.Tuple{
		{starlark.String("url"), starlark.String("/v1/files")},
		{starlark.String("file"), starlark.String(testFile)},
	})
	if err == nil {
		t.Error("expected error for relative URL without base_url")
	}
}
