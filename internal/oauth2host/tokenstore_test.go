// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package oauth2host

import (
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// newTestStore returns an in-memory DuckDB session with the tokens
// table created. Mirrors how state.Open prepares a real session.
func newTestStore(t *testing.T) *sql.DB {
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
	return db
}

func TestValidateProvider(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		wantErr bool
	}{
		{"fortnox", false},
		{"hub-spot", false},
		{"my-provider-123", false},
		{"ab", false},
		{"a", true},       // too short
		{"-bad", true},    // starts with hyphen
		{"bad-", true},    // ends with hyphen
		{"Bad", true},     // uppercase
		{"../evil", true}, // path traversal
		{"foo/bar", true}, // slash
		{"foo bar", true}, // space
		{"", true},        // empty
	}
	for _, tt := range tests {
		err := ValidateProvider(tt.name)
		if (err != nil) != tt.wantErr {
			t.Errorf("ValidateProvider(%q) error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
	}
}

func TestWriteAndReadToken(t *testing.T) {
	t.Parallel()
	db := newTestStore(t)

	if err := WriteToken(db, "fortnox", "RT_test123"); err != nil {
		t.Fatalf("WriteToken: %v", err)
	}

	tf, err := ReadToken(db, "fortnox")
	if err != nil {
		t.Fatalf("ReadToken: %v", err)
	}
	if tf.Provider != "fortnox" {
		t.Errorf("provider = %q, want fortnox", tf.Provider)
	}
	if tf.RefreshToken != "RT_test123" {
		t.Errorf("refresh_token = %q, want RT_test123", tf.RefreshToken)
	}
	if tf.Local {
		t.Error("expected Local = false for WriteToken")
	}
}

func TestReadToken_NotFound(t *testing.T) {
	t.Parallel()
	db := newTestStore(t)

	if _, err := ReadToken(db, "nonexistent"); err == nil {
		t.Fatal("expected error for missing token")
	}
}

func TestWriteToken_InvalidProvider(t *testing.T) {
	t.Parallel()
	db := newTestStore(t)

	if err := WriteToken(db, "../evil", "token"); err == nil {
		t.Fatal("expected error for invalid provider name")
	}
}

func TestWriteToken_Overwrite(t *testing.T) {
	t.Parallel()
	db := newTestStore(t)

	if err := WriteToken(db, "fortnox", "old_token"); err != nil {
		t.Fatalf("first WriteToken: %v", err)
	}
	if err := WriteToken(db, "fortnox", "new_token"); err != nil {
		t.Fatalf("second WriteToken: %v", err)
	}

	tf, err := ReadToken(db, "fortnox")
	if err != nil {
		t.Fatalf("ReadToken: %v", err)
	}
	if tf.RefreshToken != "new_token" {
		t.Errorf("refresh_token = %q, want new_token", tf.RefreshToken)
	}
}

func TestWriteAndReadLocalToken(t *testing.T) {
	t.Parallel()
	db := newTestStore(t)

	if err := WriteLocalToken(db, "fortnox", "RT_local", "https://apps.fortnox.se/oauth-v1/token"); err != nil {
		t.Fatalf("WriteLocalToken: %v", err)
	}

	tf, err := ReadToken(db, "fortnox")
	if err != nil {
		t.Fatalf("ReadToken: %v", err)
	}
	if !tf.Local {
		t.Error("expected Local = true")
	}
	if tf.TokenURL != "https://apps.fortnox.se/oauth-v1/token" {
		t.Errorf("token_url = %q", tf.TokenURL)
	}
	if tf.RefreshToken != "RT_local" {
		t.Errorf("refresh_token = %q, want RT_local", tf.RefreshToken)
	}
}

func TestProviderEnvPrefix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		provider string
		want     string
	}{
		{"fortnox", "FORTNOX"},
		{"google-sheets", "GOOGLE_SHEETS"},
		{"hub-spot", "HUB_SPOT"},
	}
	for _, tt := range tests {
		got := ProviderEnvPrefix(tt.provider)
		if got != tt.want {
			t.Errorf("ProviderEnvPrefix(%q) = %q, want %q", tt.provider, got, tt.want)
		}
	}
}

func TestListLocalProviders(t *testing.T) {
	t.Setenv("TESTPROV_CLIENT_ID", "id")
	t.Setenv("TESTPROV_CLIENT_SECRET", "secret")
	t.Setenv("TESTPROV_AUTH_URL", "https://example.com/auth")
	t.Setenv("TESTPROV_TOKEN_URL", "https://example.com/token")
	t.Setenv("TESTPROV_SCOPE", "read")
	t.Setenv("NOPAIR_CLIENT_ID", "id") // missing everything else
	t.Setenv("PARTIAL_CLIENT_ID", "id")
	t.Setenv("PARTIAL_CLIENT_SECRET", "secret") // missing AUTH_URL, TOKEN_URL, SCOPE

	providers := ListLocalProviders()

	found := false
	for _, p := range providers {
		if p == "testprov" {
			found = true
		}
		if p == "nopair" {
			t.Error("should not list provider without CLIENT_SECRET")
		}
		if p == "partial" {
			t.Error("should not list provider without AUTH_URL and TOKEN_URL")
		}
	}
	if !found {
		t.Error("expected testprov in list")
	}
}
