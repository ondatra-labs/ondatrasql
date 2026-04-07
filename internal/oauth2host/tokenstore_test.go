// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package oauth2host

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

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
		{"a", true},           // too short
		{"-bad", true},        // starts with hyphen
		{"bad-", true},        // ends with hyphen
		{"Bad", true},         // uppercase
		{"../evil", true},     // path traversal
		{"foo/bar", true},     // slash
		{"foo bar", true},     // space
		{"", true},            // empty
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
	dir := t.TempDir()

	err := WriteToken(dir, "fortnox", "RT_test123")
	if err != nil {
		t.Fatalf("WriteToken: %v", err)
	}

	// Check file exists with correct permissions
	path := filepath.Join(dir, ".ondatra", "tokens", "fortnox.json")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("token file not found: %v", err)
	}
	// Windows doesn't honor Unix file mode bits
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0600 {
		t.Errorf("permissions = %o, want 0600", info.Mode().Perm())
	}

	// Read it back
	tf, err := ReadToken(dir, "fortnox")
	if err != nil {
		t.Fatalf("ReadToken: %v", err)
	}
	if tf.Provider != "fortnox" {
		t.Errorf("provider = %q, want fortnox", tf.Provider)
	}
	if tf.RefreshToken != "RT_test123" {
		t.Errorf("refresh_token = %q, want RT_test123", tf.RefreshToken)
	}
	if tf.UpdatedAt == 0 {
		t.Error("updated_at should be set")
	}
}

func TestReadToken_NotFound(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	_, err := ReadToken(dir, "nonexistent")
	if err == nil {
		t.Fatal("expected error for missing token")
	}
}

func TestWriteToken_InvalidProvider(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	err := WriteToken(dir, "../evil", "token")
	if err == nil {
		t.Fatal("expected error for invalid provider name")
	}
}

func TestWriteToken_Overwrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	WriteToken(dir, "fortnox", "old_token")
	WriteToken(dir, "fortnox", "new_token")

	tf, err := ReadToken(dir, "fortnox")
	if err != nil {
		t.Fatalf("ReadToken: %v", err)
	}
	if tf.RefreshToken != "new_token" {
		t.Errorf("refresh_token = %q, want new_token", tf.RefreshToken)
	}
}

func TestWriteAndReadLocalToken(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	err := WriteLocalToken(dir, "fortnox", "RT_local", "https://apps.fortnox.se/oauth-v1/token")
	if err != nil {
		t.Fatalf("WriteLocalToken: %v", err)
	}

	tf, err := ReadToken(dir, "fortnox")
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
