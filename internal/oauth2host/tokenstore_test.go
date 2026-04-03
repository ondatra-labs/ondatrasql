// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package oauth2host

import (
	"os"
	"path/filepath"
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
	if info.Mode().Perm() != 0600 {
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
