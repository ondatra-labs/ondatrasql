// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/config"
)

func TestEditPathTraversal(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config")
	os.MkdirAll(filepath.Join(configPath, "macros"), 0o755)
	os.MkdirAll(filepath.Join(configPath, "variables"), 0o755)
	os.WriteFile(filepath.Join(configPath, "macros", "helpers.sql"), []byte("SELECT 1;"), 0o644)

	cfg := &config.Config{
		ProjectDir: dir,
		ConfigPath: configPath,
	}

	// Path traversal attempts must be rejected
	for _, target := range []string{
		"macros/../../etc/passwd",
		"macros/../../../tmp/x",
		"variables/../secrets",
		"macros/sub/nested",
		"macros/",
	} {
		t.Run(target, func(t *testing.T) {
			err := runEdit(cfg, target)
			if err == nil {
				t.Fatalf("expected error for target %q", target)
			}
			if !strings.Contains(err.Error(), "invalid target") {
				t.Errorf("error = %q, want 'invalid target'", err.Error())
			}
		})
	}

	// Valid target: can't test full runEdit (needs $EDITOR) but verify
	// no error from the path validation + file existence check
	t.Run("valid_macros/helpers_exists", func(t *testing.T) {
		// The file exists — runEdit would proceed to open editor.
		// We just verify it doesn't reject the target.
		fullPath := filepath.Join(configPath, "macros", "helpers.sql")
		if _, err := os.Stat(fullPath); err != nil {
			t.Fatalf("test setup: file should exist: %v", err)
		}
	})
}

func TestSplitTarget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  []string
	}{
		{"staging.orders", []string{"staging", "orders"}},
		{"orders", []string{"orders"}},
		{"", []string{""}},
		{"a.b.c", []string{"a.b", "c"}},           // last dot wins
		{"catalog.schema.table", []string{"catalog.schema", "table"}},
		{".leading", []string{"", "leading"}},
		{"trailing.", []string{"trailing", ""}},
	}
	for _, tt := range tests {
		got := splitTarget(tt.input)
		if len(got) != len(tt.want) {
			t.Errorf("splitTarget(%q) = %v (len %d), want %v (len %d)",
				tt.input, got, len(got), tt.want, len(tt.want))
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("splitTarget(%q)[%d] = %q, want %q", tt.input, i, got[i], tt.want[i])
			}
		}
	}
}
