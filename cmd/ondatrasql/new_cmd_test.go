// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/config"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
)

func testConfig(t *testing.T) *config.Config {
	t.Helper()
	dir := t.TempDir()
	return &config.Config{
		ProjectDir: dir,
		ModelsPath: filepath.Join(dir, "models"),
	}
}

func TestRunNew_TwoParts_SQL(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)

	if err := runNew(cfg, "staging.orders.sql"); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(cfg.ModelsPath, "staging", "orders.sql")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected file at %s", path)
	}
}

func TestRunNew_TwoParts_Star(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)

	if err := runNew(cfg, "raw.api_data.star"); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(cfg.ModelsPath, "raw", "api_data.star")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected file at %s", path)
	}
}

func TestRunNew_ThreeParts_SQL(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)

	if err := runNew(cfg, "raw.api.orders.sql"); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(cfg.ModelsPath, "raw", "api", "orders.sql")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected file at %s, got err: %v", path, err)
	}
}

func TestRunNew_ThreeParts_Star(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)

	if err := runNew(cfg, "raw.api.orders.star"); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(cfg.ModelsPath, "raw", "api", "orders.star")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected file at %s", path)
	}
}

func TestRunNew_FourParts(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)

	if err := runNew(cfg, "raw.vendor.api.orders.sql"); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(cfg.ModelsPath, "raw", "vendor", "api", "orders.sql")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected file at %s", path)
	}
}

func TestRunNew_DirectoryOnly(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)

	if err := runNew(cfg, "staging"); err != nil {
		t.Fatal(err)
	}

	dir := filepath.Join(cfg.ModelsPath, "staging")
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("expected directory at %s", dir)
	}
	if !info.IsDir() {
		t.Error("expected a directory")
	}
}

func TestRunNew_NestedDirectoryOnly(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)

	if err := runNew(cfg, "raw.api"); err != nil {
		t.Fatal(err)
	}

	dir := filepath.Join(cfg.ModelsPath, "raw", "api")
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("expected directory at %s", dir)
	}
	if !info.IsDir() {
		t.Error("expected a directory")
	}
}

func TestRunNew_ErrorOneName_WithExt(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)

	err := runNew(cfg, "orders.sql")
	if err == nil {
		t.Fatal("expected error for single-part target with extension")
	}
	if !strings.Contains(err.Error(), "schema.name") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunNew_ErrorEmptySegment(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)

	err := runNew(cfg, "raw..orders.sql")
	if err == nil {
		t.Fatal("expected error for empty segment")
	}
	if !strings.Contains(err.Error(), "empty path segment") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunNew_ErrorAlreadyExists(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)

	if err := runNew(cfg, "staging.orders.sql"); err != nil {
		t.Fatal(err)
	}
	err := runNew(cfg, "staging.orders.sql")
	if err == nil {
		t.Fatal("expected error for existing file")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunNew_UppercaseExtension_SQL(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	if err := runNew(cfg, "staging.orders.SQL"); err != nil {
		t.Fatal(err)
	}
	// File should be created with lowercase extension
	path := filepath.Join(cfg.ModelsPath, "staging", "orders.sql")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected file at %s", path)
	}
}

func TestRunNew_MixedCaseExtension_Star(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	if err := runNew(cfg, "raw.api_data.Star"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(cfg.ModelsPath, "raw", "api_data.star")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected file at %s", path)
	}
}

func TestRunNew_ErrorUnsupportedExtension(t *testing.T) {
	t.Parallel()
	tests := []struct {
		target string
		ext    string
	}{
		{"staging.orders.py", ".py"},
		{"raw.api.data.sh", ".sh"},
		{"staging.orders.go", ".go"},
		{"staging.orders.yaml", ".yaml"},
		{"staging.orders.json", ".json"},
		{"staging.orders.csv", ".csv"},
		// Uppercase variants
		{"staging.orders.PY", ".py"},
		{"staging.orders.JSON", ".json"},
		{"staging.orders.Go", ".go"},
	}
	for _, tt := range tests {
		t.Run(tt.target, func(t *testing.T) {
			t.Parallel()
			cfg := testConfig(t)
			err := runNew(cfg, tt.target)
			if err == nil {
				t.Fatalf("expected error for %s", tt.target)
			}
			if !strings.Contains(err.Error(), "unsupported extension") {
				t.Errorf("unexpected error: %v", err)
			}
			if !strings.Contains(err.Error(), tt.ext) {
				t.Errorf("error should mention %q: %v", tt.ext, err)
			}
		})
	}
}

func TestRunNew_ThreeParts_ParseModel_Target(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)

	if err := runNew(cfg, "raw.api.orders.sql"); err != nil {
		t.Fatal(err)
	}

	// Verify ParseModel produces a 2-part target with __ separator
	filePath := filepath.Join(cfg.ModelsPath, "raw", "api", "orders.sql")
	model, err := parser.ParseModel(filePath, cfg.ProjectDir)
	if err != nil {
		t.Fatalf("ParseModel: %v", err)
	}
	if model.Target != "raw.api__orders" {
		t.Errorf("target = %q, want %q", model.Target, "raw.api__orders")
	}
	// Must be exactly 2 parts (schema.table)
	parts := strings.Split(model.Target, ".")
	if len(parts) != 2 {
		t.Errorf("target has %d parts, want 2: %q", len(parts), model.Target)
	}
}

func TestGenerateTemplate(t *testing.T) {
	t.Parallel()
	got := generateTemplate("staging", "orders")
	if !strings.Contains(got, "staging.orders") {
		t.Errorf("template missing target name, got:\n%s", got)
	}
	if !strings.Contains(got, "@kind: table") {
		t.Errorf("template missing @kind directive, got:\n%s", got)
	}
	if !strings.Contains(got, "SELECT") {
		t.Errorf("template missing SELECT, got:\n%s", got)
	}
}

func TestGenerateStarlarkTemplate(t *testing.T) {
	t.Parallel()
	got := generateStarlarkTemplate("raw", "api_data")
	if !strings.Contains(got, "raw.api_data") {
		t.Errorf("template missing target name, got:\n%s", got)
	}
	if !strings.Contains(got, "@kind: append") {
		t.Errorf("template missing @kind directive, got:\n%s", got)
	}
	if !strings.Contains(got, "http.get") {
		t.Errorf("template missing http.get call, got:\n%s", got)
	}
	if !strings.Contains(got, "save.row") {
		t.Errorf("template missing save.row call, got:\n%s", got)
	}
}
