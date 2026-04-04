// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package parser

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseYAMLModel_Basic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0755)

	yamlContent := `
kind: append
incremental: report_date
source: gam_report
config:
  network_code: "12345"
  dimensions:
    - AD_UNIT_NAME
    - DATE
description: GAM report data
`
	path := filepath.Join(modelsDir, "gam.yaml")
	os.WriteFile(path, []byte(yamlContent), 0644)

	model, err := ParseModel(path, dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if model.Kind != "append" {
		t.Errorf("expected kind 'append', got %q", model.Kind)
	}
	if model.Source != "gam_report" {
		t.Errorf("expected source 'gam_report', got %q", model.Source)
	}
	if model.Incremental != "report_date" {
		t.Errorf("expected incremental 'report_date', got %q", model.Incremental)
	}
	if model.Target != "raw.gam" {
		t.Errorf("expected target 'raw.gam', got %q", model.Target)
	}
	if !model.IsScript {
		t.Error("expected IsScript to be true")
	}
	if model.ScriptType != ScriptTypeStarlark {
		t.Errorf("expected ScriptType Starlark, got %q", model.ScriptType)
	}
	if model.Description != "GAM report data" {
		t.Errorf("expected description 'GAM report data', got %q", model.Description)
	}
	if model.SourceConfig == nil {
		t.Fatal("expected SourceConfig to be set")
	}
	if model.SourceConfig["network_code"] != "12345" {
		t.Errorf("expected network_code '12345', got %v", model.SourceConfig["network_code"])
	}
}

func TestParseYAMLModel_EnvExpansion(t *testing.T) {
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0755)

	t.Setenv("TEST_NETWORK_CODE", "99999")

	yamlContent := `
kind: append
source: gam_report
config:
  network_code: ${TEST_NETWORK_CODE}
`
	path := filepath.Join(modelsDir, "gam.yaml")
	os.WriteFile(path, []byte(yamlContent), 0644)

	model, err := ParseModel(path, dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// YAML parses unquoted numbers as integers
	if model.SourceConfig["network_code"] != 99999 {
		t.Errorf("expected network_code 99999, got %v (%T)", model.SourceConfig["network_code"], model.SourceConfig["network_code"])
	}
}

func TestParseYAMLModel_MissingSource(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0755)

	yamlContent := `
kind: append
config:
  key: value
`
	path := filepath.Join(modelsDir, "bad.yaml")
	os.WriteFile(path, []byte(yamlContent), 0644)

	_, err := ParseModel(path, dir)
	if err == nil {
		t.Fatal("expected error for missing source")
	}
	if !contains(err.Error(), "source") {
		t.Fatalf("expected source-related error, got: %v", err)
	}
}

func TestParseYAMLModel_Validation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0755)

	// Invalid kind
	yamlContent := `
kind: invalid_kind
source: some_source
`
	path := filepath.Join(modelsDir, "bad.yaml")
	os.WriteFile(path, []byte(yamlContent), 0644)

	_, err := ParseModel(path, dir)
	if err == nil {
		t.Fatal("expected validation error for invalid kind")
	}
}

func TestParseYAMLModel_InvalidSource(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0755)

	tests := []struct {
		name   string
		source string
	}{
		{"hyphen", "foo-bar"},
		{"starts with digit", "1source"},
		{"space", "foo bar"},
		{"dot", "foo.bar"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			content := "source: " + tt.source + "\n"
			path := filepath.Join(modelsDir, tt.name+".yaml")
			os.WriteFile(path, []byte(content), 0644)

			_, err := ParseModel(path, dir)
			if err == nil {
				t.Fatalf("expected error for source %q", tt.source)
			}
			if !contains(err.Error(), "source") {
				t.Fatalf("expected source error, got: %v", err)
			}
		})
	}
}

func TestParseYAMLModel_ReservedKeywordSource(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0755)

	for _, keyword := range []string{"for", "if", "return", "def", "None", "True"} {
		t.Run(keyword, func(t *testing.T) {
			t.Parallel()
			content := "source: " + keyword + "\n"
			path := filepath.Join(modelsDir, keyword+".yaml")
			os.WriteFile(path, []byte(content), 0644)

			_, err := ParseModel(path, dir)
			if err == nil {
				t.Fatalf("expected error for reserved keyword source %q", keyword)
			}
			if !contains(err.Error(), "reserved") {
				t.Fatalf("expected reserved keyword error, got: %v", err)
			}
		})
	}
}

func TestParseYAMLModel_InvalidConfigKey(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0755)

	yamlContent := `
source: valid_source
config:
  page-size: 100
`
	path := filepath.Join(modelsDir, "bad_key.yaml")
	os.WriteFile(path, []byte(yamlContent), 0644)

	_, err := ParseModel(path, dir)
	if err == nil {
		t.Fatal("expected error for config key with hyphen")
	}
	if !contains(err.Error(), "config key") {
		t.Fatalf("expected config key error, got: %v", err)
	}
}

func TestParseYAMLModel_ReservedKeywordConfigKey(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0755)

	yamlContent := `
source: valid_source
config:
  if: 1
`
	path := filepath.Join(modelsDir, "reserved_key.yaml")
	os.WriteFile(path, []byte(yamlContent), 0644)

	_, err := ParseModel(path, dir)
	if err == nil {
		t.Fatal("expected error for reserved keyword config key")
	}
	if !contains(err.Error(), "reserved") {
		t.Fatalf("expected reserved keyword error, got: %v", err)
	}
}

func TestParseYAMLModel_PathValidation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Path with __ should be rejected
	modelsDir := filepath.Join(dir, "models", "raw", "bad__name")
	os.MkdirAll(modelsDir, 0755)

	path := filepath.Join(modelsDir, "test.yaml")
	os.WriteFile(path, []byte("source: my_source\n"), 0644)

	_, err := ParseModel(path, dir)
	if err == nil {
		t.Fatal("expected error for path with __")
	}
	if !contains(err.Error(), "__") {
		t.Fatalf("expected __ error, got: %v", err)
	}
}

func TestParseYAMLModel_RejectsViewKind(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0755)

	path := filepath.Join(modelsDir, "bad.yaml")
	os.WriteFile(path, []byte("kind: view\nsource: my_source\n"), 0644)

	_, err := ParseModel(path, dir)
	if err == nil {
		t.Fatal("expected error for view kind in YAML model")
	}
	if !contains(err.Error(), "view") {
		t.Fatalf("expected view error, got: %v", err)
	}
}

func TestParseYAMLModel_RejectsEventsKind(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0755)

	path := filepath.Join(modelsDir, "bad.yaml")
	os.WriteFile(path, []byte("kind: events\nsource: my_source\n"), 0644)

	_, err := ParseModel(path, dir)
	if err == nil {
		t.Fatal("expected error for events kind in YAML model")
	}
	if !contains(err.Error(), "events") {
		t.Fatalf("expected events error, got: %v", err)
	}
}

func TestParseYAMLModel_DefaultKind(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0755)

	yamlContent := `
source: my_source
`
	path := filepath.Join(modelsDir, "simple.yaml")
	os.WriteFile(path, []byte(yamlContent), 0644)

	model, err := ParseModel(path, dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if model.Kind != "table" {
		t.Errorf("expected default kind 'table', got %q", model.Kind)
	}
}

func TestParseYAMLModel_YMLExtension(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)

	yamlContent := `
kind: merge
source: crm_sync
unique_key: id
`
	path := filepath.Join(modelsDir, "contacts.yml")
	os.WriteFile(path, []byte(yamlContent), 0644)

	model, err := ParseModel(path, dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if model.Target != "staging.contacts" {
		t.Errorf("expected target 'staging.contacts', got %q", model.Target)
	}
	if model.Kind != "merge" {
		t.Errorf("expected kind 'merge', got %q", model.Kind)
	}
}

func TestParseYAMLModel_ExposeWithKey_Rejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "mart")
	os.MkdirAll(modelsDir, 0755)

	yamlContent := `
kind: table
source: my_source
expose: order_id
`
	path := filepath.Join(modelsDir, "test.yaml")
	os.WriteFile(path, []byte(yamlContent), 0644)

	_, err := ParseModel(path, dir)
	if err == nil {
		t.Fatal("expected error: @expose not supported for scripts (YAML)")
	}
}

func TestParseYAMLModel_ExposeRejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "mart")
	os.MkdirAll(modelsDir, 0755)

	yamlContent := `
kind: table
source: my_source
expose: true
`
	path := filepath.Join(modelsDir, "test.yaml")
	os.WriteFile(path, []byte(yamlContent), 0644)

	_, err := ParseModel(path, dir)
	if err == nil {
		t.Fatal("expected error: @expose not supported for scripts")
	}
}

func TestParseYAMLModel_ExposeInvalidType(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "mart")
	os.MkdirAll(modelsDir, 0755)

	yamlContent := `
source: my_source
expose: 123
`
	path := filepath.Join(modelsDir, "test.yaml")
	os.WriteFile(path, []byte(yamlContent), 0644)

	_, err := ParseModel(path, dir)
	if err == nil {
		t.Fatal("expected error for non-bool/non-string expose value")
	}
	if !contains(err.Error(), "expose") {
		t.Fatalf("expected expose error, got: %v", err)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsSubstring(s, sub))
}

func containsSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
