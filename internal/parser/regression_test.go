// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package parser

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// --- Multi-statement models rejected ---

func TestParseModel_MultiStatement_Rejected(t *testing.T) {
	t.Parallel()
	// Before fix: extra statements were silently truncated.
	// After fix: multi-statement models produce an error.
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0o755)
	os.WriteFile(filepath.Join(modelsDir, "multi.sql"), []byte(
		"-- @kind: table\nSELECT 1 AS id; CALL some_function();"), 0o644)

	_, err := ParseModel(filepath.Join(modelsDir, "multi.sql"), dir)
	if err == nil {
		t.Fatal("multi-statement model should be rejected")
	}
	if !strings.Contains(err.Error(), "2 SQL statements") {
		t.Errorf("error should mention statement count, got: %v", err)
	}
}

func TestParseModel_SingleStatement_OK(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0o755)
	os.WriteFile(filepath.Join(modelsDir, "single.sql"), []byte(
		"-- @kind: table\nSELECT 1 AS id"), 0o644)

	model, err := ParseModel(filepath.Join(modelsDir, "single.sql"), dir)
	if err != nil {
		t.Fatalf("single-statement model should be accepted: %v", err)
	}
	if model.SQL != "SELECT 1 AS id" {
		t.Errorf("SQL = %q", model.SQL)
	}
}

// --- @sink: empty value rejected ---

func TestParseModel_EmptySink_Rejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "sync")
	os.MkdirAll(modelsDir, 0o755)
	os.WriteFile(filepath.Join(modelsDir, "empty.sql"), []byte(
		"-- @kind: merge\n-- @unique_key: id\n-- @sink:   \nSELECT 1 AS id"), 0o644)

	_, err := ParseModel(filepath.Join(modelsDir, "empty.sql"), dir)
	if err == nil {
		t.Fatal("empty @sink should be rejected")
	}
	if !strings.Contains(err.Error(), "@sink requires") {
		t.Errorf("error should mention @sink requires a name, got: %v", err)
	}
}

// --- splitStatements: block comments and double quotes ---

func TestParseModel_SemicolonInBlockComment_OK(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0o755)
	os.WriteFile(filepath.Join(modelsDir, "blockcomment.sql"), []byte(
		"-- @kind: table\nSELECT 1 /* docs; see section 3 */ AS id"), 0o644)

	model, err := ParseModel(filepath.Join(modelsDir, "blockcomment.sql"), dir)
	if err != nil {
		t.Fatalf("semicolon inside block comment should not split: %v", err)
	}
	if !strings.Contains(model.SQL, "/* docs; see section 3 */") {
		t.Errorf("block comment should be preserved, got: %s", model.SQL)
	}
}

func TestParseModel_SemicolonInDoubleQuotedIdentifier_OK(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0o755)
	os.WriteFile(filepath.Join(modelsDir, "dblquote.sql"), []byte(
		"-- @kind: table\nSELECT \"col;name\" FROM t"), 0o644)

	model, err := ParseModel(filepath.Join(modelsDir, "dblquote.sql"), dir)
	if err != nil {
		t.Fatalf("semicolon inside double-quoted identifier should not split: %v", err)
	}
	if !strings.Contains(model.SQL, `"col;name"`) {
		t.Errorf("double-quoted identifier should be preserved, got: %s", model.SQL)
	}
}

func TestParseModel_SemicolonInString_OK(t *testing.T) {
	t.Parallel()
	// Semicolon inside a string literal should NOT split
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0o755)
	os.WriteFile(filepath.Join(modelsDir, "stringsemi.sql"), []byte(
		"-- @kind: table\nSELECT 'hello;world' AS val"), 0o644)

	model, err := ParseModel(filepath.Join(modelsDir, "stringsemi.sql"), dir)
	if err != nil {
		t.Fatalf("semicolon inside string should not split: %v", err)
	}
	if !strings.Contains(model.SQL, "hello;world") {
		t.Errorf("SQL should preserve semicolon in string, got: %s", model.SQL)
	}
}
