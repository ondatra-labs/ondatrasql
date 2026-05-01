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

// --- @sink rejected with migration message (renamed to @push in v0.30.0) ---

func TestParseModel_SinkDirective_RejectedWithMigrationError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "sync")
	os.MkdirAll(modelsDir, 0o755)
	os.WriteFile(filepath.Join(modelsDir, "old_sink.sql"), []byte(
		"-- @kind: merge\n-- @unique_key: id\n-- @sink: hubspot_push\nSELECT 1::BIGINT AS id"), 0o644)

	_, err := ParseModel(filepath.Join(modelsDir, "old_sink.sql"), dir)
	if err == nil {
		t.Fatal("@sink should be rejected after rename to @push")
	}
	if !strings.Contains(err.Error(), "renamed to @push") {
		t.Errorf("error should mention rename to @push, got: %v", err)
	}
}

// --- @push: empty value rejected ---

func TestParseModel_EmptyPush_Rejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "sync")
	os.MkdirAll(modelsDir, 0o755)
	os.WriteFile(filepath.Join(modelsDir, "empty.sql"), []byte(
		"-- @kind: merge\n-- @unique_key: id\n-- @push:   \nSELECT 1 AS id"), 0o644)

	_, err := ParseModel(filepath.Join(modelsDir, "empty.sql"), dir)
	if err == nil {
		t.Fatal("empty @push should be rejected")
	}
	if !strings.Contains(err.Error(), "@push requires") {
		t.Errorf("error should mention @push requires a name, got: %v", err)
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

// --- Directives in SQL body rejected ---
//
// Before fix: a `-- @kind: ...`-style line anywhere in the file matched
// the directive regex and overwrote the model's field, silently. A leftover
// from a refactor or a generated-template glitch could land in the SQL body
// and quietly change the model's kind / unique_key / sink / etc.
//
// After fix: directives must be in the header (before any SQL line). A
// directive line that appears after SQL has started returns an error.

func TestParseModel_DirectiveAfterSQLBody_Rejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0o755)
	os.WriteFile(filepath.Join(modelsDir, "bad.sql"), []byte(
		"-- @kind: table\nSELECT 1 AS id\n-- @kind: view"), 0o644)

	_, err := ParseModel(filepath.Join(modelsDir, "bad.sql"), dir)
	if err == nil {
		t.Fatal("directive after SQL body should be rejected")
	}
	if !strings.Contains(err.Error(), "after SQL body") {
		t.Errorf("error should mention 'after SQL body', got: %v", err)
	}
	if !strings.Contains(err.Error(), "@kind") {
		t.Errorf("error should name the offending directive, got: %v", err)
	}
}

// Same check for push/unique_key/incremental — every directive shape, not
// just @kind. Picks @push because it's a recent addition and easy to leave
// behind when refactoring.
func TestParseModel_PushDirectiveAfterSQLBody_Rejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0o755)
	os.WriteFile(filepath.Join(modelsDir, "push_late.sql"), []byte(
		"-- @kind: append\nSELECT 1 AS id\n-- @push: my_push"), 0o644)

	_, err := ParseModel(filepath.Join(modelsDir, "push_late.sql"), dir)
	if err == nil {
		t.Fatal("@push after SQL body should be rejected")
	}
	if !strings.Contains(err.Error(), "after SQL body") {
		t.Errorf("error should mention 'after SQL body', got: %v", err)
	}
}

// Comments that aren't directives must still work in the SQL body — the
// fix only rejects directive-shaped comments, not all comments.
func TestParseModel_PlainCommentInSQLBody_OK(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0o755)
	os.WriteFile(filepath.Join(modelsDir, "comments.sql"), []byte(
		"-- @kind: table\nSELECT\n  -- this is a normal comment\n  id, name\nFROM source"), 0o644)

	model, err := ParseModel(filepath.Join(modelsDir, "comments.sql"), dir)
	if err != nil {
		t.Fatalf("plain comments in SQL body should be allowed, got error: %v", err)
	}
	if model.Kind != "table" {
		t.Errorf("model.Kind = %q, want table", model.Kind)
	}
}
