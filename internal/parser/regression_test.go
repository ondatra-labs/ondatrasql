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

// --- Unknown directives rejected (R7 #2) ---

// TestParseModel_UnknownDirective_Rejected pins that a typo like
// `-- @ftech: ...` is rejected at parse time instead of silently
// falling through to the generic comment branch (which would let the
// directive be ignored as if absent).
func TestParseModel_UnknownDirective_Rejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	if err := os.MkdirAll(modelsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cases := []string{
		"-- @ftech\nSELECT 1 AS id",                             // typo of @fetch
		"-- @kind: table\n-- @uniqe_key: id\nSELECT 1 AS id",    // typo of @unique_key
		"-- @kind: table\n-- @incremenntal: x\nSELECT 1 AS id",  // typo of @incremental
		"-- @kind: table\n-- @bogus: anything\nSELECT 1 AS id",  // genuinely unknown
	}
	for i, src := range cases {
		path := filepath.Join(modelsDir, "bad_"+string(rune('a'+i))+".sql")
		if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := ParseModel(path, dir)
		if err == nil {
			t.Errorf("case %d: expected unknown-directive error, got nil for: %q", i, src)
			continue
		}
		if !strings.Contains(err.Error(), "unknown directive") {
			t.Errorf("case %d: error should mention 'unknown directive', got: %v", i, err)
		}
	}
}

// TestParseModel_BlockCommentDirectiveTreatedAsComment pins R9 #5:
// `/* @ftech */` is treated as a regular block comment — content
// inside `/* ... */` is stripped before classification, so a
// directive-shaped token within a block comment is interpreted as
// "comment about a directive", not as a typoed directive. This
// matches user intent: if you wanted @ftech to be parsed as a
// directive, you wouldn't wrap it in a block comment.
//
// (R8 #11 originally pinned the opposite behaviour — that was
// reversed in R9 once we realised intent: a block-commented token
// is a comment, full stop.)
func TestParseModel_BlockCommentDirectiveTreatedAsComment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	if err := os.MkdirAll(modelsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	src := "/* @ftech */\n-- @kind: table\nSELECT 1 AS id"
	path := filepath.Join(modelsDir, "block.sql")
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	model, err := ParseModel(path, dir)
	if err != nil {
		t.Fatalf("block-commented @ftech must parse cleanly, got: %v", err)
	}
	if model.Kind != "table" {
		t.Errorf("expected @kind: table to be parsed past the block comment, got kind=%q", model.Kind)
	}
}

// TestParseModel_MultiLineBlockCommentInHeader pins R9 #3: a header
// block comment that spans multiple lines doesn't flip inHeader=false
// on the first line. Real directives below it parse normally.
func TestParseModel_MultiLineBlockCommentInHeader(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	if err := os.MkdirAll(modelsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	src := "/* multi\nline\ncomment */\n-- @kind: table\nSELECT 1 AS id"
	path := filepath.Join(modelsDir, "multi.sql")
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	model, err := ParseModel(path, dir)
	if err != nil {
		t.Fatalf("multi-line block comment in header must parse, got: %v", err)
	}
	if model.Kind != "table" {
		t.Errorf("expected @kind: table parsed, got %q", model.Kind)
	}
}

// TestParseModel_MixedLineBlockCommentBeforeDirective pins R9 #5:
// `/* foo */ -- @kind: table` on one line — block comment is
// stripped, the line-comment directive after it is parsed.
func TestParseModel_MixedLineBlockCommentBeforeDirective(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	if err := os.MkdirAll(modelsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	src := "/* @ftech */ -- @kind: table\nSELECT 1 AS id"
	path := filepath.Join(modelsDir, "mixed.sql")
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	model, err := ParseModel(path, dir)
	if err != nil {
		t.Fatalf("mixed block+line comment must parse, got: %v", err)
	}
	if model.Kind != "table" {
		t.Errorf("expected @kind: table parsed, got %q", model.Kind)
	}
}

// TestParseModel_BlockCommentNonDirective_OK pins that legitimate
// non-directive block comments at the top of a model don't trigger
// the unknown-directive check or the inHeader=false flip.
func TestParseModel_BlockCommentNonDirective_OK(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	if err := os.MkdirAll(modelsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	src := "/* leading note about the model */\n-- @kind: table\nSELECT 1 AS id"
	path := filepath.Join(modelsDir, "blockok.sql")
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ParseModel(path, dir); err != nil {
		t.Errorf("non-directive block comment must parse cleanly, got: %v", err)
	}
}

// TestParseModel_UnterminatedBlockCommentInHeader_Rejected pins
// R10 #6: a `/*` in the header with no closing `*/` must produce a
// clear error. Pre-R10 the stripper consumed the rest of the file
// silently, and with `@kind: events` already parsed the model was
// accepted with empty SQL (events-kind is exempt from the
// empty-SQL check).
func TestParseModel_UnterminatedBlockCommentInHeader_Rejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	if err := os.MkdirAll(modelsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	src := "-- @kind: events\n/* unterminated note about the model\n@id: int"
	path := filepath.Join(modelsDir, "broken.sql")
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := ParseModel(path, dir)
	if err == nil {
		t.Fatal("expected error for unterminated /* in header")
	}
	if !strings.Contains(err.Error(), "unterminated") {
		t.Errorf("error should mention 'unterminated', got: %v", err)
	}
}

// TestParseModel_BlockCommentInsideStringLiteral_NotStripped pins
// R10 #3: a SQL body line containing `'/* x */'` as a string literal
// must NOT have the block-comment range stripped. Pre-R10 the
// stripper ran on every header line, including the first SQL body
// line that flips inHeader=false — so `SELECT '/* x */'` had `/* x */`
// removed from inside the string before flipping.
func TestParseModel_BlockCommentInsideStringLiteral_NotStripped(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	if err := os.MkdirAll(modelsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Body line begins with SELECT (SQL prefix), contains a block
	// comment substring inside a string literal. The stripper must
	// not run on this line.
	src := "-- @kind: table\nSELECT '/* not a comment */' AS lit"
	path := filepath.Join(modelsDir, "lit.sql")
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	model, err := ParseModel(path, dir)
	if err != nil {
		t.Fatalf("string-literal-with-block-comment-shape must parse cleanly, got: %v", err)
	}
	if !strings.Contains(model.SQL, "/* not a comment */") {
		t.Errorf("string-literal block-comment substring must be preserved, got SQL: %q", model.SQL)
	}
}

// TestParseModel_PlainAtMentionInDescription_OK pins that @-mentions
// embedded in @description / @column text don't trigger the
// unknown-directive check — the regexp only fires on lines whose
// first non-space content after the comment prefix is `@<word>`.
func TestParseModel_PlainAtMentionInDescription_OK(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	if err := os.MkdirAll(modelsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	src := "-- @kind: table\n-- @description: invoice from @vendor\nSELECT 1 AS id"
	path := filepath.Join(modelsDir, "ok.sql")
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ParseModel(path, dir); err != nil {
		t.Errorf("should not flag @-mention inside @description body, got: %v", err)
	}
}

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

// --- @fetch directive (v0.30.0) ---

func TestParseModel_FetchDirective_BareMarker_OK(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0o755)
	os.WriteFile(filepath.Join(modelsDir, "fetched.sql"), []byte(
		"-- @kind: append\n-- @fetch\nSELECT id::BIGINT AS id FROM mylib()"), 0o644)

	model, err := ParseModel(filepath.Join(modelsDir, "fetched.sql"), dir)
	if err != nil {
		t.Fatalf("bare @fetch should parse: %v", err)
	}
	if !model.Fetch {
		t.Error("model.Fetch should be true after parsing @fetch")
	}
}

func TestParseModel_FetchDirective_WithColon_Rejected(t *testing.T) {
	t.Parallel()
	// @fetch is a bare marker; arguments belong on the FROM lib(args) call.
	// A user typing `@fetch: lib_name` is mixing it up with @push.
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0o755)
	os.WriteFile(filepath.Join(modelsDir, "bad.sql"), []byte(
		"-- @kind: append\n-- @fetch: mylib\nSELECT id::BIGINT AS id FROM mylib()"), 0o644)

	_, err := ParseModel(filepath.Join(modelsDir, "bad.sql"), dir)
	if err == nil {
		t.Fatal("@fetch with colon-arg should be rejected")
	}
	if !strings.Contains(err.Error(), "bare marker") {
		t.Errorf("error should explain @fetch is a bare marker, got: %v", err)
	}
}

func TestParseModel_FetchDirective_WithSpaceArg_Rejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0o755)
	os.WriteFile(filepath.Join(modelsDir, "bad.sql"), []byte(
		"-- @kind: append\n-- @fetch arg1\nSELECT id::BIGINT AS id FROM mylib()"), 0o644)

	_, err := ParseModel(filepath.Join(modelsDir, "bad.sql"), dir)
	if err == nil {
		t.Fatal("@fetch with trailing arg should be rejected")
	}
	if !strings.Contains(err.Error(), "bare marker") {
		t.Errorf("error should explain @fetch is a bare marker, got: %v", err)
	}
}

func TestParseModel_FetchDirective_WithEventsKind_Rejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0o755)
	os.WriteFile(filepath.Join(modelsDir, "bad.sql"), []byte(
		"-- @kind: events\n-- @fetch\nid BIGINT NOT NULL"), 0o644)

	_, err := ParseModel(filepath.Join(modelsDir, "bad.sql"), dir)
	if err == nil {
		t.Fatal("@fetch + @kind: events should be rejected")
	}
	if !strings.Contains(err.Error(), "events") {
		t.Errorf("error should mention events, got: %v", err)
	}
}

func TestParseModel_FetchDirective_WithPush_Rejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0o755)
	os.WriteFile(filepath.Join(modelsDir, "bad.sql"), []byte(
		"-- @kind: merge\n-- @unique_key: id\n-- @fetch\n-- @push: hubspot_push\nSELECT 1::BIGINT AS id FROM mylib()"), 0o644)

	_, err := ParseModel(filepath.Join(modelsDir, "bad.sql"), dir)
	if err == nil {
		t.Fatal("@fetch + @push should be rejected")
	}
	if !strings.Contains(err.Error(), "@fetch and @push") {
		t.Errorf("error should mention @fetch and @push, got: %v", err)
	}
}

// @fetch is a recognised directive — appearing in the SQL body must
// trigger the same "directive after SQL body" rejection as other directives.
func TestParseModel_FetchDirective_AfterSQLBody_Rejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelsDir, 0o755)
	os.WriteFile(filepath.Join(modelsDir, "bad.sql"), []byte(
		"-- @kind: append\nSELECT 1 AS id\n-- @fetch"), 0o644)

	_, err := ParseModel(filepath.Join(modelsDir, "bad.sql"), dir)
	if err == nil {
		t.Fatal("@fetch after SQL body should be rejected")
	}
	if !strings.Contains(err.Error(), "after SQL body") {
		t.Errorf("error should mention 'after SQL body', got: %v", err)
	}
}
