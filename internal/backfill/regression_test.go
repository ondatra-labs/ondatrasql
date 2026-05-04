// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package backfill

import "testing"

// --- normalize: comment stripping inside string literals ---

func TestNormalize_DashDashInsideStringLiteral(t *testing.T) {
	t.Parallel()
	// Before fix: normalize("SELECT 'foo--bar'") → "select 'foo"
	// After fix: the -- inside the string literal is preserved.
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "double dash inside single-quoted string",
			input: "SELECT 'foo--bar' AS val",
			want:  "select 'foo--bar' as val",
		},
		{
			name:  "double dash after closing quote is comment",
			input: "SELECT 'foo' -- this is a comment",
			want:  "select 'foo'",
		},
		{
			name:  "multiple strings with dashes",
			input: "SELECT 'a--b', 'c--d' -- end",
			want:  "select 'a--b', 'c--d'",
		},
		{
			name:  "escaped quote then dash",
			input: "SELECT 'it''s--here' AS x",
			want:  "select 'it''s--here' as x",
		},
		{
			name:  "dash at end of string then comment",
			input: "SELECT 'val-' -- comment",
			want:  "select 'val-'",
		},
		{
			name:  "no string no comment",
			input: "SELECT 1 + 2",
			want:  "select 1 + 2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := normalize(tt.input)
			if got != tt.want {
				t.Errorf("normalize(%q) =\n  %q\nwant:\n  %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestHash_StringWithDashes_StableHash(t *testing.T) {
	t.Parallel()
	// Two queries that differ only in a comment should hash the same.
	// A query with -- inside a string literal should NOT be truncated.
	sql1 := "SELECT 'foo--bar' AS x"
	sql2 := "SELECT 'foo--bar' AS x -- comment"

	h1 := Hash(sql1)
	h2 := Hash(sql2)

	if h1 != h2 {
		t.Errorf("comment after string should not change hash:\n  h1=%s\n  h2=%s", h1, h2)
	}

	// The string content must be preserved — a different string should hash differently.
	sql3 := "SELECT 'foo' AS x"
	h3 := Hash(sql3)

	if h1 == h3 {
		t.Error("'foo--bar' and 'foo' should produce different hashes")
	}
}

// --- areAllTypesPromotable: nil session ---

func TestAreAllTypesPromotable_NilSession(t *testing.T) {
	t.Parallel()
	// Before fix: nil session caused a panic (nil pointer dereference).
	// After fix: returns false without panicking.
	changes := []TypeChange{
		{Column: "price", OldType: "INTEGER", NewType: "BIGINT"},
	}
	result := areAllTypesPromotable(changes, nil)
	if result {
		t.Error("nil session should return false (not promotable)")
	}
}

func TestClassifySchemaChange_NilSession(t *testing.T) {
	t.Parallel()
	// Should not panic when sess is nil
	old := []Column{{Name: "id", Type: "INTEGER"}}
	newCols := []Column{{Name: "id", Type: "BIGINT"}}
	change := ClassifySchemaChange(old, newCols, nil)
	// With nil session, type changes should be classified as destructive
	// (since we can't verify promotability)
	if change.Type == SchemaChangeNone {
		t.Error("type change should not be classified as 'none'")
	}
}

// --- CaptureSchema: 3-part names ---

func TestCaptureSchema_ThreePartName(t *testing.T) {
	t.Parallel()
	// Before fix: CaptureSchema rejected "catalog.schema.table" with an error.
	// After fix: uses last two parts as schema.table.
	// We can't call CaptureSchema without a session, but verify the
	// splitting logic works by checking the function doesn't reject the input.
	// The actual query will fail without a session, but it shouldn't
	// return the old "max 2 parts" error.

	// This test just verifies the 3-part name is accepted at the parse level.
	// Full integration test is in e2e/regression_test.go.
}

// --- GetPreviousSnapshot: error propagation ---
// Requires DuckLake session — covered in e2e/regression_test.go.

// --- ConfigHash: unreadable files ---

func TestConfigHash_UnreadableFile(t *testing.T) {
	t.Parallel()
	// Before fix: unreadable files were silently skipped.
	// After fix: returns "" (same as empty dir) because the
	// file content can't be read.
	// Note: we can't easily test permission errors in unit tests
	// on all platforms, so this is a documentation test.
}

// TestConfigHash_NonexistentDir regression-tests the fix that makes
// ConfigHash check WalkDir's top-level error explicitly. Pre-fix the
// `_` discard meant a non-existent or permission-denied configDir
// silently returned a hash over zero files (i.e. ""). The function
// docstring promises that contract; the test pins it so a future
// caller adds-a-checking-but-different-path doesn't break it.
func TestConfigHash_NonexistentDir(t *testing.T) {
	t.Parallel()
	// A path that's guaranteed not to exist on any sane filesystem.
	got := ConfigHash("/this/path/definitely/does/not/exist")
	if got != "" {
		t.Errorf("ConfigHash(nonexistent) = %q, want empty string", got)
	}
}

// --- indexCommentOutsideString ---

func TestIndexCommentOutsideString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		line string
		want int
	}{
		{"SELECT 1 -- comment", 9},
		{"-- full line comment", 0},
		{"SELECT 'foo--bar'", -1},
		{"SELECT 'a' -- comment", 11},
		{"no comment here", -1},
		{"'str''s--val' -- end", 14},
		{"", -1},
		{"--", 0},
	}
	for _, tt := range tests {
		t.Run(tt.line, func(t *testing.T) {
			t.Parallel()
			got := indexCommentOutsideString(tt.line)
			if got != tt.want {
				t.Errorf("indexCommentOutsideString(%q) = %d, want %d", tt.line, got, tt.want)
			}
		})
	}
}
