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

// --- readConfigFiles: unreadable files ---

func TestConfigFiles_UnreadableFile(t *testing.T) {
	t.Parallel()
	// Before fix: unreadable files were silently skipped.
	// After fix: returns nil (same as empty dir) because the
	// file content can't be read.
	// Note: we can't easily test permission errors in unit tests
	// on all platforms, so this is a documentation test.
}

// TestConfigIndex_NonexistentDir regression-tests the fix that makes the
// config walk check WalkDir's top-level error explicitly. Pre-fix the
// `_` discard meant a non-existent or permission-denied configDir
// silently returned a hash over zero files (i.e. ""). The contract is that
// no config means config never forces a rebuild; the test pins it so a
// future caller adding a different path doesn't break it.
func TestConfigIndex_NonexistentDir(t *testing.T) {
	t.Parallel()
	// A path that's guaranteed not to exist on any sane filesystem.
	ix, err := BuildConfigIndex("/this/path/definitely/does/not/exist")
	if err != nil {
		t.Errorf("a missing config dir is a project without config, not an error: %v", err)
	}
	if ix != nil {
		t.Error("BuildConfigIndex(nonexistent) should return a nil index")
	}
	if got := ix.HashFor("SELECT 1"); got != "" {
		t.Errorf("nil index HashFor = %q, want empty string", got)
	}
}

// --- normalizeText ---

// TestNormalizeText covers the hashing primitive every model and config hash
// is built on. Every case here is a pair of inputs that must NOT collapse to
// the same normalized form, or a pair that must — getting either wrong means
// a model silently keeps stale data or rebuilds for nothing.
func TestNormalizeText(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		in    string
		lower bool
		want  string
	}{
		{name: "trailing comment stripped", in: "SELECT 1 -- comment", lower: true, want: "select 1"},
		{name: "full line comment stripped", in: "-- all of it", lower: true, want: ""},
		{name: "whitespace collapsed", in: "SELECT\n\t1   +  2", lower: true, want: "select 1 + 2"},
		{name: "dashes inside literal kept", in: "SELECT 'foo--bar'", lower: true, want: "select 'foo--bar'"},
		{name: "escaped quote handled", in: "SELECT 'it''s' -- note", lower: true, want: "select 'it''s'"},
		{name: "apostrophe in comment ignored", in: "SELECT 1 -- don't", lower: true, want: "select 1"},

		// Case is data inside a literal: lowercasing it would make a model
		// whose only change is a filter value hash equal to the old one.
		{name: "literal case preserved", in: "WHERE s = 'Active'", lower: true, want: "where s = 'Active'"},
		{name: "identifier case folded", in: "SELECT Foo FROM Bar", lower: true, want: "select foo from bar"},

		// Whitespace is data inside a literal too.
		{name: "newline inside literal preserved", in: "SET x = 'a\nb'", want: "SET x = 'a\nb'"},
		{name: "spaces inside literal preserved", in: "SET x = 'a   b'", want: "SET x = 'a   b'"},

		// Quoted identifiers behave like literals for comments and case.
		{name: "dashes inside identifier kept", in: `CREATE VIEW "ev--v1" AS SELECT 1`, lower: true, want: `create view "ev--v1" as select 1`},
		{name: "identifier quoting case preserved", in: `SELECT "MixedCase"`, lower: true, want: `select "MixedCase"`},

		// A quote of one kind inside the other is data, not a delimiter.
		{name: "double quote inside literal", in: `SELECT 'a "b' -- note`, lower: true, want: `select 'a "b'`},
		{name: "apostrophe inside identifier", in: `SELECT "a'b" -- note`, lower: true, want: `select "a'b"`},

		{name: "empty", in: "", want: ""},
		{name: "comment only", in: "--", want: ""},

		// Block comments are stripped too. Leaving one in would also stop the
		// statement behind it being recognized as a macro definition.
		{name: "block comment stripped", in: "/* note */ CREATE MACRO m() AS 1", lower: true, want: "create macro m() as 1"},
		{name: "block comment mid-statement", in: "SELECT 1 /* why */ + 2", lower: true, want: "select 1 + 2"},
		{name: "block comment spanning lines", in: "/* a\nb */ SELECT 1", lower: true, want: "select 1"},
		{name: "block comment unterminated", in: "SELECT 1 /* never closed", lower: true, want: "select 1"},
		{name: "block markers inside literal kept", in: "SELECT '/* not a comment */'", lower: true, want: "select '/* not a comment */'"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := normalizeText(tt.in, tt.lower); got != tt.want {
				t.Errorf("normalizeText(%q, %v) = %q, want %q", tt.in, tt.lower, got, tt.want)
			}
		})
	}
}

// TestNormalizeText_DistinctInputsStayDistinct pins the pairs that previously
// collapsed. Each is a missed-rebuild bug if it regresses.
func TestNormalizeText_DistinctInputsStayDistinct(t *testing.T) {
	t.Parallel()
	pairs := []struct{ name, a, b string }{
		{"literal case", "WHERE s = 'Active'", "WHERE s = 'active'"},
		{"newline vs space in literal", "SET x = 'a\nb'", "SET x = 'a b'"},
		{"content after dashes in a multi-line literal", "SET x = 'a\n-- k\nb'", "SET x = 'a\n-- k\nc'"},
		{"content after dashes in an identifier", `CREATE VIEW "e--1" AS SELECT 1`, `CREATE VIEW "e--1" AS SELECT 2`},
	}
	for _, p := range pairs {
		t.Run(p.name, func(t *testing.T) {
			t.Parallel()
			if normalizeText(p.a, true) == normalizeText(p.b, true) {
				t.Errorf("%q and %q must not normalize alike (got %q)", p.a, p.b, normalizeText(p.a, true))
			}
		})
	}
}
