// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/output"
	"github.com/ondatra-labs/ondatrasql/internal/validate"
)

func TestParseValidateArgs_Defaults(t *testing.T) {
	output.Reset()
	opts, err := parseValidateArgs(nil)
	if err != nil {
		t.Fatal(err)
	}
	if opts.format != validate.OutputHuman {
		t.Errorf("format = %q, want human", opts.format)
	}
	if opts.strict {
		t.Error("strict should default to false")
	}
	if len(opts.paths) != 0 {
		t.Errorf("paths = %v, want empty", opts.paths)
	}
}

func TestParseValidateArgs_Paths(t *testing.T) {
	output.Reset()
	opts, err := parseValidateArgs([]string{"models/foo.sql", "lib/bar.star"})
	if err != nil {
		t.Fatal(err)
	}
	if len(opts.paths) != 2 {
		t.Errorf("paths = %v, want 2 entries", opts.paths)
	}
}

func TestParseValidateArgs_Strict(t *testing.T) {
	output.Reset()
	opts, err := parseValidateArgs([]string{"--strict"})
	if err != nil {
		t.Fatal(err)
	}
	if !opts.strict {
		t.Error("--strict should set strict=true")
	}
}

func TestParseValidateArgs_OutputFlag(t *testing.T) {
	output.Reset()
	cases := []struct {
		args []string
		want validate.OutputFormat
	}{
		{[]string{"--output", "json"}, validate.OutputJSON},
		{[]string{"--output=ndjson"}, validate.OutputNDJSON},
		{[]string{"--output=human"}, validate.OutputHuman},
	}
	for _, tc := range cases {
		opts, err := parseValidateArgs(tc.args)
		if err != nil {
			t.Errorf("parseValidateArgs(%v): %v", tc.args, err)
			continue
		}
		if opts.format != tc.want {
			t.Errorf("parseValidateArgs(%v): format=%q, want %q", tc.args, opts.format, tc.want)
		}
	}
}

func TestParseValidateArgs_GlobalJSONFlag(t *testing.T) {
	output.Init(true)
	defer output.Reset()
	opts, err := parseValidateArgs(nil)
	if err != nil {
		t.Fatal(err)
	}
	if opts.format != validate.OutputJSON {
		t.Errorf("format with global --json = %q, want json", opts.format)
	}
}

func TestParseValidateArgs_UnknownFlag(t *testing.T) {
	output.Reset()
	if _, err := parseValidateArgs([]string{"--nope"}); err == nil {
		t.Error("expected error for unknown flag")
	}
}

func TestParseValidateArgs_BadOutputValue(t *testing.T) {
	output.Reset()
	if _, err := parseValidateArgs([]string{"--output", "yaml"}); err == nil {
		t.Error("expected error for unsupported output format")
	}
}

func TestParseValidateArgs_OutputMissingValue(t *testing.T) {
	output.Reset()
	if _, err := parseValidateArgs([]string{"--output"}); err == nil {
		t.Error("expected error for missing --output value")
	}
}

func TestIsValidatable(t *testing.T) {
	cases := []struct {
		path string
		want bool
	}{
		{"models/raw/foo.sql", true},
		{"lib/bar.star", true},
		{"config/macros/x.sql", true}, // surface; ParseModel filters later
		{"README.md", false},
		{"models/raw/foo.txt", false},
	}
	for _, tc := range cases {
		if got := isValidatable(tc.path); got != tc.want {
			t.Errorf("isValidatable(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}

func TestSafeResolveSymlink_RejectsEscape(t *testing.T) {
	tmp := t.TempDir()
	outside := filepath.Join(tmp, "..", "outside")
	if err := os.MkdirAll(filepath.Join(tmp, "..", "outside"), 0o755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(filepath.Join(tmp, "..", "outside"))

	target := filepath.Join(outside, "secret.sql")
	if err := os.WriteFile(target, []byte("SELECT 1"), 0o644); err != nil {
		t.Fatal(err)
	}

	link := filepath.Join(tmp, "leak.sql")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	if _, err := safeResolveSymlink(tmp, link); err == nil {
		t.Error("expected error for symlink escaping projectDir")
	}
}

func TestSafeResolveSymlink_AllowsInternalSymlink(t *testing.T) {
	tmp := t.TempDir()
	target := filepath.Join(tmp, "real.sql")
	if err := os.WriteFile(target, []byte("SELECT 1"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(tmp, "alias.sql")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}
	got, err := safeResolveSymlink(tmp, link)
	if err != nil {
		t.Fatal(err)
	}
	// Resolved path should be the real file, not the link.
	if !strings.HasSuffix(got, "real.sql") {
		t.Errorf("safeResolveSymlink resolved to %q, want real.sql", got)
	}
}

func TestStrictFetchRuleFromMessage(t *testing.T) {
	cases := []struct {
		msg  string
		want validate.RuleID
	}{
		{"projection must use CAST", validate.RuleStrictFetchCastRequired},
		{"every projection requires alias", validate.RuleStrictFetchAliasRequired},
		{"WHERE clause not allowed", validate.RuleStrictFetchWhereDisallowed},
		{"GROUP BY not allowed", validate.RuleStrictFetchGroupByDisallowed},
		{"ORDER BY not allowed", validate.RuleStrictFetchOrderByDisallowed},
		{"DISTINCT not allowed", validate.RuleStrictFetchDistinctDisallow},
		{"UNION not allowed", validate.RuleStrictFetchUnionDisallowed},
		{"@fetch must contain exactly one lib", validate.RuleStrictFetchMultipleLibCalls},
		{"missing TABLE_FUNCTION node", validate.RuleStrictFetchNoLibCall},
		{"some other thing", validate.RuleStrictFetchOther},
	}
	for _, tc := range cases {
		if got := strictFetchRuleFromMessage(tc.msg); got != tc.want {
			t.Errorf("strictFetchRuleFromMessage(%q) = %q, want %q", tc.msg, got, tc.want)
		}
	}
}

func TestStrictPushRuleFromMessage(t *testing.T) {
	cases := []struct {
		msg  string
		want validate.RuleID
	}{
		{"SELECT * not allowed", validate.RuleStrictPushSelectStar},
		{"@push cannot have lib call", validate.RuleStrictPushLibCallInFrom},
		{"missing CAST", validate.RuleStrictPushCastRequired},
		{"alias required", validate.RuleStrictPushAliasRequired},
		{"unrelated", validate.RuleStrictPushOther},
	}
	for _, tc := range cases {
		if got := strictPushRuleFromMessage(tc.msg); got != tc.want {
			t.Errorf("strictPushRuleFromMessage(%q) = %q", tc.msg, got)
		}
	}
}

func TestReadOnlyRuleFromMessage(t *testing.T) {
	if got := readOnlyRuleFromMessage("OFFSET disallowed"); got != validate.RuleReadOnlyOffsetDisallowed {
		t.Errorf("OFFSET → %q", got)
	}
	if got := readOnlyRuleFromMessage("LIMIT disallowed"); got != validate.RuleReadOnlyLimitDisallowed {
		t.Errorf("LIMIT → %q", got)
	}
}

func TestMapParserError_DeprecatedDirectives(t *testing.T) {
	cases := []struct {
		msg  string
		want validate.RuleID
	}{
		{"@sink_detect_deletes is deprecated", validate.RuleParserDeprecatedSinkDetectDeletes},
		{"@sink_delete_threshold is deprecated", validate.RuleParserDeprecatedSinkDeleteThresh},
		{"@sink directive renamed to @push", validate.RuleParserDeprecatedSink},
		{"@script directive removed", validate.RuleParserDeprecatedScript},
		{"multi-statement model not allowed", validate.RuleParserMultiStatement},
	}
	for _, tc := range cases {
		got := mapParserError("x.sql", &fakeErr{tc.msg})
		if got.Rule != tc.want {
			t.Errorf("msg=%q: rule=%q, want %q", tc.msg, got.Rule, tc.want)
		}
	}
}

type fakeErr struct{ s string }

func (e *fakeErr) Error() string { return e.s }

// TestHasDegradedRunFinding pins that any validate.* finding triggers
// the degraded-run signal regardless of which specific rule fires.
// Critical regression target — Codex round 7 flagged that validate.*
// WARN findings were silently swallowed without --strict.
func TestHasDegradedRunFinding(t *testing.T) {
	cases := []struct {
		name      string
		report    *validate.Report
		want      bool
	}{
		{
			name:   "empty report",
			report: validate.NewReport(),
			want:   false,
		},
		{
			name: "only code-level WARN",
			report: func() *validate.Report {
				r := validate.NewReport()
				r.AddFile(validate.FileResult{
					Path: "a.sql",
					Findings: []validate.Finding{validate.NewFinding(
						"a.sql", 1, validate.RuleParserDeprecatedSink, "")},
				})
				return r
			}(),
			want: false,
		},
		{
			name: "validate.extensions_load_failed present",
			report: func() *validate.Report {
				r := validate.NewReport()
				r.AddFile(validate.FileResult{
					Path: "config/extensions.sql",
					Findings: []validate.Finding{validate.NewFinding(
						"config/extensions.sql", 0, validate.RuleValidateExtensionsLoadFailed, "")},
				})
				return r
			}(),
			want: true,
		},
		{
			name: "validate.blueprint_load_failed present",
			report: func() *validate.Report {
				r := validate.NewReport()
				r.AddFile(validate.FileResult{
					Path: "lib/x.star",
					Findings: []validate.Finding{validate.NewFinding(
						"lib/x.star", 0, validate.RuleValidateBlueprintLoadFailed, "")},
				})
				return r
			}(),
			want: true,
		},
		{
			name: "validate.builtin_introspection_failed present",
			report: func() *validate.Report {
				r := validate.NewReport()
				r.AddFile(validate.FileResult{
					Path: "<duckdb-builtins>",
					Findings: []validate.Finding{validate.NewFinding(
						"<duckdb-builtins>", 0, validate.RuleValidateBuiltinIntrospectionFail, "")},
				})
				return r
			}(),
			want: true,
		},
		{
			name: "BLOCKER + degraded coexist",
			report: func() *validate.Report {
				r := validate.NewReport()
				r.AddFile(validate.FileResult{
					Path: "x.sql",
					Findings: []validate.Finding{
						validate.NewFinding("x.sql", 1, validate.RuleParserMultiStatement, ""),
						validate.NewFinding("config/extensions.sql", 0, validate.RuleValidateExtensionsLoadFailed, ""),
					},
				})
				return r
			}(),
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasDegradedRunFinding(tc.report); got != tc.want {
				t.Errorf("hasDegradedRunFinding = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestIsValidatableDir pins tree-membership rules (Codex round 4).
// Explicit dir args must be under models/ or lib/, not arbitrary
// directories like config/ or docs/.
func TestIsValidatableDir(t *testing.T) {
	tmp := t.TempDir()
	cases := []struct {
		dir  string
		want bool
	}{
		{filepath.Join(tmp, "models"), true},
		{filepath.Join(tmp, "models", "raw"), true},
		{filepath.Join(tmp, "models", "raw", "deeply", "nested"), true},
		{filepath.Join(tmp, "lib"), true},
		{filepath.Join(tmp, "lib", "internal"), true},
		{filepath.Join(tmp, "config"), false},
		{filepath.Join(tmp, "config", "macros"), false},
		{filepath.Join(tmp, "docs"), false},
		{filepath.Join(tmp, "model"), false},  // singular, not "models"
		{filepath.Join(tmp, "models2"), false}, // prefix-match defense
		{tmp, false},
	}
	for _, tc := range cases {
		if got := isValidatableDir(tmp, tc.dir); got != tc.want {
			t.Errorf("isValidatableDir(%q) = %v, want %v", tc.dir, got, tc.want)
		}
	}
}

// TestFileBelongsToScope pins file-extension-vs-tree rules (Codex round 4).
// *.sql must live under models/, *.star under lib/. Mismatches like
// models/foo.star or lib/helper.sql are rejected.
func TestFileBelongsToScope(t *testing.T) {
	tmp := t.TempDir()
	cases := []struct {
		path string
		want bool
	}{
		{filepath.Join(tmp, "models", "x.sql"), true},
		{filepath.Join(tmp, "models", "raw", "x.sql"), true},
		{filepath.Join(tmp, "lib", "x.star"), true},
		{filepath.Join(tmp, "lib", "_helper.star"), true},
		// Mismatched: file extension wrong for the tree
		{filepath.Join(tmp, "models", "x.star"), false},
		{filepath.Join(tmp, "lib", "x.sql"), false},
		// Out-of-tree
		{filepath.Join(tmp, "config", "x.sql"), false},
		{filepath.Join(tmp, "docs", "x.star"), false},
		{filepath.Join(tmp, "x.sql"), false},
	}
	for _, tc := range cases {
		if got := fileBelongsToScope(tmp, tc.path); got != tc.want {
			t.Errorf("fileBelongsToScope(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}

// TestUnderTree pins prefix-matching defense — `/foo/models2` must NOT
// be considered "under" `/foo/models`.
func TestUnderTree(t *testing.T) {
	cases := []struct {
		path string
		root string
		want bool
	}{
		{"/foo/models", "/foo/models", true},
		{"/foo/models/x", "/foo/models", true},
		{"/foo/models/deeply/nested", "/foo/models", true},
		{"/foo/models2", "/foo/models", false},
		{"/foo/models2/x", "/foo/models", false},
		{"/foo", "/foo/models", false},
		{"/bar", "/foo/models", false},
	}
	for _, tc := range cases {
		if got := underTree(tc.path, tc.root); got != tc.want {
			t.Errorf("underTree(%q, %q) = %v, want %v", tc.path, tc.root, got, tc.want)
		}
	}
}

// TestDiscoverFiles_ExplicitDirRejection pins that explicit dirs
// outside models/+lib/ produce an invocation error rather than a
// silent no-op (Codex round 5).
func TestDiscoverFiles_ExplicitDirRejection(t *testing.T) {
	tmp := t.TempDir()
	if err := os.MkdirAll(filepath.Join(tmp, "config"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmp, "config", "x.sql"), []byte("SELECT 1"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := discoverFiles(tmp, []string{filepath.Join(tmp, "config")})
	if err == nil {
		t.Error("expected error for explicit config/ dir, got nil")
	}
	if !strings.Contains(err.Error(), "scope") {
		t.Errorf("error message should mention 'scope', got: %v", err)
	}
}

// TestDiscoverFiles_FileExtensionMismatchRejection pins that a *.sql
// file under lib/ or *.star under models/ is rejected (tree-membership
// rule). Codex round 4.
func TestDiscoverFiles_FileExtensionMismatchRejection(t *testing.T) {
	tmp := t.TempDir()
	if err := os.MkdirAll(filepath.Join(tmp, "lib"), 0o755); err != nil {
		t.Fatal(err)
	}
	libSQL := filepath.Join(tmp, "lib", "wrong.sql")
	if err := os.WriteFile(libSQL, []byte("SELECT 1"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := discoverFiles(tmp, []string{libSQL})
	if err == nil {
		t.Error("expected error for *.sql under lib/, got nil")
	}
}

// TestDiscoverFiles_NonValidatableExtensionRejection pins that
// `validate README.md` returns an invocation error rather than
// silent-clean (Codex round 2).
func TestDiscoverFiles_NonValidatableExtensionRejection(t *testing.T) {
	tmp := t.TempDir()
	readme := filepath.Join(tmp, "README.md")
	if err := os.WriteFile(readme, []byte("# README"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := discoverFiles(tmp, []string{readme})
	if err == nil {
		t.Error("expected error for README.md, got nil")
	}
	if !strings.Contains(err.Error(), "validatable") {
		t.Errorf("error should mention 'validatable', got: %v", err)
	}
}

// TestDiscoverFiles_DefaultScope_NoLibOK pins that default scope (no args)
// silently skips a missing lib/ directory — projects without blueprints
// should still be validatable.
func TestDiscoverFiles_DefaultScope_NoLibOK(t *testing.T) {
	tmp := t.TempDir()
	if err := os.MkdirAll(filepath.Join(tmp, "models"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmp, "models", "x.sql"), []byte("-- @kind: table\nSELECT 1"), 0o644); err != nil {
		t.Fatal(err)
	}
	files, err := discoverFiles(tmp, nil)
	if err != nil {
		t.Errorf("default scope without lib/ should not error: %v", err)
	}
	if len(files) != 1 {
		t.Errorf("expected 1 file (the model), got %d", len(files))
	}
}

// TestNormalizeCheckName pins the snake_case → kebab-case mapping
// that resolves Codex round 1 finding 3 (check-name inconsistency).
func TestNormalizeCheckName(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"strict_fetch", "strict-fetch"},
		{"strict_push", "strict-push"},
		{"read_only", "read-only"},
		{"parser", "parser"}, // no underscore, unchanged
		{"already-kebab", "already-kebab"},
		{"", ""},
	}
	for _, tc := range cases {
		if got := normalizeCheckName(tc.in); got != tc.want {
			t.Errorf("normalizeCheckName(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
