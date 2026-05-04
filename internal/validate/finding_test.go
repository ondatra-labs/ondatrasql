// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package validate

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestSeverityFor_DefaultBlocker(t *testing.T) {
	cases := []RuleID{
		RuleParserMultiStatement,
		RuleStrictFetchCastRequired,
		RuleStrictPushSelectStar,
		RuleReadOnlyLimitDisallowed,
		RuleDagCycleDetected,
		RuleBlueprintInvalidAPIDict,
		RulePushKindNotSupported,
		RuleCrossASTArgsMismatch,
	}
	for _, id := range cases {
		if got := SeverityFor(id); got != SeverityBlocker {
			t.Errorf("SeverityFor(%q) = %q, want BLOCKER", id, got)
		}
	}
}

// TestSeverityFor_DeprecatedDirectivesAreBlocker pins that deprecated
// directives map to BLOCKER (parser.ParseModel rejects them at run-time,
// so validate must not contradict the runtime contract by reporting
// them as WARN). Codex Phase A round caught a contract drift here.
func TestSeverityFor_DeprecatedDirectivesAreBlocker(t *testing.T) {
	cases := []RuleID{
		RuleParserDeprecatedSink,
		RuleParserDeprecatedScript,
		RuleParserDeprecatedSinkDetectDeletes,
		RuleParserDeprecatedSinkDeleteThresh,
	}
	for _, id := range cases {
		if got := SeverityFor(id); got != SeverityBlocker {
			t.Errorf("SeverityFor(%q) = %q, want BLOCKER (must match parser.ParseModel runtime contract)", id, got)
		}
	}
}

func TestSeverityFor_ExternalRefsAreInfo(t *testing.T) {
	if got := SeverityFor(RuleDagExternalRef); got != SeverityInfo {
		t.Errorf("SeverityFor(RuleDagExternalRef) = %q, want INFO", got)
	}
	if got := SeverityFor(RuleBlueprintHelperWithoutAPI); got != SeverityInfo {
		t.Errorf("SeverityFor(RuleBlueprintHelperWithoutAPI) = %q, want INFO", got)
	}
}

// TestSeverityContract_WARNRulesAreValidateNamespaced pins the invariant
// documented in docs/reference/pipeline/cli.md: every WARN rule belongs
// to the `validate.*` namespace (signals "validate's own analysis was
// degraded"). Adding a non-validate WARN rule means either (a) the doc
// table needs updating, or (b) the rule belongs in BLOCKER/INFO. This
// test forces that conversation.
func TestSeverityContract_WARNRulesAreValidateNamespaced(t *testing.T) {
	for id, sev := range severityOverrides {
		if sev != SeverityWarn {
			continue
		}
		if !strings.HasPrefix(string(id), "validate.") {
			t.Errorf("rule %q is WARN but not in validate.* namespace; either rename it or change severity (docs/reference/pipeline/cli.md describes WARN as a degraded-validate-environment signal)", id)
		}
	}
}

// TestSeverityContract_DocumentedValidateWARNRules pins the specific
// list of `validate.*` WARN rules that docs/reference/pipeline/cli.md
// promises will trigger exit 1 regardless of `--strict`. Adding a new
// validate.* rule without listing it here AND in the docs is a
// documentation gap — this test forces both updates together.
func TestSeverityContract_DocumentedValidateWARNRules(t *testing.T) {
	documented := map[RuleID]struct{}{
		RuleValidateExtensionsLoadFailed:     {},
		RuleValidateBlueprintLoadFailed:      {},
		RuleValidateBuiltinIntrospectionFail: {},
		RuleValidateWalkSkipped:              {},
		RuleValidateLineageExtractFailed:     {},
	}
	for id := range documented {
		if got := SeverityFor(id); got != SeverityWarn {
			t.Errorf("documented WARN rule %q has severity %q in code", id, got)
		}
	}
	// Detect a new validate.* WARN rule that isn't in the documented
	// list — fails the test until docs/reference/pipeline/cli.md and
	// `documented` above are updated together.
	for id, sev := range severityOverrides {
		if sev != SeverityWarn {
			continue
		}
		if _, ok := documented[id]; !ok {
			t.Errorf("validate.* WARN rule %q is not in the documented list — update docs/reference/pipeline/cli.md (the validate.* WARN exit-code paragraph) and add %q to TestSeverityContract_DocumentedValidateWARNRules", id, id)
		}
	}
}

func TestSeverityFor_UnknownIDDefaultsBlocker(t *testing.T) {
	// An unregistered rule ID falls back to BLOCKER (safer default).
	if got := SeverityFor(RuleID("made.up.rule")); got != SeverityBlocker {
		t.Errorf("SeverityFor(made-up) = %q, want BLOCKER", got)
	}
}

func TestNewFinding_AppliesSeverity(t *testing.T) {
	// Use an explicitly WARN-mapped rule (validate.* family) — deprecated
	// directives are now BLOCKER, so they don't exercise the WARN path.
	f := NewFinding("models/x.sql", 5, RuleValidateExtensionsLoadFailed, "ext")
	if f.Severity != SeverityWarn {
		t.Errorf("Severity = %q, want WARN", f.Severity)
	}
	if f.Rule != RuleValidateExtensionsLoadFailed {
		t.Errorf("Rule = %q", f.Rule)
	}
	if f.Path != "models/x.sql" || f.Line != 5 || f.Message != "ext" {
		t.Errorf("unexpected finding: %+v", f)
	}
}

func TestReport_AddFile_UpdatesSummary(t *testing.T) {
	r := NewReport()
	if r.SchemaVersion != SchemaVersion {
		t.Errorf("NewReport SchemaVersion = %d, want %d", r.SchemaVersion, SchemaVersion)
	}

	r.AddFile(FileResult{Path: "a.sql"}) // clean
	r.AddFile(FileResult{
		Path: "b.sql",
		Findings: []Finding{
			NewFinding("b.sql", 1, RuleParserMultiStatement, "x"),
			NewFinding("b.sql", 2, RuleValidateExtensionsLoadFailed, "y"), // WARN
		},
	})
	r.AddFile(FileResult{
		Path: "c.sql",
		Findings: []Finding{
			NewFinding("c.sql", 1, RuleDagExternalRef, "z"),
		},
	})

	want := Summary{Files: 3, Blockers: 1, Warns: 1, Infos: 1, Clean: 1}
	if r.Summary != want {
		t.Errorf("Summary = %+v, want %+v", r.Summary, want)
	}
	if !r.HasBlockers() || !r.HasWarns() {
		t.Error("expected HasBlockers and HasWarns true")
	}
}

func TestReport_JSON_IncludesSchemaVersion(t *testing.T) {
	r := NewReport()
	r.AddFile(FileResult{Path: "a.sql", Status: "ok"})

	var buf bytes.Buffer
	if err := Render(&buf, r, OutputJSON); err != nil {
		t.Fatal(err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatal(err)
	}
	if got := parsed["schema_version"]; got != float64(SchemaVersion) {
		t.Errorf("schema_version = %v, want %d", got, SchemaVersion)
	}
}

func TestRender_NDJSON_OneLinePerFilePlusSummary(t *testing.T) {
	r := NewReport()
	r.AddFile(FileResult{Path: "a.sql", Status: "ok"})
	r.AddFile(FileResult{Path: "b.sql", Status: "fail"})

	var buf bytes.Buffer
	if err := Render(&buf, r, OutputNDJSON); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3 (2 files + summary)", len(lines))
	}
	if !strings.Contains(lines[2], `"summary"`) {
		t.Errorf("last line missing summary: %q", lines[2])
	}
}

func TestRender_Human_CleanFile(t *testing.T) {
	r := NewReport()
	r.AddFile(FileResult{
		Path: "a.sql",
		Checks: map[string]string{
			"parser":       "ok",
			"strict-fetch": "ok",
		},
	})

	var buf bytes.Buffer
	if err := Render(&buf, r, OutputHuman); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "a.sql") {
		t.Error("missing path")
	}
	if !strings.Contains(out, "✓ parser") || !strings.Contains(out, "✓ strict-fetch") {
		t.Errorf("missing check markers in: %s", out)
	}
}

func TestRender_Human_FindingHasMarkerAndRuleID(t *testing.T) {
	r := NewReport()
	r.AddFile(FileResult{
		Path: "b.sql",
		Findings: []Finding{
			NewFinding("b.sql", 5, RuleParserMultiStatement, "multi-statement"),
		},
	})

	var buf bytes.Buffer
	if err := Render(&buf, r, OutputHuman); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "✗") {
		t.Error("missing BLOCKER marker")
	}
	if !strings.Contains(out, string(RuleParserMultiStatement)) {
		t.Error("missing rule ID")
	}
	if !strings.Contains(out, ":5") {
		t.Error("missing line number")
	}
}

// TestSchemaVersion_Is2 pins the schema version. Bump only when
// rule-IDs/severity/output structure change. The test exists so a casual
// edit doesn't silently regress consumers' expectations.
func TestSchemaVersion_Is2(t *testing.T) {
	if SchemaVersion != 2 {
		t.Errorf("SchemaVersion = %d, want 2 (validate.* family added in v2)", SchemaVersion)
	}
}

// TestSeverityFor_ValidateFamily_AlwaysWarn pins that all `validate.*`
// rule-IDs map to WARN severity. These are environmental diagnostics —
// not BLOCKER (don't block code merge) but also not silent (need to
// signal degraded run). Critical regression target — Codex round 6
// flagged that validate.* findings were silently swallowed.
func TestSeverityFor_ValidateFamily_AlwaysWarn(t *testing.T) {
	cases := []RuleID{
		RuleValidateExtensionsLoadFailed,
		RuleValidateBlueprintLoadFailed,
		RuleValidateBuiltinIntrospectionFail,
	}
	for _, id := range cases {
		if got := SeverityFor(id); got != SeverityWarn {
			t.Errorf("SeverityFor(%q) = %q, want WARN — validate.* family must be WARN", id, got)
		}
	}
}

// TestRender_NDJSON_ContainsSchemaVersion pins that NDJSON's trailing
// summary row carries schema_version, so streaming consumers can
// detect schema bumps without buffering the whole report.
func TestRender_NDJSON_ContainsSchemaVersion(t *testing.T) {
	r := NewReport()
	r.AddFile(FileResult{Path: "a.sql", Status: "ok"})
	var buf bytes.Buffer
	if err := Render(&buf, r, OutputNDJSON); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), `"schema_version"`) {
		t.Errorf("NDJSON output missing schema_version: %s", buf.String())
	}
}

// TestRender_JSON_StableFieldOrder pins deterministic JSON field order
// (per design G5). Encoding the same report twice must produce
// byte-identical output so diff-based CI works.
func TestRender_JSON_StableFieldOrder(t *testing.T) {
	r1 := NewReport()
	r1.AddFile(FileResult{
		Path: "x.sql",
		Findings: []Finding{
			NewFinding("x.sql", 1, RuleParserMultiStatement, "msg"),
			NewFinding("x.sql", 2, RuleParserDeprecatedSink, "msg2"),
		},
	})
	r2 := NewReport()
	r2.AddFile(FileResult{
		Path: "x.sql",
		Findings: []Finding{
			NewFinding("x.sql", 1, RuleParserMultiStatement, "msg"),
			NewFinding("x.sql", 2, RuleParserDeprecatedSink, "msg2"),
		},
	})
	var b1, b2 bytes.Buffer
	if err := Render(&b1, r1, OutputJSON); err != nil {
		t.Fatal(err)
	}
	if err := Render(&b2, r2, OutputJSON); err != nil {
		t.Fatal(err)
	}
	if b1.String() != b2.String() {
		t.Errorf("identical reports rendered differently:\n%s\n---\n%s", b1.String(), b2.String())
	}
}

// TestReport_AddFile_PreservesFindingOrderWithinFile pins that findings
// are kept in caller-provided order — critical because human-mode
// renderer sorts by line number for display, but JSON consumers may
// rely on the order findings were added (e.g. for chronological dedup).
func TestReport_AddFile_PreservesFindingOrderWithinFile(t *testing.T) {
	r := NewReport()
	findings := []Finding{
		NewFinding("a.sql", 5, RuleParserMultiStatement, "first"),
		NewFinding("a.sql", 1, RuleParserDeprecatedSink, "second"),
		NewFinding("a.sql", 3, RuleParserOther, "third"),
	}
	r.AddFile(FileResult{Path: "a.sql", Findings: findings})
	got := r.Files[0].Findings
	for i, want := range findings {
		if got[i].Message != want.Message {
			t.Errorf("findings[%d].Message = %q, want %q", i, got[i].Message, want.Message)
		}
	}
}

// TestReport_Counters_MultipleAdditions pins summary correctness when
// the same severity is added across multiple files. Codex round 6 flagged
// a bug where Clean was over-counted; this test pins the corrected logic.
func TestReport_Counters_MultipleAdditions(t *testing.T) {
	r := NewReport()
	// 3 clean files
	for _, p := range []string{"a.sql", "b.sql", "c.sql"} {
		r.AddFile(FileResult{Path: p})
	}
	// 1 file with 2 BLOCKERs (counts as 2 blockers, 0 clean)
	r.AddFile(FileResult{
		Path: "d.sql",
		Findings: []Finding{
			NewFinding("d.sql", 1, RuleParserMultiStatement, ""),
			NewFinding("d.sql", 2, RuleStrictFetchCastRequired, ""),
		},
	})
	// 1 file with 1 WARN (validate.* family — only WARN family left)
	r.AddFile(FileResult{
		Path:     "e.sql",
		Findings: []Finding{NewFinding("e.sql", 1, RuleValidateExtensionsLoadFailed, "")},
	})
	want := Summary{Files: 5, Blockers: 2, Warns: 1, Infos: 0, Clean: 3}
	if r.Summary != want {
		t.Errorf("Summary = %+v, want %+v", r.Summary, want)
	}
}

func TestParseFormat(t *testing.T) {
	cases := []struct {
		in   string
		want OutputFormat
		err  bool
	}{
		{"", OutputHuman, false},
		{"human", OutputHuman, false},
		{"HUMAN", OutputHuman, false},
		{"json", OutputJSON, false},
		{"ndjson", OutputNDJSON, false},
		{"github", "", true},
		{"yaml", "", true},
	}
	for _, tc := range cases {
		got, err := ParseFormat(tc.in)
		if tc.err {
			if err == nil {
				t.Errorf("ParseFormat(%q): expected error, got %q", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseFormat(%q): unexpected error: %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseFormat(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
