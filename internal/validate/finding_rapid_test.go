// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package validate

import (
	"testing"

	"pgregory.net/rapid"
)

// TestRapid_Report_SummaryInvariants pins that Summary counters always
// match the actual content of Files. Property-based — generates random
// reports with random findings and verifies the invariants hold.
//
// This catches Codex round 6 (Clean overcount) as a regression — and
// any future similar bookkeeping bug — by stress-testing the AddFile
// path with thousands of random inputs.
func TestRapid_Report_SummaryInvariants(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		r := NewReport()
		fileCount := rapid.IntRange(0, 50).Draw(t, "fileCount")
		for i := 0; i < fileCount; i++ {
			path := rapid.StringMatching(`[a-z]+\.sql`).Draw(t, "path")
			findingCount := rapid.IntRange(0, 5).Draw(t, "findingCount")
			findings := make([]Finding, 0, findingCount)
			for j := 0; j < findingCount; j++ {
				ruleIdx := rapid.IntRange(0, 4).Draw(t, "ruleIdx")
				rules := []RuleID{
					RuleParserMultiStatement,         // BLOCKER
					RuleStrictFetchCastRequired,      // BLOCKER
					RuleParserDeprecatedSink,         // WARN
					RuleDagExternalRef,               // INFO
					RuleValidateExtensionsLoadFailed, // WARN (validate.*)
				}
				findings = append(findings, NewFinding(path, j+1, rules[ruleIdx], "msg"))
			}
			r.AddFile(FileResult{Path: path, Findings: findings})
		}

		// Invariant 1: Summary.Files equals len(Files)
		if r.Summary.Files != len(r.Files) {
			t.Errorf("Summary.Files=%d but len(Files)=%d", r.Summary.Files, len(r.Files))
		}

		// Invariant 2: Clean count equals number of files with no findings
		actualClean := 0
		for _, f := range r.Files {
			if len(f.Findings) == 0 {
				actualClean++
			}
		}
		if r.Summary.Clean != actualClean {
			t.Errorf("Summary.Clean=%d but actual=%d", r.Summary.Clean, actualClean)
		}

		// Invariant 3: severity counters sum equals total non-clean findings
		actualB, actualW, actualI := 0, 0, 0
		for _, f := range r.Files {
			for _, fn := range f.Findings {
				switch fn.Severity {
				case SeverityBlocker:
					actualB++
				case SeverityWarn:
					actualW++
				case SeverityInfo:
					actualI++
				}
			}
		}
		if r.Summary.Blockers != actualB {
			t.Errorf("Summary.Blockers=%d but actual=%d", r.Summary.Blockers, actualB)
		}
		if r.Summary.Warns != actualW {
			t.Errorf("Summary.Warns=%d but actual=%d", r.Summary.Warns, actualW)
		}
		if r.Summary.Infos != actualI {
			t.Errorf("Summary.Infos=%d but actual=%d", r.Summary.Infos, actualI)
		}

		// Invariant 4: HasBlockers/HasWarns track the counters
		if (r.Summary.Blockers > 0) != r.HasBlockers() {
			t.Errorf("HasBlockers() inconsistent with Blockers=%d", r.Summary.Blockers)
		}
		if (r.Summary.Warns > 0) != r.HasWarns() {
			t.Errorf("HasWarns() inconsistent with Warns=%d", r.Summary.Warns)
		}
	})
}

// TestRapid_SeverityFor_DefaultsToBlocker pins that any RuleID NOT
// explicitly listed in severityOverrides defaults to BLOCKER. Property:
// for any string input that doesn't match a known override, the
// severity is BLOCKER. Catches accidental override-table reorderings.
func TestRapid_SeverityFor_DefaultsToBlocker(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		s := rapid.String().Draw(t, "ruleID")
		id := RuleID(s)
		if _, isOverride := severityOverrides[id]; isOverride {
			return // skip — known override
		}
		if got := SeverityFor(id); got != SeverityBlocker {
			t.Errorf("SeverityFor(%q) = %q, want BLOCKER (default)", id, got)
		}
	})
}
