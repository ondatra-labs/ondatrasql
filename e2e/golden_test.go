// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build e2e

package e2e

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/execute"
)

var updateGolden = flag.Bool("update-golden", false, "update golden files")

// goldenSnapshot captures deterministic test output for golden comparison.
type goldenSnapshot struct {
	lines []string
}

func newSnapshot() *goldenSnapshot {
	return &goldenSnapshot{}
}

func (s *goldenSnapshot) addResult(r *execute.Result) {
	if r == nil {
		s.lines = append(s.lines, "result: <nil>")
		return
	}
	s.lines = append(s.lines, fmt.Sprintf("target: %s", r.Target))
	s.lines = append(s.lines, fmt.Sprintf("kind: %s", r.Kind))
	s.lines = append(s.lines, fmt.Sprintf("run_type: %s", r.RunType))
	s.lines = append(s.lines, fmt.Sprintf("rows_affected: %d", r.RowsAffected))
	if r.RunReason != "" {
		s.lines = append(s.lines, fmt.Sprintf("run_reason: %s", r.RunReason))
	}
	for _, e := range r.Errors {
		s.lines = append(s.lines, fmt.Sprintf("error: %s", e))
	}
	for _, w := range r.Warnings {
		s.lines = append(s.lines, fmt.Sprintf("warning: %s", w))
	}
}

func (s *goldenSnapshot) addDAGResults(results map[string]*execute.Result) {
	// Sort by target name for deterministic output
	targets := make([]string, 0, len(results))
	for t := range results {
		targets = append(targets, t)
	}
	sort.Strings(targets)

	for _, target := range targets {
		s.addResult(results[target])
		s.lines = append(s.lines, "---")
	}
}

func (s *goldenSnapshot) addDAGResultsWithErrors(results map[string]*execute.Result, errors map[string]error) {
	// Collect all targets from both maps
	seen := make(map[string]bool)
	for t := range results {
		seen[t] = true
	}
	for t := range errors {
		seen[t] = true
	}
	targets := make([]string, 0, len(seen))
	for t := range seen {
		targets = append(targets, t)
	}
	sort.Strings(targets)

	for _, target := range targets {
		if r, ok := results[target]; ok {
			s.addResult(r)
		} else {
			s.lines = append(s.lines, fmt.Sprintf("target: %s", target))
		}
		if err, ok := errors[target]; ok && err != nil {
			s.lines = append(s.lines, fmt.Sprintf("exec_error: %s", normalizeError(err.Error())))
		}
		s.lines = append(s.lines, "---")
	}
}

func (s *goldenSnapshot) addQuery(sess *duckdb.Session, label, query string) {
	val, err := sess.QueryValue(query)
	if err != nil {
		s.lines = append(s.lines, fmt.Sprintf("%s: <error: %s>", label, normalizeError(err.Error())))
		return
	}
	s.lines = append(s.lines, fmt.Sprintf("%s: %s", label, val))
}

func (s *goldenSnapshot) addQueryRows(sess *duckdb.Session, label, query string) {
	result, err := sess.Query(query)
	if err != nil {
		s.lines = append(s.lines, fmt.Sprintf("%s: <error>", label))
		return
	}
	s.lines = append(s.lines, fmt.Sprintf("%s:", label))
	for _, line := range strings.Split(strings.TrimSpace(result), "\n") {
		s.lines = append(s.lines, "  "+line)
	}
}

func (s *goldenSnapshot) addLine(line string) {
	s.lines = append(s.lines, line)
}

func (s *goldenSnapshot) String() string {
	return strings.Join(s.lines, "\n") + "\n"
}

// assertDAGAccountsForAll verifies every model appears in results (no drops).
func assertDAGAccountsForAll(t *testing.T, totalModels int, results map[string]*execute.Result) {
	t.Helper()
	assertSummaryCounts(t, totalModels, results, nil)
}

// assertSummaryCounts verifies that DAG results account for every model —
// each model is either in results or errors, and the total matches expected.
// This guards against the scenario where query failures silently drop models
// from summary counts (the "warnings" counter fix in sandbox.go).
func assertSummaryCounts(t *testing.T, totalModels int, results map[string]*execute.Result, errors map[string]error) {
	t.Helper()

	var skipped, ran, failed int
	for target, r := range results {
		if errors[target] != nil {
			failed++
			continue
		}
		if r != nil && r.RunType == "skip" {
			skipped++
		} else {
			ran++
		}
	}
	// Models only in errors (no result)
	for target := range errors {
		if _, inResults := results[target]; !inResults {
			failed++
		}
	}

	accounted := skipped + ran + failed
	if accounted != totalModels {
		t.Errorf("summary count mismatch: skipped(%d) + ran(%d) + failed(%d) = %d, want %d",
			skipped, ran, failed, accounted, totalModels)
	}
}

// normalizeError removes path-specific or non-deterministic parts of error messages.
func normalizeError(s string) string {
	// Remove temp directory paths
	// Pattern: /tmp/TestXxx123456789/001/...
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		// Normalize temp paths
		if idx := strings.Index(line, "/tmp/"); idx >= 0 {
			if end := strings.Index(line[idx:], "/models/"); end >= 0 {
				lines[i] = line[:idx] + "<tmpdir>" + line[idx+end:]
			}
		}
	}
	return strings.Join(lines, "\n")
}

// assertGolden compares snapshot output against a golden file.
// With -update-golden flag, it writes/updates the golden file instead.
func assertGolden(t *testing.T, name string, snap *goldenSnapshot) {
	t.Helper()

	goldenPath := filepath.Join("testdata", name+".golden")
	got := snap.String()

	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(goldenPath), 0755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(goldenPath, []byte(got), 0644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		t.Logf("updated %s", goldenPath)
		return
	}

	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden file %s: %v\n\nRun with -update-golden to create it.\n\nGot:\n%s", goldenPath, err, got)
	}

	if got != string(want) {
		t.Errorf("golden mismatch for %s\n\n--- want ---\n%s\n--- got ---\n%s\n\nRun with -update-golden to update.", goldenPath, string(want), got)
	}
}
