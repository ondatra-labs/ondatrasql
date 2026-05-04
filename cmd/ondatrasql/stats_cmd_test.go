// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/output"
)

func TestPrintStatsBox_Output(t *testing.T) {

	old := output.Human
	defer func() { output.Human = old }()

	var buf strings.Builder
	output.Human = &buf

	stats := &ProjectStats{
		ModelCount:  3,
		Snapshots:   10,
		TotalRuns:   25,
		TotalRows:   1000,
		AvgDuration: 150,
		LastRun:     "2024-01-15",
		KindBreakdown: []KindCount{
			{Kind: "table", Count: 3},
		},
		AllModels: []ModelStats{
			{Name: "raw.customers", Kind: "table", RunType: "full", Rows: 500, Duration: 120, LastRun: "01-15"},
			{Name: "staging.orders", Kind: "table", RunType: "incremental", Rows: 300, Duration: 80, LastRun: "01-14"},
		},
	}

	printStatsBox(stats)

	got := buf.String()

	checks := []string{
		"Project Statistics",
		"3",  // model count
		"10", // snapshots
		"25", // total runs
		"table",
		"customers",
		"orders",
		"raw",
		"staging",
	}
	for _, want := range checks {
		if !strings.Contains(got, want) {
			t.Errorf("output missing %q\n--- output ---\n%s", want, got)
		}
	}
}

func TestPrintStatsBox_Empty(t *testing.T) {

	old := output.Human
	defer func() { output.Human = old }()

	var buf strings.Builder
	output.Human = &buf

	stats := &ProjectStats{}
	printStatsBox(stats)

	got := buf.String()
	if !strings.Contains(got, "Project Statistics") {
		t.Errorf("expected header in output, got: %s", got)
	}
	// Should not contain kind breakdown or models sections
	if strings.Contains(got, "Models by Kind") {
		t.Error("expected no 'Models by Kind' section for empty stats")
	}
}

// TestPrintStatsBox_RendersWarnings regression-tests that partial-output
// failures collected on ProjectStats.Warnings are surfaced in the human
// view rather than silently dropped. Previously sub-query errors
// (snapshots, kind breakdown, ducklake_settings) were swallowed.
func TestPrintStatsBox_RendersWarnings(t *testing.T) {
	old := output.Human
	defer func() { output.Human = old }()
	var buf strings.Builder
	output.Human = &buf

	stats := &ProjectStats{
		ModelCount: 1,
		Warnings:   []string{"snapshot count: connection refused", "ducklake settings: catalog detached"},
	}
	printStatsBox(stats)

	got := buf.String()
	if !strings.Contains(got, "Warnings") {
		t.Errorf("expected 'Warnings' section header in output, got: %s", got)
	}
	if !strings.Contains(got, "snapshot count") || !strings.Contains(got, "ducklake settings") {
		t.Errorf("expected both warning messages, got: %s", got)
	}
}

// TestProjectStats_JSON_AlwaysEmitsWarnings pins the R7 #5 fix:
// warnings is no longer omitempty — it always emits, as `[]` when
// empty, for shape parity with history --json's envelope. Typed
// clients can decode unconditionally instead of branching on key
// presence.
func TestProjectStats_JSON_AlwaysEmitsWarnings(t *testing.T) {
	// Empty warnings → `"warnings":[]` (NOT omitted).
	// Use an explicit empty slice; a nil slice would marshal to `null`
	// which is also an envelope drift — runStats now initialises the
	// slice non-nil for this reason.
	clean, err := json.Marshal(&ProjectStats{ModelCount: 5, Warnings: []string{}})
	if err != nil {
		t.Fatalf("marshal clean: %v", err)
	}
	if !strings.Contains(string(clean), `"warnings":[]`) {
		t.Errorf("clean stats should emit warnings:[], got: %s", clean)
	}

	dirty, err := json.Marshal(&ProjectStats{ModelCount: 5, Warnings: []string{"x"}})
	if err != nil {
		t.Fatalf("marshal dirty: %v", err)
	}
	if !strings.Contains(string(dirty), `"warnings":["x"]`) {
		t.Errorf("dirty stats should include warnings, got: %s", dirty)
	}
}

// TestProjectStats_JSON_StableArrayShape regression-tests that
// kind_breakdown and all_models always emit as JSON arrays (possibly
// empty) rather than being omitted when nil. JSON consumers (agents,
// CI scripts) rely on the field being present so they can iterate
// without first checking existence — and so an empty project doesn't
// emit a different shape than a populated one.
func TestProjectStats_JSON_StableArrayShape(t *testing.T) {
	// Empty stats — must still emit kind_breakdown:[] and all_models:[].
	empty, err := json.Marshal(&ProjectStats{
		SchemaVersion: 1,
		KindBreakdown: []KindCount{},
		AllModels:     []ModelStats{},
	})
	if err != nil {
		t.Fatalf("marshal empty: %v", err)
	}
	got := string(empty)
	if !strings.Contains(got, `"kind_breakdown":[]`) {
		t.Errorf("missing kind_breakdown:[] in empty stats: %s", got)
	}
	if !strings.Contains(got, `"all_models":[]`) {
		t.Errorf("missing all_models:[] in empty stats: %s", got)
	}

	// Populated stats — same key, populated array.
	populated, err := json.Marshal(&ProjectStats{
		SchemaVersion: 1,
		KindBreakdown: []KindCount{{Kind: "table", Count: 3}},
		AllModels:     []ModelStats{{Name: "x.y", Kind: "table"}},
	})
	if err != nil {
		t.Fatalf("marshal populated: %v", err)
	}
	got = string(populated)
	if !strings.Contains(got, `"kind_breakdown":[{`) {
		t.Errorf("missing populated kind_breakdown: %s", got)
	}
	if !strings.Contains(got, `"all_models":[{`) {
		t.Errorf("missing populated all_models: %s", got)
	}
}

func TestPrintStatsBox_LargeNumbers(t *testing.T) {

	old := output.Human
	defer func() { output.Human = old }()

	var buf strings.Builder
	output.Human = &buf

	stats := &ProjectStats{
		ModelCount: 50,
		TotalRows:  2500000,
		LastRun:    "2024-06-01",
	}
	printStatsBox(stats)

	got := buf.String()
	// formatNumber should render 2500000 as "2.5M"
	if !strings.Contains(got, "2.5M") {
		t.Errorf("expected formatted large number '2.5M' in output, got: %s", got)
	}
}
