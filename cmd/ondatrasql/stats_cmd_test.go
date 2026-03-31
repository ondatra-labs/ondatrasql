// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
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
