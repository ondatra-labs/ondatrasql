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

func TestFormatNumber(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0"},
		{1, "1"},
		{999, "999"},
		{1000, "1.0K"},
		{1500, "1.5K"},
		{10000, "10.0K"},
		{999999, "1000.0K"},
		{1000000, "1.0M"},
		{2500000, "2.5M"},
		{-1, "-1"},
	}
	for _, tt := range tests {
		got := formatNumber(tt.input)
		if got != tt.want {
			t.Errorf("formatNumber(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestShortenPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{"", "N/A"},
		{"models/staging/orders.sql", "orders.sql"},
		{"orders.sql", "orders.sql"},
		{"/absolute/path/to/file.star", "file.star"},
	}
	for _, tt := range tests {
		got := shortenPath(tt.input)
		if got != tt.want {
			t.Errorf("shortenPath(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestShortenTime(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{"", "N/A"},
		{"2024-01-15", "2024-01-15"},
		{"2024-01-15 14:30", "14:30"},
		{"2024-01-15 14:30:00", "14:30:00"},
	}
	for _, tt := range tests {
		got := shortenTime(tt.input)
		if got != tt.want {
			t.Errorf("shortenTime(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestRuneLen(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  int
	}{
		{"", 0},
		{"hello", 5},
		{"日本語", 3},
		{"abc日", 4},
	}
	for _, tt := range tests {
		got := runeLen(tt.input)
		if got != tt.want {
			t.Errorf("runeLen(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestDisplayWidth(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  int
	}{
		{"", 0},
		{"hello", 5},
		{"日本語", 3},
	}
	for _, tt := range tests {
		got := displayWidth(tt.input)
		if got != tt.want {
			t.Errorf("displayWidth(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

// TestPrintModelBox_RendersGatherWarnings regression-tests that
// non-fatal stats-collection failures collected on
// ModelInfo.GatherWarnings are surfaced in the human view rather than
// silently dropped. Previously parse failures (Atoi/ParseInt/ParseFloat)
// and helper-query errors (size, recent runs, downstream models) were
// swallowed.
func TestPrintModelBox_RendersGatherWarnings(t *testing.T) {
	old := output.Human
	defer func() { output.Human = old }()
	var buf strings.Builder
	output.Human = &buf

	info := &ModelInfo{
		SchemaVersion: 1,
		Target:        "staging.orders",
		Kind:          "table",
		GatherWarnings: []string{
			"stats query: connection refused",
			"parse total_runs: invalid syntax",
		},
	}
	printModelBox(info)

	got := buf.String()
	if !strings.Contains(got, "Gather Warnings") {
		t.Errorf("expected 'Gather Warnings' section, got: %s", got)
	}
	if !strings.Contains(got, "connection refused") || !strings.Contains(got, "parse total_runs") {
		t.Errorf("expected both warning messages, got: %s", got)
	}
}

// TestModelInfo_JSON_BlueprintCrossLink locks in the JSON contract for
// the v0.31 blueprint cross-link fields:
//   - blueprint: name of the lib function the model fetches from.
//     Omitted when empty so consumers can detect "no lib call" directly.
//   - blueprint_error: structured signal that blueprint detection
//     couldn't complete (lib/ failed to parse, AST serialisation failed,
//     or model references a broken blueprint).
//
// The two fields are independent: a model can have one, both, or
// neither populated. JSON consumers (agents) rely on this exact shape
// to decide whether to follow up with `describe blueprint <name>` or
// surface the detection failure to the user.
func TestModelInfo_JSON_BlueprintCrossLink(t *testing.T) {
	cases := []struct {
		name           string
		info           *ModelInfo
		mustContain    []string
		mustNotContain []string
	}{
		{
			name:           "no_lib_call_omits_both",
			info:           &ModelInfo{Target: "x.y"},
			mustNotContain: []string{`"blueprint"`, `"blueprint_error"`},
		},
		{
			name:           "blueprint_resolved",
			info:           &ModelInfo{Target: "x.y", Blueprint: "mistral_ocr"},
			mustContain:    []string{`"blueprint":"mistral_ocr"`},
			mustNotContain: []string{`"blueprint_error"`},
		},
		{
			name:           "detection_failure",
			info:           &ModelInfo{Target: "x.y", BlueprintError: "AST serialization failed: ..."},
			mustContain:    []string{`"blueprint_error":"AST serialization failed: ..."`},
			mustNotContain: []string{`"blueprint":`},
		},
		{
			name: "both_populated_when_partial_resolution",
			info: &ModelInfo{Target: "x.y", Blueprint: "mistral_ocr",
				BlueprintError: "model uses blueprint \"other_lib\" which failed to parse"},
			mustContain: []string{`"blueprint":"mistral_ocr"`, `"blueprint_error":`},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal(tc.info)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			got := string(b)
			for _, want := range tc.mustContain {
				if !strings.Contains(got, want) {
					t.Errorf("missing %q in: %s", want, got)
				}
			}
			for _, dont := range tc.mustNotContain {
				if strings.Contains(got, dont) {
					t.Errorf("unexpected %q in: %s", dont, got)
				}
			}
		})
	}
}

// TestModelInfo_JSON_GatherWarnings ensures GatherWarnings serialises
// under the snake_case key `gather_warnings` and is omitted when empty.
// Pairs with the rendering test above to lock in the schema.
func TestModelInfo_JSON_GatherWarnings(t *testing.T) {
	clean, err := json.Marshal(&ModelInfo{Target: "x.y"})
	if err != nil {
		t.Fatalf("marshal clean: %v", err)
	}
	if strings.Contains(string(clean), "gather_warnings") {
		t.Errorf("clean ModelInfo should omit gather_warnings, got: %s", clean)
	}

	dirty, err := json.Marshal(&ModelInfo{Target: "x.y", GatherWarnings: []string{"oops"}})
	if err != nil {
		t.Fatalf("marshal dirty: %v", err)
	}
	if !strings.Contains(string(dirty), `"gather_warnings":["oops"]`) {
		t.Errorf("dirty ModelInfo should include gather_warnings, got: %s", dirty)
	}
}

func TestMax(t *testing.T) {
	t.Parallel()
	tests := []struct {
		a, b int
		want int
	}{
		{1, 2, 2},
		{2, 1, 2},
		{0, 0, 0},
		{-1, -2, -1},
	}
	for _, tt := range tests {
		got := max(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("max(%d, %d) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}
