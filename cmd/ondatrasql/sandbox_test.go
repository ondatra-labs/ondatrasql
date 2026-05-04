// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"strings"
	"testing"
)

func TestTruncate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input  string
		maxLen int
		want   string
	}{
		{"hello", 10, "hello"},
		{"hello world", 5, "he..."},
		{"", 5, ""},
		{"ab", 2, "ab"},
		{"abc", 3, "abc"},
		{"abcd", 3, "..."},
	}
	for _, tt := range tests {
		got := truncate(tt.input, tt.maxLen)
		if got != tt.want {
			t.Errorf("truncate(%q, %d) = %q, want %q", tt.input, tt.maxLen, got, tt.want)
		}
	}
}

func TestMin(t *testing.T) {
	t.Parallel()
	tests := []struct {
		a, b int
		want int
	}{
		{1, 2, 1},
		{2, 1, 1},
		{0, 0, 0},
		{-1, -2, -2},
	}
	for _, tt := range tests {
		got := min(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("min(%d, %d) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

// TestQuoteTarget verifies that schema.table targets get properly quoted
// for inclusion in SQL queries (defends against injection via model names).
func TestQuoteTarget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{"raw.orders", `"raw"."orders"`},
		{"staging.customer_history", `"staging"."customer_history"`},
		{"orders", `"orders"`}, // single-part fallback
	}
	for _, tt := range tests {
		got := quoteTarget(tt.input)
		if got != tt.want {
			t.Errorf("quoteTarget(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// TestTableExistsIn_InvalidTargetShape covers the shape-validation path.
// Targets without a schema.table separator must produce a clear error
// before any SQL is constructed — silently returning (false, nil) would
// classify a misformatted input as "missing" and feed it to the
// downstream "new table" branch.
func TestTableExistsIn_InvalidTargetShape(t *testing.T) {
	t.Parallel()
	_, err := tableExistsIn(nil, "lake", "no_schema_separator")
	if err == nil {
		t.Fatal("expected error for invalid target shape")
	}
	if !strings.Contains(err.Error(), "schema.table") {
		t.Errorf("error should mention expected format, got: %v", err)
	}
}

// TestParseSandboxCount regression-tests the helper that replaced 11
// fmt.Sscanf("%d") sites in sandbox.go. The earlier Sscanf calls
// silently dropped parse failures, leaving the destination int64 at 0
// — Codex round 3 flagged this as an actual bug risk because a
// malformed COUNT() result (NULL, empty, non-numeric) would corrupt
// the sandbox diff display.
//
// Contract: returns the parsed value on success; returns 0 on parse
// failure rather than the previous undefined behaviour. The query
// templates that feed this all produce numeric strings so a parse
// failure indicates a programmer bug in the SQL template — falling
// back to 0 keeps the diff rendering instead of crashing the summary.
func TestParseSandboxCount(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want int64
	}{
		{"0", 0},
		{"42", 42},
		{"9223372036854775807", 9223372036854775807}, // max int64
		{"-1", -1},
		{"", 0},          // empty → 0
		{"   ", 0},       // whitespace → 0
		{"abc", 0},       // non-numeric → 0
		{"12abc", 0},     // partially numeric → 0 (strconv stricter than Sscanf)
		{"1.5", 0},       // float → 0
		{"NULL", 0},      // SQL NULL string representation → 0
	}
	for _, tc := range cases {
		got := parseSandboxCount(tc.in)
		if got != tc.want {
			t.Errorf("parseSandboxCount(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}
