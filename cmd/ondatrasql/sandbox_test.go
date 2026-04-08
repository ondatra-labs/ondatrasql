// OndatraSQL - You don't need a data stack anymore
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
