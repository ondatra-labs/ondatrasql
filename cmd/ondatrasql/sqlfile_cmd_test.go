// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"strings"
	"testing"
)

func TestSplitStatements(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "single statement",
			input:    "SELECT 1;",
			expected: []string{"SELECT 1"},
		},
		{
			name:     "multiple statements",
			input:    "SELECT 1; SELECT 2;",
			expected: []string{"SELECT 1", "SELECT 2"},
		},
		{
			name:     "semicolon in string",
			input:    "SELECT 'a;b'; SELECT 2;",
			expected: []string{"SELECT 'a;b'", "SELECT 2"},
		},
		{
			name:     "no trailing semicolon",
			input:    "SELECT 1",
			expected: []string{"SELECT 1"},
		},
		{
			name:     "with comments",
			input:    "-- comment\nSELECT 1;",
			expected: []string{"-- comment\nSELECT 1"},
		},
		{
			name:     "CALL statement with interval",
			input:    "CALL ducklake_expire_snapshots('lake', older_than => now() - INTERVAL '30 days');",
			expected: []string{"CALL ducklake_expire_snapshots('lake', older_than => now() - INTERVAL '30 days')"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := splitStatements(tt.input)
			if len(result) != len(tt.expected) {
				t.Errorf("splitStatements() got %d statements, want %d", len(result), len(tt.expected))
				return
			}
			for i, stmt := range result {
				if strings.TrimSpace(stmt) != strings.TrimSpace(tt.expected[i]) {
					t.Errorf("statement %d: got %q, want %q", i, stmt, tt.expected[i])
				}
			}
		})
	}
}

func TestTruncateSQL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    string
		maxLen   int
		expected string
	}{
		{
			name:     "short SQL",
			input:    "SELECT 1",
			maxLen:   20,
			expected: "SELECT 1",
		},
		{
			name:     "long SQL truncated",
			input:    "SELECT * FROM very_long_table_name WHERE condition = 'value'",
			maxLen:   30,
			expected: "SELECT * FROM very_long_tab...",
		},
		{
			name:     "multiline collapsed",
			input:    "SELECT *\nFROM table\nWHERE x = 1",
			maxLen:   50,
			expected: "SELECT * FROM table WHERE x = 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := truncateSQL(tt.input, tt.maxLen)
			if result != tt.expected {
				t.Errorf("truncateSQL() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestIsOnlyComments(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "only comments",
			input:    "-- comment\n-- another",
			expected: true,
		},
		{
			name:     "has SQL",
			input:    "-- comment\nSELECT 1",
			expected: false,
		},
		{
			name:     "empty",
			input:    "",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := isOnlyComments(tt.input)
			if result != tt.expected {
				t.Errorf("isOnlyComments() = %v, want %v", result, tt.expected)
			}
		})
	}
}
