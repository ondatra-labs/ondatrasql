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

func TestSplitStatements_BlockComment(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  int
	}{
		{"semicolon inside block comment", "SELECT 1 /* ; */ AS id", 1},
		{"block comment spanning lines", "SELECT /* multi\n; line */ 1", 1},
		{"real semicolon after block comment", "SELECT /* x */ 1; SELECT 2", 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stmts := splitStatements(tt.input)
			if len(stmts) != tt.want {
				t.Errorf("splitStatements(%q) = %d stmts %v, want %d",
					tt.input, len(stmts), stmts, tt.want)
			}
		})
	}
}

func TestSplitStatements_DoubleQuotedIdentifier(t *testing.T) {
	t.Parallel()
	stmts := splitStatements(`SELECT "col;name" FROM t`)
	if len(stmts) != 1 {
		t.Errorf("semicolon inside double-quoted identifier should not split, got %d: %v", len(stmts), stmts)
	}
}

func TestSplitStatements_LineComment(t *testing.T) {
	t.Parallel()
	stmts := splitStatements("SELECT 1 -- ; comment\n; SELECT 2")
	if len(stmts) != 2 {
		t.Errorf("line comment should not consume real semicolon on next line, got %d: %v", len(stmts), stmts)
	}
}

func TestSplitStatements_EscapedQuotes(t *testing.T) {
	t.Parallel()
	// Before fix: '' inside a string was treated as close+open, causing
	// a semicolon between the two quotes to split the statement.
	tests := []struct {
		name string
		input string
		want int
	}{
		{"escaped single quote with semicolon", "SELECT 'it''s; here'", 1},
		{"multiple escaped quotes", "SELECT 'a''b''c; still one'", 1},
		{"escaped quote at end then new stmt", "SELECT 'val'''; SELECT 2", 2},
		{"double-quoted with semicolon", `SELECT "col;name" FROM t`, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stmts := splitStatements(tt.input)
			if len(stmts) != tt.want {
				t.Errorf("splitStatements(%q) = %d statements %v, want %d",
					tt.input, len(stmts), stmts, tt.want)
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
