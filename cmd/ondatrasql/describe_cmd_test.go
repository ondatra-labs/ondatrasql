// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"testing"
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
