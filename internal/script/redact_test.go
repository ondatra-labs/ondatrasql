// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"testing"
)

func TestRedactBearer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in, want string
	}{
		{"Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.abc.def", "Bearer [REDACTED]"},
		{"bearer abc123", "bearer [REDACTED]"},
		{"prefix Bearer tok suffix", "prefix Bearer [REDACTED] suffix"},
	}
	for _, tt := range tests {
		got := RedactSecrets(tt.in)
		if got != tt.want {
			t.Errorf("RedactSecrets(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestRedactBasic(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in, want string
	}{
		{"Basic dXNlcjpwYXNz", "Basic [REDACTED]"},
		{"basic dXNlcjpwYXNz", "basic [REDACTED]"},
	}
	for _, tt := range tests {
		got := RedactSecrets(tt.in)
		if got != tt.want {
			t.Errorf("RedactSecrets(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestRedactKeyValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in, want string
	}{
		{`token=abc123`, `token=[REDACTED]`},
		{`secret: "xyz"`, `secret: [REDACTED]`},
		{`api_key=sk-1234`, `api_key=[REDACTED]`},
		{`client_secret=mysecret`, `client_secret=[REDACTED]`},
		{`password: hunter2`, `password: [REDACTED]`},
	}
	for _, tt := range tests {
		got := RedactSecrets(tt.in)
		if got != tt.want {
			t.Errorf("RedactSecrets(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestRedactPreservesNonSecret(t *testing.T) {
	t.Parallel()
	inputs := []string{
		"hello world",
		"status=200",
		"name: John",
		"count=42",
		"https://example.com/api/v1",
	}
	for _, in := range inputs {
		got := RedactSecrets(in)
		if got != in {
			t.Errorf("RedactSecrets(%q) = %q, want unchanged", in, got)
		}
	}
}

func TestRedactURL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in, want string
	}{
		{
			"https://api.com?api_key=abc123&format=json",
			"https://api.com?api_key=%5BREDACTED%5D&format=json",
		},
		{
			"https://api.com?token=secret&page=1",
			"https://api.com?page=1&token=%5BREDACTED%5D",
		},
		{
			"https://api.com/data",
			"https://api.com/data",
		},
		{
			"https://api.com?client_secret=xyz&password=abc",
			"https://api.com?client_secret=%5BREDACTED%5D&password=%5BREDACTED%5D",
		},
	}
	for _, tt := range tests {
		got := RedactURL(tt.in)
		if got != tt.want {
			t.Errorf("RedactURL(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestRedactURLPreservesNonSensitive(t *testing.T) {
	t.Parallel()
	in := "https://api.com?page=1&limit=50"
	got := RedactURL(in)
	if got != in {
		t.Errorf("RedactURL(%q) = %q, want unchanged", in, got)
	}
}
