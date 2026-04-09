// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package backfill

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHash(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		sql1   string
		sql2   string
		equal  bool
	}{
		{
			name:  "identical SQL",
			sql1:  "SELECT * FROM orders",
			sql2:  "SELECT * FROM orders",
			equal: true,
		},
		{
			name:  "different whitespace",
			sql1:  "SELECT * FROM orders",
			sql2:  "SELECT  *  FROM  orders",
			equal: true,
		},
		{
			name:  "different newlines",
			sql1:  "SELECT * FROM orders",
			sql2:  "SELECT *\nFROM orders",
			equal: true,
		},
		{
			name:  "with comments",
			sql1:  "SELECT * FROM orders",
			sql2:  "-- comment\nSELECT * FROM orders",
			equal: true,
		},
		{
			name:  "case insensitive",
			sql1:  "SELECT * FROM orders",
			sql2:  "select * from orders",
			equal: true,
		},
		{
			name:  "different SQL",
			sql1:  "SELECT * FROM orders",
			sql2:  "SELECT * FROM customers",
			equal: false,
		},
		{
			name:  "different columns",
			sql1:  "SELECT id FROM orders",
			sql2:  "SELECT name FROM orders",
			equal: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h1 := Hash(tt.sql1)
			h2 := Hash(tt.sql2)

			if tt.equal && h1 != h2 {
				t.Errorf("expected equal hashes:\n  sql1: %q -> %s\n  sql2: %q -> %s", tt.sql1, h1, tt.sql2, h2)
			}
			if !tt.equal && h1 == h2 {
				t.Errorf("expected different hashes:\n  sql1: %q -> %s\n  sql2: %q -> %s", tt.sql1, h1, tt.sql2, h2)
			}
		})
	}
}

func TestModelHash(t *testing.T) {
	t.Parallel()
	baseSQL := "SELECT * FROM orders"
	baseDirectives := ModelDirectives{
		Kind:               "append",
		UniqueKey:          "",
		Incremental:        "updated_at",
		IncrementalInitial: "2026-02-28",
	}

	t.Run("same code and directives produce same hash", func(t *testing.T) {
		t.Parallel()
		h1 := ModelHash(baseSQL, baseDirectives)
		h2 := ModelHash(baseSQL, baseDirectives)
		if h1 != h2 {
			t.Errorf("same inputs should produce same hash: %s != %s", h1, h2)
		}
	})

	t.Run("changing incremental_initial changes hash", func(t *testing.T) {
		t.Parallel()
		d := baseDirectives
		d.IncrementalInitial = "2026-02-25"
		h1 := ModelHash(baseSQL, baseDirectives)
		h2 := ModelHash(baseSQL, d)
		if h1 == h2 {
			t.Error("changing @incremental_initial should produce different hash")
		}
	})

	t.Run("changing kind changes hash", func(t *testing.T) {
		t.Parallel()
		d := baseDirectives
		d.Kind = "merge"
		h1 := ModelHash(baseSQL, baseDirectives)
		h2 := ModelHash(baseSQL, d)
		if h1 == h2 {
			t.Error("changing @kind should produce different hash")
		}
	})

	t.Run("changing unique_key changes hash", func(t *testing.T) {
		t.Parallel()
		d := baseDirectives
		d.UniqueKey = "id"
		h1 := ModelHash(baseSQL, baseDirectives)
		h2 := ModelHash(baseSQL, d)
		if h1 == h2 {
			t.Error("changing @unique_key should produce different hash")
		}
	})

	t.Run("changing partitioned_by changes hash", func(t *testing.T) {
		t.Parallel()
		d := baseDirectives
		d.PartitionedBy = []string{"region"}
		h1 := ModelHash(baseSQL, baseDirectives)
		h2 := ModelHash(baseSQL, d)
		if h1 == h2 {
			t.Error("changing @partitioned_by should produce different hash")
		}
	})

	t.Run("changing incremental cursor changes hash", func(t *testing.T) {
		t.Parallel()
		d := baseDirectives
		d.Incremental = "created_at"
		h1 := ModelHash(baseSQL, baseDirectives)
		h2 := ModelHash(baseSQL, d)
		if h1 == h2 {
			t.Error("changing @incremental cursor should produce different hash")
		}
	})

	t.Run("changing SQL changes hash", func(t *testing.T) {
		t.Parallel()
		h1 := ModelHash("SELECT * FROM orders", baseDirectives)
		h2 := ModelHash("SELECT * FROM customers", baseDirectives)
		if h1 == h2 {
			t.Error("changing SQL should produce different hash")
		}
	})

	t.Run("ModelHash differs from Hash for same SQL", func(t *testing.T) {
		t.Parallel()
		h1 := Hash(baseSQL)
		h2 := ModelHash(baseSQL, baseDirectives)
		if h1 == h2 {
			t.Error("ModelHash should differ from Hash (includes directives)")
		}
	})
}

func TestHash_Length(t *testing.T) {
	t.Parallel()
	// SHA256 should always produce 64 hex chars
	for _, input := range []string{"", "SELECT 1", "-- only comment"} {
		h := Hash(input)
		if len(h) != 64 {
			t.Errorf("Hash(%q) length = %d, want 64", input, len(h))
		}
	}
}

func TestHash_OnlyComments(t *testing.T) {
	t.Parallel()
	// Two different comment-only SQLs should hash the same (both normalize to empty)
	h1 := Hash("-- comment A")
	h2 := Hash("-- comment B")
	if h1 != h2 {
		t.Errorf("comment-only SQL should hash to same value")
	}
}

func TestModelHash_ConfigHash(t *testing.T) {
	t.Parallel()
	sql := "SELECT mask_ssn(ssn) FROM users"
	d := ModelDirectives{Kind: "table"}

	t.Run("empty config hash preserves backward compat", func(t *testing.T) {
		t.Parallel()
		h1 := ModelHash(sql, d)
		d2 := d
		d2.ConfigHash = ""
		h2 := ModelHash(sql, d2)
		if h1 != h2 {
			t.Error("empty ConfigHash should produce same hash as no ConfigHash")
		}
	})

	t.Run("config change busts hash", func(t *testing.T) {
		t.Parallel()
		d1 := d
		d1.ConfigHash = "aaa"
		d2 := d
		d2.ConfigHash = "bbb"
		h1 := ModelHash(sql, d1)
		h2 := ModelHash(sql, d2)
		if h1 == h2 {
			t.Error("different ConfigHash should produce different model hash")
		}
	})
}

func TestConfigHash(t *testing.T) {
	t.Parallel()

	t.Run("missing dir returns empty", func(t *testing.T) {
		t.Parallel()
		h := ConfigHash("/nonexistent/path")
		if h != "" {
			t.Errorf("missing dir should return empty, got %q", h)
		}
	})

	t.Run("empty dir returns empty", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		h := ConfigHash(dir)
		if h != "" {
			t.Errorf("empty dir should return empty, got %q", h)
		}
	})

	t.Run("same content same hash", func(t *testing.T) {
		t.Parallel()
		dir1 := t.TempDir()
		dir2 := t.TempDir()
		os.WriteFile(filepath.Join(dir1, "macros.sql"), []byte("CREATE MACRO m() AS 1;"), 0o644)
		os.WriteFile(filepath.Join(dir2, "macros.sql"), []byte("CREATE MACRO m() AS 1;"), 0o644)
		if ConfigHash(dir1) != ConfigHash(dir2) {
			t.Error("same content should produce same hash")
		}
	})

	t.Run("different content different hash", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		os.WriteFile(filepath.Join(dir, "macros.sql"), []byte("CREATE MACRO m() AS 1;"), 0o644)
		h1 := ConfigHash(dir)
		os.WriteFile(filepath.Join(dir, "macros.sql"), []byte("CREATE MACRO m() AS 2;"), 0o644)
		h2 := ConfigHash(dir)
		if h1 == h2 {
			t.Error("different content should produce different hash")
		}
	})

	t.Run("ignores non-sql files", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		os.WriteFile(filepath.Join(dir, "macros.sql"), []byte("CREATE MACRO m() AS 1;"), 0o644)
		h1 := ConfigHash(dir)
		os.WriteFile(filepath.Join(dir, "README.md"), []byte("ignore me"), 0o644)
		h2 := ConfigHash(dir)
		if h1 != h2 {
			t.Error("non-sql files should not affect hash")
		}
	})
}

func TestNormalize_EdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"empty", "", ""},
		{"only comments", "-- comment\n-- another", ""},
		{"only whitespace", "   \t\n  ", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := normalize(tt.input)
			if got != tt.want {
				t.Errorf("normalize(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestNormalize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple",
			input:    "SELECT * FROM orders",
			expected: "select * from orders",
		},
		{
			name:     "with comment",
			input:    "-- this is a comment\nSELECT * FROM orders",
			expected: "select * from orders",
		},
		{
			name:     "inline comment",
			input:    "SELECT * -- all columns\nFROM orders",
			expected: "select * from orders",
		},
		{
			name:     "multiple spaces",
			input:    "SELECT   *   FROM   orders",
			expected: "select * from orders",
		},
		{
			name:     "tabs and newlines",
			input:    "SELECT\t*\n\tFROM\n\t\torders",
			expected: "select * from orders",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := normalize(tt.input)
			if result != tt.expected {
				t.Errorf("normalize(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}
