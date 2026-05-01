// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
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

	t.Run("changing group_key changes hash", func(t *testing.T) {
		t.Parallel()
		d := baseDirectives
		d.GroupKey = "source_file"
		h1 := ModelHash(baseSQL, baseDirectives)
		h2 := ModelHash(baseSQL, d)
		if h1 == h2 {
			t.Error("changing @group_key should produce different hash")
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

	// v0.30.0: @fetch and @push are part of the hash so adding/removing
	// them triggers backfill — and therefore validators run on the next
	// run instead of being silently bypassed by the skip path.
	t.Run("toggling @fetch changes hash", func(t *testing.T) {
		t.Parallel()
		d := baseDirectives
		d.Fetch = true
		h1 := ModelHash(baseSQL, baseDirectives)
		h2 := ModelHash(baseSQL, d)
		if h1 == h2 {
			t.Error("toggling @fetch should produce different hash")
		}
	})

	t.Run("changing @push changes hash", func(t *testing.T) {
		t.Parallel()
		d := baseDirectives
		d.Push = "hubspot_push"
		h1 := ModelHash(baseSQL, baseDirectives)
		h2 := ModelHash(baseSQL, d)
		if h1 == h2 {
			t.Error("setting @push should produce different hash")
		}
		// And changing the push name should also flip
		d2 := d
		d2.Push = "salesforce_push"
		h3 := ModelHash(baseSQL, d2)
		if h2 == h3 {
			t.Error("changing @push lib name should produce different hash")
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

// TestModelHash_FormatStability pins the absolute hash format. Bumping
// these snapshot values silently invalidates every existing model's
// stored hash and forces a backfill across the fleet — it should only
// happen as part of an intentional release-note-grade migration. If
// you change the WriteString sequence in ModelHash, update these
// values explicitly so reviewers see the cost on the diff.
func TestModelHash_FormatStability(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		sql  string
		d    ModelDirectives
		want string
	}{
		{
			"plain table",
			"SELECT 1 AS id",
			ModelDirectives{Kind: "table"},
			"469e018fdea495b95da7807bc3271559fd35ca362450db5a301fd7187b0396cc",
		},
		{
			"fetch+incremental",
			"SELECT id::BIGINT AS id FROM api()",
			ModelDirectives{Kind: "append", Fetch: true, Incremental: "id"},
			"0e062843492bb86b0e6404add0fada4b90b78fc1ff52d72a275ddae8378bd0db",
		},
		{
			"push merge",
			"SELECT id::BIGINT AS id FROM staging.t",
			ModelDirectives{Kind: "merge", UniqueKey: "id", Push: "hubspot_push"},
			"ecdbaf20aea4851d5876da951865bb724df7984d8ee0a12eed1669f192433130",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ModelHash(c.sql, c.d)
			if got != c.want {
				t.Errorf("hash format drift!\n got:  %s\n want: %s\n\nIf this change is intentional (and you accept that every existing model rebuilds), update the snapshot. Otherwise revert the WriteString sequence in ModelHash.", got, c.want)
			}
		})
	}
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

func TestConfigHash_Subdirectories(t *testing.T) {
	t.Parallel()

	t.Run("includes macros subdirectory", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		os.WriteFile(filepath.Join(dir, "catalog.sql"), []byte("ATTACH 'lake';"), 0o644)
		h1 := ConfigHash(dir)

		os.MkdirAll(filepath.Join(dir, "macros"), 0o755)
		os.WriteFile(filepath.Join(dir, "macros", "audits.sql"), []byte("CREATE MACRO m() AS 1;"), 0o644)
		h2 := ConfigHash(dir)

		if h1 == h2 {
			t.Error("adding macros/audits.sql must change config hash")
		}
	})

	t.Run("includes variables subdirectory", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		os.WriteFile(filepath.Join(dir, "catalog.sql"), []byte("ATTACH 'lake';"), 0o644)
		h1 := ConfigHash(dir)

		os.MkdirAll(filepath.Join(dir, "variables"), 0o755)
		os.WriteFile(filepath.Join(dir, "variables", "local.sql"), []byte("SET VARIABLE x = 1;"), 0o644)
		h2 := ConfigHash(dir)

		if h1 == h2 {
			t.Error("adding variables/local.sql must change config hash")
		}
	})

	t.Run("change in subdirectory file changes hash", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "macros"), 0o755)
		os.WriteFile(filepath.Join(dir, "macros", "audits.sql"), []byte("v1"), 0o644)
		h1 := ConfigHash(dir)

		os.WriteFile(filepath.Join(dir, "macros", "audits.sql"), []byte("v2"), 0o644)
		h2 := ConfigHash(dir)

		if h1 == h2 {
			t.Error("changing macros/audits.sql must change config hash")
		}
	})

	t.Run("subdirectory deterministic across runs", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "macros"), 0o755)
		os.MkdirAll(filepath.Join(dir, "variables"), 0o755)
		os.WriteFile(filepath.Join(dir, "catalog.sql"), []byte("ATTACH 'lake';"), 0o644)
		os.WriteFile(filepath.Join(dir, "macros", "a.sql"), []byte("a"), 0o644)
		os.WriteFile(filepath.Join(dir, "macros", "b.sql"), []byte("b"), 0o644)
		os.WriteFile(filepath.Join(dir, "variables", "local.sql"), []byte("x"), 0o644)

		h1 := ConfigHash(dir)
		h2 := ConfigHash(dir)
		if h1 != h2 {
			t.Error("same content must produce same hash across calls")
		}
	})

	t.Run("ignores non-sql in subdirectories", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "macros"), 0o755)
		os.WriteFile(filepath.Join(dir, "macros", "audits.sql"), []byte("m"), 0o644)
		h1 := ConfigHash(dir)

		os.WriteFile(filepath.Join(dir, "macros", "README.md"), []byte("ignore"), 0o644)
		h2 := ConfigHash(dir)

		if h1 != h2 {
			t.Error("non-sql files in subdirectories should not affect hash")
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
