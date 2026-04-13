// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package backfill

import (
	"testing"
)

func TestEscapeSQL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{"", ""},
		{"hello", "hello"},
		{"it's", "it''s"},
		{"''", "''''"},
		{"a'b'c", "a''b''c"},
		{"no quotes here", "no quotes here"},
	}
	for _, tt := range tests {
		got := escapeSQL(tt.input)
		if got != tt.want {
			t.Errorf("escapeSQL(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestComputeSchemaHash(t *testing.T) {
	t.Parallel()
	t.Run("empty returns empty", func(t *testing.T) {
		t.Parallel()
		got := ComputeSchemaHash(nil)
		if got != "" {
			t.Errorf("got %q, want empty", got)
		}
		got2 := ComputeSchemaHash([]Column{})
		if got2 != "" {
			t.Errorf("got %q, want empty", got2)
		}
	})

	t.Run("deterministic", func(t *testing.T) {
		t.Parallel()
		cols := []Column{{Name: "id", Type: "INTEGER"}, {Name: "name", Type: "VARCHAR"}}
		h1 := ComputeSchemaHash(cols)
		h2 := ComputeSchemaHash(cols)
		if h1 != h2 {
			t.Errorf("not deterministic: %s != %s", h1, h2)
		}
	})

	t.Run("order independent", func(t *testing.T) {
		t.Parallel()
		cols1 := []Column{{Name: "a", Type: "INT"}, {Name: "b", Type: "TEXT"}}
		cols2 := []Column{{Name: "b", Type: "TEXT"}, {Name: "a", Type: "INT"}}
		h1 := ComputeSchemaHash(cols1)
		h2 := ComputeSchemaHash(cols2)
		if h1 != h2 {
			t.Errorf("order should not matter: %s != %s", h1, h2)
		}
	})

	t.Run("type change produces different hash", func(t *testing.T) {
		t.Parallel()
		cols1 := []Column{{Name: "id", Type: "INTEGER"}}
		cols2 := []Column{{Name: "id", Type: "BIGINT"}}
		h1 := ComputeSchemaHash(cols1)
		h2 := ComputeSchemaHash(cols2)
		if h1 == h2 {
			t.Error("different types should produce different hash")
		}
	})

	t.Run("extra column produces different hash", func(t *testing.T) {
		t.Parallel()
		cols1 := []Column{{Name: "id", Type: "INTEGER"}}
		cols2 := []Column{{Name: "id", Type: "INTEGER"}, {Name: "name", Type: "VARCHAR"}}
		h1 := ComputeSchemaHash(cols1)
		h2 := ComputeSchemaHash(cols2)
		if h1 == h2 {
			t.Error("different column count should produce different hash")
		}
	})

	t.Run("hash is 32 hex chars (MD5)", func(t *testing.T) {
		t.Parallel()
		cols := []Column{{Name: "x", Type: "INT"}}
		h := ComputeSchemaHash(cols)
		if len(h) != 32 {
			t.Errorf("hash length = %d, want 32", len(h))
		}
	})
}

func TestQuoteTarget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{"orders", `"orders"`},
		{"staging.orders", `"staging"."orders"`},
		{"a.b.c", `"a"."b"."c"`},
	}
	for _, tt := range tests {
		got := quoteTarget(tt.input)
		if got != tt.want {
			t.Errorf("quoteTarget(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
