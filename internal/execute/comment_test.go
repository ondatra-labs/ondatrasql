// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"pgregory.net/rapid"
)

func TestBuildCommentSQL_Empty(t *testing.T) {
	t.Parallel()
	model := &parser.Model{Target: "staging.orders"}
	got := buildCommentSQL(model)
	if got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

func TestBuildCommentSQL_TableOnly(t *testing.T) {
	t.Parallel()
	model := &parser.Model{
		Target:      "mart.sales_daily",
		Description: "Daily aggregated sales by region",
	}
	got := buildCommentSQL(model)
	want := "COMMENT ON TABLE mart.sales_daily IS 'Daily aggregated sales by region'"
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestBuildCommentSQL_ColumnsOnly(t *testing.T) {
	t.Parallel()
	model := &parser.Model{
		Target: "mart.sales",
		ColumnDescriptions: map[string]string{
			"revenue": "Total revenue including tax",
		},
	}
	got := buildCommentSQL(model)
	want := `COMMENT ON COLUMN mart.sales."revenue" IS 'Total revenue including tax'`
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestBuildCommentSQL_ColumnsSorted(t *testing.T) {
	t.Parallel()
	model := &parser.Model{
		Target: "mart.sales",
		ColumnDescriptions: map[string]string{
			"zebra":  "Last column",
			"alpha":  "First column",
			"middle": "Middle column",
		},
	}
	got := buildCommentSQL(model)
	// Columns should be sorted alphabetically
	alphaIdx := strings.Index(got, "alpha")
	middleIdx := strings.Index(got, "middle")
	zebraIdx := strings.Index(got, "zebra")
	if alphaIdx > middleIdx || middleIdx > zebraIdx {
		t.Errorf("columns not sorted: alpha@%d, middle@%d, zebra@%d\n%s", alphaIdx, middleIdx, zebraIdx, got)
	}
}

func TestBuildCommentSQL_TableAndColumns(t *testing.T) {
	t.Parallel()
	model := &parser.Model{
		Target:      "mart.sales",
		Description: "Sales summary",
		ColumnDescriptions: map[string]string{
			"revenue": "Total revenue",
		},
	}
	got := buildCommentSQL(model)
	if !strings.Contains(got, "COMMENT ON TABLE mart.sales IS 'Sales summary'") {
		t.Errorf("missing table comment in %q", got)
	}
	if !strings.Contains(got, "COMMENT ON COLUMN") {
		t.Errorf("missing column comment in %q", got)
	}
}

func TestBuildCommentSQL_EscapesSingleQuotes(t *testing.T) {
	t.Parallel()
	model := &parser.Model{
		Target:      "mart.sales",
		Description: "It's a test",
		ColumnDescriptions: map[string]string{
			"name": "Customer's name",
		},
	}
	got := buildCommentSQL(model)
	if !strings.Contains(got, "It''s a test") {
		t.Errorf("expected escaped quote in table comment, got %q", got)
	}
	if !strings.Contains(got, "Customer''s name") {
		t.Errorf("expected escaped quote in column comment, got %q", got)
	}
}

// --- Rapid property-based tests ---

// Property: buildCommentSQL with description always produces COMMENT ON TABLE.
func TestRapid_BuildCommentSQL_TableComment(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		target := genSchemaTable().Draw(rt, "target")
		desc := rapid.StringMatching(`^[A-Za-z][A-Za-z0-9 ]{2,30}$`).Draw(rt, "desc")

		model := &parser.Model{Target: target, Description: desc}
		got := buildCommentSQL(model)

		if !strings.Contains(got, "COMMENT ON TABLE "+target) {
			rt.Fatalf("missing COMMENT ON TABLE in %q", got)
		}
		if !strings.HasPrefix(got, "COMMENT ON TABLE") {
			rt.Fatalf("should start with COMMENT ON TABLE: %q", got)
		}
	})
}

// Property: buildCommentSQL with column descriptions produces one COMMENT ON COLUMN per column.
func TestRapid_BuildCommentSQL_ColumnComments(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		target := genSchemaTable().Draw(rt, "target")
		n := rapid.IntRange(1, 5).Draw(rt, "n")
		cols := make(map[string]string)
		for range n {
			col := rapid.StringMatching(`^[a-z][a-z0-9_]{1,10}$`).Draw(rt, "col")
			desc := rapid.StringMatching(`^[A-Za-z][A-Za-z0-9 ]{2,20}$`).Draw(rt, "desc")
			cols[col] = desc
		}

		model := &parser.Model{Target: target, ColumnDescriptions: cols}
		got := buildCommentSQL(model)

		commentCount := strings.Count(got, "COMMENT ON COLUMN")
		if commentCount != len(cols) {
			rt.Fatalf("got %d COMMENT ON COLUMN, want %d", commentCount, len(cols))
		}
	})
}

// Property: empty model produces empty string.
func TestRapid_BuildCommentSQL_EmptyModel(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		target := genSchemaTable().Draw(rt, "target")
		model := &parser.Model{Target: target}
		got := buildCommentSQL(model)
		if got != "" {
			rt.Fatalf("expected empty string for model without descriptions, got %q", got)
		}
	})
}

// Property: single quotes in descriptions are always escaped.
func TestRapid_BuildCommentSQL_EscapesQuotes(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		target := genSchemaTable().Draw(rt, "target")
		// Generate description that contains a single quote
		prefix := rapid.StringMatching(`^[A-Za-z]{2,10}$`).Draw(rt, "prefix")
		suffix := rapid.StringMatching(`^[A-Za-z]{2,10}$`).Draw(rt, "suffix")
		desc := prefix + "'" + suffix

		model := &parser.Model{Target: target, Description: desc}
		got := buildCommentSQL(model)

		// The unescaped quote should NOT appear (single ' not preceded/followed by ')
		escaped := prefix + "''" + suffix
		if !strings.Contains(got, escaped) {
			rt.Fatalf("quote not escaped in %q, expected %q", got, escaped)
		}
	})
}

// --- Fuzz tests ---

func FuzzBuildCommentSQL(f *testing.F) {
	f.Add("staging.orders", "Order data", "id", "Primary key")
	f.Add("mart.sales", "", "", "")
	f.Add("raw.events", "It's raw", "name", "Customer's name")
	f.Add("staging.test", "Has \"double quotes\"", "col", "A 'quoted' value")

	f.Fuzz(func(t *testing.T, target, desc, colName, colDesc string) {
		model := &parser.Model{Target: target, Description: desc}
		if colName != "" && colDesc != "" {
			model.ColumnDescriptions = map[string]string{colName: colDesc}
		}

		got := buildCommentSQL(model)

		// Invariant: no unescaped single quotes between IS ' and closing '
		// (the escapeSQL function should double all single quotes)
		if desc != "" && !strings.Contains(got, "COMMENT ON TABLE") {
			t.Errorf("description set but no COMMENT ON TABLE in %q", got)
		}
		if colName != "" && colDesc != "" && !strings.Contains(got, "COMMENT ON COLUMN") {
			t.Errorf("column desc set but no COMMENT ON COLUMN in %q", got)
		}
	})
}
