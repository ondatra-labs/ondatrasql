// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"pgregory.net/rapid"
)

// --- Unit tests ---

func TestIsMaskingTag(t *testing.T) {
	t.Parallel()
	tests := []struct {
		tag  string
		want bool
	}{
		// Masking tags (should return true)
		{"mask", true},
		{"mask_email", true},
		{"mask_ssn", true},
		{"hash", true},
		{"hash_pii", true},
		{"redact", true},
		{"redact_full", true},
		{"MASK", true},      // case insensitive
		{"Mask_Email", true}, // mixed case
		{"HASH_PII", true},

		// Non-masking tags (metadata-only, should return false)
		{"PII", false},
		{"sensitive", false},
		{"internal", false},
		{"classified", false},
		{"public", false},
		{"", false},

		// Edge cases: prefix match but not exact prefix or prefix_
		{"masking", false},   // "masking" != "mask" and doesn't start with "mask_"
		{"hashing", false},   // "hashing" != "hash" and doesn't start with "hash_"
		{"redacting", false}, // "redacting" != "redact" and doesn't start with "redact_"
	}

	for _, tt := range tests {
		t.Run(tt.tag, func(t *testing.T) {
			t.Parallel()
			got := isMaskingTag(tt.tag)
			if got != tt.want {
				t.Errorf("isMaskingTag(%q) = %v, want %v", tt.tag, got, tt.want)
			}
		})
	}
}

func TestGetMaskingMacro(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		tags []string
		want string
	}{
		{"single masking tag", []string{"mask_ssn"}, "mask_ssn"},
		{"masking with metadata", []string{"PII", "mask_ssn"}, "mask_ssn"},
		{"first masking wins", []string{"mask_email", "hash_pii"}, "mask_email"},
		{"hash tag", []string{"sensitive", "hash"}, "hash"},
		{"redact tag", []string{"redact"}, "redact"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := getMaskingMacro(tt.tags)
			if got != tt.want {
				t.Errorf("getMaskingMacro(%v) = %q, want %q", tt.tags, got, tt.want)
			}
		})
	}
}

func TestGetMaskingMacro_NoMasking(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		tags []string
	}{
		{"metadata only", []string{"PII", "sensitive"}},
		{"single metadata", []string{"internal"}},
		{"empty tags", nil},
		{"empty slice", []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := getMaskingMacro(tt.tags)
			if got != "" {
				t.Errorf("getMaskingMacro(%v) = %q, want empty", tt.tags, got)
			}
		})
	}
}

func TestApplyColumnMasking_NoTags(t *testing.T) {
	t.Parallel()
	sql := "SELECT id, name, email FROM raw.customers"
	model := &parser.Model{
		Target: "staging.customers",
	}
	got := applyColumnMasking(sql, model)
	if got != sql {
		t.Errorf("expected SQL unchanged, got %q", got)
	}
}

func TestApplyColumnMasking_SingleColumn(t *testing.T) {
	t.Parallel()
	sql := "SELECT id, name, ssn FROM raw.customers"
	model := &parser.Model{
		Target: "staging.customers",
		ColumnTags: map[string][]string{
			"ssn": {"PII", "mask_ssn"},
		},
	}
	got := applyColumnMasking(sql, model)

	if !strings.Contains(got, "SELECT * REPLACE") {
		t.Errorf("expected SELECT * REPLACE, got %q", got)
	}
	if !strings.Contains(got, `mask_ssn("ssn") AS "ssn"`) {
		t.Errorf(`expected mask_ssn("ssn") AS "ssn", got %q`, got)
	}
	if !strings.Contains(got, "FROM ("+sql+")") {
		t.Errorf("expected original SQL as subquery, got %q", got)
	}
}

func TestApplyColumnMasking_MultipleColumns(t *testing.T) {
	t.Parallel()
	sql := "SELECT id, name, email, ssn FROM raw.customers"
	model := &parser.Model{
		Target: "staging.customers",
		ColumnTags: map[string][]string{
			"email": {"mask_email"},
			"ssn":   {"PII", "mask_ssn"},
		},
	}
	got := applyColumnMasking(sql, model)

	if !strings.Contains(got, "SELECT * REPLACE") {
		t.Errorf("expected SELECT * REPLACE, got %q", got)
	}
	if !strings.Contains(got, `mask_email("email") AS "email"`) {
		t.Errorf(`expected mask_email("email") AS "email", got %q`, got)
	}
	if !strings.Contains(got, `mask_ssn("ssn") AS "ssn"`) {
		t.Errorf(`expected mask_ssn("ssn") AS "ssn", got %q`, got)
	}
}

func TestApplyColumnMasking_SkipsUniqueKey(t *testing.T) {
	t.Parallel()
	sql := "SELECT id, name FROM raw.customers"
	model := &parser.Model{
		Target:    "staging.customers",
		UniqueKey: "id",
		ColumnTags: map[string][]string{
			"id":   {"mask"},  // Should be skipped (unique key)
			"name": {"mask"},  // Should be masked
		},
	}
	got := applyColumnMasking(sql, model)

	// name should be masked
	if !strings.Contains(got, `mask("name") AS "name"`) {
		t.Errorf(`expected mask("name") AS "name", got %q`, got)
	}
	// id should NOT be masked (it's the unique key)
	if strings.Contains(got, `mask("id")`) {
		t.Errorf("unique key id should not be masked, got %q", got)
	}
}

func TestApplyColumnMasking_SkipsMultiColumnUniqueKey(t *testing.T) {
	t.Parallel()
	sql := "SELECT region, year, name FROM raw.events"
	model := &parser.Model{
		Target:    "staging.events",
		UniqueKey: "region, year",
		ColumnTags: map[string][]string{
			"region": {"mask"},  // Should be skipped (partition key)
			"year":   {"mask"},  // Should be skipped (partition key)
			"name":   {"mask"},  // Should be masked
		},
	}
	got := applyColumnMasking(sql, model)

	// name should be masked
	if !strings.Contains(got, `mask("name") AS "name"`) {
		t.Errorf(`expected mask("name") AS "name", got %q`, got)
	}
	// region and year should NOT be masked (they are unique key columns)
	if strings.Contains(got, "mask(region)") {
		t.Errorf("unique key column region should not be masked, got %q", got)
	}
	if strings.Contains(got, "mask(year)") {
		t.Errorf("unique key column year should not be masked, got %q", got)
	}
}

func TestApplyColumnMasking_MetadataOnlyTags(t *testing.T) {
	t.Parallel()
	sql := "SELECT id, ssn FROM raw.customers"
	model := &parser.Model{
		Target: "staging.customers",
		ColumnTags: map[string][]string{
			"ssn": {"PII", "sensitive"}, // no masking tags
		},
	}
	got := applyColumnMasking(sql, model)

	// No masking macro -> SQL unchanged
	if got != sql {
		t.Errorf("expected SQL unchanged for metadata-only tags, got %q", got)
	}
}

// --- Rapid property-based tests ---

// Property: when no masking tags exist, SQL is returned unchanged.
func TestRapid_ApplyColumnMasking_PreservesSQL(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		col := rapid.StringMatching(`^[a-z][a-z0-9_]{1,10}$`).Draw(rt, "col")
		sql := "SELECT " + col + " FROM staging.test"

		// Only metadata tags, no masking
		tag := rapid.SampledFrom([]string{"PII", "sensitive", "internal", "classified"}).Draw(rt, "tag")
		model := &parser.Model{
			Target: "staging.test",
			ColumnTags: map[string][]string{
				col: {tag},
			},
		}

		got := applyColumnMasking(sql, model)
		if got != sql {
			rt.Fatalf("SQL changed for non-masking tag %q: got %q", tag, got)
		}
	})
}

// Property: when masking tags exist, output contains SELECT * REPLACE.
func TestRapid_ApplyColumnMasking_ContainsREPLACE(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		col := rapid.StringMatching(`^[a-z][a-z0-9_]{1,10}$`).Draw(rt, "col")
		sql := "SELECT " + col + " FROM staging.test"

		maskTag := rapid.SampledFrom([]string{"mask", "mask_email", "mask_ssn", "hash", "hash_pii", "redact"}).Draw(rt, "mask")
		model := &parser.Model{
			Target: "staging.test",
			ColumnTags: map[string][]string{
				col: {maskTag},
			},
		}

		got := applyColumnMasking(sql, model)
		if !strings.Contains(got, "SELECT * REPLACE") {
			rt.Fatalf("missing SELECT * REPLACE for masking tag %q: got %q", maskTag, got)
		}
		qcol := `"` + col + `"`
		if !strings.Contains(got, maskTag+"("+qcol+") AS "+qcol) {
			rt.Fatalf("missing %s(%s) AS %s: got %q", maskTag, qcol, qcol, got)
		}
	})
}
