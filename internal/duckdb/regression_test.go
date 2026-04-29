// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package duckdb

import "testing"

// TestSanitizeSchemaAlias pins the alphanumeric-or-underscore filter used
// when building system schema names like `__ducklake_metadata_<alias>`.
//
// Bug class: a future change interpolates a user-controlled alias into
// raw SQL without sanitising. `QuoteIdentifier` does not work here — the
// alias is part of the schema-name itself, not a quotable identifier —
// so a dedicated sanitiser is required and must be regression-tested.
func TestSanitizeSchemaAlias(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		// Allowed runes pass through unchanged
		{"lake", "lake"},
		{"prod_warehouse", "prod_warehouse"},
		{"L4ke_2024", "L4ke_2024"},
		{"_underscore_first", "_underscore_first"},
		{"UPPER", "UPPER"},

		// Each disallowed rune becomes underscore
		{"lake-prod", "lake_prod"},
		{"lake.warehouse", "lake_warehouse"},
		{"lake prod", "lake_prod"},

		// Quote / semicolon / comment chars must all be neutralised
		{"a';DROP", "a__DROP"},
		{`a"b`, "a_b"},
		{"a/*b*/c", "a__b__c"},
		{"a--b", "a__b"},

		// Newlines + control chars
		{"a\nb", "a_b"},
		{"a\tb", "a_b"},
		{"a\x00b", "a_b"},

		// Unicode is not in [a-zA-Z0-9_], so it's flattened
		{"låke", "l_ke"},
		{"日本語", "___"},

		// Empty stays empty
		{"", ""},
	}
	for _, tt := range tests {
		got := sanitizeSchemaAlias(tt.input)
		if got != tt.want {
			t.Errorf("sanitizeSchemaAlias(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
