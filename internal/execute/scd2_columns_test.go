// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import "testing"

// TestSCD2SyntheticColumnNames pins that the scd2 synthetic columns match
// case-insensitively, as DuckDB resolves column names: a result column
// VALID_FROM_SNAPSHOT is the synthetic column, not a model column beside it.
func TestSCD2SyntheticColumnNames(t *testing.T) {
	for _, name := range []string{"valid_from_snapshot", "VALID_TO_SNAPSHOT", "Is_Current", "valid_from_at", "VALID_TO_AT"} {
		if !isSCD2SyntheticColumn(name) {
			t.Errorf("isSCD2SyntheticColumn(%q) = false, want true", name)
		}
	}
	for _, name := range []string{"Valid_From_At", "valid_to_at"} {
		if !isSCD2TimeColumn(name) {
			t.Errorf("isSCD2TimeColumn(%q) = false, want true", name)
		}
	}
	for _, name := range []string{"valid_from", "is_current_flag", "id"} {
		if isSCD2SyntheticColumn(name) {
			t.Errorf("isSCD2SyntheticColumn(%q) = true, want false", name)
		}
	}
	if isSCD2TimeColumn("is_current") {
		t.Error("isSCD2TimeColumn(\"is_current\") = true, want false")
	}
}
