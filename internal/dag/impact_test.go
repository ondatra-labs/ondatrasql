// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package dag

import (
	"testing"
)

func TestFormatReason(t *testing.T) {
	t.Parallel()
	tests := []struct {
		columns  []string
		model    string
		expected string
	}{
		// v0.12.1 (Bug S8 fix): wording changed from "uses" to "reads" and the
		// argument is now SourceColumns (input columns from the upstream),
		// not AffectedColumns (output columns of the downstream).
		{[]string{"id"}, "raw.orders", "reads column id from raw.orders"},
		{[]string{"id", "amount"}, "raw.orders", "reads id, amount from raw.orders"},
		// New: deduplication when multiple downstream output cols share an input
		{[]string{"id", "id", "amount"}, "raw.orders", "reads id, amount from raw.orders"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			t.Parallel()
			result := formatReason(tt.columns, tt.model)
			if result != tt.expected {
				t.Errorf("formatReason() = %s, expected %s", result, tt.expected)
			}
		})
	}
}
