// OndatraSQL - You don't need a data stack anymore
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
		{[]string{"id"}, "raw.orders", "uses column id from raw.orders"},
		{[]string{"id", "amount"}, "raw.orders", "uses id, amount from raw.orders"},
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
