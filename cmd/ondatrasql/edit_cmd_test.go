// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"testing"
)

func TestSplitTarget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  []string
	}{
		{"staging.orders", []string{"staging", "orders"}},
		{"orders", []string{"orders"}},
		{"", []string{""}},
		{"a.b.c", []string{"a.b", "c"}},           // last dot wins
		{"catalog.schema.table", []string{"catalog.schema", "table"}},
		{".leading", []string{"", "leading"}},
		{"trailing.", []string{"trailing", ""}},
	}
	for _, tt := range tests {
		got := splitTarget(tt.input)
		if len(got) != len(tt.want) {
			t.Errorf("splitTarget(%q) = %v (len %d), want %v (len %d)",
				tt.input, got, len(got), tt.want, len(tt.want))
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("splitTarget(%q)[%d] = %q, want %q", tt.input, i, got[i], tt.want[i])
			}
		}
	}
}
