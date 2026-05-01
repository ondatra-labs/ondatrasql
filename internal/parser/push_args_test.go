package parser

import (
	"testing"
)

func TestParseSinkArgs(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"'abc', 'def'", []string{"abc", "def"}},
		{"'value, with comma', 'other'", []string{"value, with comma", "other"}},
		{`"sheet1", "sheet2"`, []string{"sheet1", "sheet2"}},
		{"'single'", []string{"single"}},
		{"", nil},
		{"  'spaced' , 'args'  ", []string{"spaced", "args"}},
		{"'1DYJCOd', 'Sheet1!A1:Z'", []string{"1DYJCOd", "Sheet1!A1:Z"}},
		// Escaped quotes
		{"'it''s', 'fine'", []string{"it's", "fine"}},
		{`"she said ""hello""", "ok"`, []string{`she said "hello"`, "ok"}},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parsePushArgs(tt.input)
			if len(got) != len(tt.want) {
				t.Fatalf("parsePushArgs(%q) = %v, want %v", tt.input, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("parsePushArgs(%q)[%d] = %q, want %q", tt.input, i, got[i], tt.want[i])
				}
			}
		})
	}
}
