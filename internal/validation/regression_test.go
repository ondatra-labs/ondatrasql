// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package validation

import "testing"

// --- formatArg: already-quoted bypass fixed ---

func TestFormatArg_QuotedBypass(t *testing.T) {
	t.Parallel()
	// Before fix: 'x' OR 1=1; --' passed through verbatim.
	// After fix: inner quotes are escaped.
	result := formatArg("'x' OR 1=1; --'")
	if result == "'x' OR 1=1; --'" {
		t.Error("already-quoted arg should have inner quotes escaped")
	}
	// Should contain doubled quotes
	if result != "'x'' OR 1=1; --'" {
		t.Errorf("formatArg = %q, want inner quotes doubled", result)
	}
}

func TestFormatArg_NormalQuoted(t *testing.T) {
	t.Parallel()
	result := formatArg("'hello'")
	if result != "'hello'" {
		t.Errorf("formatArg('hello') = %q, want 'hello'", result)
	}
}

// --- isNumeric: rejects +. and -. ---

func TestIsNumeric_PlusDot(t *testing.T) {
	t.Parallel()
	if isNumeric("+.") {
		t.Error("+. should not be numeric")
	}
}

func TestIsNumeric_MinusDot(t *testing.T) {
	t.Parallel()
	if isNumeric("-.") {
		t.Error("-. should not be numeric")
	}
}

func TestIsNumeric_ValidCases(t *testing.T) {
	t.Parallel()
	for _, s := range []string{"42", "-3.14", "+0.5", "0", "100"} {
		if !isNumeric(s) {
			t.Errorf("%q should be numeric", s)
		}
	}
}
