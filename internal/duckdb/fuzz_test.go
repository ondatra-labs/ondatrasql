// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package duckdb

import (
	"strings"
	"testing"
)

// Stateful: fuzz sequences of Session operations to find state machine bugs
// (use-after-close, double-close, operation ordering).
func FuzzSessionStateful(f *testing.F) {
	if testing.Short() {
		f.Skip("skipping in short mode")
	}
	// Sequence of operation codes: 0=Exec, 1=Query, 2=QueryValue, 3=QueryRows,
	// 4=QueryRowsMap, 5=GetVersion, 6=Close, 7=Close(again)
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6})       // normal sequence then close
	f.Add([]byte{6, 0, 1})                     // close first, then operations
	f.Add([]byte{6, 6})                        // double close
	f.Add([]byte{0, 6, 0, 6})                  // interleaved close
	f.Add([]byte{5, 5, 5})                     // repeated version
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 0})  // everything including post-close

	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) > 20 {
			return // limit sequence length
		}

		s, err := NewSession(":memory:")
		if err != nil {
			t.Fatalf("NewSession: %v", err)
		}
		defer s.Close()

		closed := false
		for _, op := range ops {
			switch op % 8 {
			case 0:
				err := s.Exec("SELECT 1")
				// Property: after close, operations must error
				if closed && err == nil {
					t.Error("Exec should error after Close")
				}
			case 1:
				_, err := s.Query("SELECT 1 AS a")
				if closed && err == nil {
					t.Error("Query should error after Close")
				}
			case 2:
				_, err := s.QueryValue("SELECT 42")
				if closed && err == nil {
					t.Error("QueryValue should error after Close")
				}
			case 3:
				_, err := s.QueryRows("SELECT 1 AS a UNION ALL SELECT 2")
				if closed && err == nil {
					t.Error("QueryRows should error after Close")
				}
			case 4:
				_, err := s.QueryRowsMap("SELECT 1 AS a, 2 AS b")
				if closed && err == nil {
					t.Error("QueryRowsMap should error after Close")
				}
			case 5:
				v := s.GetVersion()
				// Property: after close, version should be "unknown"
				if closed && v != "unknown" {
					t.Errorf("GetVersion after Close = %q, want unknown", v)
				}
			case 6, 7:
				_ = s.Close()
				closed = true
			}
		}
	})
}

// Structural invariant: output must be properly quoted, and unquoting must
// recover the original string (SQL safety).
func FuzzQuoteIdentifier(f *testing.F) {
	if testing.Short() {
		f.Skip("skipping in short mode")
	}
	f.Add("simple")
	f.Add(`has"quote`)
	f.Add("")
	f.Add("schema.table")
	f.Add(`"already_quoted"`)
	f.Add("with space")
	f.Add("with\nnewline")
	f.Add("with\x00null")
	f.Add("SELECT") // reserved word
	f.Add("a]b")
	f.Add("very_" + string(make([]byte, 1000))) // long name

	f.Fuzz(func(t *testing.T, s string) {
		result := QuoteIdentifier(s)
		// Must always be quoted
		if len(result) < 2 || result[0] != '"' || result[len(result)-1] != '"' {
			t.Errorf("QuoteIdentifier(%q) = %q, not properly quoted", s, result)
		}
		// SQL safety: unquoting (strip outer quotes, unescape "") must recover original
		inner := result[1 : len(result)-1]
		recovered := strings.ReplaceAll(inner, `""`, `"`)
		if recovered != s {
			t.Errorf("QuoteIdentifier not recoverable: input %q, quoted %q, recovered %q",
				s, result, recovered)
		}
		// Structural: interior must not contain unescaped quotes
		noEscaped := strings.ReplaceAll(inner, `""`, ``)
		if strings.Contains(noEscaped, `"`) {
			t.Errorf("QuoteIdentifier(%q) has unescaped quote in interior: %q", s, result)
		}
	})
}
