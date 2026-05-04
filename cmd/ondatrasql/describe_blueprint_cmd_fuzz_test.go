// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"strings"
	"testing"
)

// FuzzFieldMaskParse pins that --fields=<input> parsing handles
// arbitrary input without panicking — path traversal attempts, unicode,
// gigantic strings, control characters. Critical because field-mask
// values come straight from agent prompts and could contain anything.
//
// Validates: splitFieldMask returns sensible slice, projectFields
// either returns a result map or an error — never panics, never leaks.
func FuzzFieldMaskParse(f *testing.F) {
	f.Add("")
	f.Add("a")
	f.Add("a,b")
	f.Add("a.b.c,d.e")
	f.Add("../../etc/passwd")
	f.Add("\x00\x01\x02")
	f.Add(strings.Repeat("a.", 1000))
	f.Add("a,,b,,,c")
	f.Add("   a   ,   b   ")

	src := map[string]any{
		"name":  "x",
		"path":  "lib/x.star",
		"fetch": map[string]any{"args": []any{"a"}, "mode": "sync"},
	}

	f.Fuzz(func(t *testing.T, input string) {
		paths := splitFieldMask(input)
		// Never panics, always returns sane slice.
		if paths == nil {
			t.Fatal("splitFieldMask returned nil — should be empty slice")
		}
		// projectFields either succeeds or returns an error; never panics.
		_, _ = projectFields(src, paths)
	})
}

// FuzzIsValidBlueprintName pins that arbitrary input doesn't crash
// the identifier validator. The function is called on user input from
// `describe blueprint <name>` and must reject anything weird without
// panicking on unicode, control chars, etc.
func FuzzIsValidBlueprintName(f *testing.F) {
	f.Add("")
	f.Add("riksbank")
	f.Add("a")
	f.Add("../../etc/passwd")
	f.Add("foo\x00bar")
	f.Add("foo bar")
	f.Add("123")
	f.Add(strings.Repeat("a", 10000))

	f.Fuzz(func(t *testing.T, input string) {
		// Never panics. Result is bool, no further invariants.
		_ = isValidBlueprintName(input)
	})
}

// FuzzLookupPath pins that arbitrary dotted-path input doesn't crash
// the JSON projection lookup, regardless of the source map's shape.
// Critical for field-mask robustness against agent-supplied paths.
func FuzzLookupPath(f *testing.F) {
	f.Add("a", "b")
	f.Add("a.b.c", "x")
	f.Add("", "y")
	f.Add(strings.Repeat("a.", 100), "z")
	f.Add("a..b", "")

	src := map[string]any{
		"a": map[string]any{
			"b": map[string]any{
				"c": "value",
				"d": nil,
			},
		},
		"x": []any{1, 2, 3},
	}

	f.Fuzz(func(t *testing.T, path, _ string) {
		// Never panics; ok=true XOR ok=false determinism.
		_, _ = lookupPath(src, path)
	})
}
