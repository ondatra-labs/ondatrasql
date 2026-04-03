// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"math"
	"testing"

	"go.starlark.net/starlark"
	"pgregory.net/rapid"
)

// --- goToStarlark / starlarkToGo Roundtrip ---

// Property: string roundtrip is lossless.
func TestRapid_Convert_String_Roundtrip(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		s := rapid.String().Draw(t, "s")

		sv, err := goToStarlark(s)
		if err != nil {
			t.Fatalf("goToStarlark: %v", err)
		}
		gv, err := starlarkToGo(sv)
		if err != nil {
			t.Fatalf("starlarkToGo: %v", err)
		}
		if gv != s {
			t.Fatalf("roundtrip: %q -> %v", s, gv)
		}
	})
}

// Property: int64 roundtrip is lossless.
func TestRapid_Convert_Int64_Roundtrip(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.Int64().Draw(t, "n")

		sv, err := goToStarlark(n)
		if err != nil {
			t.Fatalf("goToStarlark: %v", err)
		}
		gv, err := starlarkToGo(sv)
		if err != nil {
			t.Fatalf("starlarkToGo: %v", err)
		}
		got, ok := gv.(int64)
		if !ok {
			t.Fatalf("expected int64, got %T: %v", gv, gv)
		}
		if got != n {
			t.Fatalf("roundtrip: %d -> %d", n, got)
		}
	})
}

// Property: float64 roundtrip is lossless (for non-NaN, non-Inf).
func TestRapid_Convert_Float64_Roundtrip(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		f := rapid.Float64().Draw(t, "f")
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return // skip special values
		}

		sv, err := goToStarlark(f)
		if err != nil {
			t.Fatalf("goToStarlark: %v", err)
		}
		gv, err := starlarkToGo(sv)
		if err != nil {
			t.Fatalf("starlarkToGo: %v", err)
		}
		got, ok := gv.(float64)
		if !ok {
			t.Fatalf("expected float64, got %T: %v", gv, gv)
		}
		if got != f {
			t.Fatalf("roundtrip: %v -> %v", f, got)
		}
	})
}

// Property: bool roundtrip is lossless.
func TestRapid_Convert_Bool_Roundtrip(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		b := rapid.Bool().Draw(t, "b")

		sv, err := goToStarlark(b)
		if err != nil {
			t.Fatalf("goToStarlark: %v", err)
		}
		gv, err := starlarkToGo(sv)
		if err != nil {
			t.Fatalf("starlarkToGo: %v", err)
		}
		got, ok := gv.(bool)
		if !ok {
			t.Fatalf("expected bool, got %T: %v", gv, gv)
		}
		if got != b {
			t.Fatalf("roundtrip: %v -> %v", b, got)
		}
	})
}

// Property: nil roundtrip produces nil.
func TestRapid_Convert_Nil_Roundtrip(t *testing.T) {
	t.Parallel()
	sv, err := goToStarlark(nil)
	if err != nil {
		t.Fatalf("goToStarlark: %v", err)
	}
	if sv != starlark.None {
		t.Fatalf("expected None, got %v", sv)
	}
	gv, err := starlarkToGo(sv)
	if err != nil {
		t.Fatalf("starlarkToGo: %v", err)
	}
	if gv != nil {
		t.Fatalf("expected nil, got %v", gv)
	}
}

// Property: []interface{} of strings roundtrips losslessly.
func TestRapid_Convert_StringSlice_Roundtrip(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(0, 10).Draw(t, "n")
		slice := make([]any, n)
		for i := range slice {
			slice[i] = rapid.String().Draw(t, "elem")
		}

		sv, err := goToStarlark(slice)
		if err != nil {
			t.Fatalf("goToStarlark: %v", err)
		}
		gv, err := starlarkToGo(sv)
		if err != nil {
			t.Fatalf("starlarkToGo: %v", err)
		}
		got, ok := gv.([]any)
		if !ok {
			t.Fatalf("expected []any, got %T", gv)
		}
		if len(got) != len(slice) {
			t.Fatalf("length: %d != %d", len(got), len(slice))
		}
		for i := range slice {
			if got[i] != slice[i] {
				t.Fatalf("elem[%d]: %v != %v", i, got[i], slice[i])
			}
		}
	})
}

// Property: map[string]interface{} of strings roundtrips losslessly.
func TestRapid_Convert_StringMap_Roundtrip(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(0, 8).Draw(t, "n")
		m := make(map[string]any)
		for range n {
			key := rapid.StringMatching(`^[a-z]{1,8}$`).Draw(t, "key")
			val := rapid.String().Draw(t, "val")
			m[key] = val
		}

		sv, err := goToStarlark(m)
		if err != nil {
			t.Fatalf("goToStarlark: %v", err)
		}
		gv, err := starlarkToGo(sv)
		if err != nil {
			t.Fatalf("starlarkToGo: %v", err)
		}
		got, ok := gv.(map[string]any)
		if !ok {
			t.Fatalf("expected map[string]any, got %T", gv)
		}
		if len(got) != len(m) {
			t.Fatalf("length: %d != %d", len(got), len(m))
		}
		for k, v := range m {
			if got[k] != v {
				t.Fatalf("key %q: %v != %v", k, got[k], v)
			}
		}
	})
}

// --- Nested Structure Roundtrip ---

// Property: list of maps roundtrips losslessly.
func TestRapid_Convert_ListOfMaps_Roundtrip(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(1, 5).Draw(t, "n")
		var list []any
		for range n {
			m := make(map[string]any)
			nKeys := rapid.IntRange(1, 4).Draw(t, "nKeys")
			for range nKeys {
				key := rapid.StringMatching(`^[a-z]{1,6}$`).Draw(t, "key")
				// Mix of types
				choice := rapid.IntRange(0, 2).Draw(t, "choice")
				switch choice {
				case 0:
					m[key] = rapid.String().Draw(t, "str_val")
				case 1:
					m[key] = rapid.Int64().Draw(t, "int_val")
				case 2:
					m[key] = rapid.Bool().Draw(t, "bool_val")
				}
			}
			list = append(list, m)
		}

		sv, err := goToStarlark(list)
		if err != nil {
			t.Fatalf("goToStarlark: %v", err)
		}
		gv, err := starlarkToGo(sv)
		if err != nil {
			t.Fatalf("starlarkToGo: %v", err)
		}
		got, ok := gv.([]any)
		if !ok {
			t.Fatalf("expected []any, got %T", gv)
		}
		if len(got) != len(list) {
			t.Fatalf("length: %d != %d", len(got), len(list))
		}
	})
}

// Property: map of lists roundtrips losslessly.
func TestRapid_Convert_MapOfLists_Roundtrip(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		m := make(map[string]any)
		nKeys := rapid.IntRange(1, 4).Draw(t, "nKeys")
		for range nKeys {
			key := rapid.StringMatching(`^[a-z]{1,6}$`).Draw(t, "key")
			nVals := rapid.IntRange(0, 5).Draw(t, "nVals")
			vals := make([]any, nVals)
			for i := range vals {
				vals[i] = rapid.String().Draw(t, "elem")
			}
			m[key] = vals
		}

		sv, err := goToStarlark(m)
		if err != nil {
			t.Fatalf("goToStarlark: %v", err)
		}
		gv, err := starlarkToGo(sv)
		if err != nil {
			t.Fatalf("starlarkToGo: %v", err)
		}
		got, ok := gv.(map[string]any)
		if !ok {
			t.Fatalf("expected map[string]any, got %T", gv)
		}
		if len(got) != len(m) {
			t.Fatalf("length: %d != %d", len(got), len(m))
		}
		// Verify each key's list length
		for k, v := range m {
			origList := v.([]any)
			gotVal, ok := got[k]
			if !ok {
				t.Fatalf("missing key %q", k)
			}
			gotList, ok := gotVal.([]any)
			if !ok {
				t.Fatalf("key %q: expected []any, got %T", k, gotVal)
			}
			if len(gotList) != len(origList) {
				t.Fatalf("key %q: length %d != %d", k, len(gotList), len(origList))
			}
		}
	})
}

// --- Type Preservation ---

// Property: goToStarlark preserves the Go type as the correct Starlark type.
func TestRapid_GoToStarlark_TypePreservation(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		choice := rapid.IntRange(0, 3).Draw(t, "choice")
		switch choice {
		case 0:
			s := rapid.String().Draw(t, "s")
			sv, err := goToStarlark(s)
			if err != nil {
				t.Fatalf("goToStarlark(%q): %v", s, err)
			}
			if _, ok := sv.(starlark.String); !ok {
				t.Fatalf("expected starlark.String, got %T", sv)
			}
		case 1:
			n := rapid.Int64().Draw(t, "n")
			sv, err := goToStarlark(n)
			if err != nil {
				t.Fatalf("goToStarlark(%d): %v", n, err)
			}
			if _, ok := sv.(starlark.Int); !ok {
				t.Fatalf("expected starlark.Int, got %T", sv)
			}
		case 2:
			f := rapid.Float64().Draw(t, "f")
			sv, err := goToStarlark(f)
			if err != nil {
				t.Fatalf("goToStarlark(%v): %v", f, err)
			}
			if _, ok := sv.(starlark.Float); !ok {
				t.Fatalf("expected starlark.Float, got %T", sv)
			}
		case 3:
			b := rapid.Bool().Draw(t, "b")
			sv, err := goToStarlark(b)
			if err != nil {
				t.Fatalf("goToStarlark(%v): %v", b, err)
			}
			if _, ok := sv.(starlark.Bool); !ok {
				t.Fatalf("expected starlark.Bool, got %T", sv)
			}
		}
	})
}

// Property: starlarkToGo returns the correct Go type for each Starlark type.
func TestRapid_StarlarkToGo_TypePreservation(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		choice := rapid.IntRange(0, 3).Draw(t, "choice")
		switch choice {
		case 0:
			s := rapid.String().Draw(t, "s")
			gv, err := starlarkToGo(starlark.String(s))
			if err != nil {
				t.Fatalf("starlarkToGo: %v", err)
			}
			if _, ok := gv.(string); !ok {
				t.Fatalf("expected string, got %T", gv)
			}
		case 1:
			n := rapid.Int64().Draw(t, "n")
			gv, err := starlarkToGo(starlark.MakeInt64(n))
			if err != nil {
				t.Fatalf("starlarkToGo: %v", err)
			}
			if _, ok := gv.(int64); !ok {
				t.Fatalf("expected int64, got %T", gv)
			}
		case 2:
			b := rapid.Bool().Draw(t, "b")
			gv, err := starlarkToGo(starlark.Bool(b))
			if err != nil {
				t.Fatalf("starlarkToGo: %v", err)
			}
			if _, ok := gv.(bool); !ok {
				t.Fatalf("expected bool, got %T", gv)
			}
		case 3:
			gv, err := starlarkToGo(starlark.None)
			if err != nil {
				t.Fatalf("starlarkToGo: %v", err)
			}
			if gv != nil {
				t.Fatalf("expected nil for None, got %T: %v", gv, gv)
			}
		}
	})
}
