// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package lineage

import (
	"strings"
	"testing"
	"unicode/utf8"

	"pgregory.net/rapid"
)

// --- Canvas Properties ---

// Property: Set then Get returns the same rune for in-bounds coordinates.
func TestRapid_Canvas_SetGet_Roundtrip(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		w := rapid.IntRange(1, 50).Draw(t, "w")
		h := rapid.IntRange(1, 50).Draw(t, "h")
		c := NewCanvas(w, h)

		x := rapid.IntRange(0, w-1).Draw(t, "x")
		y := rapid.IntRange(0, h-1).Draw(t, "y")
		ch := rapid.Rune().Draw(t, "ch")

		c.Set(x, y, ch)
		got := c.Get(x, y)
		if got != ch {
			t.Fatalf("Set(%d,%d,%c) then Get = %c", x, y, ch, got)
		}
	})
}

// Property: Get on out-of-bounds always returns space.
func TestRapid_Canvas_Get_OutOfBounds(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		w := rapid.IntRange(1, 20).Draw(t, "w")
		h := rapid.IntRange(1, 20).Draw(t, "h")
		c := NewCanvas(w, h)

		// Generate coordinates that are out of bounds
		x := rapid.OneOf(
			rapid.IntRange(-100, -1),
			rapid.IntRange(w, w+100),
		).Draw(t, "x")
		y := rapid.OneOf(
			rapid.IntRange(-100, -1),
			rapid.IntRange(h, h+100),
		).Draw(t, "y")

		if c.Get(x, y) != ' ' {
			t.Fatal("out-of-bounds Get should return space")
		}
	})
}

// Property: Set on out-of-bounds is a no-op (no panic).
func TestRapid_Canvas_Set_OutOfBounds_NoPanic(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		w := rapid.IntRange(1, 20).Draw(t, "w")
		h := rapid.IntRange(1, 20).Draw(t, "h")
		c := NewCanvas(w, h)

		x := rapid.IntRange(-100, w+100).Draw(t, "x")
		y := rapid.IntRange(-100, h+100).Draw(t, "y")

		// Should never panic
		c.Set(x, y, 'X')
	})
}

// Property: NewCanvas is all spaces initially.
func TestRapid_Canvas_NewCanvas_AllSpaces(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		w := rapid.IntRange(1, 30).Draw(t, "w")
		h := rapid.IntRange(1, 30).Draw(t, "h")
		c := NewCanvas(w, h)

		x := rapid.IntRange(0, w-1).Draw(t, "x")
		y := rapid.IntRange(0, h-1).Draw(t, "y")
		if c.Get(x, y) != ' ' {
			t.Fatal("new canvas should be all spaces")
		}
	})
}

// Property: WriteString writes each rune at consecutive positions.
func TestRapid_Canvas_WriteString_Positions(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		w := rapid.IntRange(20, 80).Draw(t, "w")
		h := rapid.IntRange(1, 10).Draw(t, "h")
		c := NewCanvas(w, h)

		s := rapid.StringMatching(`^[a-zA-Z0-9]{1,15}$`).Draw(t, "s")
		y := rapid.IntRange(0, h-1).Draw(t, "y")

		c.WriteString(0, y, s)

		for i, ch := range s {
			if i >= w {
				break
			}
			got := c.Get(i, y)
			if got != ch {
				t.Fatalf("position %d: got %c, want %c", i, got, ch)
			}
		}
	})
}

// Property: Canvas.String() has exactly height lines (before final trim).
func TestRapid_Canvas_String_LineCount(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		w := rapid.IntRange(1, 20).Draw(t, "w")
		h := rapid.IntRange(1, 20).Draw(t, "h")
		c := NewCanvas(w, h)

		// Write something on first and last row to prevent trimming
		c.Set(0, 0, 'A')
		c.Set(0, h-1, 'Z')

		str := c.String()
		lines := strings.Split(str, "\n")
		if len(lines) != h {
			t.Fatalf("expected %d lines, got %d", h, len(lines))
		}
	})
}

// --- runeLen Properties ---

// Property: runeLen equals utf8.RuneCountInString.
func TestRapid_RuneLen_EqualsRuneCount(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		s := rapid.String().Draw(t, "s")
		got := runeLen(s)
		want := utf8.RuneCountInString(s)
		if got != want {
			t.Fatalf("runeLen(%q) = %d, want %d", s, got, want)
		}
	})
}

// --- padRight Properties ---

// Property: padRight output has at least width runes.
func TestRapid_PadRight_MinWidth(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		s := rapid.StringMatching(`^[a-z]{0,10}$`).Draw(t, "s")
		width := rapid.IntRange(0, 30).Draw(t, "width")

		result := padRight(s, width)
		resultLen := utf8.RuneCountInString(result)
		sLen := utf8.RuneCountInString(s)
		// Result should be max(len(s), width) in length
		want := width
		if sLen > want {
			want = sLen
		}
		if resultLen != want {
			t.Fatalf("padRight(%q, %d) has len %d, want %d", s, width, resultLen, want)
		}
	})
}

// Property: padRight preserves original string as prefix.
func TestRapid_PadRight_PreservesPrefix(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		s := rapid.StringMatching(`^[a-z]{0,10}$`).Draw(t, "s")
		width := rapid.IntRange(0, 30).Draw(t, "width")

		result := padRight(s, width)
		if !strings.HasPrefix(result, s) {
			t.Fatalf("padRight(%q, %d) = %q, does not start with input", s, width, result)
		}
	})
}

// Property: padRight only adds spaces.
func TestRapid_PadRight_OnlySpaces(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		s := rapid.StringMatching(`^[a-z]{0,10}$`).Draw(t, "s")
		width := rapid.IntRange(len(s), len(s)+20).Draw(t, "width")

		result := padRight(s, width)
		suffix := result[len(s):]
		if strings.TrimRight(suffix, " ") != "" {
			t.Fatalf("padRight added non-space chars: %q", suffix)
		}
	})
}

// --- padCenter Properties ---

// Property: padCenter output has at least width runes.
func TestRapid_PadCenter_MinWidth(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		s := rapid.StringMatching(`^[a-z]{0,8}$`).Draw(t, "s")
		width := rapid.IntRange(len(s), len(s)+20).Draw(t, "width")

		result := padCenter(s, width)
		resultLen := utf8.RuneCountInString(result)
		if resultLen != width {
			t.Fatalf("padCenter(%q, %d) has len %d", s, width, resultLen)
		}
	})
}

// Property: padCenter output contains the original string.
func TestRapid_PadCenter_ContainsOriginal(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		s := rapid.StringMatching(`^[a-z]{1,8}$`).Draw(t, "s")
		width := rapid.IntRange(len(s), len(s)+20).Draw(t, "width")

		result := padCenter(s, width)
		if !strings.Contains(result, s) {
			t.Fatalf("padCenter(%q, %d) = %q, missing original", s, width, result)
		}
	})
}

// --- dotFill Properties ---

// Property: dotFill output contains both left and right strings.
func TestRapid_DotFill_ContainsBoth(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		left := rapid.StringMatching(`^[a-z]{1,8}$`).Draw(t, "left")
		right := rapid.StringMatching(`^[a-z]{1,8}$`).Draw(t, "right")
		width := rapid.IntRange(len(left)+len(right)+5, 60).Draw(t, "width")

		result := dotFill(left, right, width)
		if !strings.HasPrefix(result, left) {
			t.Fatalf("dotFill missing left: %q", result)
		}
		if !strings.HasSuffix(result, right) {
			t.Fatalf("dotFill missing right: %q", result)
		}
	})
}

// Property: dotFill always contains dot characters.
func TestRapid_DotFill_ContainsDots(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		left := rapid.StringMatching(`^[a-z]{1,5}$`).Draw(t, "left")
		right := rapid.StringMatching(`^[a-z]{1,5}$`).Draw(t, "right")
		width := rapid.IntRange(20, 40).Draw(t, "width")

		result := dotFill(left, right, width)
		if !strings.Contains(result, "·") {
			t.Fatalf("dotFill has no dots: %q", result)
		}
	})
}

// --- CalcWidth Properties ---

// Property: CalcWidth is at least name length + padding.
func TestRapid_CalcWidth_FitsName(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		name := rapid.StringMatching(`^[a-z]{1,30}$`).Draw(t, "name")
		box := &GraphBox{Name: name, Columns: []string{}}
		w := box.CalcWidth()
		nameWidth := utf8.RuneCountInString(name) + 6 // CalcWidth uses name + 6
		if w < nameWidth {
			t.Fatalf("CalcWidth = %d, too small for name %q (need >= %d)", w, name, nameWidth)
		}
	})
}

// Property: CalcWidth is at least as wide as the longest column + padding.
func TestRapid_CalcWidth_FitsColumns(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		name := rapid.StringMatching(`^[a-z]{1,5}$`).Draw(t, "name")
		col := rapid.StringMatching(`^[a-z_]{1,40}$`).Draw(t, "col")

		box := &GraphBox{Name: name, Columns: []string{col}}
		w := box.CalcWidth()
		colWidth := utf8.RuneCountInString(col) + 4 // CalcWidth uses col + 4
		if w < colWidth {
			t.Fatalf("CalcWidth = %d, too small for column %q (need >= %d)", w, col, colWidth)
		}
	})
}
