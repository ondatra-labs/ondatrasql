// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package backfill

import (
	"testing"

	"pgregory.net/rapid"
)

// --- Generators ---

var duckDBTypes = []string{
	"INTEGER", "BIGINT", "SMALLINT", "TINYINT",
	"DOUBLE", "FLOAT", "BOOLEAN", "VARCHAR",
	"TIMESTAMP", "DATE", "HUGEINT",
}

func genColumn() *rapid.Generator[Column] {
	return rapid.Custom[Column](func(t *rapid.T) Column {
		return Column{
			Name: rapid.StringMatching(`^[a-z][a-z0-9_]{0,15}$`).Draw(t, "name"),
			Type: rapid.SampledFrom(duckDBTypes).Draw(t, "type"),
		}
	})
}

func genUniqueColumns() *rapid.Generator[[]Column] {
	return rapid.Custom[[]Column](func(t *rapid.T) []Column {
		n := rapid.IntRange(1, 10).Draw(t, "n")
		seen := make(map[string]bool)
		var cols []Column
		for len(cols) < n {
			col := genColumn().Draw(t, "col")
			if !seen[col.Name] {
				seen[col.Name] = true
				cols = append(cols, col)
			}
		}
		return cols
	})
}

// --- Property Tests ---

// Property: ComputeSchemaHash is deterministic.
func TestRapid_SchemaHash_Deterministic(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		cols := genUniqueColumns().Draw(t, "cols")
		h1 := ComputeSchemaHash(cols)
		h2 := ComputeSchemaHash(cols)
		if h1 != h2 {
			t.Fatalf("not deterministic: %s != %s", h1, h2)
		}
	})
}

// Property: ComputeSchemaHash is order-independent.
func TestRapid_SchemaHash_OrderIndependent(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		cols := genUniqueColumns().Draw(t, "cols")
		if len(cols) < 2 {
			return
		}

		// Create a reversed copy
		reversed := make([]Column, len(cols))
		for i, c := range cols {
			reversed[len(cols)-1-i] = c
		}

		h1 := ComputeSchemaHash(cols)
		h2 := ComputeSchemaHash(reversed)
		if h1 != h2 {
			t.Fatalf("hash not order-independent:\n  original: %v -> %s\n  reversed: %v -> %s",
				cols, h1, reversed, h2)
		}
	})
}

// Property: ComputeSchemaHash contains only lowercase hex characters.
func TestRapid_SchemaHash_ValidHex(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		cols := genUniqueColumns().Draw(t, "cols")
		h := ComputeSchemaHash(cols)
		if h == "" {
			t.Fatal("expected non-empty hash for non-empty columns")
		}
		for _, c := range h {
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
				t.Fatalf("hash contains non-hex char %q: %s", string(c), h)
			}
		}
	})
}

// Property: ComputeSchemaHash is different for different schemas (collision resistance).
func TestRapid_SchemaHash_DifferentSchemas(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		cols1 := genUniqueColumns().Draw(t, "cols1")
		cols2 := genUniqueColumns().Draw(t, "cols2")

		// Only test if schemas are actually different
		if len(cols1) == len(cols2) {
			same := true
			m := make(map[string]string)
			for _, c := range cols1 {
				m[c.Name] = c.Type
			}
			for _, c := range cols2 {
				if m[c.Name] != c.Type {
					same = false
					break
				}
			}
			if same {
				return // Schemas are equal, skip
			}
		}

		h1 := ComputeSchemaHash(cols1)
		h2 := ComputeSchemaHash(cols2)
		if h1 == h2 {
			t.Fatalf("hash collision:\n  cols1: %v -> %s\n  cols2: %v -> %s",
				cols1, h1, cols2, h2)
		}
	})
}

// Property: ClassifySchemaChange(old, old) == SchemaChangeNone.
func TestRapid_ClassifySchemaChange_Identity(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		cols := genUniqueColumns().Draw(t, "cols")
		change := ClassifySchemaChange(cols, cols, nil)
		if change.Type != SchemaChangeNone {
			t.Fatalf("same schema classified as %q, want none", change.Type)
		}
		if len(change.Added) != 0 || len(change.Dropped) != 0 || len(change.TypeChanged) != 0 {
			t.Fatalf("same schema has changes: added=%d dropped=%d changed=%d",
				len(change.Added), len(change.Dropped), len(change.TypeChanged))
		}
	})
}

// Property: added columns in ClassifySchemaChange are exactly the new ones.
func TestRapid_ClassifySchemaChange_AddedColumns(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		old := genUniqueColumns().Draw(t, "old")
		extra := genColumn().Draw(t, "extra")

		// Ensure extra is not already in old
		for _, c := range old {
			if c.Name == extra.Name {
				return // Skip: name collision
			}
		}

		new := append(append([]Column{}, old...), extra)
		change := ClassifySchemaChange(old, new, nil)

		if len(change.Added) != 1 {
			t.Fatalf("expected 1 added column, got %d", len(change.Added))
		}
		if change.Added[0].Name != extra.Name {
			t.Fatalf("added column = %q, want %q", change.Added[0].Name, extra.Name)
		}
		if change.Type != SchemaChangeAdditive {
			t.Fatalf("type = %q, want additive", change.Type)
		}
	})
}

// Property: dropping a column is always destructive.
func TestRapid_ClassifySchemaChange_DroppedIsDestructive(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		cols := genUniqueColumns().Draw(t, "cols")
		if len(cols) < 2 {
			return
		}

		// Drop the last column
		old := cols
		new := cols[:len(cols)-1]
		change := ClassifySchemaChange(old, new, nil)

		if change.Type != SchemaChangeDestructive {
			t.Fatalf("dropped column classified as %q, want destructive", change.Type)
		}
		if len(change.Dropped) != 1 {
			t.Fatalf("expected 1 dropped column, got %d", len(change.Dropped))
		}
	})
}

// --- Schema Classification State Machine ---
// Tests that ClassifySchemaChange maintains invariants across random sequences
// of column additions, drops, and type changes applied to a schema.

type schemaClassifyMachine struct {
	columns []Column // current schema state
}

func (sm *schemaClassifyMachine) AddColumn(t *rapid.T) {
	col := genColumn().Draw(t, "col")
	// Skip if name collision
	for _, c := range sm.columns {
		if c.Name == col.Name {
			t.Skip("name collision")
		}
	}
	sm.columns = append(sm.columns, col)
}

func (sm *schemaClassifyMachine) DropColumn(t *rapid.T) {
	if len(sm.columns) < 2 {
		t.Skip("need at least 2 columns to drop")
	}
	idx := rapid.IntRange(0, len(sm.columns)-1).Draw(t, "idx")
	sm.columns = append(sm.columns[:idx], sm.columns[idx+1:]...)
}

func (sm *schemaClassifyMachine) ChangeType(t *rapid.T) {
	if len(sm.columns) == 0 {
		t.Skip("no columns")
	}
	idx := rapid.IntRange(0, len(sm.columns)-1).Draw(t, "idx")
	newType := rapid.SampledFrom(duckDBTypes).Filter(func(s string) bool {
		return s != sm.columns[idx].Type
	}).Draw(t, "newType")
	sm.columns[idx] = Column{Name: sm.columns[idx].Name, Type: newType}
}

func (sm *schemaClassifyMachine) Check(t *rapid.T) {
	if len(sm.columns) == 0 {
		return
	}

	// Compare original (identity) → should be None
	change := ClassifySchemaChange(sm.columns, sm.columns, nil)
	if change.Type != SchemaChangeNone {
		t.Fatalf("identity should be None, got %s", change.Type)
	}
	if len(change.Added) != 0 || len(change.Dropped) != 0 || len(change.TypeChanged) != 0 {
		t.Fatal("identity should have no diffs")
	}

	// Verify adding a column → Additive
	extra := Column{Name: "zz_test_extra_col", Type: "VARCHAR"}
	exists := false
	for _, c := range sm.columns {
		if c.Name == extra.Name {
			exists = true
			break
		}
	}
	if !exists {
		newCols := append(append([]Column{}, sm.columns...), extra)
		change = ClassifySchemaChange(sm.columns, newCols, nil)
		if change.Type != SchemaChangeAdditive {
			t.Fatalf("adding column should be Additive, got %s", change.Type)
		}
	}

	// Verify dropping a column → Destructive
	if len(sm.columns) >= 2 {
		shorter := sm.columns[:len(sm.columns)-1]
		change = ClassifySchemaChange(sm.columns, shorter, nil)
		if change.Type != SchemaChangeDestructive {
			t.Fatalf("dropping column should be Destructive, got %s", change.Type)
		}
	}
}

func TestRapid_SchemaClassify_StateMachine(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		// Start with 2-3 columns
		initial := genUniqueColumns().Draw(rt, "initial")
		sm := &schemaClassifyMachine{columns: initial}
		rt.Repeat(rapid.StateMachineActions(sm))
	})
}

// Property: type change + drop together is always destructive.
// Uses sess=nil which makes any type change classify as destructive.
func TestRapid_ClassifySchemaChange_TypeChangeIsDestructive(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		cols := genUniqueColumns().Draw(t, "cols")
		if len(cols) < 2 {
			return
		}

		// Change the type of the first column to a different type
		modified := make([]Column, len(cols))
		copy(modified, cols)
		newType := rapid.SampledFrom(duckDBTypes).Filter(func(s string) bool {
			return s != cols[0].Type
		}).Draw(t, "newType")
		modified[0] = Column{Name: cols[0].Name, Type: newType}

		// Also drop the last column to ensure ClassifySchemaChange reaches
		// the type-change detection (with sess=nil, type changes are destructive)
		modified = modified[:len(modified)-1]

		change := ClassifySchemaChange(cols, modified, nil)

		if change.Type != SchemaChangeDestructive {
			t.Fatalf("type change + drop classified as %q, want destructive", change.Type)
		}
		if len(change.Dropped) != 1 {
			t.Fatalf("expected 1 dropped column, got %d", len(change.Dropped))
		}
	})
}
