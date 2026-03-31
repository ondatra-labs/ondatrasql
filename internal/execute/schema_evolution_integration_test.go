// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/backfill"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
	"pgregory.net/rapid"
)

// createTableWithSchema creates a table in DuckLake, ensuring the schema exists first.
func createTableWithSchema(t testing.TB, sess *testutil.Project, ddl string) {
	t.Helper()
	// Extract schema name from DDL (e.g., "CREATE TABLE staging.foo" → "staging")
	// Ensure schema exists in the DuckLake catalog
	if err := sess.Sess.Exec("CREATE SCHEMA IF NOT EXISTS staging"); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if err := sess.Sess.Exec(ddl); err != nil {
		t.Fatalf("%s: %v", ddl, err)
	}
}

// --- Schema evolution state machine ---
// Tests that applySchemaEvolution handles random sequences of
// add column, rename column, and type promotion without crashing,
// and that the final schema matches expectations.

type columnDef struct {
	name    string
	colType string
}

type schemaEvolutionMachine struct {
	runner  *Runner
	target  string
	columns []columnDef // current expected schema
}

// availableTypes are types we can add/promote between.
var availableTypes = []string{"INTEGER", "BIGINT", "VARCHAR", "BOOLEAN", "DOUBLE"}

// promotablePairs maps old type → valid wider types.
var promotablePairs = map[string][]string{
	"INTEGER":  {"BIGINT"},
	"SMALLINT": {"INTEGER", "BIGINT"},
	"FLOAT":    {"DOUBLE"},
}

func (sm *schemaEvolutionMachine) Check(t *rapid.T) {
	// Verify real schema matches our model
	rows, err := sm.runner.sess.QueryRows(
		fmt.Sprintf("SELECT column_name || '|' || data_type FROM information_schema.columns WHERE table_name = '%s' ORDER BY ordinal_position",
			strings.Split(sm.target, ".")[1]))
	if err != nil {
		t.Fatalf("query schema: %v", err)
	}

	if len(rows) != len(sm.columns) {
		t.Fatalf("column count mismatch: real=%d model=%d\n  real: %v\n  model: %v",
			len(rows), len(sm.columns), rows, sm.columns)
	}

	for i, row := range rows {
		parts := strings.Split(row, "|")
		if len(parts) < 2 {
			continue
		}
		realName := parts[0]
		realType := strings.ToUpper(parts[1])
		if realName != sm.columns[i].name {
			t.Fatalf("column[%d] name: real=%q model=%q", i, realName, sm.columns[i].name)
		}
		// Normalize type comparison
		modelType := strings.ToUpper(sm.columns[i].colType)
		if realType != modelType {
			t.Fatalf("column[%d] %s type: real=%q model=%q", i, realName, realType, modelType)
		}
	}
}

func (sm *schemaEvolutionMachine) AddColumn(t *rapid.T) {
	// Generate a unique column name
	colName := rapid.StringMatching(`^[a-z]{3,8}$`).Draw(t, "col_name")

	// Check name doesn't already exist
	for _, c := range sm.columns {
		if c.name == colName {
			t.Skip("column already exists")
		}
	}

	colType := rapid.SampledFrom(availableTypes).Draw(t, "col_type")

	change := backfill.SchemaChange{
		Added: []backfill.Column{{Name: colName, Type: colType}},
	}

	err := sm.runner.applySchemaEvolution(sm.target, change)
	if err != nil {
		t.Fatalf("add column %s %s: %v", colName, colType, err)
	}

	sm.columns = append(sm.columns, columnDef{name: colName, colType: colType})
}

func (sm *schemaEvolutionMachine) RenameColumn(t *rapid.T) {
	if len(sm.columns) <= 1 {
		t.Skip("need at least 2 columns to rename safely")
	}

	// Pick a column to rename (not the first one — keep 'id' stable)
	idx := rapid.IntRange(1, len(sm.columns)-1).Draw(t, "col_idx")
	newName := rapid.StringMatching(`^[a-z]{3,8}$`).Draw(t, "new_name")

	// Check new name doesn't collide
	for _, c := range sm.columns {
		if c.name == newName {
			t.Skip("new name already exists")
		}
	}

	change := backfill.SchemaChange{
		Renamed: []backfill.ColumnRename{
			{OldName: sm.columns[idx].name, NewName: newName},
		},
	}

	err := sm.runner.applySchemaEvolution(sm.target, change)
	if err != nil {
		t.Fatalf("rename %s to %s: %v", sm.columns[idx].name, newName, err)
	}

	sm.columns[idx].name = newName
}

func (sm *schemaEvolutionMachine) PromoteType(t *rapid.T) {
	if len(sm.columns) <= 1 {
		t.Skip("need columns to promote")
	}

	// Find columns with promotable types
	var candidates []int
	for i, c := range sm.columns {
		if _, ok := promotablePairs[c.colType]; ok {
			candidates = append(candidates, i)
		}
	}
	if len(candidates) == 0 {
		t.Skip("no promotable columns")
	}

	idx := rapid.SampledFrom(candidates).Draw(t, "col_idx")
	targets := promotablePairs[sm.columns[idx].colType]
	newType := rapid.SampledFrom(targets).Draw(t, "new_type")

	change := backfill.SchemaChange{
		TypeChanged: []backfill.TypeChange{
			{Column: sm.columns[idx].name, OldType: sm.columns[idx].colType, NewType: newType},
		},
	}

	err := sm.runner.applySchemaEvolution(sm.target, change)
	if err != nil {
		t.Fatalf("promote %s from %s to %s: %v",
			sm.columns[idx].name, sm.columns[idx].colType, newType, err)
	}

	sm.columns[idx].colType = newType
}

func TestRapid_SchemaEvolution_StateMachine(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	rapid.Check(t, func(rt *rapid.T) {
		p := testutil.NewProject(t)

		// Create initial table with id column
		target := "staging.evo_rapid"
		createTableWithSchema(t, p, "CREATE TABLE staging.evo_rapid (id INTEGER, value INTEGER)")

		runner := NewRunner(p.Sess, ModeRun, "rapid-test")
		sm := &schemaEvolutionMachine{
			runner:  runner,
			target:  target,
			columns: []columnDef{{name: "id", colType: "INTEGER"}, {name: "value", colType: "INTEGER"}},
		}

		rt.Repeat(rapid.StateMachineActions(sm))
	})
}

// --- Explicit error path tests ---
// These directly test the error branches in applySchemaEvolution
// by providing SchemaChange structs that cause DuckDB ALTER TABLE to fail.

func TestApplySchemaEvolution_RenameToExistingColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Rename column to a name that already exists → ALTER TABLE RENAME fails
	p := testutil.NewProject(t)

	createTableWithSchema(t, p, "CREATE TABLE staging.rename_conflict (id INTEGER, name VARCHAR, full_name VARCHAR)")

	runner := NewRunner(p.Sess, ModeRun, "test")
	change := backfill.SchemaChange{
		Renamed: []backfill.ColumnRename{
			{OldName: "name", NewName: "full_name"}, // full_name already exists!
		},
	}

	err := runner.applySchemaEvolution("staging.rename_conflict", change)
	if err == nil {
		t.Fatal("expected error when renaming to existing column name")
	}
	if !strings.Contains(err.Error(), "rename column") {
		t.Errorf("error = %v, expected 'rename column' error", err)
	}
}

func TestApplySchemaEvolution_RenameNonExistentColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Rename a column that doesn't exist → ALTER TABLE RENAME fails
	p := testutil.NewProject(t)

	createTableWithSchema(t, p, "CREATE TABLE staging.rename_missing (id INTEGER)")

	runner := NewRunner(p.Sess, ModeRun, "test")
	change := backfill.SchemaChange{
		Renamed: []backfill.ColumnRename{
			{OldName: "nonexistent", NewName: "something"},
		},
	}

	err := runner.applySchemaEvolution("staging.rename_missing", change)
	if err == nil {
		t.Fatal("expected error when renaming non-existent column")
	}
	if !strings.Contains(err.Error(), "rename column") {
		t.Errorf("error = %v, expected 'rename column' error", err)
	}
}

func TestApplySchemaEvolution_AddExistingColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Add a column that already exists → ALTER TABLE ADD COLUMN fails
	p := testutil.NewProject(t)

	createTableWithSchema(t, p, "CREATE TABLE staging.add_dup (id INTEGER, name VARCHAR)")

	runner := NewRunner(p.Sess, ModeRun, "test")
	change := backfill.SchemaChange{
		Added: []backfill.Column{
			{Name: "name", Type: "VARCHAR"}, // already exists!
		},
	}

	err := runner.applySchemaEvolution("staging.add_dup", change)
	if err == nil {
		t.Fatal("expected error when adding column that already exists")
	}
	if !strings.Contains(err.Error(), "add column") {
		t.Errorf("error = %v, expected 'add column' error", err)
	}
}

func TestApplySchemaEvolution_TypeChangeInvalid(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Type change to incompatible type → ALTER TYPE fails
	// DuckDB can't ALTER VARCHAR to INTEGER if there's data
	p := testutil.NewProject(t)

	createTableWithSchema(t, p, "CREATE TABLE staging.type_fail (id INTEGER, name VARCHAR)")
	// Insert data that can't be converted to INTEGER
	if err := p.Sess.Exec("INSERT INTO staging.type_fail VALUES (1, 'not_a_number')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	runner := NewRunner(p.Sess, ModeRun, "test")
	change := backfill.SchemaChange{
		TypeChanged: []backfill.TypeChange{
			{Column: "name", OldType: "VARCHAR", NewType: "INTEGER"},
		},
	}

	err := runner.applySchemaEvolution("staging.type_fail", change)
	if err == nil {
		t.Fatal("expected error for incompatible type change")
	}
	if !strings.Contains(err.Error(), "alter column") {
		t.Errorf("error = %v, expected 'alter column' error", err)
	}
}

func TestApplySchemaEvolution_TypeChangeOnNonExistentColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	createTableWithSchema(t, p, "CREATE TABLE staging.type_missing (id INTEGER)")

	runner := NewRunner(p.Sess, ModeRun, "test")
	change := backfill.SchemaChange{
		TypeChanged: []backfill.TypeChange{
			{Column: "missing_col", OldType: "INTEGER", NewType: "BIGINT"},
		},
	}

	err := runner.applySchemaEvolution("staging.type_missing", change)
	if err == nil {
		t.Fatal("expected error for type change on non-existent column")
	}
	if !strings.Contains(err.Error(), "alter column") {
		t.Errorf("error = %v, expected 'alter column' error", err)
	}
}

func TestApplySchemaEvolution_CombinedRenameAndAdd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Test that rename happens before add (order matters)
	p := testutil.NewProject(t)

	createTableWithSchema(t, p, "CREATE TABLE staging.combined_ops (id INTEGER, old_name VARCHAR)")

	runner := NewRunner(p.Sess, ModeRun, "test")
	change := backfill.SchemaChange{
		Renamed: []backfill.ColumnRename{
			{OldName: "old_name", NewName: "new_name"},
		},
		Added: []backfill.Column{
			{Name: "extra", Type: "BOOLEAN"},
		},
	}

	err := runner.applySchemaEvolution("staging.combined_ops", change)
	if err != nil {
		t.Fatalf("combined rename+add: %v", err)
	}

	// Verify schema
	cols, err := p.Sess.QueryRows("SELECT column_name FROM information_schema.columns WHERE table_name = 'combined_ops' ORDER BY ordinal_position")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	expected := []string{"id", "new_name", "extra"}
	if len(cols) != len(expected) {
		t.Fatalf("columns = %v, want %v", cols, expected)
	}
	for i, col := range cols {
		if col != expected[i] {
			t.Errorf("column[%d] = %q, want %q", i, col, expected[i])
		}
	}
}

func TestApplySchemaEvolution_CombinedAllThree(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Test rename + add + type promotion all in one change
	p := testutil.NewProject(t)

	createTableWithSchema(t, p, "CREATE TABLE staging.all_three (id INTEGER, old_name VARCHAR, amount INTEGER)")

	runner := NewRunner(p.Sess, ModeRun, "test")
	change := backfill.SchemaChange{
		Renamed: []backfill.ColumnRename{
			{OldName: "old_name", NewName: "full_name"},
		},
		Added: []backfill.Column{
			{Name: "status", Type: "VARCHAR"},
		},
		TypeChanged: []backfill.TypeChange{
			{Column: "amount", OldType: "INTEGER", NewType: "BIGINT"},
		},
	}

	err := runner.applySchemaEvolution("staging.all_three", change)
	if err != nil {
		t.Fatalf("combined all three: %v", err)
	}

	// Verify schema
	rows, err := p.Sess.QueryRows("SELECT column_name || '|' || data_type FROM information_schema.columns WHERE table_name = 'all_three' ORDER BY ordinal_position")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	expected := []string{"id|INTEGER", "full_name|VARCHAR", "amount|BIGINT", "status|VARCHAR"}
	if len(rows) != len(expected) {
		t.Fatalf("schema = %v, want %v", rows, expected)
	}
	for i, row := range rows {
		if row != expected[i] {
			t.Errorf("column[%d] = %q, want %q", i, row, expected[i])
		}
	}
}

func TestApplySchemaEvolution_EmptyChange(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Empty change should succeed without modifying anything
	p := testutil.NewProject(t)

	createTableWithSchema(t, p, "CREATE TABLE staging.no_change (id INTEGER, name VARCHAR)")

	runner := NewRunner(p.Sess, ModeRun, "test")
	change := backfill.SchemaChange{} // empty

	err := runner.applySchemaEvolution("staging.no_change", change)
	if err != nil {
		t.Fatalf("empty change should succeed: %v", err)
	}
}
