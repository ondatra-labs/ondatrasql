// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package parser

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"pgregory.net/rapid"
)

// --- Generators ---

func genSchemaName() *rapid.Generator[string] {
	return rapid.SampledFrom([]string{"raw", "staging", "mart", "analytics"})
}

func genValidTableName() *rapid.Generator[string] {
	return rapid.StringMatching(`^[a-z][a-z0-9_]{1,12}$`).Filter(func(s string) bool {
		return !strings.Contains(s, "__") && !strings.Contains(s, ".")
	})
}

func genKind() *rapid.Generator[string] {
	return rapid.SampledFrom([]string{"table", "append", "merge", "scd2", "partition"})
}

func genColumnName() *rapid.Generator[string] {
	return rapid.StringMatching(`^[a-z][a-z0-9_]{0,10}$`)
}

// --- Path Flattening Properties ---

// Property: models/X.sql → main.X (single-level always uses "main" schema).
func TestRapid_PathFlattening_SingleLevel(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		table := genValidTableName().Draw(rt, "table")
		dir := t.TempDir()
		modelsDir := filepath.Join(dir, "models")
		os.MkdirAll(modelsDir, 0o755)

		path := filepath.Join(modelsDir, table+".sql")
		os.WriteFile(path, []byte("-- @kind: table\nSELECT 1 AS id"), 0o644)

		m, err := ParseModel(path, dir)
		if err != nil {
			rt.Fatalf("ParseModel: %v", err)
		}
		if m.Target != "main."+table {
			rt.Fatalf("target = %q, want main.%s", m.Target, table)
		}
	})
}

// Property: models/schema/table.sql → schema.table (two-level).
func TestRapid_PathFlattening_TwoLevel(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		schema := genSchemaName().Draw(rt, "schema")
		table := genValidTableName().Draw(rt, "table")
		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "models", schema), 0o755)

		path := filepath.Join(dir, "models", schema, table+".sql")
		os.WriteFile(path, []byte("-- @kind: table\nSELECT 1 AS id"), 0o644)

		m, err := ParseModel(path, dir)
		if err != nil {
			rt.Fatalf("ParseModel: %v", err)
		}
		if m.Target != schema+"."+table {
			rt.Fatalf("target = %q, want %s.%s", m.Target, schema, table)
		}
	})
}

// Property: models/schema/sub/table.sql → schema.sub__table (three-level uses __ separator).
func TestRapid_PathFlattening_ThreeLevel(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		schema := genSchemaName().Draw(rt, "schema")
		sub := genValidTableName().Draw(rt, "sub")
		table := genValidTableName().Draw(rt, "table")
		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "models", schema, sub), 0o755)

		path := filepath.Join(dir, "models", schema, sub, table+".sql")
		os.WriteFile(path, []byte("-- @kind: table\nSELECT 1 AS id"), 0o644)

		m, err := ParseModel(path, dir)
		if err != nil {
			rt.Fatalf("ParseModel: %v", err)
		}
		expected := schema + "." + sub + "__" + table
		if m.Target != expected {
			rt.Fatalf("target = %q, want %q", m.Target, expected)
		}
	})
}

// Property: target always has exactly 2 parts (schema.table).
func TestRapid_PathFlattening_AlwaysTwoParts(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		// Random depth 1-3
		depth := rapid.IntRange(1, 3).Draw(rt, "depth")
		parts := make([]string, depth)
		if depth == 1 {
			parts[0] = genValidTableName().Draw(rt, "name")
		} else {
			parts[0] = genSchemaName().Draw(rt, "schema")
			for i := 1; i < depth; i++ {
				parts[i] = genValidTableName().Draw(rt, "part")
			}
		}

		dir := t.TempDir()
		modelPath := filepath.Join(append([]string{dir, "models"}, parts...)...)
		os.MkdirAll(filepath.Dir(modelPath+".sql"), 0o755)
		path := modelPath + ".sql"
		os.WriteFile(path, []byte("-- @kind: table\nSELECT 1 AS id"), 0o644)

		m, err := ParseModel(path, dir)
		if err != nil {
			rt.Fatalf("ParseModel: %v", err)
		}

		targetParts := strings.Split(m.Target, ".")
		if len(targetParts) != 2 {
			rt.Fatalf("target %q has %d parts, want 2", m.Target, len(targetParts))
		}
	})
}

// --- Directive Parsing Properties ---

// Property: @kind directive is always preserved in the parsed model.
func TestRapid_Directive_KindPreserved(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		kind := genKind().Draw(rt, "kind")
		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "models", "staging"), 0o755)

		// Partition kind requires @unique_key
		extra := ""
		if kind == "partition" {
			extra = "-- @unique_key: id\n"
		}
		content := fmt.Sprintf("-- @kind: %s\n%sSELECT 1 AS id", kind, extra)
		path := filepath.Join(dir, "models", "staging", "test.sql")
		os.WriteFile(path, []byte(content), 0o644)

		m, err := ParseModel(path, dir)
		if err != nil {
			rt.Fatalf("ParseModel: %v", err)
		}
		if m.Kind != kind {
			rt.Fatalf("kind = %q, want %q", m.Kind, kind)
		}
	})
}

// Property: multiple @constraint directives are all collected in order.
func TestRapid_Directive_MultipleConstraints(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(1, 5).Draw(rt, "n")
		var constraints []string
		var lines []string
		lines = append(lines, "-- @kind: table")
		for i := 0; i < n; i++ {
			col := genColumnName().Draw(rt, "col")
			c := col + " NOT NULL"
			constraints = append(constraints, c)
			lines = append(lines, "-- @constraint: "+c)
		}
		lines = append(lines, "SELECT 1 AS id")

		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "models", "staging"), 0o755)
		path := filepath.Join(dir, "models", "staging", "test.sql")
		os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o644)

		m, err := ParseModel(path, dir)
		if err != nil {
			rt.Fatalf("ParseModel: %v", err)
		}
		if len(m.Constraints) != n {
			rt.Fatalf("got %d constraints, want %d", len(m.Constraints), n)
		}
		for i, c := range m.Constraints {
			if c != constraints[i] {
				rt.Fatalf("constraint[%d] = %q, want %q", i, c, constraints[i])
			}
		}
	})
}

// Property: mixed @audit, @warning, @extension directives are all collected.
func TestRapid_Directive_MixedDirectives(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		nAudits := rapid.IntRange(0, 3).Draw(rt, "nAudits")
		nWarnings := rapid.IntRange(0, 3).Draw(rt, "nWarnings")
		nExtensions := rapid.IntRange(0, 2).Draw(rt, "nExtensions")

		var lines []string
		lines = append(lines, "-- @kind: table")
		for i := 0; i < nAudits; i++ {
			lines = append(lines, "-- @audit: row_count >= 0")
		}
		for i := 0; i < nWarnings; i++ {
			lines = append(lines, "-- @warning: row_count >= 0")
		}
		for i := 0; i < nExtensions; i++ {
			lines = append(lines, "-- @extension: httpfs")
		}
		lines = append(lines, "SELECT 1 AS id")

		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "models", "staging"), 0o755)
		path := filepath.Join(dir, "models", "staging", "test.sql")
		os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o644)

		m, err := ParseModel(path, dir)
		if err != nil {
			rt.Fatalf("ParseModel: %v", err)
		}
		if len(m.Audits) != nAudits {
			rt.Fatalf("audits = %d, want %d", len(m.Audits), nAudits)
		}
		if len(m.Warnings) != nWarnings {
			rt.Fatalf("warnings = %d, want %d", len(m.Warnings), nWarnings)
		}
		if len(m.Extensions) != nExtensions {
			rt.Fatalf("extensions = %d, want %d", len(m.Extensions), nExtensions)
		}
	})
}

// --- Description & Column Directive Properties ---

// Property: @description is always preserved in parsed model.
func TestRapid_Directive_DescriptionPreserved(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		desc := rapid.StringMatching(`^[A-Za-z][A-Za-z0-9 ]{2,29}[A-Za-z0-9]$`).Draw(rt, "desc")
		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "models", "staging"), 0o755)

		content := fmt.Sprintf("-- @kind: table\n-- @description: %s\nSELECT 1 AS id", desc)
		path := filepath.Join(dir, "models", "staging", "test.sql")
		os.WriteFile(path, []byte(content), 0o644)

		m, err := ParseModel(path, dir)
		if err != nil {
			rt.Fatalf("ParseModel: %v", err)
		}
		if m.Description != desc {
			rt.Fatalf("description = %q, want %q", m.Description, desc)
		}
	})
}

// Property: multiple @column directives are all collected.
func TestRapid_Directive_MultipleColumns(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(1, 5).Draw(rt, "n")
		wantDescs := make(map[string]string)
		var lines []string
		lines = append(lines, "-- @kind: table")
		for i := 0; i < n; i++ {
			col := genColumnName().Draw(rt, "col")
			desc := rapid.StringMatching(`^[A-Za-z][A-Za-z0-9 ]{2,19}[A-Za-z0-9]$`).Draw(rt, "desc")
			wantDescs[col] = desc
			lines = append(lines, fmt.Sprintf("-- @column: %s = %s", col, desc))
		}
		lines = append(lines, "SELECT 1 AS id")

		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "models", "staging"), 0o755)
		path := filepath.Join(dir, "models", "staging", "test.sql")
		os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o644)

		m, err := ParseModel(path, dir)
		if err != nil {
			rt.Fatalf("ParseModel: %v", err)
		}
		// Due to duplicate column names from rapid, map may have fewer entries
		for k, v := range m.ColumnDescriptions {
			if wantDescs[k] != v {
				rt.Fatalf("column %q desc = %q, want %q", k, v, wantDescs[k])
			}
		}
	})
}

// Property: @column with pipe strips tags, keeps only description before |.
func TestRapid_Directive_ColumnPipeStripped(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		col := genColumnName().Draw(rt, "col")
		desc := rapid.StringMatching(`^[A-Za-z][A-Za-z0-9 ]{2,19}[A-Za-z0-9]$`).Draw(rt, "desc")
		tag := rapid.SampledFrom([]string{"PII", "sensitive", "internal"}).Draw(rt, "tag")

		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "models", "staging"), 0o755)

		content := fmt.Sprintf("-- @kind: table\n-- @column: %s = %s | %s\nSELECT 1 AS id", col, desc, tag)
		path := filepath.Join(dir, "models", "staging", "test.sql")
		os.WriteFile(path, []byte(content), 0o644)

		m, err := ParseModel(path, dir)
		if err != nil {
			rt.Fatalf("ParseModel: %v", err)
		}
		got := m.ColumnDescriptions[col]
		if got != desc {
			rt.Fatalf("column %q desc = %q, want %q (tag %q should be stripped)", col, got, desc, tag)
		}
	})
}

// Property: @description is not included in SQL body.
func TestRapid_Directive_DescriptionNotInSQL(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		desc := rapid.StringMatching(`^[A-Za-z][A-Za-z0-9 ]{5,29}[A-Za-z0-9]$`).Draw(rt, "desc")
		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "models", "staging"), 0o755)

		content := fmt.Sprintf("-- @description: %s\nSELECT 1 AS id", desc)
		path := filepath.Join(dir, "models", "staging", "test.sql")
		os.WriteFile(path, []byte(content), 0o644)

		m, err := ParseModel(path, dir)
		if err != nil {
			rt.Fatalf("ParseModel: %v", err)
		}
		if strings.Contains(m.SQL, "@description") {
			rt.Fatalf("SQL should not contain @description directive: %q", m.SQL)
		}
	})
}

// --- Column Tag Properties ---

// Property: @column with single-word pipe tag stores tag in ColumnTags.
func TestRapid_Directive_ColumnTagsPreserved(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		col := genColumnName().Draw(rt, "col")
		desc := rapid.StringMatching(`^[A-Za-z][A-Za-z0-9 ]{2,19}[A-Za-z0-9]$`).Draw(rt, "desc")
		tag := rapid.SampledFrom([]string{"PII", "sensitive", "mask", "mask_email", "mask_ssn", "hash_pii", "redact"}).Draw(rt, "tag")

		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "models", "staging"), 0o755)

		content := fmt.Sprintf("-- @kind: table\n-- @column: %s = %s | %s\nSELECT 1 AS id", col, desc, tag)
		path := filepath.Join(dir, "models", "staging", "test.sql")
		os.WriteFile(path, []byte(content), 0o644)

		m, err := ParseModel(path, dir)
		if err != nil {
			rt.Fatalf("ParseModel: %v", err)
		}

		// Tag must be present in ColumnTags
		tags, ok := m.ColumnTags[col]
		if !ok {
			rt.Fatalf("ColumnTags missing column %q", col)
		}
		found := false
		for _, t := range tags {
			if t == tag {
				found = true
				break
			}
		}
		if !found {
			rt.Fatalf("ColumnTags[%q] = %v, missing tag %q", col, tags, tag)
		}

		// Description should not contain the tag
		gotDesc := m.ColumnDescriptions[col]
		if strings.Contains(gotDesc, tag) {
			rt.Fatalf("description %q should not contain tag %q", gotDesc, tag)
		}
	})
}

// --- Validation Properties ---

// Property: invalid kind always produces an error.
func TestRapid_Validation_InvalidKind(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		invalid := rapid.StringMatching(`^[a-z]{3,10}$`).Filter(func(s string) bool {
			switch s {
			case "table", "append", "merge", "scd2", "partition", "events", "tracked":
				return false
			}
			return true
		}).Draw(rt, "kind")

		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "models", "staging"), 0o755)
		path := filepath.Join(dir, "models", "staging", "test.sql")
		os.WriteFile(path, []byte("-- @kind: "+invalid+"\nSELECT 1 AS id"), 0o644)

		_, err := ParseModel(path, dir)
		if err == nil {
			rt.Fatalf("expected error for invalid kind %q", invalid)
		}
		if !strings.Contains(err.Error(), "invalid kind") {
			rt.Fatalf("error = %v, expected 'invalid kind'", err)
		}
	})
}

// Property: ValidateIdentifier accepts all valid schema.table names.
func TestRapid_ValidateIdentifier_Valid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		schema := genSchemaName().Draw(rt, "schema")
		table := genValidTableName().Draw(rt, "table")
		id := schema + "." + table

		err := ValidateIdentifier(id)
		if err != nil {
			rt.Fatalf("ValidateIdentifier(%q) = %v", id, err)
		}
	})
}

// Property: ValidateIdentifier rejects identifiers starting with a digit.
func TestRapid_ValidateIdentifier_RejectsDigitStart(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		digit := rapid.SampledFrom([]string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}).Draw(rt, "digit")
		rest := genValidTableName().Draw(rt, "rest")
		id := digit + rest

		err := ValidateIdentifier(id)
		if err == nil {
			rt.Fatalf("expected error for identifier starting with digit: %q", id)
		}
	})
}

// Property: ValidateColumnName rejects names with dots.
func TestRapid_ValidateColumnName_RejectsDots(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		a := genValidTableName().Draw(rt, "a")
		b := genValidTableName().Draw(rt, "b")
		name := a + "." + b

		err := ValidateColumnName(name)
		if err == nil {
			rt.Fatalf("expected error for column name with dot: %q", name)
		}
	})
}

// Property: path components with __ are rejected.
func TestRapid_PathFlattening_RejectsDoubleUnderscore(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		schema := genSchemaName().Draw(rt, "schema")
		// Generate a table name containing __
		prefix := rapid.StringMatching(`^[a-z]{1,5}$`).Draw(rt, "prefix")
		suffix := rapid.StringMatching(`^[a-z]{1,5}$`).Draw(rt, "suffix")
		badName := prefix + "__" + suffix

		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "models", schema, badName), 0o755)
		path := filepath.Join(dir, "models", schema, badName, "test.sql")
		os.WriteFile(path, []byte("-- @kind: table\nSELECT 1 AS id"), 0o644)

		_, err := ParseModel(path, dir)
		if err == nil {
			rt.Fatalf("expected error for path with __ component: %q", badName)
		}
		if !strings.Contains(err.Error(), "__") {
			rt.Fatalf("error = %v, expected mention of '__'", err)
		}
	})
}

// Property: SQL trailing semicolon is always stripped.
func TestRapid_Parse_StripsSemicolon(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		col := genColumnName().Draw(rt, "col")
		sql := "SELECT 1 AS " + col + ";"

		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "models", "staging"), 0o755)
		path := filepath.Join(dir, "models", "staging", "test.sql")
		os.WriteFile(path, []byte("-- @kind: table\n"+sql), 0o644)

		m, err := ParseModel(path, dir)
		if err != nil {
			rt.Fatalf("ParseModel: %v", err)
		}
		if strings.HasSuffix(m.SQL, ";") {
			rt.Fatalf("SQL still has trailing semicolon: %q", m.SQL)
		}
		if !strings.Contains(m.SQL, col) {
			rt.Fatalf("SQL missing column %q: %q", col, m.SQL)
		}
	})
}

// --- Events Kind Properties ---

// Property: events model preserves the number of columns.
func TestRapid_Events_ColumnCountPreserved(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(1, 6).Draw(rt, "n")
		var lines []string
		lines = append(lines, "-- @kind: events")
		for i := 0; i < n; i++ {
			col := genColumnName().Draw(rt, "col")
			typ := rapid.SampledFrom([]string{"VARCHAR", "INTEGER", "BIGINT", "BOOLEAN", "TIMESTAMPTZ", "JSON", "DOUBLE"}).Draw(rt, "type")
			lines = append(lines, col+" "+typ+",")
		}

		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "models", "raw"), 0o755)
		path := filepath.Join(dir, "models", "raw", "events.sql")
		os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o644)

		m, err := ParseModel(path, dir)
		if err != nil {
			return // Invalid column names may cause errors
		}
		if len(m.Columns) != n {
			rt.Fatalf("got %d columns, want %d", len(m.Columns), n)
		}
	})
}

// Property: events model preserves NOT NULL on columns.
func TestRapid_Events_NotNullPreserved(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		col := genColumnName().Draw(rt, "col")
		notNull := rapid.Bool().Draw(rt, "notNull")
		typ := rapid.SampledFrom([]string{"VARCHAR", "INTEGER", "BIGINT"}).Draw(rt, "type")

		line := col + " " + typ
		if notNull {
			line += " NOT NULL"
		}

		content := "-- @kind: events\n" + line

		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "models", "raw"), 0o755)
		path := filepath.Join(dir, "models", "raw", "events.sql")
		os.WriteFile(path, []byte(content), 0o644)

		m, err := ParseModel(path, dir)
		if err != nil {
			return
		}
		if len(m.Columns) != 1 {
			rt.Fatalf("got %d columns, want 1", len(m.Columns))
		}
		if m.Columns[0].NotNull != notNull {
			rt.Fatalf("NotNull = %v, want %v", m.Columns[0].NotNull, notNull)
		}
	})
}

// Property: events models reject disallowed directives.
func TestRapid_Events_RejectsInvalidDirectives(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		directive := rapid.SampledFrom([]string{
			"-- @unique_key: id",
			"-- @incremental: ts",
			"-- @constraint: x NOT NULL",
			"-- @audit: COUNT(*) > 0",
			"-- @warning: COUNT(*) > 0",
		}).Draw(rt, "directive")

		content := "-- @kind: events\n" + directive + "\nevent_name VARCHAR"

		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "models", "raw"), 0o755)
		path := filepath.Join(dir, "models", "raw", "events.sql")
		os.WriteFile(path, []byte(content), 0o644)

		_, err := ParseModel(path, dir)
		if err == nil {
			rt.Fatalf("expected error for events model with %s", directive)
		}
	})
}

// Property: events kind is always "events" in the parsed model.
func TestRapid_Events_KindPreserved(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		col := genColumnName().Draw(rt, "col")
		content := "-- @kind: events\n" + col + " VARCHAR"

		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "models", "raw"), 0o755)
		path := filepath.Join(dir, "models", "raw", "events.sql")
		os.WriteFile(path, []byte(content), 0o644)

		m, err := ParseModel(path, dir)
		if err != nil {
			return
		}
		if m.Kind != "events" {
			rt.Fatalf("kind = %q, want events", m.Kind)
		}
	})
}

// --- YAML Model Properties ---

func genSourceName() *rapid.Generator[string] {
	return rapid.StringMatching(`^[a-z][a-z0-9_]{1,12}$`).Filter(func(s string) bool {
		// Exclude Starlark reserved keywords
		return validateStarlarkIdent(s) == nil
	})
}

func genYAMLKind() *rapid.Generator[string] {
	// view and events are not supported for YAML models
	return rapid.SampledFrom([]string{"table", "append", "merge", "scd2", "partition"})
}

// Property: YAML model target is always schema.table (two parts).
func TestRapid_YAML_TargetAlwaysTwoParts(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		schema := genSchemaName().Draw(rt, "schema")
		table := genValidTableName().Draw(rt, "table")
		source := genSourceName().Draw(rt, "source")

		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "models", schema), 0o755)

		content := fmt.Sprintf("source: %s\n", source)
		path := filepath.Join(dir, "models", schema, table+".yaml")
		os.WriteFile(path, []byte(content), 0o644)

		m, err := ParseModel(path, dir)
		if err != nil {
			rt.Fatalf("ParseModel: %v", err)
		}

		parts := strings.Split(m.Target, ".")
		if len(parts) != 2 {
			rt.Fatalf("target %q has %d parts, want 2", m.Target, len(parts))
		}
		if parts[0] != schema {
			rt.Fatalf("target schema = %q, want %q", parts[0], schema)
		}
		if parts[1] != table {
			rt.Fatalf("target table = %q, want %q", parts[1], table)
		}
	})
}

// Property: YAML model always has ScriptType=starlark.
func TestRapid_YAML_AlwaysScript(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		kind := genYAMLKind().Draw(rt, "kind")
		source := genSourceName().Draw(rt, "source")

		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "models", "raw"), 0o755)

		extra := ""
		if kind == "merge" || kind == "scd2" {
			extra = "unique_key: id\n"
		}
		if kind == "partition" {
			extra = "unique_key: id\n"
		}
		content := fmt.Sprintf("kind: %s\nsource: %s\n%s", kind, source, extra)
		path := filepath.Join(dir, "models", "raw", "test.yaml")
		os.WriteFile(path, []byte(content), 0o644)

		m, err := ParseModel(path, dir)
		if err != nil {
			rt.Fatalf("ParseModel: %v", err)
		}

		if m.ScriptType != ScriptTypeStarlark {
			rt.Fatalf("expected ScriptType=starlark, got %q", m.ScriptType)
		}
	})
}

// Property: YAML model kind is preserved from the file.
func TestRapid_YAML_KindPreserved(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		kind := genYAMLKind().Draw(rt, "kind")
		source := genSourceName().Draw(rt, "source")

		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "models", "raw"), 0o755)

		extra := ""
		if kind == "partition" || kind == "merge" || kind == "scd2" {
			extra = "unique_key: id\n"
		}
		content := fmt.Sprintf("kind: %s\nsource: %s\n%s", kind, source, extra)
		path := filepath.Join(dir, "models", "raw", "test.yaml")
		os.WriteFile(path, []byte(content), 0o644)

		m, err := ParseModel(path, dir)
		if err != nil {
			rt.Fatalf("ParseModel: %v", err)
		}

		if m.Kind != kind {
			rt.Fatalf("kind = %q, want %q", m.Kind, kind)
		}
	})
}

// Property: YAML model source is preserved from the file.
func TestRapid_YAML_SourcePreserved(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		source := genSourceName().Draw(rt, "source")

		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "models", "raw"), 0o755)

		content := fmt.Sprintf("source: %s\n", source)
		path := filepath.Join(dir, "models", "raw", "test.yaml")
		os.WriteFile(path, []byte(content), 0o644)

		m, err := ParseModel(path, dir)
		if err != nil {
			rt.Fatalf("ParseModel: %v", err)
		}

		if m.Source != source {
			rt.Fatalf("source = %q, want %q", m.Source, source)
		}
	})
}

// Property: YAML model config values are preserved.
func TestRapid_YAML_ConfigPreserved(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		source := genSourceName().Draw(rt, "source")
		// Filter out Starlark reserved keywords — the parser correctly
		// rejects them, but this test asserts successful round-trip.
		var key string
		for {
			key = rapid.StringMatching(`^[a-z][a-z0-9_]{1,8}$`).Draw(rt, "key")
			if !starlarkKeywords[key] {
				break
			}
		}
		val := rapid.StringMatching(`^[a-zA-Z0-9]{1,20}$`).Draw(rt, "val")

		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "models", "raw"), 0o755)

		content := fmt.Sprintf("source: %s\nconfig:\n  %s: \"%s\"\n", source, key, val)
		path := filepath.Join(dir, "models", "raw", "test.yaml")
		os.WriteFile(path, []byte(content), 0o644)

		m, err := ParseModel(path, dir)
		if err != nil {
			rt.Fatalf("ParseModel: %v", err)
		}

		if m.SourceConfig == nil {
			rt.Fatal("expected SourceConfig to be set")
		}
		got, ok := m.SourceConfig[key]
		if !ok {
			rt.Fatalf("SourceConfig missing key %q", key)
		}
		if got != val {
			rt.Fatalf("SourceConfig[%q] = %v, want %q", key, got, val)
		}
	})
}

// Property: YAML model without kind defaults to "table".
func TestRapid_YAML_DefaultKind(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		source := genSourceName().Draw(rt, "source")

		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "models", "raw"), 0o755)

		content := fmt.Sprintf("source: %s\n", source)
		path := filepath.Join(dir, "models", "raw", "test.yaml")
		os.WriteFile(path, []byte(content), 0o644)

		m, err := ParseModel(path, dir)
		if err != nil {
			rt.Fatalf("ParseModel: %v", err)
		}

		if m.Kind != "table" {
			rt.Fatalf("kind = %q, want 'table' (default)", m.Kind)
		}
	})
}

// Property: YAML and SQL produce same target for same path.
func TestRapid_YAML_TargetMatchesSQL(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		schema := genSchemaName().Draw(rt, "schema")
		table := genValidTableName().Draw(rt, "table")

		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "models", schema), 0o755)

		// Create SQL model
		sqlPath := filepath.Join(dir, "models", schema, table+".sql")
		os.WriteFile(sqlPath, []byte("-- @kind: table\nSELECT 1 AS id"), 0o644)

		// Create YAML model (different name to avoid conflict)
		yamlPath := filepath.Join(dir, "models", schema, table+".yaml")
		os.WriteFile(yamlPath, []byte("source: my_source\n"), 0o644)

		sqlModel, err := ParseModel(sqlPath, dir)
		if err != nil {
			rt.Fatalf("parse SQL: %v", err)
		}
		yamlModel, err := ParseModel(yamlPath, dir)
		if err != nil {
			rt.Fatalf("parse YAML: %v", err)
		}

		if sqlModel.Target != yamlModel.Target {
			rt.Fatalf("SQL target %q != YAML target %q", sqlModel.Target, yamlModel.Target)
		}
	})
}
