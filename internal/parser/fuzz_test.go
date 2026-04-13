// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package parser

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func FuzzParseModel(f *testing.F) {
	// Seed with various SQL model contents
	f.Add(`-- @kind: table
SELECT 1 AS id, 'hello' AS name`)

	f.Add(`-- @kind: append
-- @incremental: updated_at
-- @unique_key: id
-- @constraint: not_null(id)
-- @constraint: compare(amount, >=, 0)
-- @audit: row_count(>=, 1)
-- @warning: null_percent(email, 10)
SELECT * FROM raw.events WHERE updated_at > '{{ last_value }}'`)

	f.Add(`-- @kind: merge
-- @unique_key: id
-- @partitioned_by: region
SELECT id, region, amount FROM staging.orders`)

	f.Add(`-- @kind: scd2
-- @unique_key: customer_id
SELECT * FROM raw.customers`)

	f.Add(`-- @kind: partition
-- @unique_key: date
SELECT * FROM raw.events`)

	// Description and column directives
	f.Add(`-- @kind: table
-- @description: Daily aggregated sales
-- @column: revenue = Total revenue including tax
-- @column: region = Geographic region
SELECT 1 AS revenue, 'US' AS region`)

	f.Add(`-- @kind: table
-- @column: ssn = Social security number | PII
SELECT '123-45-6789' AS ssn`)

	// Column tags (masking)
	f.Add(`-- @kind: table
-- @column: ssn = SSN | PII | mask_ssn
SELECT '123-45-6789' AS ssn`)

	f.Add(`-- @kind: table
-- @column: email = Customer email | mask_email
-- @column: name = Full name | mask
-- @column: id = Primary key
SELECT 1 AS id, 'test@test.com' AS email, 'Alice' AS name`)

	f.Add(`-- @kind: merge
-- @unique_key: id
-- @column: ssn = SSN | hash_pii
-- @column: notes = Free text | redact
SELECT 1 AS id, '123' AS ssn, 'secret' AS notes`)

	// Events model
	f.Add(`-- @kind: events
event_name VARCHAR NOT NULL,
page_url VARCHAR,
received_at TIMESTAMPTZ`)

	f.Add(`-- @kind: events
-- @description: Analytics events
user_id VARCHAR NOT NULL,
event_params JSON`)

	// Edge cases
	f.Add(``)
	f.Add(`-- no kind directive
SELECT 1`)
	f.Add(`-- @kind: unknown_kind
SELECT 1`)
	f.Add(`-- @unique_key: has.dot.in.it
SELECT 1`)
	f.Add(`-- @extension: httpfs
SELECT 1`)
	f.Add(`-- @extension: spatial FROM core
SELECT 1`)
	f.Add(`-- @kind: table
-- @expose
SELECT 1`)
	f.Add(`-- @kind: table
-- @expose
SELECT 1`)

	f.Fuzz(func(t *testing.T, content string) {
		dir := t.TempDir()
		modelDir := filepath.Join(dir, "models", "raw")
		os.MkdirAll(modelDir, 0o755)
		modelPath := filepath.Join(modelDir, "test.sql")
		os.WriteFile(modelPath, []byte(content), 0o644)

		m, err := ParseModel(modelPath, dir)
		if err != nil {
			return
		}
		// Target must always be derived from path
		if m.Target == "" {
			t.Error("successful parse should produce non-empty Target")
		}
		// Constraints and audits should be non-empty strings if present
		for i, c := range m.Constraints {
			if c == "" {
				t.Errorf("Constraints[%d] is empty", i)
			}
		}
		for i, a := range m.Audits {
			if a == "" {
				t.Errorf("Audits[%d] is empty", i)
			}
		}
		// Column descriptions should have non-empty keys and values
		for k, v := range m.ColumnDescriptions {
			if k == "" {
				t.Errorf("ColumnDescriptions has empty key")
			}
			if v == "" {
				t.Errorf("ColumnDescriptions[%q] is empty", k)
			}
		}
		// Events models must have columns with valid names and types
		if m.Kind == "events" {
			for i, col := range m.Columns {
				if col.Name == "" {
					t.Errorf("Columns[%d].Name is empty", i)
				}
				if col.Type == "" {
					t.Errorf("Columns[%d].Type is empty", i)
				}
			}
		}
		// Column tags should have non-empty keys and non-empty single-word values
		for k, tags := range m.ColumnTags {
			if k == "" {
				t.Errorf("ColumnTags has empty key")
			}
			for i, tag := range tags {
				if tag == "" {
					t.Errorf("ColumnTags[%q][%d] is empty", k, i)
				}
				if strings.Contains(tag, " ") {
					t.Errorf("ColumnTags[%q][%d] = %q contains space (must be single word)", k, i, tag)
				}
			}
		}
	})
}

func FuzzParseColumnDefs(f *testing.F) {
	f.Add("event_name VARCHAR NOT NULL,\npage_url VARCHAR")
	f.Add("id INTEGER,\nname VARCHAR NOT NULL,\ncount BIGINT")
	f.Add("received_at TIMESTAMPTZ")
	f.Add("")
	f.Add("invalid")
	f.Add(",,,")
	f.Add("-- comment\nevent_name VARCHAR")
	f.Add("a B NOT NULL,\nc D,\ne F NOT NULL")

	f.Fuzz(func(t *testing.T, input string) {
		cols, err := parseColumnDefs(input)
		if err != nil {
			return
		}
		// Each column must have non-empty Name and Type
		for i, col := range cols {
			if col.Name == "" {
				t.Errorf("col[%d].Name is empty", i)
			}
			if col.Type == "" {
				t.Errorf("col[%d].Type is empty", i)
			}
			// Name must pass ValidateColumnName
			if err := ValidateColumnName(col.Name); err != nil {
				t.Errorf("col[%d].Name %q invalid: %v", i, col.Name, err)
			}
		}
	})
}

func FuzzParseYAMLModel(f *testing.F) {
	f.Add(`kind: table
source: my_source
config:
  key: value`)

	f.Add(`kind: append
incremental: updated_at
source: api_fetch
config:
  url: https://api.example.com
  page_size: 100`)

	f.Add(`kind: merge
unique_key: id
source: crm_sync
config:
  account_id: "12345"`)

	f.Add(`source: minimal`)

	f.Add(`kind: scd2
unique_key: customer_id
source: customer_loader
description: Customer SCD2 loader`)

	// Edge cases
	f.Add(``)
	f.Add(`kind: table`)                              // missing source
	f.Add(`kind: invalid_kind` + "\n" + `source: x`)  // invalid kind
	f.Add(`source: x` + "\n" + `config:` + "\n" + `  nested:` + "\n" + `    deep: value`)
	f.Add(`source: x` + "\n" + `config:` + "\n" + `  list:` + "\n" + `    - a` + "\n" + `    - b`)
	f.Add(`not valid yaml: [`)

	f.Fuzz(func(t *testing.T, content string) {
		dir := t.TempDir()
		modelDir := filepath.Join(dir, "models", "raw")
		os.MkdirAll(modelDir, 0o755)
		modelPath := filepath.Join(modelDir, "test.yaml")
		os.WriteFile(modelPath, []byte(content), 0o644)

		m, err := ParseModel(modelPath, dir)
		if err != nil {
			return
		}
		// Target must always be derived from path
		if m.Target == "" {
			t.Error("successful parse should produce non-empty Target")
		}
		// Source must be non-empty for YAML models
		if m.Source == "" {
			t.Error("successful parse should produce non-empty Source")
		}
		// Must be a script model
		if m.ScriptType != ScriptTypeStarlark {
			t.Errorf("YAML model should have ScriptType=starlark, got %q", m.ScriptType)
		}
	})
}

func FuzzValidateIdentifier(f *testing.F) {
	f.Add("staging.orders")
	f.Add("main.users")
	f.Add("raw.api__events")
	f.Add("")
	f.Add(".")
	f.Add("..")
	f.Add("schema.")
	f.Add(".table")
	f.Add("schema..table")
	f.Add("a.b.c")
	f.Add("123bad")
	f.Add("good_name")
	f.Add("has space")
	f.Add("has-hyphen")
	f.Add("schema.table; DROP TABLE x;--")

	f.Fuzz(func(t *testing.T, s string) {
		err := ValidateIdentifier(s)
		if err != nil {
			return // Invalid is fine
		}
		// If validation passed, verify structural properties:
		// 1. Must not be empty
		if s == "" {
			t.Error("empty string should not pass validation")
		}
		// 2. Must not contain consecutive dots
		if strings.Contains(s, "..") {
			t.Errorf("consecutive dots should not pass: %q", s)
		}
		// 3. Must not end with a dot
		if strings.HasSuffix(s, ".") {
			t.Errorf("trailing dot should not pass: %q", s)
		}
		// 4. Must not start with a dot
		if strings.HasPrefix(s, ".") {
			t.Errorf("leading dot should not pass: %q", s)
		}
		// 5. Each part must start with letter or underscore
		for _, part := range strings.Split(s, ".") {
			if len(part) > 0 && part[0] >= '0' && part[0] <= '9' {
				t.Errorf("part %q starts with digit in %q", part, s)
			}
		}
		// 6. Must not contain SQL-dangerous characters
		for _, c := range s {
			if c == ';' || c == '\'' || c == '"' || c == '-' || c == ' ' {
				t.Errorf("dangerous character %q in validated identifier %q", string(c), s)
			}
		}
	})
}

func FuzzIsModelFile(f *testing.F) {
	f.Add("models/raw/orders.sql")
	f.Add("models/staging/transform.star")
	f.Add("scripts/load.sh")
	f.Add("scripts/run.ps1")
	f.Add("README.md")
	f.Add("")
	f.Add("file.SQL")
	f.Add("...")
	f.Add("models/test.sql.bak")

	f.Fuzz(func(t *testing.T, path string) {
		result := IsModelFile(path)
		ext := filepath.Ext(path)
		// Consistency: .sql, .star, .yaml, .yml files must return true, others false
		validExt := ext == ".sql" || ext == ".star" || ext == ".yaml" || ext == ".yml"
		if validExt && !result {
			t.Errorf("IsModelFile(%q) = false, want true for %s extension", path, ext)
		}
		if !validExt && result {
			t.Errorf("IsModelFile(%q) = true, want false for %s extension", path, ext)
		}
	})
}
