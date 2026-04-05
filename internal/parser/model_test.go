// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package parser

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseModel_Kind(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{"default is table", "SELECT 1", "table"},
		{"explicit table", "-- @kind: table\nSELECT 1", "table"},
		{"append", "-- @kind: append\nSELECT 1", "append"},
		{"merge", "-- @kind: merge\nSELECT 1", "merge"},
		{"scd2", "-- @kind: scd2\nSELECT 1", "scd2"},
		{"partition", "-- @kind: partition\n-- @unique_key: id\nSELECT 1", "partition"},
		{"with spaces", "-- @kind:   table  \nSELECT 1", "table"},
		{"events", "-- @kind: events\nevent_name VARCHAR NOT NULL,\npage_url VARCHAR", "events"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tmpDir := t.TempDir()
			modelsDir := filepath.Join(tmpDir, "models", "staging")
			os.MkdirAll(modelsDir, 0755)

			modelFile := filepath.Join(modelsDir, "test.sql")
			os.WriteFile(modelFile, []byte(tt.content), 0644)

			model, err := ParseModel(modelFile, tmpDir)
			if err != nil {
				t.Fatalf("ParseModel failed: %v", err)
			}
			if model.Kind != tt.want {
				t.Errorf("got Kind=%q, want %q", model.Kind, tt.want)
			}
		})
	}
}

func TestParseModel_Target(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		relPath    string
		wantTarget string
	}{
		{"staging model", "staging/orders.sql", "staging.orders"},
		{"marts model", "marts/fact_orders.sql", "marts.fact_orders"},
		// Nested paths are flattened: schema = first folder, table = rest joined with __
		{"nested", "staging/raw/events.sql", "staging.raw__events"},
		{"deeply nested", "raw/api/v2/orders.sql", "raw.api__v2__orders"},
		// Root models use "main" schema
		{"root model", "test.sql", "main.test"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tmpDir := t.TempDir()
			modelFile := filepath.Join(tmpDir, "models", tt.relPath)
			os.MkdirAll(filepath.Dir(modelFile), 0755)
			os.WriteFile(modelFile, []byte("SELECT 1"), 0644)

			model, err := ParseModel(modelFile, tmpDir)
			if err != nil {
				t.Fatalf("ParseModel failed: %v", err)
			}
			if model.Target != tt.wantTarget {
				t.Errorf("got Target=%q, want %q", model.Target, tt.wantTarget)
			}
		})
	}
}

func TestParseModel_Constraints(t *testing.T) {
	t.Parallel()
	content := `-- @kind: table
-- @constraint: id PRIMARY KEY
-- @constraint: name NOT NULL
-- @constraint: amount >= 0

SELECT 1 AS id, 'test' AS name, 100 AS amount`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	model, err := ParseModel(modelFile, tmpDir)
	if err != nil {
		t.Fatalf("ParseModel failed: %v", err)
	}

	if len(model.Constraints) != 3 {
		t.Fatalf("got %d constraints, want 3", len(model.Constraints))
	}

	want := []string{"id PRIMARY KEY", "name NOT NULL", "amount >= 0"}
	for i, c := range model.Constraints {
		if c != want[i] {
			t.Errorf("constraint[%d] = %q, want %q", i, c, want[i])
		}
	}
}

func TestParseModel_Audits(t *testing.T) {
	t.Parallel()
	content := `-- @kind: table
-- @audit: row_count >= 1
-- @audit: freshness(updated_at, 24h)

SELECT 1 AS id`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	model, err := ParseModel(modelFile, tmpDir)
	if err != nil {
		t.Fatalf("ParseModel failed: %v", err)
	}

	if len(model.Audits) != 2 {
		t.Fatalf("got %d audits, want 2", len(model.Audits))
	}
}

func TestParseModel_Warnings(t *testing.T) {
	t.Parallel()
	content := `-- @kind: table
-- @warning: mean(amount) BETWEEN 10 AND 100

SELECT 1 AS id`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	model, err := ParseModel(modelFile, tmpDir)
	if err != nil {
		t.Fatalf("ParseModel failed: %v", err)
	}

	if len(model.Warnings) != 1 {
		t.Fatalf("got %d warnings, want 1", len(model.Warnings))
	}
}

func TestParseModel_SQL(t *testing.T) {
	t.Parallel()
	content := `-- @kind: table
-- @constraint: id PRIMARY KEY

SELECT 1 AS id, 'hello' AS name
FROM source_table
WHERE active = true;`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	model, err := ParseModel(modelFile, tmpDir)
	if err != nil {
		t.Fatalf("ParseModel failed: %v", err)
	}

	// SQL should not include directives, and semicolon should be stripped
	wantSQL := `SELECT 1 AS id, 'hello' AS name
FROM source_table
WHERE active = true`

	if model.SQL != wantSQL {
		t.Errorf("got SQL:\n%s\n\nwant:\n%s", model.SQL, wantSQL)
	}
}

func TestParseModel_UniqueKey(t *testing.T) {
	t.Parallel()
	content := `-- @kind: merge
-- @unique_key: id

SELECT 1 AS id`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	model, err := ParseModel(modelFile, tmpDir)
	if err != nil {
		t.Fatalf("ParseModel failed: %v", err)
	}

	if model.UniqueKey != "id" {
		t.Errorf("got UniqueKey=%q, want %q", model.UniqueKey, "id")
	}
}

func TestParseModel_PartitionedBy(t *testing.T) {
	t.Parallel()
	content := `-- @kind: table
-- @partitioned_by: year, month

SELECT year(event_date) AS year, month(event_date) AS month, event_id
FROM staging.events`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	model, err := ParseModel(modelFile, tmpDir)
	if err != nil {
		t.Fatalf("ParseModel failed: %v", err)
	}

	if len(model.PartitionedBy) != 2 {
		t.Fatalf("got %d partitioned_by columns, want 2", len(model.PartitionedBy))
	}

	if model.PartitionedBy[0] != "year" || model.PartitionedBy[1] != "month" {
		t.Errorf("got PartitionedBy=%v, want [year, month]", model.PartitionedBy)
	}
}

func TestParseModel_PartitionKind_RequiresUniqueKey(t *testing.T) {
	t.Parallel()
	content := `-- @kind: partition

SELECT 1 AS id`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	_, err := ParseModel(modelFile, tmpDir)
	if err == nil {
		t.Error("expected error for partition kind without unique_key")
	}
	if !strings.Contains(err.Error(), "partition kind requires @unique_key") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestParseModel_PartitionKind_UniqueKey(t *testing.T) {
	t.Parallel()
	content := `-- @kind: partition
-- @unique_key: year, month

SELECT year(event_date) AS year, month(event_date) AS month, event_id
FROM staging.events`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	model, err := ParseModel(modelFile, tmpDir)
	if err != nil {
		t.Fatalf("ParseModel failed: %v", err)
	}

	if model.UniqueKey != "year, month" {
		t.Errorf("got UniqueKey=%q, want %q", model.UniqueKey, "year, month")
	}
}

func TestParseModel_MergeKind_MultiColumnUniqueKeyRejected(t *testing.T) {
	t.Parallel()
	content := `-- @kind: merge
-- @unique_key: id, region

SELECT 1 AS id, 'EU' AS region`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	_, err := ParseModel(modelFile, tmpDir)
	if err == nil {
		t.Error("expected error for merge with multi-column unique_key")
	}
	if !strings.Contains(err.Error(), "only a single unique_key column is supported") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestParseModel_SCD2Kind_MultiColumnUniqueKeyRejected(t *testing.T) {
	t.Parallel()
	content := `-- @kind: scd2
-- @unique_key: id, region

SELECT 1 AS id, 'EU' AS region`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	_, err := ParseModel(modelFile, tmpDir)
	if err == nil {
		t.Error("expected error for scd2 with multi-column unique_key")
	}
	if !strings.Contains(err.Error(), "only a single unique_key column is supported") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestParseModel_PartitionKind_PartitionedByRejected(t *testing.T) {
	t.Parallel()
	content := `-- @kind: partition
-- @unique_key: region
-- @partitioned_by: region

SELECT 'EU' AS region, 1 AS id`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	_, err := ParseModel(modelFile, tmpDir)
	if err == nil {
		t.Error("expected error for partition kind with @partitioned_by")
	}
	if !strings.Contains(err.Error(), "@partitioned_by is not supported for partition kind") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestParseModel_ViewKind_PartitionedByRejected(t *testing.T) {
	t.Parallel()
	content := `-- @kind: view
-- @partitioned_by: region

SELECT 1 AS id, 'EU' AS region`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	_, err := ParseModel(modelFile, tmpDir)
	if err == nil {
		t.Error("expected error for view with @partitioned_by")
	}
	if !strings.Contains(err.Error(), "@partitioned_by is not supported for views") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestParseModel_AppendKind_MultiColumnUniqueKeyRejected(t *testing.T) {
	t.Parallel()
	content := `-- @kind: append
-- @unique_key: id, region

SELECT 1 AS id, 'EU' AS region`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	_, err := ParseModel(modelFile, tmpDir)
	if err == nil {
		t.Error("expected error for append with multi-column unique_key")
	}
	if !strings.Contains(err.Error(), "only a single unique_key column is supported") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestParseModel_UniqueKey_TrailingCommaRejected(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		content string
	}{
		{"trailing comma", "-- @kind: partition\n-- @unique_key: region,\nSELECT 1 AS id"},
		{"leading comma", "-- @kind: partition\n-- @unique_key: ,region\nSELECT 1 AS id"},
		{"double comma", "-- @kind: partition\n-- @unique_key: region,,year\nSELECT 1 AS id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tmpDir := t.TempDir()
			modelsDir := filepath.Join(tmpDir, "models", "staging")
			os.MkdirAll(modelsDir, 0755)
			modelFile := filepath.Join(modelsDir, "test.sql")
			os.WriteFile(modelFile, []byte(tt.content), 0644)

			_, err := ParseModel(modelFile, tmpDir)
			if err == nil {
				t.Error("expected error for invalid unique_key")
			}
			if !strings.Contains(err.Error(), "empty column name") {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestParseModel_InvalidKind(t *testing.T) {
	t.Parallel()
	content := `-- @kind: invalid

SELECT 1`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	_, err := ParseModel(modelFile, tmpDir)
	if err == nil {
		t.Error("expected error for invalid kind")
	}
}

func TestParseModel_ReservedPattern(t *testing.T) {
	t.Parallel()
	// Files with __ in the path should be rejected (reserved as folder separator)
	tests := []struct {
		name    string
		relPath string
	}{
		{"double underscore in filename", "staging/api__orders.sql"},
		{"double underscore in folder", "raw__api/orders.sql"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tmpDir := t.TempDir()
			modelFile := filepath.Join(tmpDir, "models", tt.relPath)
			os.MkdirAll(filepath.Dir(modelFile), 0755)
			os.WriteFile(modelFile, []byte("SELECT 1"), 0644)

			_, err := ParseModel(modelFile, tmpDir)
			if err == nil {
				t.Error("expected error for path with __ pattern")
			}
		})
	}
}

func TestParseModel_StarlarkDirectives(t *testing.T) {
	t.Parallel()
	content := `# @kind: append
# @incremental: report_date
# @incremental_initial: 2024-01-01
# @constraint: id IS NOT NULL
# @audit: count(*) > 0
# @warning: count(*) < 10000
# @extension: httpfs

resp = http.get("https://api.example.com/data")
`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "raw")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "api_data.star")
	os.WriteFile(modelFile, []byte(content), 0644)

	model, err := ParseModel(modelFile, tmpDir)
	if err != nil {
		t.Fatalf("ParseModel failed: %v", err)
	}

	if model.Kind != "append" {
		t.Errorf("Kind = %q, want %q", model.Kind, "append")
	}
	if model.Incremental != "report_date" {
		t.Errorf("Incremental = %q, want %q", model.Incremental, "report_date")
	}
	if model.IncrementalInitial != "2024-01-01" {
		t.Errorf("IncrementalInitial = %q, want %q", model.IncrementalInitial, "2024-01-01")
	}
	if !model.IsScript {
		t.Error("IsScript should be true for .star files")
	}
	if model.ScriptType != ScriptTypeStarlark {
		t.Errorf("ScriptType = %q, want %q", model.ScriptType, ScriptTypeStarlark)
	}
	if len(model.Constraints) != 1 {
		t.Errorf("got %d constraints, want 1", len(model.Constraints))
	}
	if len(model.Audits) != 1 {
		t.Errorf("got %d audits, want 1", len(model.Audits))
	}
	if len(model.Warnings) != 1 {
		t.Errorf("got %d warnings, want 1", len(model.Warnings))
	}
	if len(model.Extensions) != 1 {
		t.Errorf("got %d extensions, want 1", len(model.Extensions))
	}
	if model.Target != "raw.api_data" {
		t.Errorf("Target = %q, want %q", model.Target, "raw.api_data")
	}
}

func TestValidateIdentifier(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input   string
		wantErr bool
	}{
		{"foo", false},
		{"foo_bar", false},
		{"foo.bar", false},
		{"staging.orders", false},
		{"_private", false},
		{"Table123", false},
		{"", true},
		{"123abc", true},
		{"foo--bar", true},
		{"foo;bar", true},
		{"foo'bar", true},
		{"foo\"bar", true},
		{"foo bar", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			err := ValidateIdentifier(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateIdentifier(%q) error = %v, wantErr = %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestValidateColumnName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input   string
		wantErr bool
	}{
		{"foo", false},
		{"foo_bar", false},
		{"_private", false},
		{"Table123", false},
		{"created_at", false},
		// Dots not allowed in column names (unlike identifiers)
		{"foo.bar", true},
		{"schema.column", true},
		// Empty and invalid patterns
		{"", true},
		{"123abc", true},
		{"foo--bar", true},
		{"foo;bar", true},
		{"foo'bar", true},
		{"foo\"bar", true},
		{"foo bar", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			err := ValidateColumnName(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateColumnName(%q) error = %v, wantErr = %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestParseModel_ColumnWithDotRejected(t *testing.T) {
	t.Parallel()
	// @unique_key with a dot should be rejected
	content := `-- @kind: merge
-- @unique_key: schema.id

SELECT 1 AS id`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	_, err := ParseModel(modelFile, tmpDir)
	if err == nil {
		t.Error("expected error for @unique_key with dot")
	}
}

func TestParseModel_DotInFilenameRejected(t *testing.T) {
	t.Parallel()
	// A file like models/raw/api.orders.sql has a dot in the filename,
	// which would create a 3-part target (raw.api.orders). Must be rejected.
	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "raw")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "api.orders.sql")
	os.WriteFile(modelFile, []byte("SELECT 1 AS id"), 0644)

	_, err := ParseModel(modelFile, tmpDir)
	if err == nil {
		t.Error("expected error for filename with dot")
	}
}

func TestParseModel_FileOutsideModelsDir_DotInName(t *testing.T) {
	t.Parallel()
	// File outside models/ with dot in name (e.g. adhoc.v2.sql)
	// Should sanitize dots to underscores: tmp.adhoc_v2
	tmpDir := t.TempDir()
	otherDir := filepath.Join(tmpDir, "other")
	os.MkdirAll(otherDir, 0755)
	modelFile := filepath.Join(otherDir, "adhoc.v2.sql")
	os.WriteFile(modelFile, []byte("SELECT 1 AS id"), 0644)

	model, err := ParseModel(modelFile, tmpDir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if model.Target != "tmp.adhoc_v2" {
		t.Errorf("target = %q, want %q", model.Target, "tmp.adhoc_v2")
	}
	// Must be exactly 2 parts
	parts := strings.Split(model.Target, ".")
	if len(parts) != 2 {
		t.Errorf("target has %d parts, want 2: %q", len(parts), model.Target)
	}
}

func TestParseModel_FileOutsideModelsDir(t *testing.T) {
	t.Parallel()
	// File outside models/ should get tmp schema
	tmpDir := t.TempDir()
	otherDir := filepath.Join(tmpDir, "other")
	os.MkdirAll(otherDir, 0755)
	modelFile := filepath.Join(otherDir, "adhoc.sql")
	os.WriteFile(modelFile, []byte("SELECT 1 AS id"), 0644)

	model, err := ParseModel(modelFile, tmpDir)
	if err != nil {
		t.Fatalf("ParseModel failed: %v", err)
	}
	if model.Target != "tmp.adhoc" {
		t.Errorf("Target = %q, want tmp.adhoc", model.Target)
	}
}

func TestParseModel_ScriptDirective(t *testing.T) {
	t.Parallel()
	// @script directive in .sql file should set IsScript
	content := "-- @script\n-- @kind: append\nSELECT 1 AS id"
	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "scripted.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	model, err := ParseModel(modelFile, tmpDir)
	if err != nil {
		t.Fatalf("ParseModel failed: %v", err)
	}
	if !model.IsScript {
		t.Error("expected IsScript=true with @script directive")
	}
	if model.ScriptType != ScriptTypeStarlark {
		t.Errorf("ScriptType = %q, want starlark", model.ScriptType)
	}
}

func TestParseModel_FileNotFound(t *testing.T) {
	t.Parallel()
	_, err := ParseModel("/nonexistent/path.sql", "/nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestParseModel_InvalidUniqueKey(t *testing.T) {
	t.Parallel()
	content := "-- @kind: merge\n-- @unique_key: schema.id\nSELECT 1 AS id"
	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	_, err := ParseModel(modelFile, tmpDir)
	if err == nil {
		t.Error("expected error for unique_key column with dot")
	}
}

func TestParseModel_InvalidPartitionedByColumn(t *testing.T) {
	t.Parallel()
	content := "-- @kind: table\n-- @partitioned_by: year.month\nSELECT 1 AS id"
	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	_, err := ParseModel(modelFile, tmpDir)
	if err == nil {
		t.Error("expected error for partitioned_by column with dot")
	}
}

func TestParseModel_InvalidIncrementalColumn(t *testing.T) {
	t.Parallel()
	content := "-- @kind: append\n-- @incremental: schema.col\nSELECT 1 AS id"
	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	_, err := ParseModel(modelFile, tmpDir)
	if err == nil {
		t.Error("expected error for incremental column with dot")
	}
}

func TestParseModel_DeepNestedPath(t *testing.T) {
	t.Parallel()
	// models/raw/api/orders.sql -> raw.api__orders
	content := "SELECT 1 AS id"
	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "raw", "api")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "orders.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	model, err := ParseModel(modelFile, tmpDir)
	if err != nil {
		t.Fatalf("ParseModel failed: %v", err)
	}
	if model.Target != "raw.api__orders" {
		t.Errorf("Target = %q, want raw.api__orders", model.Target)
	}
}

func TestParseModel_SingleFileInModelsRoot(t *testing.T) {
	t.Parallel()
	// models/orders.sql -> main.orders
	content := "SELECT 1 AS id"
	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "orders.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	model, err := ParseModel(modelFile, tmpDir)
	if err != nil {
		t.Fatalf("ParseModel failed: %v", err)
	}
	if model.Target != "main.orders" {
		t.Errorf("Target = %q, want main.orders", model.Target)
	}
}

func TestParseModel_ExtensionDirective(t *testing.T) {
	t.Parallel()
	content := "-- @kind: table\n-- @extension: httpfs\n-- @extension: json\nSELECT 1 AS id"
	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	model, err := ParseModel(modelFile, tmpDir)
	if err != nil {
		t.Fatalf("ParseModel failed: %v", err)
	}
	if len(model.Extensions) != 2 {
		t.Fatalf("got %d extensions, want 2", len(model.Extensions))
	}
	if model.Extensions[0] != "httpfs" || model.Extensions[1] != "json" {
		t.Errorf("Extensions = %v, want [httpfs, json]", model.Extensions)
	}
}

func TestParseModel_Description(t *testing.T) {
	t.Parallel()
	content := `-- @kind: table
-- @description: Daily aggregated sales by region

SELECT 1 AS id`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "mart")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "sales_daily.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	model, err := ParseModel(modelFile, tmpDir)
	if err != nil {
		t.Fatalf("ParseModel failed: %v", err)
	}

	if model.Description != "Daily aggregated sales by region" {
		t.Errorf("Description = %q, want %q", model.Description, "Daily aggregated sales by region")
	}
}

func TestParseModel_ColumnDescriptions(t *testing.T) {
	t.Parallel()
	content := `-- @kind: table
-- @description: Sales summary
-- @column: revenue = Total revenue including tax
-- @column: region = Geographic sales region

SELECT sum(amount) AS revenue, region FROM source`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "mart")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "sales.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	model, err := ParseModel(modelFile, tmpDir)
	if err != nil {
		t.Fatalf("ParseModel failed: %v", err)
	}

	if len(model.ColumnDescriptions) != 2 {
		t.Fatalf("got %d column descriptions, want 2", len(model.ColumnDescriptions))
	}

	if model.ColumnDescriptions["revenue"] != "Total revenue including tax" {
		t.Errorf("revenue desc = %q, want %q", model.ColumnDescriptions["revenue"], "Total revenue including tax")
	}
	if model.ColumnDescriptions["region"] != "Geographic sales region" {
		t.Errorf("region desc = %q, want %q", model.ColumnDescriptions["region"], "Geographic sales region")
	}
}

func TestParseModel_ColumnDescriptions_WithPipeTags(t *testing.T) {
	t.Parallel()
	content := `-- @kind: table
-- @column: ssn = Social security number | PII
-- @column: email = Customer email | PII | sensitive
-- @column: revenue = Total revenue
-- @column: metric = Revenue | Cost ratio

SELECT 1 AS ssn, 2 AS email, 3 AS revenue, 4 AS metric`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "mart")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	model, err := ParseModel(modelFile, tmpDir)
	if err != nil {
		t.Fatalf("ParseModel failed: %v", err)
	}

	if len(model.ColumnDescriptions) != 4 {
		t.Fatalf("got %d column descriptions, want 4", len(model.ColumnDescriptions))
	}

	// Single-word pipe tags should be stripped
	if model.ColumnDescriptions["ssn"] != "Social security number" {
		t.Errorf("ssn desc = %q, want %q", model.ColumnDescriptions["ssn"], "Social security number")
	}
	if model.ColumnDescriptions["email"] != "Customer email" {
		t.Errorf("email desc = %q, want %q", model.ColumnDescriptions["email"], "Customer email")
	}
	// No pipe — full description kept
	if model.ColumnDescriptions["revenue"] != "Total revenue" {
		t.Errorf("revenue desc = %q, want %q", model.ColumnDescriptions["revenue"], "Total revenue")
	}
	// Multi-word after | is NOT a tag — preserved in description
	if model.ColumnDescriptions["metric"] != "Revenue | Cost ratio" {
		t.Errorf("metric desc = %q, want %q", model.ColumnDescriptions["metric"], "Revenue | Cost ratio")
	}
}

func TestParseModel_ColumnDescriptions_Starlark(t *testing.T) {
	t.Parallel()
	content := `# @kind: append
# @description: API data ingestion
# @column: report_date = Date of the report

resp = http.get("https://api.example.com/data")
`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "raw")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "api_data.star")
	os.WriteFile(modelFile, []byte(content), 0644)

	model, err := ParseModel(modelFile, tmpDir)
	if err != nil {
		t.Fatalf("ParseModel failed: %v", err)
	}

	if model.Description != "API data ingestion" {
		t.Errorf("Description = %q, want %q", model.Description, "API data ingestion")
	}
	if model.ColumnDescriptions["report_date"] != "Date of the report" {
		t.Errorf("report_date desc = %q, want %q", model.ColumnDescriptions["report_date"], "Date of the report")
	}
}

func TestParseModel_ColumnDescriptions_InvalidName(t *testing.T) {
	t.Parallel()
	content := `-- @kind: table
-- @column: bad;name = Some description

SELECT 1 AS id`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	_, err := ParseModel(modelFile, tmpDir)
	if err == nil {
		t.Error("expected error for @column with invalid column name")
	}
}

func TestParseModel_ColumnTags_Single(t *testing.T) {
	t.Parallel()
	content := `-- @kind: table
-- @column: ssn = SSN | mask_ssn

SELECT '123-45-6789' AS ssn`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	model, err := ParseModel(modelFile, tmpDir)
	if err != nil {
		t.Fatalf("ParseModel failed: %v", err)
	}

	if model.ColumnDescriptions["ssn"] != "SSN" {
		t.Errorf("ssn desc = %q, want %q", model.ColumnDescriptions["ssn"], "SSN")
	}
	tags, ok := model.ColumnTags["ssn"]
	if !ok {
		t.Fatal("ColumnTags missing ssn")
	}
	if len(tags) != 1 || tags[0] != "mask_ssn" {
		t.Errorf("ColumnTags[ssn] = %v, want [mask_ssn]", tags)
	}
}

func TestParseModel_ColumnTags_Multiple(t *testing.T) {
	t.Parallel()
	content := `-- @kind: table
-- @column: ssn = SSN | PII | mask_ssn

SELECT '123-45-6789' AS ssn`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	model, err := ParseModel(modelFile, tmpDir)
	if err != nil {
		t.Fatalf("ParseModel failed: %v", err)
	}

	if model.ColumnDescriptions["ssn"] != "SSN" {
		t.Errorf("ssn desc = %q, want %q", model.ColumnDescriptions["ssn"], "SSN")
	}
	tags := model.ColumnTags["ssn"]
	if len(tags) != 2 {
		t.Fatalf("ColumnTags[ssn] has %d tags, want 2: %v", len(tags), tags)
	}
	if tags[0] != "PII" || tags[1] != "mask_ssn" {
		t.Errorf("ColumnTags[ssn] = %v, want [PII, mask_ssn]", tags)
	}
}

func TestParseModel_ColumnTags_NoTags(t *testing.T) {
	t.Parallel()
	content := `-- @kind: table
-- @column: revenue = Total revenue

SELECT 100.0 AS revenue`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	model, err := ParseModel(modelFile, tmpDir)
	if err != nil {
		t.Fatalf("ParseModel failed: %v", err)
	}

	if model.ColumnDescriptions["revenue"] != "Total revenue" {
		t.Errorf("revenue desc = %q, want %q", model.ColumnDescriptions["revenue"], "Total revenue")
	}
	if len(model.ColumnTags) != 0 {
		t.Errorf("ColumnTags should be empty, got %v", model.ColumnTags)
	}
}

func TestParseModel_ColumnTags_MultiWordNotTag(t *testing.T) {
	t.Parallel()
	content := `-- @kind: table
-- @column: metric = Revenue | Cost ratio

SELECT 1.5 AS metric`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	model, err := ParseModel(modelFile, tmpDir)
	if err != nil {
		t.Fatalf("ParseModel failed: %v", err)
	}

	// Multi-word "Cost ratio" is not a tag, stays in description
	if model.ColumnDescriptions["metric"] != "Revenue | Cost ratio" {
		t.Errorf("metric desc = %q, want %q", model.ColumnDescriptions["metric"], "Revenue | Cost ratio")
	}
	if len(model.ColumnTags) != 0 {
		t.Errorf("ColumnTags should be empty for multi-word pipe content, got %v", model.ColumnTags)
	}
}

func TestParseModel_ColumnTags_DescriptionPreserved(t *testing.T) {
	t.Parallel()
	content := `-- @kind: table
-- @column: email = Customer email address | PII | mask_email
-- @column: name = Full name | mask

SELECT 'test@test.com' AS email, 'Alice' AS name`

	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "staging")
	os.MkdirAll(modelsDir, 0755)
	modelFile := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	model, err := ParseModel(modelFile, tmpDir)
	if err != nil {
		t.Fatalf("ParseModel failed: %v", err)
	}

	// Description should be clean (tags stripped)
	if model.ColumnDescriptions["email"] != "Customer email address" {
		t.Errorf("email desc = %q, want %q", model.ColumnDescriptions["email"], "Customer email address")
	}
	if model.ColumnDescriptions["name"] != "Full name" {
		t.Errorf("name desc = %q, want %q", model.ColumnDescriptions["name"], "Full name")
	}

	// Tags stored separately
	emailTags := model.ColumnTags["email"]
	if len(emailTags) != 2 || emailTags[0] != "PII" || emailTags[1] != "mask_email" {
		t.Errorf("ColumnTags[email] = %v, want [PII, mask_email]", emailTags)
	}
	nameTags := model.ColumnTags["name"]
	if len(nameTags) != 1 || nameTags[0] != "mask" {
		t.Errorf("ColumnTags[name] = %v, want [mask]", nameTags)
	}
}

func TestParseModel_Expose(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelDir := filepath.Join(dir, "models", "mart")
	os.MkdirAll(modelDir, 0755)

	content := "-- @kind: table\n-- @expose\nSELECT 1 AS id"
	path := filepath.Join(modelDir, "test.sql")
	os.WriteFile(path, []byte(content), 0644)

	model, err := ParseModel(path, dir)
	if err != nil {
		t.Fatalf("ParseModel: %v", err)
	}
	if !model.Expose {
		t.Error("expected Expose = true")
	}
	if model.ExposeKey != "" {
		t.Errorf("expected empty ExposeKey, got %q", model.ExposeKey)
	}
}

func TestParseModel_ExposeWithKey(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelDir := filepath.Join(dir, "models", "mart")
	os.MkdirAll(modelDir, 0755)

	content := "-- @kind: table\n-- @expose order_id\nSELECT 1 AS order_id, 100 AS amount"
	path := filepath.Join(modelDir, "orders.sql")
	os.WriteFile(path, []byte(content), 0644)

	model, err := ParseModel(path, dir)
	if err != nil {
		t.Fatalf("ParseModel: %v", err)
	}
	if !model.Expose {
		t.Error("expected Expose = true")
	}
	if model.ExposeKey != "order_id" {
		t.Errorf("expected ExposeKey = 'order_id', got %q", model.ExposeKey)
	}
}

func TestParseModel_ExposeInvalidKey(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelDir := filepath.Join(dir, "models", "mart")
	os.MkdirAll(modelDir, 0755)

	content := "-- @kind: table\n-- @expose bad-key\nSELECT 1 AS id"
	path := filepath.Join(modelDir, "test.sql")
	os.WriteFile(path, []byte(content), 0644)

	_, err := ParseModel(path, dir)
	if err == nil {
		t.Fatal("expected error for invalid expose key")
	}
}

func TestParseModel_NoExpose(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelDir := filepath.Join(dir, "models", "staging")
	os.MkdirAll(modelDir, 0755)

	content := "-- @kind: table\nSELECT 1 AS id"
	path := filepath.Join(modelDir, "test.sql")
	os.WriteFile(path, []byte(content), 0644)

	model, err := ParseModel(path, dir)
	if err != nil {
		t.Fatalf("ParseModel: %v", err)
	}
	if model.Expose {
		t.Error("expected Expose = false")
	}
}

func TestParseModel_ExposeStarlark_Rejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelDir := filepath.Join(dir, "models", "mart")
	os.MkdirAll(modelDir, 0755)

	content := "# @kind: table\n# @expose\nfor i in range(1):\n    save.row({\"id\": i})"
	path := filepath.Join(modelDir, "test.star")
	os.WriteFile(path, []byte(content), 0644)

	_, err := ParseModel(path, dir)
	if err == nil {
		t.Fatal("expected error: @expose not supported for scripts")
	}
}

func TestParseModel_ExposeView_Rejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelDir := filepath.Join(dir, "models", "mart")
	os.MkdirAll(modelDir, 0755)

	content := "-- @kind: view\n-- @expose\nSELECT 1 AS id"
	path := filepath.Join(modelDir, "test.sql")
	os.WriteFile(path, []byte(content), 0644)

	_, err := ParseModel(path, dir)
	if err == nil {
		t.Fatal("expected error: @expose not supported for views")
	}
}

func TestParseModel_ExposeEvents_Rejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelDir, 0755)

	content := "-- @kind: events\n-- @expose\nid INTEGER NOT NULL"
	path := filepath.Join(modelDir, "test.sql")
	os.WriteFile(path, []byte(content), 0644)

	_, err := ParseModel(path, dir)
	if err == nil {
		t.Fatal("expected error: @expose not supported for events")
	}
}

func TestIsModelFile(t *testing.T) {
	t.Parallel()
	tests := []struct {
		path string
		want bool
	}{
		{"models/staging/orders.sql", true},
		{"models/staging/orders.star", true},
		{"models/staging/orders.yaml", true},
		{"models/staging/orders.yml", true},
		{"models/staging/orders.SQL", false}, // case sensitive
		{"models/staging/orders", false},     // no extension
		{"config.yml", true},
		{".sql", true}, // just extension
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()
			got := IsModelFile(tt.path)
			if got != tt.want {
				t.Errorf("IsModelFile(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

func TestParseModel_Events(t *testing.T) {
	t.Parallel()
	content := `-- @kind: events
-- @description: Website analytics events

event_name VARCHAR NOT NULL,
page_url VARCHAR,
user_id VARCHAR,
session_id VARCHAR,
event_params JSON,
received_at TIMESTAMPTZ
`
	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models", "raw")
	os.MkdirAll(modelsDir, 0755)

	modelFile := filepath.Join(modelsDir, "events.sql")
	os.WriteFile(modelFile, []byte(content), 0644)

	model, err := ParseModel(modelFile, tmpDir)
	if err != nil {
		t.Fatalf("ParseModel failed: %v", err)
	}

	if model.Kind != "events" {
		t.Errorf("got Kind=%q, want events", model.Kind)
	}
	if model.Target != "raw.events" {
		t.Errorf("got Target=%q, want raw.events", model.Target)
	}
	if model.Description != "Website analytics events" {
		t.Errorf("got Description=%q, want 'Website analytics events'", model.Description)
	}
	if len(model.Columns) != 6 {
		t.Fatalf("got %d columns, want 6", len(model.Columns))
	}

	// Check first column
	if model.Columns[0].Name != "event_name" {
		t.Errorf("col[0].Name=%q, want event_name", model.Columns[0].Name)
	}
	if model.Columns[0].Type != "VARCHAR" {
		t.Errorf("col[0].Type=%q, want VARCHAR", model.Columns[0].Type)
	}
	if !model.Columns[0].NotNull {
		t.Error("col[0].NotNull=false, want true")
	}

	// Check nullable column
	if model.Columns[1].NotNull {
		t.Error("col[1].NotNull=true, want false")
	}
}

func TestParseModel_Events_InvalidDirectives(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		content string
		wantErr string
	}{
		{
			"unique_key not allowed",
			"-- @kind: events\n-- @unique_key: id\nevent_name VARCHAR",
			"@unique_key is not supported for events",
		},
		{
			"partitioned_by not allowed",
			"-- @kind: events\n-- @partitioned_by: region\nevent_name VARCHAR",
			"@partitioned_by is not supported for events",
		},
		{
			"incremental not allowed",
			"-- @kind: events\n-- @incremental: ts\nevent_name VARCHAR",
			"@incremental is not supported for events",
		},
		{
			"constraint not allowed",
			"-- @kind: events\n-- @constraint: event_name IS NOT NULL\nevent_name VARCHAR",
			"@constraint is not supported for events",
		},
		{
			"audit not allowed",
			"-- @kind: events\n-- @audit: COUNT(*) > 0\nevent_name VARCHAR",
			"@audit is not supported for events",
		},
		{
			"warning not allowed",
			"-- @kind: events\n-- @warning: COUNT(*) > 0\nevent_name VARCHAR",
			"@warning is not supported for events",
		},
		{
			"no columns",
			"-- @kind: events\n",
			"events model must define at least 1 column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tmpDir := t.TempDir()
			modelsDir := filepath.Join(tmpDir, "models", "raw")
			os.MkdirAll(modelsDir, 0755)

			modelFile := filepath.Join(modelsDir, "test.sql")
			os.WriteFile(modelFile, []byte(tt.content), 0644)

			_, err := ParseModel(modelFile, tmpDir)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q does not contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestParseColumnDefs(t *testing.T) {
	t.Parallel()
	input := `event_name VARCHAR NOT NULL,
page_url VARCHAR,
count INTEGER,
received_at TIMESTAMPTZ`

	cols, err := parseColumnDefs(input)
	if err != nil {
		t.Fatalf("parseColumnDefs: %v", err)
	}
	if len(cols) != 4 {
		t.Fatalf("got %d columns, want 4", len(cols))
	}

	// Verify NOT NULL
	if !cols[0].NotNull {
		t.Error("event_name should be NOT NULL")
	}
	if cols[1].NotNull {
		t.Error("page_url should be nullable")
	}
	if cols[2].Name != "count" || cols[2].Type != "INTEGER" {
		t.Errorf("col[2]: got %s %s, want count INTEGER", cols[2].Name, cols[2].Type)
	}
}

func TestParseColumnDefs_MultiWordTypes(t *testing.T) {
	t.Parallel()
	input := `price DOUBLE PRECISION NOT NULL,
created_at TIMESTAMP WITH TIME ZONE,
name CHARACTER VARYING`

	cols, err := parseColumnDefs(input)
	if err != nil {
		t.Fatalf("parseColumnDefs: %v", err)
	}
	if len(cols) != 3 {
		t.Fatalf("got %d columns, want 3", len(cols))
	}

	if cols[0].Name != "price" || cols[0].Type != "DOUBLE PRECISION" || !cols[0].NotNull {
		t.Errorf("col[0]: got %s %q notNull=%v, want price DOUBLE PRECISION NOT NULL",
			cols[0].Name, cols[0].Type, cols[0].NotNull)
	}
	if cols[1].Name != "created_at" || cols[1].Type != "TIMESTAMP WITH TIME ZONE" || cols[1].NotNull {
		t.Errorf("col[1]: got %s %q notNull=%v, want created_at TIMESTAMP WITH TIME ZONE nullable",
			cols[1].Name, cols[1].Type, cols[1].NotNull)
	}
	if cols[2].Name != "name" || cols[2].Type != "CHARACTER VARYING" || cols[2].NotNull {
		t.Errorf("col[2]: got %s %q notNull=%v, want name CHARACTER VARYING nullable",
			cols[2].Name, cols[2].Type, cols[2].NotNull)
	}
}

func TestValidateIdentifier_ConsecutiveDots(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		valid bool
	}{
		{"staging.orders", true},
		{"main.users", true},
		{"a.b.c", true},
		{"schema..table", false},  // consecutive dots
		{"schema.", false},        // trailing dot
		{".table", false},         // leading dot
		{".", false},              // just a dot
		{"..", false},             // double dot
		{"a..b", false},           // embedded consecutive dots
		{"good_name", true},       // single part, no dots
		{"_private.table", true},  // underscore start
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			err := ValidateIdentifier(tt.input)
			if tt.valid && err != nil {
				t.Errorf("ValidateIdentifier(%q) = %v, want valid", tt.input, err)
			}
			if !tt.valid && err == nil {
				t.Errorf("ValidateIdentifier(%q) = nil, want error", tt.input)
			}
		})
	}
}

func TestParseModel_PathSegmentValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		segment string
	}{
		{"apostrophe", "o'malley"},
		{"space", "my file"},
		{"hyphen", "test-data"},
		{"at sign", "user@home"},
		{"semicolon", "drop;table"},
		{"starts with digit", "1orders"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			modelDir := filepath.Join(dir, "models", "raw", tt.segment)
			os.MkdirAll(modelDir, 0755)

			path := filepath.Join(modelDir, "test.sql")
			os.WriteFile(path, []byte("SELECT 1"), 0644)

			_, err := ParseModel(path, dir)
			if err == nil {
				t.Fatalf("expected error for path segment %q", tt.segment)
			}
		})
	}
}

func TestParseModel_PathSegmentFilename(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelDir, 0755)

	// Filename with apostrophe
	path := filepath.Join(modelDir, "o'malley.sql")
	os.WriteFile(path, []byte("SELECT 1"), 0644)

	_, err := ParseModel(path, dir)
	if err == nil {
		t.Fatal("expected error for filename with apostrophe")
	}
}

func TestParseModel_TrackedKind(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelDir, 0755)

	content := "-- @kind: tracked\n-- @unique_key: source_file\nSELECT 'a.pdf' AS source_file, 1 AS id"
	path := filepath.Join(modelDir, "test.sql")
	os.WriteFile(path, []byte(content), 0644)

	model, err := ParseModel(path, dir)
	if err != nil {
		t.Fatalf("ParseModel: %v", err)
	}
	if model.Kind != "tracked" {
		t.Errorf("kind = %q, want tracked", model.Kind)
	}
	if model.UniqueKey != "source_file" {
		t.Errorf("unique_key = %q, want source_file", model.UniqueKey)
	}
}

func TestParseModel_TrackedKind_RequiresUniqueKey(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelDir, 0755)

	content := "-- @kind: tracked\nSELECT 1 AS id"
	path := filepath.Join(modelDir, "test.sql")
	os.WriteFile(path, []byte(content), 0644)

	_, err := ParseModel(path, dir)
	if err == nil {
		t.Fatal("expected error for tracked without unique_key")
	}
}

func TestParseModel_TrackedKind_CompositeKeyRejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	modelDir := filepath.Join(dir, "models", "raw")
	os.MkdirAll(modelDir, 0755)

	content := "-- @kind: tracked\n-- @unique_key: a, b\nSELECT 1 AS a, 2 AS b"
	path := filepath.Join(modelDir, "test.sql")
	os.WriteFile(path, []byte(content), 0644)

	_, err := ParseModel(path, dir)
	if err == nil {
		t.Fatal("expected error for composite unique_key on tracked")
	}
}
