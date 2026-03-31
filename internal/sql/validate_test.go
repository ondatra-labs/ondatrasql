// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package sql

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// TestEmbeddedSQL_AllMacrosValid loads every macros/*.sql file against a real
// DuckDB session and verifies that all CREATE MACRO statements parse and execute.
func TestEmbeddedSQL_AllMacrosValid(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	setupDuckLake(t, db)

	// runtime.sql and cdc.sql need variables set first
	mustExec(t, db, "SET VARIABLE ondatra_run_time = TIMESTAMP '2024-01-01 00:00:00'")
	mustExec(t, db, "SET VARIABLE ondatra_load_id = 'test_123'")
	mustExec(t, db, "SET VARIABLE curr_snapshot = 0")
	mustExec(t, db, "SET VARIABLE prev_snapshot = 0")
	mustExec(t, db, "SET VARIABLE dag_start_snapshot = 0")

	macroFiles := listEmbedded(t, "macros")
	for _, name := range macroFiles {
		t.Run(name, func(t *testing.T) {
			content, err := Load("macros/" + name)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if _, err := db.ExecContext(context.Background(), content); err != nil {
				t.Fatalf("Execute macros/%s:\n%s\n\nError: %v", name, content, err)
			}
		})
	}
}

// TestEmbeddedSQL_AllExecuteTemplatesValid loads every execute/*.sql file,
// substitutes dummy values for %s placeholders, and verifies the SQL parses.
// We use EXPLAIN to validate syntax without side effects.
func TestEmbeddedSQL_AllExecuteTemplatesValid(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	setupDuckLake(t, db)

	// Create a dummy table for templates that reference real tables
	mustExec(t, db, "CREATE TABLE lake.staging.dummy (id INTEGER, name VARCHAR, amount INTEGER)")
	mustExec(t, db, "INSERT INTO lake.staging.dummy VALUES (1, 'test', 100)")

	// Define dummy arguments for each template
	templates := map[string][]any{
		"table.sql":            {`lake.staging.dummy`, `(SELECT 1 AS id, 'x' AS name, 0 AS amount)`},
		"append.sql":           {`lake.staging.dummy`, `(SELECT 2 AS id, 'y' AS name, 0 AS amount)`},
		"merge.sql":            {`lake.staging.dummy`, `(SELECT 1 AS id, 'updated' AS name, 200 AS amount)`, `"id"`, `"id"`, `SET "name" = source."name", "amount" = source."amount"`},
		"commit.sql":           {`SELECT 1`, `staging.dummy`, `{"model":"staging.dummy"}`},
		"partition_delete.sql": {`lake.staging.dummy`, `"amount"`, `"amount"`, `(SELECT 1 AS id, 'z' AS name, 100 AS amount)`, `lake.staging.dummy`, `(SELECT 1 AS id, 'z' AS name, 100 AS amount)`, `staging.dummy`, `{"model":"staging.dummy"}`},
	}

	executeFiles := listEmbedded(t, "execute")
	for _, name := range executeFiles {
		args, ok := templates[name]
		if !ok {
			// Skip files not in the map (SCD2 files have complex positional args)
			continue
		}
		t.Run(name, func(t *testing.T) {
			tmpl, err := Load("execute/" + name)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			sql := fmt.Sprintf(tmpl, args...)
			if _, err := db.ExecContext(context.Background(), sql); err != nil {
				t.Fatalf("Execute execute/%s:\n%s\n\nError: %v", name, sql, err)
			}
		})
	}
}

// TestEmbeddedSQL_AllSchemaTemplatesValid validates schema DDL templates.
func TestEmbeddedSQL_AllSchemaTemplatesValid(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	setupDuckLake(t, db)

	mustExec(t, db, "CREATE TABLE lake.staging.schema_test (id INTEGER, old_col VARCHAR)")

	templates := map[string][]any{
		"alter_add_column.sql":    {`lake.staging.schema_test`, `"new_col"`, `INTEGER`},
		"alter_column_type.sql":   {`lake.staging.schema_test`, `"id"`, `BIGINT`},
		"alter_rename_column.sql": {`lake.staging.schema_test`, `"old_col"`, `"renamed_col"`},
	}

	schemaFiles := listEmbedded(t, "schema")
	for _, name := range schemaFiles {
		args, ok := templates[name]
		if !ok {
			continue
		}
		t.Run(name, func(t *testing.T) {
			tmpl, err := Load("schema/" + name)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			ddl := fmt.Sprintf(tmpl, args...)
			if _, err := db.ExecContext(context.Background(), ddl); err != nil {
				t.Fatalf("Execute schema/%s:\n%s\n\nError: %v", name, ddl, err)
			}
		})
	}
}

// TestEmbeddedSQL_AllQueriesValid validates that query templates produce valid SQL.
func TestEmbeddedSQL_AllQueriesValid(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	setupDuckLake(t, db)

	SetCatalogAlias("lake")

	queryNames := []string{"history", "stats_basic", "stats_kind_breakdown", "stats_all_models", "lineage_all_models"}
	for _, name := range queryNames {
		t.Run(name, func(t *testing.T) {
			content, err := LoadQuery(name)
			if err != nil {
				t.Fatalf("LoadQuery: %v", err)
			}
			rows, err := db.QueryContext(context.Background(), content)
			if err != nil {
				t.Fatalf("Execute queries/%s:\n%s\n\nError: %v", name, content, err)
			}
			rows.Close()
		})
	}
}

// TestEmbeddedSQL_SandboxQueryTemplatesValid validates sandbox diff/sample query templates,
// including the SCD2-specific variants that exclude metadata columns.
func TestEmbeddedSQL_SandboxQueryTemplatesValid(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	setupDuckLake(t, db)

	// Create a second DuckLake catalog as "sandbox" to test cross-catalog queries
	dir := t.TempDir()
	sandboxCatalog := filepath.Join(dir, "sandbox.sqlite")
	sandboxData := filepath.Join(dir, "sandbox_data")
	mustExec(t, db, fmt.Sprintf("ATTACH 'ducklake:sqlite:%s' AS sandbox (DATA_PATH '%s')", sandboxCatalog, sandboxData))
	mustExec(t, db, "CREATE SCHEMA sandbox.staging")

	// Create matching tables in both catalogs
	mustExec(t, db, "CREATE TABLE lake.staging.diff_test (id INTEGER, name VARCHAR, amount INTEGER)")
	mustExec(t, db, "INSERT INTO lake.staging.diff_test VALUES (1, 'Alice', 100), (2, 'Bob', 200)")
	mustExec(t, db, "CREATE TABLE sandbox.staging.diff_test (id INTEGER, name VARCHAR, amount INTEGER)")
	mustExec(t, db, "INSERT INTO sandbox.staging.diff_test VALUES (1, 'Alice', 100), (3, 'Charlie', 300)")

	// Create SCD2 tables with metadata columns
	mustExec(t, db, `CREATE TABLE lake.staging.scd2_diff (
		id INTEGER, name VARCHAR,
		valid_from_snapshot BIGINT, valid_to_snapshot BIGINT, is_current BOOLEAN
	)`)
	mustExec(t, db, "INSERT INTO lake.staging.scd2_diff VALUES (1, 'Alice', 1, 9223372036854775807, true)")
	mustExec(t, db, `CREATE TABLE sandbox.staging.scd2_diff (
		id INTEGER, name VARCHAR,
		valid_from_snapshot BIGINT, valid_to_snapshot BIGINT, is_current BOOLEAN
	)`)
	mustExec(t, db, "INSERT INTO sandbox.staging.scd2_diff VALUES (1, 'Alice', 99, 9223372036854775807, true)")

	// Test generic diff templates
	t.Run("sandbox_diff_count.sql", func(t *testing.T) {
		tmpl, err := Load("queries/sandbox_diff_count.sql")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		q := fmt.Sprintf(tmpl, "sandbox", "lake", "staging.diff_test")
		rows, err := db.QueryContext(context.Background(), q)
		if err != nil {
			t.Fatalf("Execute:\n%s\n\nError: %v", q, err)
		}
		rows.Close()
	})

	t.Run("sandbox_sample.sql", func(t *testing.T) {
		tmpl, err := Load("queries/sandbox_sample.sql")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		q := fmt.Sprintf(tmpl, "sandbox", "lake", "staging.diff_test", "2")
		rows, err := db.QueryContext(context.Background(), q)
		if err != nil {
			t.Fatalf("Execute:\n%s\n\nError: %v", q, err)
		}
		rows.Close()
	})

	t.Run("sandbox_schema_diff.sql", func(t *testing.T) {
		tmpl, err := Load("queries/sandbox_schema_diff.sql")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		q := fmt.Sprintf(tmpl, "sandbox", "lake", "diff_test")
		rows, err := db.QueryContext(context.Background(), q)
		if err != nil {
			t.Fatalf("Execute:\n%s\n\nError: %v", q, err)
		}
		rows.Close()
	})

	// Test SCD2-specific diff templates
	t.Run("sandbox_diff_count_scd2.sql", func(t *testing.T) {
		tmpl, err := Load("queries/sandbox_diff_count_scd2.sql")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		q := fmt.Sprintf(tmpl, "sandbox", "lake", "staging.scd2_diff")
		rows, err := db.QueryContext(context.Background(), q)
		if err != nil {
			t.Fatalf("Execute:\n%s\n\nError: %v", q, err)
		}
		rows.Close()
	})

	t.Run("sandbox_sample_scd2.sql", func(t *testing.T) {
		tmpl, err := Load("queries/sandbox_sample_scd2.sql")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		q := fmt.Sprintf(tmpl, "sandbox", "lake", "staging.scd2_diff", "2")
		rows, err := db.QueryContext(context.Background(), q)
		if err != nil {
			t.Fatalf("Execute:\n%s\n\nError: %v", q, err)
		}
		rows.Close()
	})

	// Verify SCD2 diff correctly ignores metadata columns:
	// Both tables have same (id, name) but different valid_from_snapshot.
	// SCD2 diff should report 0 differences.
	t.Run("scd2_metadata_excluded", func(t *testing.T) {
		tmpl, err := Load("queries/sandbox_diff_count_scd2.sql")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		q := fmt.Sprintf(tmpl, "sandbox", "lake", "staging.scd2_diff")
		var count int
		if err := db.QueryRowContext(context.Background(), q).Scan(&count); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if count != 0 {
			t.Errorf("SCD2 diff count = %d, want 0 (metadata-only difference should be excluded)", count)
		}
	})

	// Verify SCD2 diff detects real data changes (not just metadata).
	t.Run("scd2_data_change_detected", func(t *testing.T) {
		// Change actual data in sandbox (name column), not just metadata
		mustExec(t, db, "DELETE FROM sandbox.staging.scd2_diff")
		mustExec(t, db, "INSERT INTO sandbox.staging.scd2_diff VALUES (1, 'Alice Updated', 99, 9223372036854775807, true)")

		tmpl, err := Load("queries/sandbox_diff_count_scd2.sql")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		q := fmt.Sprintf(tmpl, "sandbox", "lake", "staging.scd2_diff")
		var count int
		if err := db.QueryRowContext(context.Background(), q).Scan(&count); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if count == 0 {
			t.Error("SCD2 diff count = 0, want > 0 for real data change (name: Alice → Alice Updated)")
		}

		// Restore original data for other tests
		mustExec(t, db, "DELETE FROM sandbox.staging.scd2_diff")
		mustExec(t, db, "INSERT INTO sandbox.staging.scd2_diff VALUES (1, 'Alice', 99, 9223372036854775807, true)")
	})

	// Verify generic diff detects known differences.
	t.Run("generic_diff_detects_changes", func(t *testing.T) {
		tmpl, err := Load("queries/sandbox_diff_count.sql")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		// sandbox has (1,Alice,100),(3,Charlie,300) vs lake has (1,Alice,100),(2,Bob,200)
		// sandbox→lake diff (rows in sandbox not in lake) should be > 0
		q := fmt.Sprintf(tmpl, "sandbox", "lake", "staging.diff_test")
		var count int
		if err := db.QueryRowContext(context.Background(), q).Scan(&count); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if count != 1 {
			t.Errorf("diff count (sandbox-lake) = %d, want 1 (Charlie row)", count)
		}

		// Reverse: rows in lake not in sandbox
		q = fmt.Sprintf(tmpl, "lake", "sandbox", "staging.diff_test")
		if err := db.QueryRowContext(context.Background(), q).Scan(&count); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if count != 1 {
			t.Errorf("diff count (lake-sandbox) = %d, want 1 (Bob row)", count)
		}
	})

	// Verify information_schema table-existence check (used by showDagSandboxSummary).
	t.Run("information_schema_table_exists", func(t *testing.T) {
		// Table that exists in sandbox
		var count int
		err := db.QueryRowContext(context.Background(),
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_catalog = 'sandbox' AND table_schema = 'staging' AND table_name = 'diff_test'",
		).Scan(&count)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if count != 1 {
			t.Errorf("existing table count = %d, want 1", count)
		}

		// Table that does NOT exist in sandbox
		err = db.QueryRowContext(context.Background(),
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_catalog = 'sandbox' AND table_schema = 'staging' AND table_name = 'nonexistent'",
		).Scan(&count)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if count != 0 {
			t.Errorf("nonexistent table count = %d, want 0", count)
		}
	})

	// Verify identical tables produce zero diff.
	t.Run("identical_tables_zero_diff", func(t *testing.T) {
		mustExec(t, db, "CREATE TABLE lake.staging.identical (id INTEGER, val VARCHAR)")
		mustExec(t, db, "INSERT INTO lake.staging.identical VALUES (1, 'a'), (2, 'b')")
		mustExec(t, db, "CREATE TABLE sandbox.staging.identical (id INTEGER, val VARCHAR)")
		mustExec(t, db, "INSERT INTO sandbox.staging.identical VALUES (1, 'a'), (2, 'b')")

		tmpl, err := Load("queries/sandbox_diff_count.sql")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		q := fmt.Sprintf(tmpl, "sandbox", "lake", "staging.identical")
		var count int
		if err := db.QueryRowContext(context.Background(), q).Scan(&count); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if count != 0 {
			t.Errorf("identical table diff = %d, want 0", count)
		}
	})
}

// TestEmbeddedSQL_SCD2TemplatesValid validates the complex SCD2 templates.
func TestEmbeddedSQL_SCD2TemplatesValid(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	setupDuckLake(t, db)

	// SCD2 requires a target table with SCD2 columns and a tmp table
	mustExec(t, db, `CREATE TABLE lake.staging.scd2_tgt (
		id INTEGER, name VARCHAR,
		valid_from_snapshot BIGINT, valid_to_snapshot BIGINT, is_current BOOLEAN
	)`)
	mustExec(t, db, `INSERT INTO lake.staging.scd2_tgt VALUES
		(1, 'Alice', 1, 9223372036854775807, true),
		(2, 'Bob', 1, 9223372036854775807, true)`)

	mustExec(t, db, `CREATE TEMP TABLE __ondatra_tmp_scd2 AS
		SELECT 1 AS id, 'Alice Updated' AS name
		UNION ALL SELECT 3, 'Charlie'`)

	t.Run("scd2_init.sql", func(t *testing.T) {
		content, err := Load("execute/scd2_init.sql")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		// scd2_init uses positional %[N]s format
		// It creates the SCD2 target table from a tmp table
		mustExec(t, db, "DROP TABLE lake.staging.scd2_tgt")
		sql := fmt.Sprintf(content,
			`lake.staging.scd2_tgt`,     // %[1]s target
			`"id", "name"`,              // %[2]s col_list
			1,                           // %[3]s snapshot
			`__ondatra_tmp_scd2`,        // %[4]s tmp_table
		)
		if _, err := db.ExecContext(context.Background(), sql); err != nil {
			t.Fatalf("Execute:\n%s\n\nError: %v", sql, err)
		}
	})

	// Recreate for remaining tests
	mustExec(t, db, `DROP TABLE IF EXISTS lake.staging.scd2_tgt`)
	mustExec(t, db, `CREATE TABLE lake.staging.scd2_tgt (
		id INTEGER, name VARCHAR,
		valid_from_snapshot BIGINT, valid_to_snapshot BIGINT, is_current BOOLEAN
	)`)
	mustExec(t, db, `INSERT INTO lake.staging.scd2_tgt VALUES
		(1, 'Alice', 1, 9223372036854775807, true),
		(2, 'Bob', 1, 9223372036854775807, true)`)

	t.Run("scd2_detect.sql", func(t *testing.T) {
		content, err := Load("execute/scd2_detect.sql")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		sql := fmt.Sprintf(content,
			`lake.staging.scd2_tgt`,   // target
			`__ondatra_tmp_scd2`,      // tmp_table (changes)
			`"id"`,                    // unique_key
			`"id"`,                    // unique_key
			`__ondatra_tmp_scd2`,      // tmp_table (changes WHERE)
			`"id"`,                    // unique_key
			`"id"`,                    // unique_key
			`1=1`,                     // change_where
			`"id"`,                    // unique_key (deleted)
			`lake.staging.scd2_tgt`,   // target (deleted)
			`"id"`,                    // unique_key
			`"id"`,                    // unique_key
			`__ondatra_tmp_scd2`,      // tmp_table (deleted)
		)
		if _, err := db.ExecContext(context.Background(), sql); err != nil {
			t.Fatalf("Execute:\n%s\n\nError: %v", sql, err)
		}
	})
}

// --- helpers ---

func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })

	// Load DuckLake extension
	mustExec(t, db, "INSTALL ducklake")
	mustExec(t, db, "LOAD ducklake")
	mustExec(t, db, "LOAD sqlite")
	mustExec(t, db, "SET enable_progress_bar = false")

	return db
}

func setupDuckLake(t *testing.T, db *sql.DB) {
	t.Helper()
	dir := t.TempDir()
	catalogPath := filepath.Join(dir, "test.sqlite")
	dataPath := filepath.Join(dir, "data")
	attachSQL := fmt.Sprintf("ATTACH 'ducklake:sqlite:%s' AS lake (DATA_PATH '%s')", catalogPath, dataPath)
	mustExec(t, db, attachSQL)
	mustExec(t, db, "CREATE SCHEMA lake.staging")
	mustExec(t, db, "CREATE TABLE IF NOT EXISTS lake._ondatra_registry (target VARCHAR, kind VARCHAR, updated_at TIMESTAMP)")
	mustExec(t, db, "USE lake")
	SetCatalogAlias("lake")
}

func mustExec(t *testing.T, db *sql.DB, sql string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func listEmbedded(t *testing.T, dir string) []string {
	t.Helper()
	var files []string
	fs.WalkDir(sqlFiles, dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, ".sql") {
			files = append(files, d.Name())
		}
		return nil
	})
	return files
}
