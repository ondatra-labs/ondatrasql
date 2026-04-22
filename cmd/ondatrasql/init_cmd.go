// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/ondatra-labs/ondatrasql/internal/output"
	sqlfiles "github.com/ondatra-labs/ondatrasql/internal/sql"
)

func runInit() error {
	// Initialize in the current directory
	dir := "."

	// Check if config/ directory already exists (already a project)
	if info, err := os.Stat(filepath.Join(dir, "config")); err == nil && info.IsDir() {
		return fmt.Errorf("config/ directory already exists (already an ondatrasql project)")
	}

	// Use current directory name as project name
	abs, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("resolve directory: %w", err)
	}
	name := filepath.Base(abs)

	// Create directory structure
	dirs := []string{
		"config",
		"lib",
		"models/raw",
		"models/staging",
		"models/intermediate",
		"models/mart",
		"models/sync",
		"config/macros",
		"config/variables",
		"sql",
	}
	for _, d := range dirs {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
			return fmt.Errorf("create directory %s: %w", d, err)
		}
	}

	// Write all template files
	files := map[string]string{
		".env":                  initDotEnv(),
		".gitignore":            initGitignore(),
		"config/catalog.sql":    initCatalog(),
		"config/extensions.sql": initExtensions(),
		"config/macros/helpers.sql":     initMacroFile("macros_helpers.sql"),
		"config/macros/masking.sql":     initMacroFile("macros_masking.sql"),
		"config/macros/constraints.sql": initMacroFile("macros_constraint.sql"),
		"config/macros/audits.sql":      initMacroFile("macros_audit.sql"),
		"config/macros/warnings.sql":    initMacroFile("macros_warning.sql"),
		"config/variables/constants.sql": initMacroFile("variables_constants.sql"),
		"config/variables/global.sql":   initMacroFile("variables_global.sql"),
		"config/variables/local.sql":    initMacroFile("variables_models.sql"),
		"config/sources.sql":    initSources(),
		"config/secrets.sql":    initSecrets(),
		"config/settings.sql":   initSettings(),
		"sql/flush.sql":         initFlush(),
		"sql/merge.sql":         initMerge(),
		"sql/expire.sql":        initExpire(),
		"sql/cleanup.sql":       initCleanup(),
		"sql/orphaned.sql":      initOrphaned(),
		"sql/rewrite.sql":       initRewrite(),
		"sql/checkpoint.sql":    initCheckpoint(),
		"README.md":             initReadmeMD(name),
	}

	for path, content := range files {
		fullPath := filepath.Join(dir, path)
		// Don't overwrite existing files
		if _, err := os.Stat(fullPath); err == nil {
			continue
		}
		if err := os.WriteFile(fullPath, []byte(content), 0o644); err != nil {
			return fmt.Errorf("write %s: %w", path, err)
		}
	}

	// Create .gitkeep files in empty model directories
	for _, d := range []string{"lib", "models/raw", "models/staging", "models/intermediate", "models/mart", "models/sync"} {
		gitkeep := filepath.Join(dir, d, ".gitkeep")
		if err := os.WriteFile(gitkeep, nil, 0o644); err != nil {
			return fmt.Errorf("write %s/.gitkeep: %w", d, err)
		}
	}

	output.Fprintf("Initialized project %q\n", name)
	output.Println("\nNext steps:")
	output.Println("  ondatrasql stats")

	return nil
}

func initDotEnv() string {
	return `# OndatraSQL Environment Variables
# ==================================

# === AWS (required for S3 storage) ===
# AWS_ACCESS_KEY_ID=
# AWS_SECRET_ACCESS_KEY=
# AWS_REGION=eu-north-1
`
}

func initGitignore() string {
	return `# Secrets - DO NOT COMMIT
.env
secrets.sql

# Binaries
ondatrasql
*.exe

# Databases
*.ducklake
*.duckdb
ducklake.db
ducklake.sqlite
ducklake.sqlite.files/
ondatrasql.db

# IDE
.idea/
.vscode/

# Data files
data/

# Temp files
*.log
.sandbox/

# Ondatra runtime state — but commit .ondatra/project-id (stable schedule identity)
.ondatra/*
!.ondatra/project-id
`
}

func initCatalog() string {
	return `-- catalog.sql - DuckLake catalog attachment
-- This file defines how the DuckLake catalog is attached.
-- The catalog stores table metadata; DATA_PATH stores Parquet files.

--------------------------------------------------------------------------------
-- LOCAL STORAGE (default)
--------------------------------------------------------------------------------

-- SQLite catalog with local Parquet files
ATTACH 'ducklake:sqlite:ducklake.sqlite' AS lake (DATA_PATH 'ducklake.sqlite.files');

-- DuckLake options (uncomment to customize)
-- CALL lake.set_option('parquet_compression', 'zstd');          -- default: snappy
-- CALL lake.set_option('target_file_size', '256MB');             -- default: 512MB
-- CALL lake.set_option('rewrite_delete_threshold', 0.5);        -- default: 0.95
-- CALL lake.set_option('write_deletion_vectors', true);          -- default: false

--------------------------------------------------------------------------------
-- CLOUD STORAGE (S3)
--------------------------------------------------------------------------------

-- Example: SQLite catalog with S3 data storage
-- Requires: extensions.sql to load httpfs
-- ATTACH 'ducklake:sqlite:ducklake.sqlite' AS lake (DATA_PATH 's3://my-bucket/data/');
-- USE lake;

--------------------------------------------------------------------------------
-- DUCKDB CATALOG
--------------------------------------------------------------------------------

-- Example: DuckDB file as catalog backend (alternative to SQLite)
-- ATTACH 'ducklake:duckdb:ducklake_catalog.duckdb' AS lake (DATA_PATH 'ducklake.duckdb.files');
-- USE lake;

--------------------------------------------------------------------------------
-- MULTI-USER (PostgreSQL catalog)
--------------------------------------------------------------------------------

-- Example: PostgreSQL catalog for multi-user access
-- Requires: extensions.sql to load postgres, secrets.sql for credentials
-- ATTACH 'ducklake:postgres:dbname=ducklake_catalog host=localhost' AS lake (DATA_PATH 's3://my-bucket/data/');
-- USE lake;

--------------------------------------------------------------------------------
-- MULTI-USER (MySQL catalog)
--------------------------------------------------------------------------------

-- Example: MySQL catalog for multi-user access
-- Requires: extensions.sql to load mysql, secrets.sql for credentials
-- ATTACH 'ducklake:mysql:db=ducklake_catalog host=localhost' AS lake (DATA_PATH 's3://my-bucket/data/');
-- USE lake;
`
}

func initExtensions() string {
	return `-- extensions.sql - Global DuckDB extensions
-- Runs BEFORE DuckLake catalog is attached.
-- Use for: extensions needed by catalog.sql or globally by all models.
-- Model-specific extensions should use @extension directive instead.

--------------------------------------------------------------------------------
-- CLOUD STORAGE (required if DATA_PATH uses s3://)
--------------------------------------------------------------------------------

-- Example: Enable S3 access
-- INSTALL httpfs;
-- LOAD httpfs;

--------------------------------------------------------------------------------
-- EXTERNAL DATABASES (required if sources.sql uses ATTACH)
--------------------------------------------------------------------------------

-- Example: PostgreSQL connector
-- INSTALL postgres;
-- LOAD postgres;

-- Example: MySQL connector
-- INSTALL mysql;
-- LOAD mysql;
`
}

func initMacroFile(name string) string {
	content, err := sqlfiles.Load("init/" + name)
	if err != nil {
		// Embedded file missing — return empty with comment so user knows
		return fmt.Sprintf("-- WARNING: embedded template %q not found\n", name)
	}
	return content
}



func initSources() string {
	return `-- sources.sql - External data sources
-- Runs AFTER DuckLake catalog is attached.
-- Use for: attaching external databases, creating views on external data.

--------------------------------------------------------------------------------
-- EXTERNAL DATABASES
--------------------------------------------------------------------------------

-- Example: Attach PostgreSQL (read-only)
-- Requires: extensions.sql to load postgres, secrets.sql for credentials
-- ATTACH 'postgresql://user:pass@host:5432/warehouse' AS warehouse (READ_ONLY);

--------------------------------------------------------------------------------
-- EXTERNAL FILES / OBJECT STORAGE
--------------------------------------------------------------------------------

-- Example: View on S3 Parquet files
-- CREATE VIEW raw.external_events AS
--     SELECT * FROM read_parquet('s3://data-lake/events/*.parquet');

-- Example: CSV files
-- CREATE VIEW raw.daily_import AS
--     SELECT * FROM read_csv('https://example.com/exports/daily.csv');
`
}

func initSecrets() string {
	return `-- secrets.sql - Credentials and secrets
-- Runs BEFORE DuckLake catalog is attached.
-- Use for: S3 credentials, database passwords, API keys.
-- WARNING: Do not commit this file to version control. Use environment variables.

--------------------------------------------------------------------------------
-- EXAMPLES (uncomment to use)
--------------------------------------------------------------------------------

-- Example: S3 credentials
-- CREATE SECRET s3_secret (
--     TYPE s3,
--     KEY_ID 'AKIA...',
--     SECRET '...',
--     REGION 'eu-north-1'
-- );

-- Example: Use AWS credential chain (recommended for production)
-- CREATE SECRET aws_chain (
--     TYPE s3,
--     PROVIDER credential_chain
-- );

-- Example: PostgreSQL credentials
-- CREATE SECRET pg_secret (
--     TYPE postgres,
--     HOST 'localhost',
--     PORT 5432,
--     DATABASE 'warehouse',
--     USER 'readonly',
--     PASSWORD '...'
-- );
`
}

func initSettings() string {
	return `-- settings.sql - DuckDB configuration settings
-- Runs BEFORE DuckLake catalog is attached.
-- Use for: memory limits, thread count, temp directory, performance tuning.

--------------------------------------------------------------------------------
-- EXAMPLES (uncomment to use)
--------------------------------------------------------------------------------

-- Example: Memory and thread limits
-- SET memory_limit = '8GB';
-- SET threads = 4;

-- Example: Temporary directory for spilling
-- SET temp_directory = '/tmp/duckdb';
`
}

func initFlush() string {
	return `-- flush.sql - Flush inlined data to Parquet files
-- Run with: ondatrasql flush
--
-- Moves small writes stored in the catalog database to Parquet files.
-- DuckLake inlines small inserts (up to 10 rows) in the catalog metadata.
-- This command flushes all inlined data to object storage.
-- Run before merge for best results.

CALL ducklake_flush_inlined_data('lake');
`
}

func initMerge() string {
	return `-- merge.sql - Merge small files for better performance
-- Run with: ondatrasql merge
-- Preview with: ondatrasql merge sandbox
--
-- Merge small adjacent files into larger ones for better query performance.
-- Preserves time travel and change data feed functionality.
-- Run periodically on tables with many small incremental loads.
-- Run BEFORE expire for best results.

CALL ducklake_merge_adjacent_files('lake');
`
}

func initExpire() string {
	return `-- expire.sql - Expire old snapshots
-- Run with: ondatrasql expire
-- Preview with: ondatrasql expire sandbox
--
-- Expire old snapshots (removes metadata).
-- Default: 30 days retention.

CALL ducklake_expire_snapshots('lake', older_than => now() - INTERVAL '30 days');
`
}

func initCleanup() string {
	return `-- cleanup.sql - Delete old files from expired snapshots
-- Run with: ondatrasql cleanup
-- Preview with: ondatrasql cleanup sandbox
--
-- Delete data files from expired snapshots.
-- Only removes files that are no longer referenced by any snapshot.
-- Run AFTER expire.

CALL ducklake_cleanup_old_files('lake', older_than => now() - INTERVAL '7 days');
`
}

func initOrphaned() string {
	return `-- orphaned.sql - Delete orphaned files
-- Run with: ondatrasql orphaned
-- Preview with: ondatrasql orphaned sandbox
--
-- Delete orphaned files (files not referenced by any snapshot).
-- Useful after failed writes or interrupted operations.

CALL ducklake_delete_orphaned_files('lake', older_than => now() - INTERVAL '7 days');
`
}

func initRewrite() string {
	return `-- rewrite.sql - Rewrite data files with many deletes
-- Run with: ondatrasql rewrite
-- Preview with: ondatrasql rewrite sandbox
--
-- Rewrites Parquet files that contain a high proportion of deleted rows.
-- Improves read performance by physically removing deleted data.
-- Default threshold: 95% (rewrite if >95% of rows in a file are deleted).
-- Run periodically on tables with frequent updates/deletes.

CALL ducklake_rewrite_data_files('lake');
`
}

func initCheckpoint() string {
	return `-- checkpoint.sql - Run all maintenance in order
-- Run with: ondatrasql checkpoint
-- Preview with: ondatrasql checkpoint sandbox
--
-- Runs all maintenance operations in the correct order:
-- 1. Flush inlined data to Parquet files
-- 2. Expire old snapshots (30 days)
-- 3. Merge small adjacent files
-- 4. Rewrite files with many deletes
-- 5. Clean up old files (7 days)
-- 6. Delete orphaned files (7 days)

CALL ducklake_flush_inlined_data('lake');
CALL ducklake_expire_snapshots('lake', older_than => now() - INTERVAL '30 days');
CALL ducklake_merge_adjacent_files('lake');
CALL ducklake_rewrite_data_files('lake');
CALL ducklake_cleanup_old_files('lake', older_than => now() - INTERVAL '7 days');
CALL ducklake_delete_orphaned_files('lake', older_than => now() - INTERVAL '7 days');
`
}

func initReadmeMD(name string) string {
	return `# ` + name + `

Data pipeline project powered by [OndatraSQL](https://github.com/ondatralabs/ondatrasql) — a dbt-like framework for DuckDB and DuckLake.

## Getting Started

1. **Configure your catalog** — edit ` + "`config/catalog.sql`" + ` (default: local SQLite + Parquet)
2. **Add models** — create SQL files in ` + "`models/`" + ` (path determines schema.table)
3. **Run** — execute your pipeline

` + "```bash" + `
# Create a new model
ondatrasql new staging.customers.sql

# Preview changes in sandbox
ondatrasql sandbox

# Run the pipeline
ondatrasql run

# Check project status
ondatrasql stats
` + "```" + `

## Project Structure

| Directory | Purpose |
|---|---|
| ` + "`config/`" + ` | SQL config files (catalog, schemas, extensions, macros, variables, sources) |
| ` + "`lib/`" + ` | Shared Starlark libraries (API connectors via blueprint API dict) |
| ` + "`models/`" + ` | SQL model files organized by schema layer |
| ` + "`sql/`" + ` | Executable SQL files for maintenance (merge, expire, cleanup) |

See the [OndatraSQL documentation](https://github.com/ondatralabs/ondatrasql) for details on model directives, incremental loading, Starlark scripting, and CLI commands.
`
}
