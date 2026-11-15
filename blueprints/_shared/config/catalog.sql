-- catalog.sql - DuckLake catalog attachment
-- This file defines how the DuckLake catalog is attached.
-- The catalog stores table metadata; DATA_PATH stores Parquet files.

--------------------------------------------------------------------------------
-- LOCAL STORAGE (default)
--------------------------------------------------------------------------------

-- SQLite catalog with local Parquet files
-- A new catalog gets the current DuckLake format, which DuckDB 1.5 cannot
-- read. If 1.5 tools must read it, add DUCKLAKE_VERSION '1.0' to the options
-- before the first run. Never add AUTOMATIC_MIGRATION: it upgrades a catalog
-- in place, one way.
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
