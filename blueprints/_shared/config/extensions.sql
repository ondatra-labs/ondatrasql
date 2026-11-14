-- extensions.sql - Global DuckDB extensions
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
