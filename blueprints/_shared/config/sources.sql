-- sources.sql - External data sources
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
