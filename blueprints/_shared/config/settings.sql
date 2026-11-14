-- settings.sql - DuckDB configuration settings
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
