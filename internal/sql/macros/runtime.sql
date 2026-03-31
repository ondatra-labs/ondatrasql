-- runtime.sql - Runtime macros for consistent timestamps and IDs
-- These macros provide deterministic values within a session.
-- Must be created in memory context before USE <catalog>.

-- Returns the timestamp when this run started (consistent across all queries)
CREATE OR REPLACE MACRO ondatra_now() AS getvariable('ondatra_run_time');

-- Returns a unique load ID for this run
CREATE OR REPLACE MACRO ondatra_load_id() AS getvariable('ondatra_load_id');
