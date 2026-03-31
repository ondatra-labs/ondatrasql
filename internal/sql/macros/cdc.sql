-- cdc.sql - CDC (Change Data Capture) macros
-- These macros provide access to snapshot versioning for incremental processing.
-- Must be created in memory context before USE <catalog>.

-- Current snapshot ID (after any writes in this session)
CREATE OR REPLACE MACRO last_snapshot() AS getvariable('curr_snapshot');

-- Previous snapshot ID (before this session)
CREATE OR REPLACE MACRO prev_snapshot() AS getvariable('prev_snapshot');

-- Snapshot ID at start of DAG execution (for multi-model runs)
CREATE OR REPLACE MACRO dag_start_snapshot() AS getvariable('dag_start_snapshot');
