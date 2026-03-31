-- Materialize a table model (CREATE OR REPLACE)
-- DDL requires literal table names - use sql.MustFormat()
CREATE OR REPLACE TABLE %s AS SELECT * FROM %s
