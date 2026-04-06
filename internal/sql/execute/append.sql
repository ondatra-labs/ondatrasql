-- Append rows to existing table
-- DDL requires literal table names - use sql.MustFormat()
INSERT INTO %s BY NAME SELECT * FROM %s
