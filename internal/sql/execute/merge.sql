-- MERGE INTO for upsert operations
-- Args: target, source, quoted key, quoted key, SET clause
--
-- NOTE: Uses IS NOT DISTINCT FROM (not =) so that NULL = NULL is TRUE for the
-- merge match. With plain `=`, NULL keys would never match each other and the
-- merge would silently duplicate NULL-keyed rows on every run (Bug 22). Users
-- who want to enforce no-NULL keys should add @constraint: <key> NOT NULL.
MERGE INTO %s AS target
USING %s AS source
ON (target.%s IS NOT DISTINCT FROM source.%s)
WHEN MATCHED THEN UPDATE %s
WHEN NOT MATCHED THEN INSERT *
