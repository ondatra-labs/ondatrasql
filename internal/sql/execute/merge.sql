-- MERGE INTO for upsert operations
-- Args: target, source, quoted key, quoted key, SET clause
MERGE INTO %s AS target
USING %s AS source
ON (target.%s = source.%s)
WHEN MATCHED THEN UPDATE %s
WHEN NOT MATCHED THEN INSERT *
