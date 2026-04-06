-- SCD2: Close old versions and insert new versions
-- Args: target_escaped, target_escaped,
--       target, snapshot, unique_key, unique_key,
--       target, snapshot, unique_key, unique_key,
--       target, col_list, col_list, snapshot,
--       target, extra_info
BEGIN;
-- Registry upsert (ensures snapshot is created even with 0 data changes)
DELETE FROM _ondatra_registry WHERE target = '%s';
INSERT INTO _ondatra_registry VALUES ('%s', 'scd2', current_timestamp);
-- Close old versions for changed rows
UPDATE %s SET valid_to_snapshot = %d, is_current = false
WHERE is_current IS true AND %s IN (SELECT %s FROM scd2_changes WHERE _change_type = 'changed');

-- Close versions for deleted rows
UPDATE %s SET valid_to_snapshot = %d, is_current = false
WHERE is_current IS true AND %s IN (SELECT %s FROM scd2_deleted);

-- Insert new versions
INSERT INTO %s (%s, valid_from_snapshot, valid_to_snapshot, is_current)
SELECT %s, %d, NULL, true FROM scd2_changes;

CALL {{catalog}}.set_commit_message('ondatrasql', 'Pipeline run: %s', extra_info => '%s');
COMMIT;
