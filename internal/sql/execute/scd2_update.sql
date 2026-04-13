-- SCD2: Close old versions and insert new versions.
--
-- Args (in order):
--   1. target_escaped (registry DELETE)
--   2. target_escaped (registry INSERT)
--   3. target          (close changed)
--   4. snapshot        (close changed valid_to)
--   5. unique_key      (close changed WHERE LHS)
--   6. unique_key      (close changed SELECT)
--   7. target          (close deleted)
--   8. snapshot        (close deleted valid_to)
--   9. unique_key      (close deleted WHERE LHS)
--  10. unique_key      (close deleted SELECT)
--  11. target          (insert new)
--  12. col_list        (insert column list)
--  13. col_list        (insert SELECT list)
--  14. snapshot        (insert valid_from)
--  15. pre-commit checks (audit error() wrapper or empty)
--  16. target          (commit message)
--  17. extra info JSON (escaped)
--
-- Audits run inside the same transaction as the SCD2 update so a
-- failing audit aborts the close+insert atomically — same guarantee
-- as commit.sql for the other kinds.
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

-- Pre-commit checks (audits raise error() and abort the transaction
-- if any of them fail). Empty when the model has no audits.
%s;

CALL set_commit_message('ondatrasql', 'Pipeline run: %s', extra_info => '%s');
COMMIT;
