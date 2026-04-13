-- Transaction wrapper with commit metadata.
--
-- Args (in order):
--   1. main SQL          — the data write (ALTER + TRUNCATE/INSERT/MERGE etc.)
--   2. pre-commit checks — audits wrapped in SELECT error(...) so any
--                          violation aborts the whole transaction. Empty
--                          string when the model has no audits.
--   3. model target      — used in the human-readable commit message
--   4. extra info JSON   — escaped commit metadata blob
--
-- Audits run INSIDE the transaction, after the data write but before
-- set_commit_message + COMMIT, so a failing audit rolls back the ALTER,
-- the data write, and the commit metadata atomically. This is what
-- prevents the metadata-vs-physical schema divergence that the older
-- "commit, then audit, then call rollback()" pattern could leave behind
-- when an audit failed.
BEGIN;
%s;
%s;
CALL set_commit_message('ondatrasql', 'Pipeline run: %s', extra_info => '%s');
COMMIT
