-- Delete matching partitions and insert new data.
--
-- Args (in order):
--   1. target table             (DELETE FROM)
--   2. partition column list    (DELETE WHERE LHS)
--   3. partition column list    (DELETE WHERE SELECT DISTINCT)
--   4. temp table               (DELETE WHERE source)
--   5. target table             (INSERT INTO)
--   6. temp table               (INSERT SELECT source)
--   7. pre-commit checks        (audit error() wrapper or empty)
--   8. model target             (commit message)
--   9. extra info JSON          (escaped commit metadata)
--
-- Audits run inside the same transaction as the DELETE+INSERT, so a
-- failing audit aborts the partition rewrite and the commit metadata
-- atomically — same guarantee as commit.sql for the other kinds.
BEGIN;
-- Delete matching partitions
DELETE FROM %s
WHERE (%s) IN (SELECT DISTINCT %s FROM %s);

-- Insert new rows
INSERT INTO %s BY NAME SELECT * FROM %s;

-- Pre-commit checks (audits raise error() and abort the transaction
-- if any of them fail). Empty when the model has no audits.
%s;

CALL set_commit_message('ondatrasql', 'Pipeline run: %s', extra_info => '%s');
COMMIT
