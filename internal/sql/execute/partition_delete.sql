-- Delete matching partitions and insert new data
-- Args: target, partition cols, partition cols, temp table, target, temp table, model target, extra info
BEGIN;
-- Delete matching partitions
DELETE FROM %s
WHERE (%s) IN (SELECT DISTINCT %s FROM %s);

-- Insert new rows
INSERT INTO %s SELECT * FROM %s;

CALL ducklake_set_commit_message('{{catalog}}', 'ondatrasql', 'Pipeline run: %s', extra_info => '%s');
COMMIT
