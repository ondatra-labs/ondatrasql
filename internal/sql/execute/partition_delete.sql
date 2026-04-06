-- Delete matching partitions and insert new data
-- Args: target, partition cols, partition cols, temp table, target, temp table, model target, extra info
BEGIN;
-- Delete matching partitions
DELETE FROM %s
WHERE (%s) IN (SELECT DISTINCT %s FROM %s);

-- Insert new rows
INSERT INTO %s BY NAME SELECT * FROM %s;

CALL {{catalog}}.set_commit_message('ondatrasql', 'Pipeline run: %s', extra_info => '%s');
COMMIT
