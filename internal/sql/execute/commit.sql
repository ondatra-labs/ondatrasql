-- Transaction wrapper with commit metadata
-- Args: main SQL, model target, extra info JSON (escaped)
BEGIN;
%s;
CALL {{catalog}}.set_commit_message('ondatrasql', 'Pipeline run: %s', extra_info => '%s');
COMMIT
