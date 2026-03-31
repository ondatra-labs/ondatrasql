-- Transaction wrapper with commit metadata
-- Args: main SQL, model target, extra info JSON (escaped)
BEGIN;
%s;
CALL ducklake_set_commit_message('{{catalog}}', 'ondatrasql', 'Pipeline run: %s', extra_info => '%s');
COMMIT
