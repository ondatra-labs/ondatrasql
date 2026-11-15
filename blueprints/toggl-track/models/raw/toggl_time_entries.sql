-- @kind: merge
-- @unique_key: id
-- @fetch
-- @incremental: start
-- @incremental_initial: 2026-01-01
-- @constraint: not_null(start)
-- One row per time entry. Merge on the entry id, so an entry fetched again
-- (every run fetches last month and this month) replaces itself.
-- No personal data: user names and emails are left out (see index.md).
-- Without a workspace id, TOGGL_WORKSPACE_ID names it.
SELECT
    id::BIGINT             AS id,
    date::DATE             AS date,
    start::TIMESTAMPTZ     AS start,
    stop::TIMESTAMPTZ      AS stop,
    seconds::BIGINT        AS seconds,
    user_id::BIGINT        AS user_id,
    project_id::BIGINT     AS project_id,
    project_name::VARCHAR  AS project_name,
    client_name::VARCHAR   AS client_name,
    task_id::BIGINT        AS task_id,
    description::VARCHAR   AS description,
    billable::BOOLEAN      AS billable,
    tag_ids::BIGINT[]      AS tag_ids,
    "at"::TIMESTAMPTZ      AS updated_at
FROM toggl_track('')
