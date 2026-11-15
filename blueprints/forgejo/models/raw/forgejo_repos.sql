-- @kind: merge
-- @unique_key: id
-- @fetch
-- @constraint: not_null(full_name)
-- One row per repository of the owner. Every run reads the whole list (one
-- request per 50 repositories) and the merge on id updates the rows in place.
SELECT
    id::BIGINT                  AS id,
    full_name::VARCHAR          AS full_name,
    owner::VARCHAR              AS owner,
    name::VARCHAR               AS name,
    description::VARCHAR        AS description,
    private::BOOLEAN            AS private,
    fork::BOOLEAN               AS fork,
    mirror::BOOLEAN             AS mirror,
    archived::BOOLEAN           AS archived,
    empty::BOOLEAN              AS empty,
    default_branch::VARCHAR     AS default_branch,
    language::VARCHAR           AS language,
    stars_count::BIGINT         AS stars_count,
    forks_count::BIGINT         AS forks_count,
    open_issues_count::BIGINT   AS open_issues_count,
    open_pr_counter::BIGINT     AS open_pr_counter,
    has_pull_requests::BOOLEAN  AS has_pull_requests,
    html_url::VARCHAR           AS html_url,
    created_at::TIMESTAMPTZ     AS created_at,
    updated_at::TIMESTAMPTZ     AS updated_at
FROM forgejo('repos', 'forgejo', 'https://codeberg.org')
