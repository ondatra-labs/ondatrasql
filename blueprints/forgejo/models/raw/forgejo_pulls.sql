-- @kind: merge
-- @unique_key: id
-- @fetch
-- @incremental: updated_at
-- @incremental_initial: 2026-11-08T00:00:00Z
-- @constraint: not_null(repo)
-- One row per pull request in the owner's repositories. A run fetches the
-- pull requests updated since the newest updated_at it has (minus an hour),
-- and the merge on id replaces their older versions.
SELECT
    id::BIGINT              AS id,
    repo::VARCHAR           AS repo,
    number::BIGINT          AS number,
    title::VARCHAR          AS title,
    state::VARCHAR          AS state,
    draft::BOOLEAN          AS draft,
    author::VARCHAR         AS author,
    base_ref::VARCHAR       AS base_ref,
    head_ref::VARCHAR       AS head_ref,
    merged::BOOLEAN         AS merged,
    merged_by::VARCHAR      AS merged_by,
    comments::BIGINT        AS comments,
    html_url::VARCHAR       AS html_url,
    created_at::TIMESTAMPTZ AS created_at,
    updated_at::TIMESTAMPTZ AS updated_at,
    closed_at::TIMESTAMPTZ  AS closed_at,
    merged_at::TIMESTAMPTZ  AS merged_at
FROM forgejo('pulls', 'forgejo', 'https://codeberg.org')
