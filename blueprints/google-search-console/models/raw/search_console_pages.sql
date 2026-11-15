-- @kind: merge
-- @unique_key: row_key
-- @fetch
-- @incremental: date
-- @constraint: not_null(date)
-- One row per day and page (the canonical URL Google showed). With page as
-- a dimension Google aggregates by page, so the totals differ from
-- search_console_daily, which aggregates by property. Merge on row_key
-- (date and page); the lib fetches again from 3 days before the newest day
-- loaded.
SELECT
    row_key::VARCHAR      AS row_key,
    site::VARCHAR         AS site,
    date::DATE            AS date,
    page::VARCHAR         AS page,
    clicks::BIGINT        AS clicks,
    impressions::BIGINT   AS impressions,
    ctr::DOUBLE           AS ctr,
    position::DOUBLE      AS position
FROM google_search_console('', 'page', 3)
