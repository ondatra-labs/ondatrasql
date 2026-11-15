-- @kind: merge
-- @unique_key: row_key
-- @fetch
-- @incremental: date
-- @constraint: not_null(date)
-- One row per day and search query. Anonymised queries are left out by
-- Google, so these rows add up to less than search_console_daily. Merge on
-- row_key (date and query); the lib fetches again from 3 days before the
-- newest day loaded.
SELECT
    row_key::VARCHAR      AS row_key,
    site::VARCHAR         AS site,
    date::DATE            AS date,
    query::VARCHAR        AS query,
    clicks::BIGINT        AS clicks,
    impressions::BIGINT   AS impressions,
    ctr::DOUBLE           AS ctr,
    position::DOUBLE      AS position
FROM google_search_console('', 'query', 3)
