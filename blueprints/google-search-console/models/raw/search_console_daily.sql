-- @kind: merge
-- @unique_key: row_key
-- @fetch
-- @incremental: date
-- @constraint: not_null(date)
-- One row per day for the property: its totals, anonymised queries
-- included. The first run loads the 16 months Search Console keeps; every
-- later run fetches again from 3 days before the newest day loaded, and the
-- merge on row_key (the date) replaces those days in place.
-- With '' the property comes from GOOGLE_SEARCH_CONSOLE_SITE.
SELECT
    row_key::VARCHAR      AS row_key,
    site::VARCHAR         AS site,
    date::DATE            AS date,
    clicks::BIGINT        AS clicks,
    impressions::BIGINT   AS impressions,
    ctr::DOUBLE           AS ctr,
    position::DOUBLE      AS position
FROM google_search_console('', '', 3)
