-- @kind: merge
-- @unique_key: row_key
-- @fetch
-- @incremental: date
-- @incremental_initial: 2026-11-01
-- @constraint: not_null(date)
-- One row per network, date and ad unit. Merge on row_key (network, date and
-- every dimension) so a re-fetched day replaces itself instead of appearing
-- twice. The lib re-fetches the 3 days before the newest one loaded.
SELECT
    row_key::VARCHAR                          AS row_key,
    network_code::VARCHAR                     AS network_code,
    date::DATE                                AS date,
    ad_unit_name::VARCHAR                     AS ad_unit_name,
    ad_server_impressions::BIGINT             AS ad_server_impressions,
    ad_server_clicks::BIGINT                  AS ad_server_clicks,
    ad_server_cpm_and_cpc_revenue::DOUBLE     AS ad_server_cpm_and_cpc_revenue
FROM google_ad_manager('', 'AD_UNIT_NAME', 'AD_SERVER_IMPRESSIONS,AD_SERVER_CLICKS,AD_SERVER_CPM_AND_CPC_REVENUE', 3)
