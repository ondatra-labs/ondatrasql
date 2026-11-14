-- @kind: merge
-- @unique_key: obs_key
-- @fetch
-- @incremental: date
-- @incremental_initial: 2026-11-03
-- @constraint: not_null(value)
-- One row per series and banking day. Merge on series:date so a re-fetched day
-- replaces itself instead of appearing twice.
SELECT
    obs_key::VARCHAR  AS obs_key,
    series::VARCHAR   AS series,
    date::DATE        AS date,
    value::DOUBLE     AS value
FROM riksbank('SEKEURPMI,SEKUSDPMI')
