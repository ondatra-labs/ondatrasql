-- @kind: merge
-- @unique_key: ra_key
-- @fetch
-- @incremental: fetched_on
-- One row per registration-authority code and jurisdiction. The whole list
-- is fetched on the first run and again once a week; the merge on
-- code:jurisdiction updates the rows in place.
SELECT
    ra_key::VARCHAR              AS ra_key,
    ra_code::VARCHAR             AS ra_code,
    international_name::VARCHAR  AS international_name,
    local_name::VARCHAR          AS local_name,
    organization::VARCHAR        AS organization,
    local_organization::VARCHAR  AS local_organization,
    website::VARCHAR             AS website,
    country_code::VARCHAR        AS country_code,
    country::VARCHAR             AS country,
    jurisdiction::VARCHAR        AS jurisdiction,
    fetched_on::DATE             AS fetched_on
FROM gleif('registration-authorities')
