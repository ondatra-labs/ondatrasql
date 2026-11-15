-- @kind: table
-- @fetch
-- @constraint: unique(icd)
-- @constraint: at_least_one(scheme_id)
-- @constraint: at_least_one(scheme_name)
-- One row per participant identifier scheme (ICD) in the Peppol code list,
-- removed and deprecated ones included: old registrations still use them.
SELECT
    iso6523::VARCHAR              AS icd,
    schemeid::VARCHAR             AS scheme_id,
    country::VARCHAR              AS country,
    scheme_name::VARCHAR          AS scheme_name,
    issuing_agency::VARCHAR       AS issuing_agency,
    state::VARCHAR                AS state,
    deprecation_release::VARCHAR  AS deprecation_release,
    removal_date::VARCHAR         AS removal_date
FROM peppol('schemes')
