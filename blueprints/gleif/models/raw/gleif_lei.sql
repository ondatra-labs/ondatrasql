-- @kind: merge
-- @unique_key: lei
-- @fetch
-- @incremental: fetched_on
-- @incremental_initial: 2026-11-13
-- @constraint: not_null(name)
-- One row per LEI whose legal address is in one of the listed countries.
-- A run fetches the records updated since its last fetch day (minus a lag),
-- and the merge on lei replaces an older version of a record.
SELECT
    lei::VARCHAR                     AS lei,
    name::VARCHAR                    AS name,
    country::VARCHAR                 AS country,
    city::VARCHAR                    AS city,
    postal_code::VARCHAR             AS postal_code,
    jurisdiction::VARCHAR            AS jurisdiction,
    legal_form::VARCHAR              AS legal_form,
    entity_status::VARCHAR           AS entity_status,
    registered_as::VARCHAR           AS registered_as,
    registered_at::VARCHAR           AS registered_at,
    registration_status::VARCHAR     AS registration_status,
    last_update::TIMESTAMPTZ         AS last_update,
    next_renewal::TIMESTAMPTZ        AS next_renewal,
    fetched_on::DATE                 AS fetched_on
FROM gleif('lei-records', 'SE,NO')
