-- @kind: table
-- @constraint: not_null(lei)
-- One row per LEI, with the name of the register its local number
-- (registered_as) comes from. An RA code has one name, whatever its
-- jurisdictions, so the RA list is reduced to one row per code first.
WITH ra AS (
    SELECT
        ra_code,
        ANY_VALUE(international_name)  AS register_name,
        ANY_VALUE(local_name)          AS register_local_name,
        ANY_VALUE(organization)        AS register_organization
    FROM raw.gleif_ra
    GROUP BY ra_code
)
SELECT
    l.lei,
    l.name,
    l.country,
    l.city,
    l.postal_code,
    l.jurisdiction,
    l.legal_form,
    l.entity_status,
    l.registered_as,
    l.registered_at,
    ra.register_name,
    ra.register_local_name,
    ra.register_organization,
    l.registration_status,
    l.last_update,
    l.next_renewal
FROM raw.gleif_lei l
LEFT JOIN ra ON ra.ra_code = l.registered_at
