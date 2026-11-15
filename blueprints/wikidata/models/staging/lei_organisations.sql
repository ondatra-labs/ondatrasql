-- @kind: table
-- @constraint: not_null(item_qid)
-- @constraint: composite_unique('item_qid, lei')
-- One row per entity and LEI. An entity with more than one country or
-- inception keeps the first country QID in text order and the earliest inception.
SELECT
    item_qid,
    substr(item_qid, 2)::BIGINT                                   AS item_id,
    any_value(item_label)                                         AS name,
    lei,
    regexp_full_match(lei, '[0-9A-Z]{18}[0-9]{2}')                AS lei_well_formed,
    min(country_qid)                                              AS country_qid,
    arg_min(country_label, country_qid)                           AS country,
    -- xsd:dateTime such as 1998-01-01T00:00:00Z: the date part, so the
    -- session time zone cannot shift it; a year-precision date is 1 January
    -- of that year, and a date BCE is NULL
    min(try_cast(substr(inception, 1, 10) AS DATE))               AS inception,
    count(DISTINCT country_qid)::INTEGER                          AS countries
FROM raw.organisations_with_lei
GROUP BY item_qid, lei
