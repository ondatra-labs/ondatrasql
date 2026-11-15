-- @kind: table
-- @fetch
-- @constraint: not_null(item_qid)
-- @constraint: not_null(lei)
-- The first 1000 Wikidata entities with a Legal Entity Identifier (P1278),
-- in LEI order, with their English label, country (P17) and inception (P571).
-- One row per binding: an entity with two countries has two rows.
SELECT
    item::VARCHAR            AS item,
    item_qid::VARCHAR        AS item_qid,
    itemLabel::VARCHAR       AS item_label,
    lei::VARCHAR             AS lei,
    country_qid::VARCHAR     AS country_qid,
    countryLabel::VARCHAR    AS country_label,
    inception::VARCHAR       AS inception
FROM wikidata('
SELECT ?item ?itemLabel ?lei ?country ?countryLabel ?inception WHERE {
  {
    SELECT ?item ?lei WHERE { ?item wdt:P1278 ?lei . }
    ORDER BY ?lei
    LIMIT 1000
  }
  OPTIONAL { ?item wdt:P17 ?country . }
  OPTIONAL { ?item wdt:P571 ?inception . }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en,mul". }
}
')
