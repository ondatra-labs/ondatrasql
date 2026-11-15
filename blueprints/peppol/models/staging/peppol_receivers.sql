-- @kind: table
-- @constraint: not_null(participant_id)
-- The participants with their identifier scheme named (0007 is SE:ORGNR,
-- 0192 NO:ORG) and how many document types each can receive.
SELECT
    p.participant_id,
    p.icd,
    s.scheme_id,
    s.scheme_name,
    p.identifier,
    p.name,
    p.country_code,
    p.query_country,
    p.reg_date,
    len(p.doc_types) AS doc_type_count,
    p.doc_types
FROM raw.peppol_participants p
LEFT JOIN raw.peppol_schemes s ON s.icd = p.icd
