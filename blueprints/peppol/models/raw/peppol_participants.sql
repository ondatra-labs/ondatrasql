-- @kind: table
-- @fetch
-- @constraint: not_null(participant_id)
-- One row per participant and business entity in each listed country: who
-- can receive Peppol documents, as the Peppol Directory has it today. A run
-- replaces the table with the directory's current state.
SELECT
    participant_scheme::VARCHAR  AS participant_scheme,
    participant_id::VARCHAR      AS participant_id,
    icd::VARCHAR                 AS icd,
    identifier::VARCHAR          AS identifier,
    name::VARCHAR                AS name,
    country_code::VARCHAR        AS country_code,
    reg_date::DATE               AS reg_date,
    additional_info::VARCHAR     AS additional_info,
    doc_types::VARCHAR[]         AS doc_types,
    query_country::VARCHAR       AS query_country
FROM peppol('participants', 'SE/0007:2021,SE/0007:2120')
