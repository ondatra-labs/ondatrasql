-- @kind: tracked
-- @group_key: source_file
-- @fetch
-- @constraint: not_null(source_hash)
-- One row per PDF. Every column except the four the lib fills (source_file,
-- source_hash, pages, document_annotation) is a field Mistral extracts, and its
-- cast is the type it is asked for: DECIMAL a number, DATE a date string, a
-- STRUCT list an array of objects. Add, drop or retype a column to change what
-- is extracted; the next run sends every file again with the new schema.
SELECT
    source_file::VARCHAR          AS source_file,
    source_hash::VARCHAR          AS source_hash,
    pages::INTEGER                AS pages,
    document_annotation::VARCHAR  AS document_annotation,
    invoice_number::VARCHAR       AS invoice_number,
    invoice_date::DATE            AS invoice_date,
    due_date::DATE                AS due_date,
    vendor_name::VARCHAR          AS vendor_name,
    customer_name::VARCHAR        AS customer_name,
    currency::VARCHAR             AS currency,
    subtotal::DECIMAL(18, 2)      AS subtotal,
    tax::DECIMAL(18, 2)           AS tax,
    total::DECIMAL(18, 2)         AS total,
    line_items::STRUCT(
        description VARCHAR,
        quantity DECIMAL(18, 3),
        unit_price DECIMAL(18, 2),
        amount DECIMAL(18, 2)
    )[]                           AS line_items
FROM mistral_ocr('data/invoices/*.pdf')
