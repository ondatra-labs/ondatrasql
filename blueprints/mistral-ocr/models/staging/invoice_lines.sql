-- @kind: table
-- @constraint: not_null(source_file)
-- One row per invoice line: the line_items array of each document expanded.
SELECT
    i.source_file,
    i.invoice_number,
    i.invoice_date,
    i.currency,
    li.ordinality                    AS line_no,
    li.item.description              AS description,
    li.item.quantity                 AS quantity,
    li.item.unit_price               AS unit_price,
    li.item.amount                   AS amount
FROM raw.invoices AS i,
    UNNEST(i.line_items) WITH ORDINALITY AS li(item, ordinality)
