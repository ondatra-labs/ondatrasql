-- @kind: table
-- @fetch
-- @constraint: not_null(id)
-- The Orders tab, read whole on every run. The names inside the casts are
-- the header row of the sheet; the aliases are the column names here.
SELECT
    id::BIGINT        AS id,
    product::VARCHAR  AS product,
    quantity::INTEGER AS quantity,
    price::DOUBLE     AS price,
    updated::DATE     AS updated
FROM google_sheets('YOUR_SPREADSHEET_ID', 'Orders!A:E')
