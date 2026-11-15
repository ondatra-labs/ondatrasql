-- @kind: table
-- @fetch
-- @constraint: not_null(id)
-- The Orders tab, read whole on every run. The names inside the casts are
-- the header row of the sheet; the aliases are the column names here.
-- Without a spreadsheet id, GOOGLE_SHEETS_SPREADSHEET_ID names it.
SELECT
    id::BIGINT        AS id,
    product::VARCHAR  AS product,
    quantity::INTEGER AS quantity,
    price::DOUBLE     AS price,
    updated::DATE     AS updated
FROM google_sheets('Orders!A:E')
