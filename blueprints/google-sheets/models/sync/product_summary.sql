-- @kind: table
-- @push: google_sheets('YOUR_SPREADSHEET_ID', 'Summary')
-- One row per product, written over the Summary tab on every run. The
-- aliases become the header row of the tab.
SELECT
    product::VARCHAR                        AS product,
    SUM(quantity)::BIGINT                   AS quantity,
    ROUND(SUM(quantity * price), 2)::DOUBLE AS revenue,
    MAX(updated)::VARCHAR                   AS last_updated
FROM raw.sheet_orders
GROUP BY product
