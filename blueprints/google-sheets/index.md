---
title: Google Sheets
description: Read a range of a Google Sheet into a table and write a model's rows back to another range, authenticated as a Google service account.
vendor: Google
tags:
  - spreadsheets
  - google
  - reverse-etl
auth_method: service_account
source_type: fetch
model_kind: table
args:
  - range
  - spreadsheet_id
---
Reads a range of a spreadsheet through the [Google Sheets API v4](https://developers.google.com/sheets/api/reference/rest) into a table, and pushes the rows of a model back to another range of it. The blueprint signs in as a Google service account: share the spreadsheet with the account's email and the pipeline runs headless, with no browser and no user login.

The example reads an `Orders` tab (one row per order) and writes a summary per product to a `Summary` tab.

## What it does

- `FROM google_sheets('Orders!A:E')` reads the range in one request. Its first row is the header, and every later row becomes a row keyed by it, so the names inside the casts of the raw model are the sheet's column headers.
- Cells are read unformatted: a number formatted as `$19.90` arrives as `19.9`, and the cast in the model types it. Date cells arrive as the text they show.
- A column the model asks for that is not in the header fails the run, naming the header it found. Columns the model does not ask for are ignored.
- Empty rows are skipped. A range with a header and no rows gives an empty table.
- `-- @push: google_sheets('Summary')` replaces the range with the model's rows: it clears the range, then writes the header (the model's column aliases) and the rows from its top-left cell. Clearing first matters: writing alone would leave old rows below a result that got shorter.
- The rows are written sorted by the first column, then the second, and so on (NULLs last). Put the column to sort by first.
- Values are written as they are (`valueInputOption=RAW`): a string that looks like a number or a formula stays text.

## Set up the service account

1. In the [Google Cloud console](https://console.cloud.google.com/), pick or create a project and enable the **Google Sheets API** (APIs & Services > Library).
2. Create a service account (IAM & Admin > Service Accounts > Create service account). It needs no role in the project.
3. Open the account, then Keys > Add key > Create new key > JSON. Save the file outside your project and never commit it.
4. Copy the account's email (`name@project.iam.gserviceaccount.com`), open the spreadsheet, click **Share** and add that email as an **Editor**. Viewer is enough if you only read.
5. The spreadsheet id is the part of its URL between `/d/` and `/edit`: in `https://docs.google.com/spreadsheets/d/1AbC…xYz/edit#gid=0` it is `1AbC…xYz`.

## Quick start

```bash
mkdir sheets && cd sheets
ondatrasql init
echo "GOOGLE_SHEETS_KEY_FILE=/path/to/service-account.json" >> .env
echo "GOOGLE_SHEETS_SPREADSHEET_ID=1AbC…xYz" >> .env
```

Give the spreadsheet a tab `Orders` with the header `id`, `product`, `quantity`, `price`, `updated` in row 1 and a few orders below it (`updated` as `2026-11-01`), and an empty tab `Summary`. Copy the three files below into the project, keeping their paths, then run:

```bash
ondatrasql run
ondatrasql sql "SELECT * FROM sync.product_summary"
```

The `Summary` tab now holds the same rows.

### lib/google_sheets.star

{{< bp-file "lib/google_sheets.star" >}}

### models/raw/sheet_orders.sql

{{< bp-file "models/raw/sheet_orders.sql" >}}

### models/sync/product_summary.sql

{{< bp-file "models/sync/product_summary.sql" >}}

## Customisation

- **The spreadsheet:** either put its id in the model as the second argument, `google_sheets('Orders!A:E', '1AbC…xYz')`, or leave it out and set `GOOGLE_SHEETS_SPREADSHEET_ID` in `.env`, as the models here do. An empty `''` also means the variable. The variable suits one spreadsheet per environment (a test copy and the real one); ids in the models let each model use its own spreadsheet.
- **Another sheet:** change the range in `FROM google_sheets(…)` to any A1 range whose first row is the header: `Orders` (the whole tab), `Orders!A:E`, `'Order data'!B3:F`. Quote a tab name that has spaces, as Sheets does.
- **Columns:** each column of the raw model is `header::TYPE AS name`. Quote a header with spaces or capitals, `"Unit price"::DOUBLE AS unit_price`. A header the model does not name is ignored.
- **Dates:** a date cell is read as the text it shows, so `::DATE` needs it shown as `yyyy-mm-dd` (Format > Number > Custom date and time). For another format, read it as `VARCHAR` in raw and parse it with `strptime` downstream.
- **What is pushed:** the push model's aliases are the header written to the sheet, in their order. Cast numbers to `BIGINT` or `DOUBLE` so they land as numbers. Cast anything else (dates, decimals) to `VARCHAR`: it is written as text.
- **Where it is pushed:** the range of `@push` is cleared and written from its top-left cell, so it must be wide enough for the columns: a tab name (`Summary`), or a range such as `Summary!A1:D`. Anything else in that range is erased on every run.

## Notes

- The push model must be `@kind: table`: each run replaces the range with the whole result, which is what a table run produces. The lib says so in `supported_kinds`.
- Clearing and writing are two requests. Between them the range is empty for a moment, and if the write fails it stays empty until the next run pushes again (a failed push is retried on the next run).
- A result is pushed in one batch of up to 10,000 changes, counting the old rows' deletes. A larger one is written in several: the first replaces the range and the later ones are appended below it, each sorted on its own.
- Reading counts against the Sheets API quota of 60 read requests per minute per user; the `rate_limit` keeps to it, and `retry` with `backoff` rides out a `429` or `5xx`. Each read is one request, each push two.
- Sheets stores every number as a double: an integer above 2^53 loses precision. Push such ids as `VARCHAR`.
- The service account sees only the spreadsheets shared with it. A `403` or `404` usually means the spreadsheet is not shared with its email, or the id is wrong.
