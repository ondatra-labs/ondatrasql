---
title: Mistral OCR
description: Reads a folder of PDFs with Mistral OCR and loads one typed row per document, extracted to a JSON Schema built from the model's SELECT.
vendor: Mistral AI
tags:
  - ocr
  - documents
  - pdf
  - invoices
  - llm
auth_method: api_key
source_type: fetch
model_kind: tracked
min_version: v0.47.1
args:
  - pattern
---
Sends PDFs to [Mistral OCR](https://docs.mistral.ai/capabilities/document_ai/) and loads what it extracts as one row per document. You don't write a prompt or a parser. The SELECT is the extraction schema: each column you cast becomes a field Mistral is asked to fill in, typed by its cast.

The example reads invoices: number, dates, vendor, totals and the line items. A staging model expands the line items into one row per line.

## What it does

- `FROM mistral_ocr('data/invoices/*.pdf')` reads every file the glob matches, relative to the project directory.
- Each file is hashed (MD5). Only a new file, or one whose hash changed, is sent to the API. An unchanged file keeps the row it already has, rebuilt from the extraction stored with it, so a run where nothing changed makes no request at all.
- Each file that is sent is uploaded (`POST /v1/files`, purpose `ocr`), read with `POST /v1/ocr` and the schema as `document_annotation_format`, and then deleted from your Mistral account (`DELETE /v1/files/{id}`). The delete runs even when the OCR call fails.
- Files are sent in batches of 10 per fetch page.
- The raw model is `@kind: tracked` with `@group_key: source_file`: one group per file. A changed file replaces its row. A file you remove from the folder has its row removed on the next run.

## Quick start

```bash
mkdir invoices && cd invoices
ondatrasql init
mkdir -p data/invoices      # put your PDFs here
echo 'MISTRAL_API_KEY=...' >> .env
```

Copy the three files below into the project, keeping their paths, then run:

```bash
ondatrasql run
ondatrasql sql "SELECT invoice_number, invoice_date, vendor_name, total FROM raw.invoices"
ondatrasql sql "SELECT * FROM staging.invoice_lines LIMIT 10"
```

Add a PDF and run again, and only that file is sent.

### lib/mistral_ocr.star

{{< bp-file "lib/mistral_ocr.star" >}}

### models/raw/invoices.sql

{{< bp-file "models/raw/invoices.sql" >}}

### models/staging/invoice_lines.sql

{{< bp-file "models/staging/invoice_lines.sql" >}}

## How the casts become the schema

The runtime passes the lib each column of the SELECT with its DuckDB type. The lib turns every column into a property of a JSON Schema with `lib_helpers.to_json_schema`, except the four it fills itself:

| SQL in the model | Asked of Mistral |
|---|---|
| `invoice_number::VARCHAR` | `{"type": "string"}` |
| `total::DECIMAL(18, 2)` | `{"type": "number"}` |
| `page_count::INTEGER` (any integer type) | `{"type": "integer"}` |
| `invoice_date::DATE` | `{"type": "string", "format": "date"}` |
| `paid::BOOLEAN` | `{"type": "boolean"}` |
| `tags::VARCHAR[]` | `{"type": "array", "items": {"type": "string"}}` |
| `line_items::STRUCT(description VARCHAR, amount DECIMAL(18, 2))[]` | an array of objects with those properties |

Every field is required and may be `null`, at every level. A field the document does not have, such as a missing due date, comes back as NULL instead of being made up. Mistral's answer is then cast by the same SQL, so `DATE` and `DECIMAL` columns are real dates and numbers in the table.

The column name before the cast is the field name Mistral sees, so name it the way the document would (`invoice_date`, not `d1`), and rename it with `AS` if you want something else in the table.

The four columns the lib fills must keep their names and must not be renamed with `AS`, since the lib reads them back from the table:

| Column | Holds |
|---|---|
| `source_file` | The path as the glob returned it, the `@group_key` |
| `source_hash` | MD5 of the file's contents |
| `pages` | The pages Mistral processed (`usage_info.pages_processed`) |
| `document_annotation` | The extraction as Mistral returned it, as JSON text |

## Customisation

- **Other documents:** change the columns. Receipts, contracts or delivery notes work the same way: write the fields you want as casts and run. Changing the SELECT rebuilds the table, and that run sends every file again with the new schema.
- **Other folder:** change the glob in `FROM mistral_ocr('…')`. It can be absolute (`/srv/scans/**/*.pdf`).
- **More line fields:** add them to the `STRUCT(...)` in raw and select them in staging.
- **Raw extraction:** `document_annotation` holds Mistral's whole answer, so a field you did not cast can still be read with `document_annotation->>'field'` in a downstream model.

## Cost

Mistral bills OCR per page processed, and OCR with a document annotation (the schema extraction) has its own per-page rate. See [Mistral's pricing](https://mistral.ai/pricing) for the current rates. The `pages` column records what each document was billed for. What decides the bill:

- **First run, and any change to the SELECT:** every file is sent. Check the schema on a few files in a separate folder before you point it at thousands.
- **Later runs:** only new and changed files. A file that is re-saved or re-scanned has a new hash and is sent again.
- **Failed runs:** a run that fails part way (a rejected file, a network error after the retries) stores nothing, and the next run sends the files again, including those that were read before the failure.

## Notes

- Removing every file from the folder does not empty the table: a glob that matches nothing returns no rows, which a tracked model takes as "no change". This keeps a mistyped pattern from wiping extractions you paid for. Delete rows by hand if you mean it.
- Only PDFs are covered here. The lib uploads whatever the glob matches, so narrow the glob to `*.pdf`.
- OCR can take tens of seconds for a long document. The API dict allows 300 seconds per request and retries a `429` or `5xx` three times.
- Files are deleted from Mistral after each extraction, but Mistral's own retention of requests is governed by your account's terms, not by this blueprint.
