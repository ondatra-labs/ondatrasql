---
date: "2026-04-20"
description: 'Complete reference for the fetch() function contract: page object, return format, pagination, incremental state, and available modules.'
draft: false
title: Fetch Contract
---
Your `fetch()` function is called once per page by the runtime. It receives pagination context and returns rows. Starlark handles I/O — SQL handles transformation.

## Function signature

```python
def fetch(arg1, arg2, ..., page, columns=[], target="", is_backfill=True):
```

Arguments before `page` come from `API.fetch.args`. They receive values from the SQL call:

```python
API = {"fetch": {"args": ["resource"]}}
```

```sql
SELECT * FROM my_api('users')
--                    ↑
--                 resource
```

Declare only the parameters your blueprint uses. The runtime automatically filters kwargs to match your function signature — undeclared kwargs are silently dropped, not errors.

### Runtime-injected kwargs

| Kwarg | Type | Description |
|---|---|---|
| `page` | struct | Pagination context (see below) |
| `columns` | list | SELECT columns as typed dicts (see [Typed columns](#typed-columns)) |
| `target` | string | Model target name (e.g. `raw.orders`) |
| `is_backfill` | bool | `True` on first run or when SQL changed |
| `last_value` | string | `MAX(cursor_column)` from previous run. Empty on backfill. |
| `initial_value` | string | Value from `@incremental_initial` directive |
| `last_run` | string | Timestamp of last successful run |
| `cursor` | string | Column name from `@incremental` directive |

All are optional — declare them with defaults if your blueprint needs them. Incremental kwargs (`is_backfill`, `last_value`, etc.) are only meaningful when the SQL model uses `@incremental`.

### Typed columns {#typed-columns}

`columns` is how SQL communicates schema intent to Starlark. Each entry is a dict with `name`, `type` (normalized DuckDB type), and `json_schema_type` (JSON Schema equivalent):

```python
columns = [
    {"name": "invoice_number", "type": "string", "json_schema_type": "string"},
    {"name": "total", "type": "decimal", "json_schema_type": "number", "precision": "18", "scale": "3"},
    {"name": "date", "type": "date", "json_schema_type": "string"},
    {"name": "line_items", "type": "json", "json_schema_type": "array"},
    {"name": "tags", "type": "list", "json_schema_type": "array", "element": {"type": "string", "json_schema_type": "string"}},
    {"name": "address", "type": "struct", "json_schema_type": "object", "fields": [
        {"name": "street", "type": "string", "json_schema_type": "string"},
        {"name": "zip", "type": "integer", "json_schema_type": "integer"},
    ]},
]
```

SQL casts control the types:

| SQL | `type` | `json_schema_type` | Extra fields |
|---|---|---|---|
| `name` (no cast) | `string` | `string` | — |
| `total::DECIMAL` | `decimal` | `number` | `precision`, `scale` |
| `total::DECIMAL(10,2)` | `decimal` | `number` | `precision: "10"`, `scale: "2"` |
| `count::INTEGER` | `integer` | `integer` | — |
| `rate::DOUBLE` | `float` | `number` | — |
| `active::BOOLEAN` | `boolean` | `boolean` | — |
| `date` (DATE column) | `date` | `string` | — |
| `ts::TIMESTAMP` | `timestamp` | `string` | `tz: false`, `precision: "us"` |
| `ts::TIMESTAMPTZ` | `timestamp` | `string` | `tz: true`, `precision: "us"` |
| `items::JSON` | `json` | `array` | — |
| `id::UUID` | `uuid` | `string` | — |
| `tags::VARCHAR[]` | `list` | `array` | `element: {"type": "string"}` |
| `person::STRUCT(...)` | `struct` | `object` | `fields: [...]` (recursive) |
| `m::MAP(VARCHAR, INT)` | `map` | `object` | `key: {...}`, `value: {...}` |

Blueprints choose which field to use:

- `col["json_schema_type"]` — for APIs that accept JSON Schema (Mistral OCR, OpenAI, etc.)
- `col["type"]` — for blueprints that need the normalized type (GAM: `string` = dimension)
- Neither — for APIs that don't need type information (Riksbank, Google Sheets fetch)

## Page object

Read-only struct:

| Field | Type | Description |
|---|---|---|
| `page.cursor` | any | `None` on first page. On subsequent pages, whatever you returned as `next`. |
| `page.size` | int | From `API.fetch.page_size`. Constant across all pages. |

## Return format

```python
return {
    "rows": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
    "next": "cursor_abc123",
}
```

| Key | Required | Description |
|---|---|---|
| `"rows"` | Yes | List of dicts. Each dict is one row. |
| `"next"` | No | Cursor for the next page. Any type — string, int, dict. `None`, missing, or `""` stops pagination. |

The cursor is opaque — the runtime stores and returns it as-is. Dicts pass through directly:

```python
next_cursor = {"idx": series_idx, "from": next_date, "until": yesterday}
```

## Pagination

The runtime calls `fetch()` in a loop until `"next"` is `None` or missing.

Pagination patterns vary by API. The blueprint owns the logic:

- **Cursor-based** — API returns a next token, pass it back
- **Offset-based** — increment an offset by page size
- **Date-range** — advance a date window per page
- **Multi-resource** — iterate through resources, then dates within each

The runtime doesn't care which pattern you use — it just follows the cursor.

## Async fetch {#async-fetch}

For APIs that create reports asynchronously (submit → poll → fetch results), declare `async: True` in the API dict and implement three functions instead of `fetch()`:

```python
API = {
    "fetch": {
        "async": True,
        "poll_interval": "5s",
        "poll_timeout": "5m",
        "poll_backoff": 2,
    },
}

def submit(columns=[], is_backfill=True, last_value="", initial_value=""):
    # Create and start the report/job
    return {"job_id": resp.json["id"]}  # opaque job reference

def check(job_ref):
    # Poll job status. Return None to keep polling, dict when done.
    if not done:
        return None
    return {"download_url": resp.json["result_url"]}  # opaque result reference

def fetch_result(result_ref, page):
    # Fetch result rows with pagination — same return format as fetch()
    return {"rows": [...], "next": next_token}
```

The runtime handles the poll loop — `check()` is called with the configured interval and backoff until it returns a non-None value or the timeout expires.

| Function | Receives | Returns |
|---|---|---|
| `submit` | args + kwargs (columns, is_backfill, last_value, etc.) | dict — job reference (opaque) |
| `check` | `job_ref` (from submit) | `None` (keep polling) or dict (result reference) |
| `fetch_result` | `result_ref` (from check) + `page` | `{"rows": [...], "next": ...}` |

`abort()` in `submit()` is valid — materializes with 0 rows. `fail()` in any function stops the pipeline.

## Error handling

**Fail** — stops the pipeline with an error:

```python
if not resp.ok:
    fail("API error: " + str(resp.status_code) + " " + resp.text)
```

**Abort** — clean exit, 0 rows, no error. Use when there's nothing to fetch:

```python
if start_date > yesterday:
    abort()
```

## The two-model pattern

Blueprints return raw API data. SQL transforms it in a downstream model. This keeps the layers separate:

**Raw model** — Starlark fetches, column names match the API:

```sql
-- models/raw/data.sql
-- @kind: append
-- @incremental: date

SELECT series, date, value
FROM my_api('SERIES_A,SERIES_B')
```

**Staging model** — SQL transforms, casts types, pivots, joins:

```sql
-- models/staging/data.sql
-- @kind: table

SELECT
    date::DATE AS date,
    MAX(CASE WHEN series = 'SERIES_A' THEN value END)::DECIMAL AS series_a,
    MAX(CASE WHEN series = 'SERIES_B' THEN value END)::DECIMAL AS series_b
FROM raw.data
GROUP BY date
```

Don't alias or transform in the raw model. Don't call APIs in the staging model. Each layer does one thing.

## Available builtins and modules

### Modules

| Module | Purpose |
|---|---|
| `http` | HTTP requests. Auth, headers, retry, base_url from API dict injected automatically. |
| `env` | Environment variables via `env.get("KEY")`. |
| `json` | JSON encoding/decoding (Starlark stdlib). |
| `time` | Date/time operations (Starlark stdlib). |
| `xml` | XML parsing. |
| `csv` | CSV parsing. |

### DuckDB-backed builtins

| Builtin | Purpose |
|---|---|
| `glob(pattern)` | File paths matching pattern. Returns list of strings. |
| `md5_file(path)` | MD5 hash of file contents. |
| `read_text(path)` | Read text file. Returns string. |
| `read_blob(path)` | Read binary file. Returns string. |
| `file_exists(path)` | Check if file exists. Returns bool. |
| `md5(string)` | MD5 hash of string. |
| `sha256(string)` | SHA-256 hash of string. |
| `uuid()` | Generate UUIDv4. |
| `lookup(table, key, value, where)` | Key-value lookup against a table. Returns dict. |

### Go-native builtins

| Builtin | Purpose |
|---|---|
| `hmac_sha256(key, message)` | HMAC-SHA256 signature. |
| `base64_encode(data)` | Base64 encoding. |
| `base64_decode(data)` | Base64 decoding. |

### Control flow

| Builtin | Purpose |
|---|---|
| `abort()` | Clean exit, 0 rows, no error. |
| `fail(message)` | Stop pipeline with error. |
| `sleep(seconds)` | Pause execution. |
| `print(message)` | Log to stderr. |