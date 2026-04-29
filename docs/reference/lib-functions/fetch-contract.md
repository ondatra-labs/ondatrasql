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
SELECT id::BIGINT AS id, name::VARCHAR AS name FROM my_api('users')
--                                                          ↑
--                                                       resource
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

SQL casts control the types. Every column from a lib must be cast (see [SQL schema contract](#sql-schema-contract) below):

| SQL                    | `type`      | `json_schema_type` | Extra fields                    |
| ---------------------- | ----------- | ------------------ | ------------------------------- |
| `name::VARCHAR`        | `string`    | `string`           | —                               |
| `total::DECIMAL`       | `decimal`   | `number`           | `precision`, `scale`            |
| `total::DECIMAL(10,2)` | `decimal`   | `number`           | `precision: "10"`, `scale: "2"` |
| `count::INTEGER`       | `integer`   | `integer`          | —                               |
| `rate::DOUBLE`         | `float`     | `number`           | —                               |
| `active::BOOLEAN`      | `boolean`   | `boolean`          | —                               |
| `date::DATE`           | `date`      | `string`           | —                               |
| `ts::TIMESTAMP`        | `timestamp` | `string`           | `tz: false`, `precision: "us"`  |
| `ts::TIMESTAMPTZ`      | `timestamp` | `string`           | `tz: true`, `precision: "us"`   |
| `items::JSON`          | `json`      | `array`            | —                               |
| `id::UUID`             | `uuid`      | `string`           | —                               |
| `tags::VARCHAR[]`      | `list`      | `array`            | `element: {"type": "string"}`   |
| `person::STRUCT(...)`  | `struct`    | `object`           | `fields: [...]` (recursive)     |
| `m::MAP(VARCHAR, INT)` | `map`       | `object`           | `key: {...}`, `value: {...}`    |

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

## SQL schema contract

For lib-backed models, SQL is the complete schema source. Every output column must be selected explicitly, cast explicitly to its final DuckDB type, and named explicitly. Schema inference from returned rows, from existing targets, or from `SELECT *` is not part of the contract.

```sql
SELECT
    id::BIGINT          AS id,
    name::VARCHAR       AS name,
    total::DECIMAL(10,2) AS total,
    items::JSON         AS items,
    created_at::TIMESTAMPTZ AS created_at
FROM my_lib('items')
```

The rules apply to every projection in every SELECT in the model — top-level, CTEs, set-op branches (`UNION`, `INTERSECT`, `EXCEPT`), and subqueries:

- `SELECT *` is rejected.
- The outermost node of every projection must be a `CAST`. Bare column refs, computed expressions, function calls, and literals are all rejected unless wrapped in a cast.
- Every projection must carry an explicit alias (`AS name`). Implicit names from underlying column refs do not count.
- Two projections in the same SELECT cannot share the same output name.

The rule applies uniformly to every output column — including columns from regular tables joined with the lib. Columns referenced only in `WHERE`, `JOIN ON`, or `GROUP BY` (not projected) are unaffected; they are not in the output schema.

Valid:

```sql
SELECT
    u.name::VARCHAR AS name,
    s.score::BIGINT AS score
FROM reg.users u
JOIN my_lib('scores') s ON u.user_id = s.user_id
```

Invalid:

```sql
SELECT * FROM my_lib('items')
SELECT id, name, total FROM my_lib('items')
SELECT id::BIGINT AS id, price * qty AS total FROM my_lib('items')
SELECT id::BIGINT, name::VARCHAR FROM my_lib('items')
```

The fix for the third row is `(price * qty)::DECIMAL(10,2) AS total`. The fix for the fourth row is `AS id` and `AS name`.

Validation runs at the start of every `ondatrasql run` / `ondatrasql sandbox`. A violation aborts the run before any `fetch()` is called, naming the offending projection.

Two consequences worth knowing:

- **Empty fetches** produce a typed target from the SQL alone — the runtime never has to guess types on the first run or after a schema change.
- **Schema evolution** is detected from the SQL diff, not from runtime data inspection. Add a column to the SELECT and the next run alters the target.

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
| `"empty_result"` | No | Read from the final page; takes effect only when the fetch returned 0 rows total. `"no_change"` (default) tells `tracked` materialize to preserve the existing target. `"delete_missing"` tells it the source is fully enumerated and groups absent from the empty result should be deleted from the target. |

The cursor is opaque — the runtime stores and returns it as-is. Dicts pass through directly:

```python
next_cursor = {"idx": series_idx, "from": next_date, "until": yesterday}
```

## Empty fetches and `tracked` {#empty-fetches-and-tracked}

For `tracked`-kind models the runtime needs to know what a 0-row fetch *means*. Two cases:

1. **Source has nothing new this run** — the lib reads a cache key, last-modified timestamp, or pre-computed digest, decides nothing changed upstream, and returns 0 rows. The target should stay as-is.
2. **Source is fully enumerated and is genuinely empty** — every row in the target should be deleted.

Set `empty_result` on the final page (the one whose `next` is `None` / missing / `""`) to disambiguate. The default is `no_change`, which is safe for fetches that may return 0 rows for any reason. Use `delete_missing` only when the lib enumerates the full source on every run and an empty fetch is the authoritative "everything is gone" signal.

```python
def fetch(page):
    if cache_key_unchanged():
        return {"rows": [], "next": None}  # implicit empty_result=no_change

def fetch(page):
    rows = http.get("/items").json
    return {"rows": rows, "next": None, "empty_result": "delete_missing"}
```

`empty_result` has no effect when the fetch returned any rows in total. Unknown string values fall back to the default rather than failing the run.

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

SELECT
    series::VARCHAR AS series,
    date::DATE AS date,
    value::DECIMAL(18,6) AS value
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