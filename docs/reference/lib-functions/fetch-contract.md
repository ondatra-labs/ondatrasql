---
date: "2026-05-01"
description: 'Complete reference for the fetch() function contract: page object, return format, pagination, incremental state, and available modules.'
draft: false
title: Fetch Contract
weight: 12
---
Your `fetch()` function is called once per page by the runtime. It receives pagination context and returns rows. Starlark handles I/O — SQL handles transformation.

## The `@fetch` directive

A model that calls a lib function in its `FROM` clause must declare `@fetch`:

```sql
-- @kind: append
-- @fetch
-- @incremental: date

SELECT
    series::VARCHAR AS series,
    date::DATE AS date,
    value::DECIMAL(18,6) AS value
FROM riksbank('SEKEURPMI')
```

`@fetch` is a bare marker (no arguments). Arguments to the lib call go on the `FROM` line, not the directive. The directive activates the strict-fetch validator and signals "this model produces its schema from a lib call".

A `@fetch` model and a `@push` model are two different things — they cannot be combined on the same model. To push data fetched from one API to another, split into a `@fetch` model that materializes the raw rows and a downstream `@push` model that reads from the materialized table.

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

`columns` is how SQL communicates schema intent to Starlark. Each entry is a dict with `name` and `type`. The `type` is in DuckDB-native syntax — the same shape DuckDB's `json_structure` produces and `json_transform` consumes.

```python
columns = [
    {"name": "invoice_number", "type": "VARCHAR"},
    {"name": "total",          "type": "DECIMAL(18,3)"},
    {"name": "date",           "type": "DATE"},
    {"name": "created_at",     "type": "TIMESTAMPTZ"},
    {"name": "items",          "type": "JSON"},
    {"name": "tags",           "type": ["VARCHAR"]},
    {"name": "address",        "type": {"street": "VARCHAR", "zip": "INTEGER"}},
]
```

| SQL | `type` |
|---|---|
| `name::VARCHAR` | `"VARCHAR"` |
| `total::DECIMAL(10,2)` | `"DECIMAL(10,2)"` |
| `total::DECIMAL` | `"DECIMAL(18,3)"` (default precision/scale) |
| `count::INTEGER` | `"INTEGER"` |
| `count::BIGINT` | `"BIGINT"` |
| `rate::DOUBLE` | `"DOUBLE"` |
| `rate::FLOAT` | `"FLOAT"` |
| `active::BOOLEAN` | `"BOOLEAN"` |
| `date::DATE` | `"DATE"` |
| `ts::TIMESTAMP` | `"TIMESTAMP"` |
| `ts::TIMESTAMPTZ` | `"TIMESTAMPTZ"` |
| `items::JSON` | `"JSON"` |
| `id::UUID` | `"UUID"` |
| `tags::VARCHAR[]` | `["VARCHAR"]` |
| `person::STRUCT(name VARCHAR, age INTEGER)` | `{"name": "VARCHAR", "age": "INTEGER"}` |
| `m::MAP(VARCHAR, INTEGER)` | `"MAP(VARCHAR, INTEGER)"` |

Width and precision distinctions are preserved (`TINYINT` vs `BIGINT`, `FLOAT` vs `DOUBLE`, `TIMESTAMP_NS` vs `TIMESTAMPTZ`) — the column dict carries enough information to round-trip back to the same DuckDB type.

The `name` field is the **cast source column reference**, not the SQL alias. For

```sql
SELECT AD_UNIT_NAME::VARCHAR AS ad_unit FROM gam_report()
```

the blueprint sees `name = "AD_UNIT_NAME"` (the API field it queries by and uses as the row key). The alias `ad_unit` is the materialized column name in DuckLake; the runtime renames at projection time without involving the blueprint. This lets a model author rename API fields without modifying the lib.

#### JSON Schema mapping

Blueprints that talk to JSON-Schema-aware APIs (OpenAI structured outputs, Mistral OCR, Anthropic) use the `lib_helpers.to_json_schema` helper to convert a DuckDB type into a JSON Schema dict:

```python
def fetch(page, columns=[]):
    schema = {"type": "object", "properties": {
        col["name"]: lib_helpers.to_json_schema(col["type"])
        for col in columns
    }}
    resp = http.post("/v1/structured", json={"schema": schema, ...})
```

| DuckDB type | JSON Schema |
|---|---|
| `BIGINT`, `INTEGER`, `SMALLINT`, `TINYINT`, `HUGEINT` (and unsigned variants) | `{"type": "integer"}` |
| `DECIMAL(...)`, `DOUBLE`, `FLOAT`, `REAL` | `{"type": "number"}` |
| `BOOLEAN` | `{"type": "boolean"}` |
| `DATE` | `{"type": "string", "format": "date"}` |
| `TIMESTAMP*` (any variant) | `{"type": "string", "format": "date-time"}` |
| Other primitives (`VARCHAR`, `UUID`, `JSON`, `BLOB`, `MAP(...)`, `UNION(...)`) | `{"type": "string"}` |
| `["TYPE"]` (LIST) | `{"type": "array", "items": <recurse>}` |
| `{"field": "TYPE", ...}` (STRUCT) | `{"type": "object", "properties": {...}}` |

Width/precision distinctions inside DuckDB collapse to the same JSON Schema type — JSON Schema doesn't carry that information.

Blueprints that don't need a JSON Schema (Riksbank, Google Sheets fetch) ignore the `type` field entirely.

## Page object

Read-only struct:

| Field | Type | Description |
|---|---|---|
| `page.cursor` | any | `None` on first page. On subsequent pages, whatever you returned as `next`. |
| `page.size` | int | From `API.fetch.page_size`. Constant across all pages. |

## SQL schema contract {#sql-schema-contract}

A `@fetch` model is a passthrough projection of API rows. The strict-fetch validator runs before any `fetch()` call and rejects any model that doesn't fit the canonical shape:

```sql
-- @fetch
SELECT
    col1::TYPE AS alias1,
    col2::TYPE AS alias2,
    ...
FROM lib(args)
```

That's it. Every constraint below is mechanical:

| Rule | Why |
|---|---|
| `SELECT *` is rejected | The output schema must be explicit, not inferred |
| Every projection must be wrapped in `CAST` | Types come from SQL, not from data inspection |
| The CAST argument must be a bare or qualified `COLUMN_REF` (`col` or `tbl.col`) | The projection mirrors the API field — derived expressions, function calls, and arithmetic belong in a downstream model |
| Every projection must carry an explicit `AS alias` | The materialized column name is declared, not implicit |
| Two projections in the same SELECT cannot share an alias | Output schema must be unambiguous |
| `FROM` must be exactly one lib call | `JOIN`, regular tables, subqueries, multi-source FROM are all rejected — split into a downstream model |
| No `WHERE` | Filtering happens via lib args (server-side) or in a downstream model |
| No `GROUP BY` or aggregate functions in projection | Aggregation belongs in a downstream model |
| No `DISTINCT` | Deduplication belongs in a downstream model |
| No `ORDER BY` | Ordering belongs in a downstream model |
| No `LIMIT` / `OFFSET` (cross-cutting) | Pagination is the lib's job; sampling belongs in `ondatrasql query --limit N` |
| No `UNION` / `INTERSECT` / `EXCEPT` | One lib call → one model |

Validation runs at the start of every `ondatrasql run` / `ondatrasql sandbox`. A violation aborts the run before any `fetch()` is called, naming the offending projection.

Two consequences worth knowing:

- **Empty fetches** produce a typed target from the SQL alone — the runtime never has to guess types on the first run or after a schema change.
- **Schema evolution** is detected from the SQL diff, not from runtime data inspection. Add a column to the SELECT and the next run alters the target.

The fix for almost every rejection is the two-model pattern below.

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

`@fetch` models stay close to the API shape. Transformation — JOIN, WHERE, GROUP BY, derived expressions, multi-source combination — happens in downstream models that read from the materialized fetch table.

**Raw model** (`@fetch`) — column refs match the API:

```sql
-- models/raw/data.sql
-- @kind: append
-- @fetch
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

**Joining two libs** — write two raw models, then join in staging:

```sql
-- models/raw/users.sql
-- @kind: table
-- @fetch
SELECT user_id::BIGINT AS user_id, name::VARCHAR AS name FROM users_api('users')
```

```sql
-- models/raw/scores.sql
-- @kind: table
-- @fetch
SELECT user_id::BIGINT AS user_id, score::BIGINT AS score FROM scores_api('scores')
```

```sql
-- models/staging/joined.sql
-- @kind: table
SELECT
    u.user_id::BIGINT AS user_id,
    u.name::VARCHAR AS name,
    s.score::BIGINT AS score
FROM raw.users u
JOIN raw.scores s ON u.user_id = s.user_id
```

The split is mechanical and worth it: each `@fetch` model has its own lib lifecycle, its own incremental state, and a deterministic shape contract. The downstream model is plain SQL.

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
| `lib_helpers` | OndatraSQL helpers: `lib_helpers.to_json_schema(t)` converts a DuckDB type to a JSON Schema dict. |

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
