---
date: "2026-04-20"
description: Pull data from REST APIs into your pipeline using lib functions. Pagination, incremental loads, OAuth, and error handling.
draft: false
title: Fetch Data from APIs
weight: 7
---
Pull data from external APIs into your DuckLake pipeline. The runtime handles pagination, retries, and incremental state — your lib function handles the API-specific I/O. SQL handles the transformation.

## The pattern

Every API integration follows the same structure:

1. **API dict** — declares auth, endpoints, rate limits, pagination
2. **Starlark `fetch()`** — calls the API, parses responses, returns rows
3. **Raw SQL model** — fetches with API column names
4. **Staging SQL model** — transforms: casts types, renames, pivots, expands JSON

```
lib/my_api.star          → I/O logic
models/raw/data.sql      → FROM my_api('resource')
models/staging/data.sql   → SELECT ... FROM raw.data (transformation)
```

## Quick start

### 1. Create a lib function

```python
# lib/my_api.star
API = {
    "base_url": "https://api.example.com",
    "auth": {"env": "MY_API_KEY"},
    "fetch": {
        "args": ["resource"],
        "page_size": 100,
    },
}

def fetch(resource, page):
    resp = http.get("/v1/" + resource, params={
        "limit": page.size,
        "cursor": page.cursor,
    })
    if not resp.ok:
        fail("API error: " + str(resp.status_code))

    data = resp.json
    return {"rows": data["items"], "next": data.get("next_cursor")}
```

### 2. Write SQL models

**Raw** — fetch with API column names:

```sql
-- models/raw/users.sql
-- @kind: table

SELECT id::BIGINT AS id, name::VARCHAR AS name, email::VARCHAR AS email,
       metadata::JSON AS metadata
FROM my_api('users')
```

**Staging** — transform in SQL:

```sql
-- models/staging/users.sql
-- @kind: table

SELECT
    id::BIGINT AS id,
    name,
    LOWER(email) AS email,
    metadata->>'$.city' AS city
FROM raw.users
```

### 3. Run

```bash
ondatrasql run
```

Both models participate in the DAG. The staging model runs automatically after the raw model.

## SQL casts control the API

The runtime extracts column names and [normalized types](/reference/lib-functions/fetch-contract/#typed-columns) from your SELECT casts via DuckDB AST. These are passed to `fetch()` as the `columns` kwarg:

| SQL | Normalized type | Use case |
|---|---|---|
| `total::DECIMAL` | `decimal` | Numeric field |
| `count::INTEGER` | `integer` | Integer field |
| `items::JSON` | `json` | Structured data (arrays, objects) |
| `date::DATE` | `date` | Date field |
| `name::VARCHAR` | `string` | String field |

Blueprints can use this to adapt their API requests. For example, the GAM blueprint splits columns into dimensions (`string`) and metrics (anything else) based on normalized type.

## Pagination patterns

The runtime calls `fetch()` in a loop until `"next"` is `None`. Your blueprint owns the pagination logic:

### Cursor-based

```python
def fetch(resource, page):
    params = {"limit": page.size}
    if page.cursor:
        params["starting_after"] = page.cursor

    resp = http.get("/v1/" + resource, params=params)
    data = resp.json

    next_cursor = None
    if data.get("has_more") and len(data["data"]) > 0:
        next_cursor = data["data"][-1]["id"]

    return {"rows": data["data"], "next": next_cursor}
```

### Offset-based

```python
def fetch(resource, page):
    offset = page.cursor or 0
    resp = http.get("/v1/" + resource, params={"limit": page.size, "offset": offset})
    rows = resp.json["items"]
    next_offset = offset + page.size if len(rows) == page.size else None
    return {"rows": rows, "next": next_offset}
```

### Date-range

Fetch one time window per page:

```python
def fetch(series, page, is_backfill=True, initial_value="", last_value=""):
    if is_backfill:
        start_date = initial_value
    else:
        start_date = _next_day(last_value)

    cursor_date = start_date if page.cursor == None else page.cursor
    end_date = min(_add_days(cursor_date, 365), yesterday)

    resp = http.get("/data/" + series + "/" + cursor_date + "/" + end_date)
    rows = [{"date": obs["date"], "value": obs["value"]} for obs in resp.json]

    next_cursor = _next_day(end_date) if end_date < yesterday else None
    return {"rows": rows, "next": next_cursor}
```

### Dict cursor

When you need to carry multiple values between pages, return a dict — it passes through directly:

```python
next_cursor = {"url": fetch_url, "token": next_token, "series_idx": 3}
# On next page:
series_idx = page.cursor["series_idx"]
```

## Async fetch (report-style APIs)

For APIs that create reports asynchronously (submit → poll → fetch results), declare `async: True` and implement three functions:

```python
API = {
    "fetch": {
        "async": True,
        "poll_interval": "5s",
        "poll_timeout": "5m",
        "poll_backoff": 2,
    },
}

def submit(columns=[], is_backfill=True, last_value=""):
    resp = http.post("/reports", json={...})
    return {"job_id": resp.json["id"]}

def check(job_ref):
    resp = http.get("/reports/" + job_ref["job_id"])
    if resp.json["status"] == "complete":
        return {"url": resp.json["result_url"]}
    return None  # keep polling

def fetch_result(result_ref, page):
    resp = http.get(result_ref["url"] + "?pageSize=" + str(page.size))
    return {"rows": resp.json["data"], "next": resp.json.get("next_page")}
```

The runtime handles the poll loop. See [Fetch Contract — Async fetch](/reference/lib-functions/fetch-contract/#async-fetch).

## Nested and structured data

APIs return nested JSON. Blueprints return it as-is — SQL handles expansion in a downstream model.

### json_each — simplest, no schema needed

```sql
-- Raw: fetch with arrays as JSON
SELECT id::BIGINT AS id, name::VARCHAR AS name, tags::JSON AS tags FROM my_api('users')

-- Staging: expand
SELECT id, name, j.value->>'name' AS tag
FROM raw.users, LATERAL json_each(tags) AS j
```

### json_transform — typed expansion

```sql
SELECT id, x.*
FROM raw.users,
LATERAL (SELECT unnest(json_transform(tags,
    '[{"name":"VARCHAR","color":"VARCHAR"}]')) AS x)
```

### Arrow operators — direct access

```sql
SELECT id, metadata->>'$.address.city' AS city
FROM raw.users
```

### Pivot — normalize then pivot in SQL

When the API returns one row per measurement:

```sql
-- Raw: normalized (series, date, value)
SELECT series::VARCHAR AS series, date::DATE AS date, value::DECIMAL(18,6) AS value
FROM riksbank('SEKEURPMI,SEKUSDPMI')

-- Staging: pivoted
SELECT date::DATE,
    MAX(CASE WHEN series = 'SEKEURPMI' THEN value END)::DECIMAL AS eur,
    MAX(CASE WHEN series = 'SEKUSDPMI' THEN value END)::DECIMAL AS usd
FROM raw.exchange_rates
GROUP BY date
```

Or with DuckDB's native PIVOT:

```sql
PIVOT raw.exchange_rates ON series USING MAX(value) GROUP BY date::DATE
```

## Incremental loads

Use `@incremental` to fetch only new data on subsequent runs:

```sql
-- models/raw/events.sql
-- @kind: append
-- @incremental: created_at

SELECT id::BIGINT AS id, name::VARCHAR AS name, created_at::TIMESTAMP AS created_at
FROM my_api('events')
```

```python
def fetch(resource, page, is_backfill=True, last_value=""):
    params = {"limit": page.size, "cursor": page.cursor}
    if not is_backfill:
        params["created_after"] = last_value

    resp = http.get("/v1/" + resource, params=params)
    return {"rows": resp.json["items"], "next": resp.json.get("next")}
```

| Kwarg | Description |
|---|---|
| `is_backfill` | `True` on first run — fetch everything |
| `last_value` | `MAX(cursor_column)` from previous run |
| `initial_value` | Starting value from `@incremental_initial` |

Declare only the kwargs you use — the runtime filters automatically.

## Authentication

Configured in the API dict. Injected into every `http.*` call automatically.

```python
# API key
"auth": {"env": "API_KEY"}

# OAuth provider (browser-based)
"auth": {"provider": "hubspot"}

# Basic auth
"auth": {"user": {"env": "USER"}, "pass": {"env": "PASS"}}

# Google service account
"auth": {"service_account": {"env": "KEY_FILE"}, "scope": "https://..."}
```

## Error handling

**Fail** — stops the pipeline:

```python
if not resp.ok:
    fail("API error: " + str(resp.status_code) + " " + resp.text)
```

**Abort** — clean exit, 0 rows, no error:

```python
if start_date > yesterday:
    abort()
```

**Retry** — automatic. Configure in API dict:

```python
API = {"retry": 3, "backoff": 2, "timeout": 30}
```

**Rate limiting** — declare in API dict, runtime handles it:

```python
API = {"rate_limit": {"requests": 2, "per": "1s"}}
```

## Options via JSON arg

For API-specific configuration beyond columns:

```sql
FROM gam_report('{"custom_dimensions": [11678108], "currency": "USD"}')
```

```python
API = {"fetch": {"args": ["options"]}}

def fetch(options="", page=None, columns=[]):
    opts = json.decode(options) if options else {}
```

## Reference

- [Fetch Contract](/reference/lib-functions/fetch-contract/) — complete function spec
- [API Dict](/reference/lib-functions/api-dict/) — all configuration options
- [Starlark Modules](/reference/lib-functions/starlark-modules/) — http, env, builtins, csv, xml, time
- [Set Up OAuth](/guides/set-up-oauth/) — browser-based auth setup
- [Blueprints](/blueprints/) — working examples (Mistral OCR, Riksbank SWEA, Google Ad Manager)