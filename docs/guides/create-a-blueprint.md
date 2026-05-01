---
description: How to build a lib function for API ingestion or outbound sync. Step-by-step from API dict to working fetch/push function.
draft: false
title: Create a Lib Function
weight: 6
---
How to create a lib function that fetches data from an API or pushes data to an external system. Follow the three-layer pattern: API dict declares the contract, Starlark handles I/O, SQL handles transformation.

## 1. Create the file

```bash
mkdir -p lib
touch lib/my_api.star
```

The file name becomes the function name in SQL: `FROM my_api(...)` or `@push: my_api`.

## 2. Define the API dict

Start with `base_url` and `auth`. All values must be literals — the runtime parses the dict as AST without executing code.

```python
API = {
    "base_url": "https://api.example.com",
    "auth": {"env": "MY_API_KEY"},
    "retry": 3,
    "backoff": 2,
    "rate_limit": {"requests": 100, "per": "10s"},
}
```

Auth patterns:

```python
"auth": {"env": "API_KEY"}                                           # API key → Bearer header
"auth": {"env": "API_KEY", "header": "X-Api-Key"}                    # API key → custom header
"auth": {"env": "API_KEY", "param": "api_key"}                       # API key → query parameter
"auth": {"provider": "hubspot"}                                      # OAuth2 (run ondatrasql auth hubspot first)
"auth": {"user": {"env": "USER"}, "pass": {"env": "PASS"}}           # Basic auth
"auth": {"service_account": {"env": "KEY_FILE"}, "scope": "..."}     # Google service account
```

See [API Dict Reference](/reference/lib-functions/api-dict/) for all auth patterns.

## 3. Add a fetch section (inbound)

```python
API = {
    "base_url": "https://api.example.com",
    "auth": {"env": "MY_API_KEY"},
    "fetch": {
        "args": ["resource"],
        "page_size": 100,
    },
}
```

- `args` — parameter names passed from SQL: `FROM my_api('users')` → `resource = "users"`
- `page_size` — enables pagination (runtime manages the loop)

SQL controls the schema. Column names and [DuckDB-native types](/reference/lib-functions/fetch-contract/#typed-columns) are extracted from your SELECT casts and passed to `fetch()` as the `columns` kwarg.

## 4. Write the fetch function

Starlark does I/O only — call the API, parse the response, return rows. Declare only the parameters you need — the runtime filters kwargs automatically.

```python
def fetch(resource, page):
    resp = http.get("/v1/" + resource, params={
        "limit": page.size,
        "cursor": page.cursor,
    })
    if not resp.ok:
        fail("API error: " + str(resp.status_code) + " " + resp.text)

    data = resp.json
    return {
        "rows": data["items"],
        "next": data.get("next_cursor"),
    }
```

- `page.cursor` is `None` on first call, then whatever you returned as `next`
- `page.size` comes from `page_size` in the API dict
- Return `None` for `next` to stop pagination
- `http.get` uses `base_url` and `auth` from the API dict automatically

## 5. Create SQL models

Use the two-model pattern — raw fetches, staging transforms:

**Raw model** — column names match the API:

```sql
-- models/raw/users.sql
-- @kind: append
-- @fetch
-- @incremental: updated_at

SELECT id::BIGINT AS id, name::VARCHAR AS name, email::VARCHAR AS email,
       updated_at::TIMESTAMP AS updated_at, metadata::JSON AS metadata
FROM my_api('users')
```

`@fetch` is required on every model that calls a lib in `FROM`. Every column must be cast — see [SQL schema contract](/reference/lib-functions/fetch-contract/#sql-schema-contract). The casts flow to the blueprint as [DuckDB-native types](/reference/lib-functions/fetch-contract/#typed-columns): `::JSON` becomes `"JSON"`, `::DECIMAL(18,3)` becomes `"DECIMAL(18,3)"`, `::TIMESTAMPTZ` becomes `"TIMESTAMPTZ"`.

**Staging model** — SQL transforms, casts types, expands JSON:

```sql
-- models/staging/users.sql
-- @kind: table

SELECT
    id::BIGINT AS id,
    name,
    email,
    updated_at::TIMESTAMP AS updated_at,
    metadata->>'$.city' AS city,
    (metadata->>'$.score')::INTEGER AS score
FROM raw.users
```

Don't transform in the raw model. Don't call APIs in the staging model. Each layer does one thing.

## 6. Run and verify

```bash
# First run — backfill
ondatrasql run

# Check data
ondatrasql sql "SELECT * FROM staging.users LIMIT 5"

# Second run — only new data
ondatrasql run
```

## 7. Add a push section (outbound)

SQL controls what gets pushed. Starlark controls how:

```python
API = {
    "base_url": "https://api.example.com",
    "auth": {"env": "MY_API_KEY"},
    "push": {
        "batch_size": 100,
        "batch_mode": "sync",
        "rate_limit": {"requests": 50, "per": "10s"},
    },
}

def push(rows=[], batch_number=1):
    results = {}
    for r in rows:
        ct = r["__ondatra_change_type"]
        key = str(int(r["__ondatra_rowid"])) + ":" + ct
        payload = {k: v for k, v in r.items() if not k.startswith("_")}

        if ct == "insert":
            resp = http.post("/v1/contacts", json=payload)
        elif ct == "update_postimage":
            resp = http.patch("/v1/contacts/" + str(r["id"]), json=payload)
        elif ct == "delete":
            resp = http.delete("/v1/contacts/" + str(r["id"]))
        elif ct == "update_preimage":
            results[key] = "ok"
            continue

        results[key] = "ok" if resp.ok else "error: " + resp.text
    return results
```

SQL model with `@push`:

```sql
-- models/sync/contacts.sql
-- @kind: merge
-- @unique_key: id
-- @push: my_api

SELECT
    id::BIGINT AS id,
    name::VARCHAR AS name,
    email::VARCHAR AS email
FROM staging.contacts
```

`@push` and `@fetch` cannot be combined on the same model. To push data fetched from one API to another, split into a `@fetch` model that materializes the raw rows and a downstream `@push` model that reads from the materialized table.

SQL can build nested JSON for APIs that expect structured payloads:

```sql
SELECT
    id::BIGINT AS id,
    json_object('name', name, 'email', email, 'tags', json_group_array(tag))::JSON AS properties
FROM staging.contacts JOIN staging.tags USING (id)
GROUP BY id, name, email
```

## Common patterns

### Incremental fetch

```sql
-- @kind: append
-- @fetch
-- @incremental: updated_at
SELECT id::BIGINT AS id, kind::VARCHAR AS kind, updated_at::TIMESTAMP AS updated_at
FROM my_api('events')
```

```python
def fetch(resource, page, is_backfill=True, last_value=""):
    params = {"limit": page.size, "cursor": page.cursor}
    if not is_backfill:
        params["since"] = last_value
    resp = http.get("/v1/" + resource, params=params)
    return {"rows": resp.json["items"], "next": resp.json.get("next")}
```

### Dict cursor for complex pagination state

When you need to carry multiple values between pages, return a dict — it passes through directly:

```python
next_cursor = {"url": fetch_url, "token": next_token, "series_idx": 3}
# ...
series_idx = page.cursor["series_idx"]
```

### Options via JSON arg

For API-specific configuration beyond columns:

```sql
FROM my_api('users', '{"filter": "active", "fields": ["name", "email"]}')
```

```python
API = {"fetch": {"args": ["resource", "options"]}}

def fetch(resource, options="", page=None):
    opts = json.decode(options) if options else {}
```

### Async fetch (report-style APIs)

For APIs that create reports asynchronously:

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
    resp = http.post("/reports", json={"columns": [c["name"] for c in columns]})
    return {"job_id": resp.json["id"]}

def check(job_ref):
    resp = http.get("/reports/" + job_ref["job_id"])
    if resp.json["status"] == "complete":
        return {"url": resp.json["result_url"]}
    return None  # keep polling

def fetch_result(result_ref, page):
    resp = http.get(result_ref["url"] + "?page=" + str(page.cursor or 0))
    return {"rows": resp.json["data"], "next": resp.json.get("next_page")}
```

### Finalize callback

```python
def finalize(succeeded, failed):
    if failed == 0:
        http.post("/v1/webhooks", json={"event": "sync_complete", "rows": succeeded})
```

## Reference

- [API Dict](/reference/lib-functions/api-dict/) — all fields and options
- [Fetch Contract](/reference/lib-functions/fetch-contract/) — complete function spec
- [Push Contract](/reference/lib-functions/push-contract/) — change types, batch modes, return values
- [Starlark Modules](/reference/lib-functions/starlark-modules/) — http, env, builtins, csv, xml, time
- [Blueprints](/blueprints/) — working examples for Mistral OCR, Riksbank, Google Ad Manager