---
date: "2026-04-20"
description: API dict reference for lib/ blueprints. Unified configuration for fetch, push, HTTP, auth, and rate limiting.
draft: false
title: API Dict
weight: 10
---
The API dict declares the complete contract for a lib function. One dict per file. All values must be literals — no variables, no concatenation. The runtime parses it as AST without executing code.

```python
API = {
    # Shared config (injected into all http.* calls)
    "base_url": "https://api.example.com",
    "auth": {"env": "API_KEY"},
    "headers": {"Accept": "application/json"},
    "timeout": 30,
    "retry": 3,
    "backoff": 1,
    "rate_limit": {"requests": 100, "per": "10s"},

    # Inbound
    "fetch": {
        "args": ["resource"],
        "page_size": 100,
    },

    # Outbound
    "push": {
        "args": ["spreadsheet_id", "range"],
        "batch_size": 100,
        "batch_mode": "sync",
    },
}
```

## Top-level config (shared)

Injected into all `http.*` calls by both `fetch()` and `push()`.

| Field | Type | Default | Description |
|---|---|---|---|
| `base_url` | string | none | Prepended to relative URLs in `http.*` calls |
| `auth` | dict | none | Auth injection (see [Auth patterns](#auth-patterns)) |
| `headers` | dict | none | Default headers merged into every request |
| `timeout` | int | 30 | Request timeout in seconds |
| `retry` | int | 0 | Number of retries on 5xx/429 |
| `backoff` | int | 1 | Initial backoff in seconds (exponential) |
| `rate_limit` | dict | none | Proactive rate limiting: `{"requests": N, "per": "Ns"}` |

Per-call kwargs in `http.get(url, timeout=60)` override these defaults.

## Auth patterns

The runtime handles token refresh, header injection, and caching. Auth is only injected when the caller does NOT set `auth=` in the `http.*` call.

### API key from .env

```python
"auth": {"env": "API_KEY"}                              # → Authorization: Bearer <value>
"auth": {"env": "API_KEY", "header": "X-Api-Key"}       # → X-Api-Key: <value>
"auth": {"env": "API_KEY", "param": "api_key"}           # → ?api_key=<value>
```

### Google service account

```python
"auth": {
    "service_account": {"env": "GAM_KEY_FILE"},
    "scope": "https://www.googleapis.com/auth/admanager",
}
```

`service_account` resolves the key file path from `.env` via the `{"env": "..."}` pattern. The runtime handles JWT signing and token refresh automatically.

### OAuth2 provider (browser-based SaaS APIs)

```python
"auth": {"provider": "hubspot"}
```

Register with `ondatrasql auth <provider>`. Tokens refresh automatically.

### Basic auth

```python
"auth": {
    "user": {"env": "API_USER"},
    "pass": {"env": "API_PASS"},
}
```

Resolves both values from `.env` and sends `Authorization: Basic <base64>`.

### No authentication

Omit `auth` entirely:

```python
API = {
    "base_url": "https://api.riksbank.se/swea/v1",
    "fetch": {"args": ["series"]},
}
```

## Fetch section

| Field | Type | Default | Description |
|---|---|---|---|
| `args` | list | `[]` | Parameter names passed from SQL |
| `page_size` | int | 0 | Rows per page (0 = single call) |
| `supported_columns` | list | none | Optional whitelist of valid column names. Validated at startup. |
| `supported_kinds` | list | none | Optional whitelist of valid model kinds. Validated at startup. |

SQL controls the schema — the runtime extracts column names and [DuckDB-native types](/reference/lib-functions/fetch-contract/#typed-columns) from the SELECT via DuckDB AST and passes them to `fetch()` as the `columns` kwarg.

If `supported_columns` is declared and SQL requests an unknown column, the model fails at parse time. If not declared, any column name is accepted.

### Args from SQL

```python
API = {"fetch": {"args": ["resource", "options"]}}
```

```sql
SELECT id::BIGINT AS id, name::VARCHAR AS name FROM my_api('users', '{"filter": "active"}')
```

Args are positional strings. For structured configuration, pass JSON and decode in Starlark:

```python
def fetch(resource, options="", page=None):
    opts = json.decode(options) if options else {}
```

### Async fetch

For APIs with asynchronous report generation (submit → poll → fetch results):

```python
"fetch": {
    "args": ["options"],
    "page_size": 10000,
    "async": True,
    "poll_interval": "5s",
    "poll_timeout": "5m",
    "poll_backoff": 2,
},
```

| Field | Type | Default | Description |
|---|---|---|---|
| `async` | bool | `False` | Enable async fetch mode |
| `poll_interval` | string | `"5s"` | Minimum wait between `check()` calls |
| `poll_timeout` | string | `"5m"` | Max total poll duration before timeout error |
| `poll_backoff` | int | 1 | Backoff multiplier for poll interval (e.g. 2 = 5s, 10s, 20s, capped at 30s) |

Async mode requires `submit()`, `check()`, and `fetch_result()` functions instead of `fetch()`. See [Fetch Contract — Async fetch](/reference/lib-functions/fetch-contract/#async-fetch).

## Push section

| Field | Type | Default | Description |
|---|---|---|---|
| `args` | list | `[]` | Parameter names from `@push: name('arg1', 'arg2')` |
| `supported_kinds` | list | none | Optional whitelist of valid model kinds. Validated at startup. |
| `batch_size` | int | 1 | Rows per `push()` call |
| `batch_mode` | string | `"sync"` | `"sync"`, `"atomic"`, or `"async"` |
| `max_concurrent` | int | 1 | Parallel batch workers |
| `rate_limit` | dict | inherited | Per-direction override |
| `poll_interval` | string | `"30s"` | Async polling interval |
| `poll_timeout` | string | `"1h"` | Async polling timeout |

### Push args

```python
API = {"push": {"args": ["spreadsheet_id", "range"]}}
```

```sql
-- @push: gsheets('1DYJCOd...', 'Sheet1')
```

Args are positional strings from the `@push` directive. Mapped to push kwargs by name:

```python
def push(rows=[], spreadsheet_id="", range=""):
```

### Push function signature

```python
def push(rows=[], batch_number=1, kind="table", key_columns=[], columns=[]):
```

All parameters are kwargs. Declare only what your blueprint needs — the runtime filters automatically.

| Kwarg | Type | Description |
|---|---|---|
| `rows` | list | Row dicts with column values + internal fields |
| `batch_number` | int | 1-based batch counter |
| `kind` | string | Model kind: `table`, `append`, `merge`, `tracked` |
| `key_columns` | list | Key column names — `@unique_key` for merge, `@group_key` for tracked (e.g. `["id"]` or `["region", "year"]`) |
| `columns` | list | Typed column dicts — `[{"name": "id", "type": "BIGINT"}, ...]` from the materialized DuckLake table schema. See [Push Contract — Typed columns](/reference/lib-functions/push-contract/#typed-columns). |

Plus any push args declared in `push.args`.

### Batch modes

| Mode | Return | Behavior |
|---|---|---|
| `sync` | `{"rowid:change_type": status}` | Per-row ack/nack |
| `atomic` | `None` | All-or-nothing |
| `async` | `{"job_id": ...}` | Job-based polling via `poll()` |

## Literal values only

The API dict is parsed as Starlark AST — not executed. All values must be literals:

```python
# Works — literal values
API = {"base_url": "https://api.example.com", "timeout": 30}

# Does NOT work — variable reference
BASE = "https://api.example.com"
API = {"base_url": BASE}

# Does NOT work — concatenation
API = {"base_url": "https://" + HOST}
```

This is by design. The dict is pure configuration — readable, validatable, and inspectable without running code. Dynamic values belong in the `fetch()` or `push()` function.

## Validation

The runtime validates at startup:

- `fetch()` params checked against `args` list
- `push()` must accept `rows` parameter
- `batch_mode` must be `"sync"`, `"atomic"`, or `"async"`
- `rate_limit.per` must be valid duration
- `rate_limit.requests` must be > 0
- `async: True` requires `submit()`, `check()`, `fetch_result()` — not `fetch()`
- `scd2` kind with `@push` is not allowed

See [Fetch Contract](/reference/lib-functions/fetch-contract/) and [Push Contract](/reference/lib-functions/push-contract/) for the complete function specs.