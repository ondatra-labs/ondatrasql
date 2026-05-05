---
date: "2026-05-01"
description: 'Complete reference for the push() function contract: internal fields, change types, return values, and batch modes.'
draft: false
title: Push Contract
weight: 14
---
Your `push()` function receives rows from the runtime and sends them to an external API. SQL controls what gets pushed — Starlark controls how.

## How push fits the architecture

SQL transforms data and declares what to sync via `@push`:

```sql
-- models/sync/contacts.sql
-- @kind: merge
-- @unique_key: id
-- @push: crm('workspace_123')

SELECT
    id::BIGINT AS id,
    name::VARCHAR AS name,
    email::VARCHAR AS email
FROM staging.contacts
```

The runtime materializes the SQL, detects changes via DuckLake snapshots, and calls `push()` with the delta rows. Starlark sends them to the API — nothing else.

## Strict @push contract {#strict-push-contract}

A `@push` model produces the shape sent to the API. The strict-push validator checks the **outermost SELECT projection** only:

| Rule | Why |
|---|---|
| `SELECT *` is rejected | Output schema must be explicit |
| Every outer projection must be wrapped in `CAST` | Types come from SQL |
| Every outer projection must carry `AS alias` | The alias is the field name sent to the API |
| Lib calls in `FROM` are rejected | That would be a `@fetch` model — read from a materialized fetch table instead |

Inside the model, SQL is fully free. JOIN, WHERE, GROUP BY, aggregates, DISTINCT, ORDER BY, CTEs, subqueries, derived expressions — all legitimate shape construction. The only cross-cutting rule that still applies is the [LIMIT/OFFSET ban](/reference/pipeline/directives/) on every pipeline model.

Three valid `@push` models:

**Simple projection:**

```sql
-- @kind: merge
-- @unique_key: Id
-- @push: salesforce('Contact')

SELECT
    id::BIGINT AS Id,
    email::VARCHAR AS Email__c,
    first_name::VARCHAR AS FirstName
FROM staging.contacts
```

**Filter and derived expression:**

```sql
-- @kind: append
-- @push: api('newsletter')

SELECT
    id::BIGINT AS subscriber_id,
    email::VARCHAR AS email,
    CONCAT(first_name, ' ', last_name)::VARCHAR AS display_name,
    LOWER(country_code)::VARCHAR AS region
FROM staging.subscribers
WHERE active = true
```

**JOIN and aggregate:**

```sql
-- @kind: table
-- @push: api('customer-summary')

SELECT
    c.id::BIGINT AS customer_id,
    c.email::VARCHAR AS email,
    SUM(o.total)::DECIMAL(18,2) AS lifetime_value,
    COUNT(o.id)::INTEGER AS order_count
FROM staging.customers c
LEFT JOIN staging.orders o ON c.id = o.customer_id
GROUP BY c.id, c.email
```

### Field-name mapping is the SQL alias

The SQL alias on a `@push` projection is the field name the row carries to the blueprint, and onward to the API. There is no `field_mapping` concept in the API dict; SQL aliases do the work.

```sql
-- @push: salesforce('Contact')
SELECT
    id::BIGINT AS Id,
    email::VARCHAR AS Email__c,
    custom_data::JSON AS CustomData__c
FROM staging.contacts
```

Salesforce custom fields, Google Sheets column headers, generic REST payloads — same model, different aliases, no blueprint changes.

## Function signature

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
| `columns` | list | Typed column dicts from the materialized DuckLake table — `[{"name": "id", "type": "BIGINT"}, ...]` |

Plus any push args declared in `push.args` (see [API Dict — Push args](/reference/lib-functions/api-dict/#push-section)).

### Typed columns {#typed-columns}

`columns` mirrors the @fetch shape but uses the **materialized table's schema** as its source — names are the SQL aliases (the keys the row dicts are keyed by), types are the real DuckDB types from the DuckLake table:

```python
columns = [
    {"name": "Id",            "type": "BIGINT"},
    {"name": "Email__c",      "type": "VARCHAR"},
    {"name": "CustomData__c", "type": "JSON"},
    {"name": "Tags",          "type": ["VARCHAR"]},
]
```

The shape is identical to [@fetch's typed columns](/reference/lib-functions/fetch-contract/#typed-columns) (DuckDB-native primitives, `["LIST"]`, `{"field": "TYPE"}`), so `lib_helpers.to_json_schema(col["type"])` works the same way for push as for fetch.

When the catalog lookup fails for a transient reason, the runtime falls back to deriving names from the row dicts (still a list of dicts, but without the `type` field). Push attempts continue rather than nacking the batch.

## Push args

Push can receive arguments from the `@push` directive:

```python
API = {"push": {"args": ["spreadsheet_id", "range"]}}
```

```sql
-- @push: gsheets('1DYJCOd...', 'Sheet1')
```

```python
def push(rows=[], spreadsheet_id="", range=""):
```

## Internal fields

Every row includes two internal fields from DuckLake. Strip fields starting with `_` before sending to external APIs.

| Field | Type | Description |
|---|---|---|
| `__ondatra_rowid` | int | Stable DuckLake row identifier. Persists across snapshots. |
| `__ondatra_change_type` | string | Raw change type from `table_changes()`. |

## Change types

| Change type | Meaning | Data source |
|---|---|---|
| `insert` | New row | Current table |
| `update_postimage` | Row after update | Current table |
| `update_preimage` | Row before update | Previous snapshot |
| `delete` | Row removed | Previous snapshot |

An UPDATE produces two rows with the same `__ondatra_rowid`: `update_preimage` (old data) and `update_postimage` (new data).

### Change types per kind

| Kind | SQL operation | Change types produced |
|---|---|---|
| `table` | TRUNCATE + INSERT | `delete` + `insert` |
| `append` | INSERT | `insert` |
| `merge` | MERGE INTO | `insert`, `update_postimage` (+ `update_preimage`) |
| `tracked` | DELETE + INSERT per group | `delete` + `insert` |

`scd2` is not supported with `@push` — use `@kind: table` with `WHERE is_current = true` to push current state instead.

Your push function decides what to do with each change type. The runtime does no filtering.

## Return value

Return a dict with a status per row. Keys are `"rowid:change_type"`:

```python
key = str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]
```

| Status | Meaning | Runtime behavior |
|---|---|---|
| `"ok"` | Delivered | Acked — removed from queue |
| `"warn: message"` | Delivered with warning | Acked + warning logged |
| `"error: message"` | Temporary failure | Nacked — requeued for retry |
| `"reject: message"` | Permanent failure | Dead-lettered — never retried |

Every row in the batch must have a status. Missing keys cause an error.

```python
def push(rows=[], batch_number=1):
    payload = [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]
    resp = http.post("/api/batch", json=payload)
    if not resp.ok:
        return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "error: " + resp.text for r in rows}
    return {str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]: "ok" for r in rows}
```

## Batch modes

Configured in the API dict under `push`:

### sync (default)

Per-row status. Rows succeed and fail independently.

```python
API = {"push": {"batch_size": 50, "batch_mode": "sync"}}
```

### atomic

All-or-nothing. Return `None` for success, call `fail()` to abort.

```python
API = {"push": {"batch_size": 100, "batch_mode": "atomic"}}
```

```python
def push(rows=[], batch_number=1):
    resp = http.post("/api/batch", json=rows)
    if not resp.ok:
        fail("batch failed: " + resp.text)
```

### async

Job-based with polling. Return a job reference, implement `poll()`:

```python
API = {"push": {"batch_mode": "async", "poll_interval": "30s", "poll_timeout": "1h"}}
```

```python
def push(rows=[], batch_number=1):
    resp = http.post("/api/bulk-job", json=rows)
    return {"job_id": resp.json["id"]}

def poll(job_ref):
    resp = http.get("/api/bulk-job/" + job_ref["job_id"])
    if resp.json["status"] == "complete":
        return {"done": True, "per_row": {
            rid + ":" + ct: "ok" for rid, ct in resp.json["results"]
        }}
    if resp.json["status"] == "failed":
        fail("job failed: " + resp.json["error"])
    return {"done": False}
```

`poll()` receives `job_ref` as a kwarg. Return format:

| Field | Type | Description |
|---|---|---|
| `done` | bool | `True` when job is complete, `False` to keep polling |
| `per_row` | dict | Per-row status dict (same format as sync mode). Only required when `done` is `True`. |

## Handling change types

Route change types to appropriate API operations:

```python
def push(rows=[], batch_number=1, kind="table", key_columns=[]):
    results = {}
    for r in rows:
        ct = r["__ondatra_change_type"]
        key = str(r["__ondatra_rowid"]) + ":" + ct
        payload = {k: v for k, v in r.items() if not k.startswith("_")}

        if ct == "insert":
            resp = http.post("/api/records", json=payload)
        elif ct == "update_postimage":
            resp = http.patch("/api/records/" + str(r["id"]), json=payload)
        elif ct == "delete":
            resp = http.delete("/api/records/" + str(r["id"]))
        elif ct == "update_preimage":
            results[key] = "ok"  # skip — or use for audit logging
            continue

        results[key] = "ok" if resp.ok else "error: " + resp.text
    return results
```

## Finalize

Optional. Called once after all batches succeed. Receives kwargs:

```python
def finalize(succeeded=0, failed=0):
    if failed == 0:
        http.post("/webhooks", json={"event": "sync_complete", "rows": succeeded})
```

Not called if any batch failed OR any run-level error occurred — that includes per-row push failures, state-store ack failures (rows delivered but the queue couldn't be cleaned up), and context cancellation. The gate is "every batch succeeded AND no syncErrors were collected during the run", so finalize won't fire while a recoverable inconsistency exists; it'll fire on the next run after retries clear it. Declare only the parameters you need — the runtime filters automatically.

## Building outbound JSON in SQL

SQL controls the transformation — including building nested JSON for APIs that expect structured payloads:

```sql
-- models/sync/api_contacts.sql
-- @kind: merge
-- @unique_key: id
-- @push: crm('workspace_123')

SELECT
    id::BIGINT AS id,
    json_object(
        'name', first_name || ' ' || last_name,
        'email', email,
        'tags', json_group_array(tag)
    )::JSON AS properties
FROM staging.contacts
JOIN staging.contact_tags USING (id)
GROUP BY id, first_name, last_name, email
```

Push receives `properties` as a JSON string. DuckDB handles the nesting — Starlark just sends it.

## Delivery guarantees

At-least-once. DuckLake commits before push. Failed events retry from the state-store queue (`.ondatra/state.duckdb`). The `_sync_acked` table transiently records successful pushes for crash-safety — entries are removed once the state-store ack confirms, so the table is normally empty between runs and only populated when a run was killed mid-ack. On restart, surviving rows let the next run skip already-pushed batches before retrying.

Your push function must be idempotent. Batches are delivered in snapshot order, rows within a batch are unordered. In `sync` mode, failed rows retry independently. New columns from schema changes appear immediately in push kwargs.
