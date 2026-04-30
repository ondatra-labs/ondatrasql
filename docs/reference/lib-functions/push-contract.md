---
date: "2026-04-20"
description: 'Complete reference for the push() function contract: internal fields, change types, return values, and batch modes.'
draft: false
title: Push Contract
weight: 14
---
Your `push()` function receives rows from the runtime and sends them to an external API. SQL controls what gets pushed — Starlark controls how.

## How push fits the architecture

SQL transforms data and declares what to sync:

```sql
-- models/sync/contacts.sql
-- @kind: merge
-- @unique_key: id
-- @sink: crm('workspace_123')

SELECT id, name, email FROM staging.contacts
```

The runtime materializes the SQL, detects changes via DuckLake snapshots, and calls `push()` with the delta rows. Starlark sends them to the API — nothing else.

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
| `key_columns` | list | Key column names — @unique_key for merge, @group_key for tracked (e.g. `["id"]` or `["region", "year"]`) |
| `columns` | list | Column names (sorted, excluding internal fields) |

Plus any sink args declared in `push.args` (see [API Dict — Sink args](/reference/lib-functions/api-dict/#push-section)).

## Sink args

Push can receive arguments from the `@sink` directive:

```python
API = {"push": {"args": ["spreadsheet_id", "range"]}}
```

```sql
-- @sink: gsheets('1DYJCOd...', 'Sheet1')
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

`scd2` is not supported with `@sink` — use `@kind: table` with `WHERE is_current = true` to push current state instead.

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

Not called if any batch failed. Declare only the parameters you need — the runtime filters automatically.

## Building outbound JSON in SQL

SQL controls the transformation — including building nested JSON for APIs that expect structured payloads:

```sql
-- models/sync/api_contacts.sql
-- @kind: merge
-- @unique_key: id
-- @sink: crm('workspace_123')

SELECT
    id,
    json_object(
        'name', first_name || ' ' || last_name,
        'email', email,
        'tags', json_group_array(tag)
    ) AS properties
FROM staging.contacts
JOIN staging.contact_tags USING (id)
GROUP BY id, first_name, last_name, email
```

Push receives `properties` as a JSON string. DuckDB handles the nesting — Starlark just sends it.

## Delivery guarantees

At-least-once. DuckLake commits before push. Failed events retry from Badger queue. The `_sync_acked` table records successful pushes — on restart, already-pushed batches are skipped.

Your push function must be idempotent. Batches are delivered in snapshot order, rows within a batch are unordered. In `sync` mode, failed rows retry independently. New columns from schema changes appear immediately in push kwargs.