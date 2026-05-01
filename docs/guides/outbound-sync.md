---
description: Push pipeline data to external APIs using @push. Step-by-step setup with batching, rate limiting, and per-row tracking.
draft: false
title: Sync Data to External APIs
weight: 9
---
Push data to external systems using `@push`. The runtime handles batching, rate limiting, per-row tracking, and crash recovery.

## 1. Create a lib function

Create a `.star` file in `lib/` with an API dict and a `push()` function:

```python
# lib/hubspot_push.star
API = {
    "base_url": "https://api.hubapi.com",
    "auth": {"env": "HUBSPOT_KEY"},
    "retry": 3,
    "rate_limit": {"requests": 100, "per": "10s"},
    "push": {
        "batch_size": 100,
        "batch_mode": "sync",
    },
}

def push(rows=[], batch_number=1):
    results = {}
    for r in rows:
        ct = r["__ondatra_change_type"]
        key = str(r["__ondatra_rowid"]) + ":" + ct
        payload = {k: v for k, v in r.items() if not k.startswith("_")}

        if ct == "insert":
            resp = http.post("/crm/v3/objects/contacts", json=payload)
        elif ct == "update_postimage":
            resp = http.patch("/crm/v3/objects/contacts/" + str(r["customer_id"]), json=payload)
        elif ct == "delete":
            resp = http.delete("/crm/v3/objects/contacts/" + str(r["customer_id"]))
        elif ct == "update_preimage":
            results[key] = "ok"  # skip
            continue

        results[key] = "ok" if resp.ok else "error: " + resp.text
    return results
```

Each row includes two internal fields:

- `__ondatra_rowid` — stable DuckLake row identifier
- `__ondatra_change_type` — raw DuckLake change type: `insert`, `update_preimage`, `update_postimage`, or `delete`

Return keys are composite: `"rowid:change_type"`. This ensures that `update_preimage` and `update_postimage` for the same row can have independent statuses. See the [Push Contract](/reference/lib-functions/push-contract/) for the complete reference.

## 2. Create a sync model

```sql
-- models/sync/hubspot.sql
-- @kind: merge
-- @unique_key: customer_id
-- @push: hubspot_push

SELECT
    customer_id::BIGINT AS customer_id,
    email::VARCHAR AS email,
    name::VARCHAR AS name,
    plan::VARCHAR AS plan
FROM mart.customers
WHERE active = true
```

`@push` works with `table`, `append`, `merge`, and `tracked` kinds.

## 3. Run

```bash
ondatrasql run
```

First run syncs all rows (change type `insert`). Subsequent runs sync only changed rows — merge produces `insert` for new rows and `update_postimage` for modified rows.

## Change types per kind

Each kind produces different change types from DuckLake's `table_changes()`:

| Kind | Materialization | Change types produced |
|---|---|---|
| `table` | TRUNCATE + INSERT | `delete` (truncate) + `insert` (new rows) |
| `append` | INSERT | `insert` only |
| `merge` | MERGE INTO | `insert`, `update_postimage` (+ `update_preimage`) |
| `tracked` | DELETE + INSERT | `delete` + `insert` (your push groups by key) |

`scd2` is not supported with `@push` — use `@kind: table` with `WHERE is_current = true` to push current state.

Your Starlark push function handles the logic. For example, tracked's DELETE+INSERT pattern means you group by key to distinguish updates from real deletes.

## Sync patterns

### Upsert (merge)

```sql
-- @kind: merge
-- @unique_key: customer_id
-- @push: crm_push
SELECT
    customer_id::BIGINT AS customer_id,
    email::VARCHAR AS email,
    plan::VARCHAR AS plan
FROM mart.customers
```

Push receives `insert` for new rows, `update_postimage` for changed rows. Map to POST and PATCH respectively.

### Group sync (tracked)

```sql
-- @kind: tracked
-- @group_key: invoice_id
-- @push: erp_push
SELECT
    invoice_id::BIGINT AS invoice_id,
    line_item::VARCHAR AS line_item,
    quantity::INTEGER AS quantity,
    price::DECIMAL(18,2) AS price
FROM mart.invoice_lines
```

Push receives `delete` + `insert` events. Group by `invoice_id` — if a key has both deletes and inserts, it's an update. If only deletes, the entity was removed.

### Append (new rows only)

```sql
-- @kind: append
-- @push: slack_post
SELECT
    message::VARCHAR AS message,
    channel::VARCHAR AS channel
FROM mart.notifications
```

Push always receives `insert` — append-only data.

### Full replace (table)

```sql
-- @kind: table
-- @push: sheets_push
SELECT
    region::VARCHAR AS region,
    revenue::DECIMAL(18,2) AS revenue
FROM mart.regional_summary
```

Push receives `delete` events (from truncate) and `insert` events (new data). For a simple full replace, filter to inserts only.

## Next steps

- [Push Contract](/reference/lib-functions/push-contract/) for internal fields, return values, batch modes, and crash recovery
- [API Dict](/reference/lib-functions/api-dict/) for all push configuration options
- [Create a Lib Function](/guides/create-a-blueprint/) for the full blueprint walkthrough
- [Directives](/reference/pipeline/directives/) for `@push` and `@group_key`