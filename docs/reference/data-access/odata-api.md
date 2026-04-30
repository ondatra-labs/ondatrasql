---
description: Complete OData v4.01 API reference for OndatraSQL. Endpoints, filter functions, aggregation, navigation properties, batch requests, server-driven paging, change tracking, and data types.
draft: false
title: OData API
weight: 55
---
Complete OData v4.01 query reference. For setup and tool connections, see [Serve Data via OData](/guides/serve-data-via-odata/).

## Endpoints

| Method | Path | Response |
|---|---|---|
| `GET` | `/odata` | Service document (lists all exposed entities) |
| `GET` | `/odata/$metadata` | CSDL XML (schema for all entities) |
| `GET` | `/odata/{entity}` | Query collection |
| `GET` | `/odata/{entity}(key)` | Single entity by key (requires `@expose <column>`) |
| `GET` | `/odata/{entity}/{N}` | Single entity at ordinal `N` in default key ordering. See [Positional access](#positional-access). |
| `GET` | `/odata/{entity}/$count` | Row count (plain text) |
| `GET` | `/odata/$entity?$id=<url>` | Dereference a canonical entity-id URL (the `@odata.id` value) |
| `GET` | `/odata/$crossjoin(A,B,...)` | Cross-entity-set query. See [Cross-join](#cross-join). |
| `POST` | `/odata/$batch` | JSON batch (multiple requests in one call) |

Entity names use underscores: `mart.revenue` becomes `mart_revenue` in the URL.

### Response annotations

Every entity response carries OData v4.01 annotations alongside the row data:

| Annotation | When | Description |
|---|---|---|
| `@odata.context` | Always | Schema reference. Collection: `<baseURL>/odata/$metadata#<entity>`. Single entity: `…#<entity>/$entity`. Delta: `…#<entity>/$delta`. Cross-join: `…#Collection(Edm.ComplexType)`. |
| `@odata.id` | Per row, when entity has `@expose <key>` | Canonical URL of the row. Dereferenceable via `/odata/$entity?$id=<url>`. |
| `@odata.count` | When `$count=true` (first page only) | Total row count, independent of `$top`/`$skip`. Not repeated on `@odata.nextLink` follow-ups. |
| `@odata.deltaLink` | When the query has no `$filter`/`$apply`/`$compute`/`$search`/`$expand` and the entity has `@expose <key>`, AND the response was not capped by paging | Stateless, signed URL that returns only what changed since this snapshot. See [Delta tracking](#delta-tracking). |
| `@odata.nextLink` | When the response was capped by the server-side page size and there are more rows to fetch | Stateless, signed URL the client follows to fetch the next page. See [Server-driven paging](#server-driven-paging). |

## Query Parameters

| Parameter | Example | Effect |
|---|---|---|
| `$select` | `$select=customer,amount` | Return only these columns |
| `$filter` | `$filter=amount gt 100` | Filter rows |
| `$orderby` | `$orderby=amount desc` | Sort results |
| `$top` | `$top=10` | Limit rows |
| `$skip` | `$skip=20` | Offset |
| `$count` | `$count=true` | Include total count in response |
| `$search` | `$search=alice` | Full-text search across all string columns |
| `$expand` | `$expand=raw_orders` | Inline related entities |
| `$apply` | `$apply=aggregate(amount with sum as total)` | Aggregation |
| `$compute` | `$compute=amount mul 2 as doubled` | Computed properties (col op literal as alias) |
| `$format` | `$format=json` | Response format. Accepts `json`, `application/json`, and `application/json` with parameters (e.g. `application/json;odata.metadata=minimal`). Anything else returns 406. |
| `$deltatoken` | `$deltatoken=<opaque>` | Returned in `@odata.deltaLink` URLs; opaque to the client. Asks the server for the diff since the snapshot embedded in the token. See [Delta tracking](#delta-tracking). |
| `$skiptoken` | `$skiptoken=<opaque>` | Returned in `@odata.nextLink` URLs; opaque to the client. Server-driven paging cursor. See [Server-driven paging](#server-driven-paging). |

### Filter Operators

| Operator | Example |
|---|---|
| `eq`, `ne` | `customer eq 'Alice'` |
| `gt`, `ge`, `lt`, `le` | `amount gt 100` |
| `and`, `or`, `not` | `amount gt 100 and not (active eq false)` |
| `in` | `customer in ('Alice','Bob')` |
| `has` | Enum flag bitwise check |
| `eq null` | `order_date eq null` |

### Filter Functions

| Category | Functions |
|---|---|
| String | `contains`, `startswith`, `endswith`, `tolower`, `toupper`, `length`, `trim`, `concat`, `indexof`, `substring` |
| Date | `year`, `month`, `day`, `hour`, `minute`, `second`, `now`, `date`, `time` |
| Math | `round`, `floor`, `ceiling` |
| Arithmetic | `add`, `sub`, `mul`, `div`, `mod` |
| Type | `cast`, `isof` |
| Pattern | `matchesPattern` (regex) |
| Geo | `geo.distance`, `geo.intersects`, `geo.length` (requires DuckDB spatial) |
| Other | `case` expressions, `fractionalseconds`, `totaloffsetminutes`, `totalseconds`, `maxdatetime`, `mindatetime` |

### $apply Aggregation

```bash
# Sum
$apply=aggregate(amount with sum as total)

# Group by + aggregate
$apply=groupby((customer),aggregate(amount with sum as total))

# Supported functions: sum, avg, min, max, count, countdistinct
```

### $expand Navigation

Relationships between `@expose` models are auto-discovered when one entity has a column matching another entity's key column (FK→PK pattern). Both models must have `@expose` with explicit key columns.

```bash
# Inline related orders for each customer
curl "http://localhost:8090/odata/raw_customers?\$expand=raw_orders"
```

Navigation properties appear in `$metadata` automatically. The response nests related entities inline.

### $batch

Multiple requests in one HTTP call (JSON batch format, OData v4.01):

```bash
curl -X POST http://localhost:8090/odata/\$batch \
  -H "Content-Type: application/json" \
  -d '{"requests":[
    {"id":"1","method":"GET","url":"mart_revenue?$top=5"},
    {"id":"2","method":"GET","url":"mart_revenue/$count","headers":{"Accept":"application/json"}}
  ]}'
```

Per-request `headers` are copied onto the inner request handler (JSON Format §19.1.3). Use this to vary `Accept`, `Prefer`, etc. across sub-requests in a single batch.

**`atomicityGroup` is rejected** with `501 Not Implemented` per sub-request. The field requires transactional rollback semantics that don't apply to a read-only server (JSON Format §19.1.4). `dependsOn` is accepted but no-op'd: read-only sub-requests don't share state, so dependency ordering doesn't change observable behavior.

### Delta tracking {#delta-tracking}

Eligible collection responses include `@odata.deltaLink` — a stateless, signed URL that returns only what changed since the response was issued. Clients save the link and call it later instead of re-fetching the full collection.

```bash
# Initial query
curl http://localhost:8090/odata/mart_orders
# {
#   "@odata.context": ".../$metadata#mart_orders",
#   "value": [...],
#   "@odata.deltaLink": "http://.../odata/mart_orders?$deltatoken=eyJzIjox..."
# }

# Some time later, follow the link
curl 'http://localhost:8090/odata/mart_orders?$deltatoken=eyJzIjox...'
# {
#   "@odata.context": ".../$metadata#mart_orders/$delta",
#   "value": [
#     {"@odata.id": ".../mart_orders(7)", "id": 7, "amount": 50},
#     {"@odata.id": ".../mart_orders(2)", "id": 2, "amount": 200},
#     {"@removed": {"reason": "deleted"}, "@odata.id": ".../mart_orders(3)", "id": 3}
#   ],
#   "@odata.deltaLink": "http://.../odata/mart_orders?$deltatoken=eyJzIjoy..."
# }
```

Each response includes a fresh `@odata.deltaLink` that the client should use next.

**Eligibility.** A collection response only includes `@odata.deltaLink` when:

- the entity has `@expose <key>` (the response needs `@odata.id` to identify removed rows)
- the request has none of `$filter`, `$apply`, `$compute`, `$search`, `$expand`
- the server has a delta-link signing key configured (see below)

If any of these are missing, the response is normal but the link is omitted.

`$select` and `$orderby` are allowed on the original query and ride along in the deltaLink URL. `$select` is applied to the delta rows (same projection as the original response). `$orderby` is preserved for token validation only — delta rows come back in change order, not the original sort order.

**Token format.** The `$deltatoken` is opaque to the client — it carries the snapshot id, the entity name, the issued-at timestamp, the signing key id, and a hash of the original query options, all signed with HMAC-SHA256. The client never inspects or constructs it.

**Failure modes.** A `$deltatoken` request returns:

- `400 Bad Request DeltaLinkInvalid` — token signature is wrong (tampered, signed by a different server / different key, or signed by a key that has since been removed from the keyset), or token is for a different entity than the route. Spec Part 1 §11.4 reserves 410 for resources that no longer exist; tampered tokens are client errors.
- `410 Gone DeltaLinkExpired` — token age exceeds `ONDATRA_ODATA_DELTA_MAX_AGE` (only emitted when max-age is configured)
- `410 Gone DeltaLinkFilterChanged` — query options have changed since the link was issued; the client must re-issue the original query
- `400 Bad Request` — the delta request itself uses `$filter`, `$apply`, `$compute`, `$search`, or `$expand`

In all failure cases the client should re-run the original query and start a new delta chain from the fresh `@odata.deltaLink`.

**Signing key.** Set `ONDATRA_ODATA_DELTA_KEY` to a hex-encoded key for stable tokens that survive process restarts. The minimum is 16 bytes (32 hex chars); 32 bytes (64 hex chars) is recommended. Without it the server generates an ephemeral key on startup and logs that existing `@odata.deltaLink` URLs will be invalidated on the next restart.

```bash
# Generate a key (one-time)
openssl rand -hex 32
# 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef

export ONDATRA_ODATA_DELTA_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
ondatrasql odata 8090
```

**Key rotation.** `ONDATRA_ODATA_DELTA_KEY` accepts a comma-separated list of `kid:hex` pairs for zero-downtime rotation. The first entry signs new tokens; every entry is accepted during verification. To rotate without invalidating outstanding deltaLinks:

```bash
# Phase 1: deploy with both keys, new key first
export ONDATRA_ODATA_DELTA_KEY=v2:fedcba9876543210...,v1:0123456789abcdef...

# Phase 2: after max-age elapses (or all clients have re-issued),
# drop the old key
export ONDATRA_ODATA_DELTA_KEY=v2:fedcba9876543210...
```

Bare hex without a kid (the legacy v0.26.0 form) is accepted as a single entry with empty kid. Mixing forms in the list is allowed.

**Token expiry.** Set `ONDATRA_ODATA_DELTA_MAX_AGE` to a Go duration string to reject tokens older than the configured window. The check is opt-in — when unset, tokens are valid as long as the underlying DuckLake snapshot is still retained.

```bash
export ONDATRA_ODATA_DELTA_MAX_AGE=168h    # 7 days
export ONDATRA_ODATA_DELTA_MAX_AGE=24h
export ONDATRA_ODATA_DELTA_MAX_AGE=30m
```

The duration uses Go's syntax — `h`/`m`/`s` are supported, `d` is not. An unparseable value refuses to start the server (fail-closed: a typoed env shouldn't silently disable the security knob you configured).

**Connection pool.** OData requests are served from a fixed-size read-only connection pool, so concurrent BI clients don't serialise on the writer's mutex. Default size is `min(GOMAXPROCS, 8)`. Override with `ONDATRA_ODATA_POOL_SIZE`:

```bash
export ONDATRA_ODATA_POOL_SIZE=4
```

The DuckLake catalog is ATTACHed once at the DuckDB-instance level by the pipeline (so all conns on the same process see it). Each pool conn additionally runs `LOAD ducklake` + `USE <catalog>` + `SET search_path` at startup so it resolves table names the same way the writer does. The writer's connection is unaffected — the pipeline runner keeps its dedicated conn.

**Backed by DuckLake.** The runtime wraps DuckLake's `table_changes(table, from_snapshot, to_snapshot)` — the delta is the literal CDC diff between the snapshot the client last saw and the current one.

### Server-driven paging {#server-driven-paging}

Collection responses are capped at a server-controlled page size. When more rows exist beyond the cap, the response includes `@odata.nextLink` — a stateless, signed URL the client follows to fetch the next page.

```bash
# First page
curl 'http://localhost:8090/odata/mart_orders?$orderby=id'
# {
#   "@odata.context": ".../$metadata#mart_orders",
#   "value": [/* up to PAGE_SIZE rows */],
#   "@odata.count": 10000,           # only on first page when $count=true
#   "@odata.nextLink": "http://.../odata/mart_orders?$orderby=id&$skiptoken=eyJl..."
# }

# Follow the link
curl 'http://.../odata/mart_orders?$orderby=id&$skiptoken=eyJl...'
# next page, possibly another @odata.nextLink, until the last page
```

**Page size.** Default is `10000`. Override with `ONDATRA_ODATA_PAGE_SIZE` (positive integer; unparseable values refuse to start the server):

```bash
export ONDATRA_ODATA_PAGE_SIZE=5000
```

**Client-side `$top` interaction.**

- `$top` ≤ page size: client cap honored, no `@odata.nextLink` emitted (response holds at most `$top` rows)
- `$top` > page size or no `$top`: server-side cap applies, `@odata.nextLink` emitted if there are more rows

**Token format.** `$skiptoken` is opaque — it carries the entity name, the cumulative offset reached, the issued-at timestamp, the signing key id, and a hash of the query options, all signed with HMAC-SHA256. Reuses the same keyset as `@odata.deltaLink` (`ONDATRA_ODATA_DELTA_KEY`).

**Failure modes.**

- `400 Bad Request SkipTokenInvalid` — token signature is wrong, malformed, or for a different entity than the route
- `410 Gone SkipTokenExpired` — token age exceeds `ONDATRA_ODATA_DELTA_MAX_AGE` (only when max-age is configured)
- `410 Gone SkipTokenFilterChanged` — query options have changed since the link was issued

`@odata.count` is only emitted on the first page (no `$skiptoken` in the request). Subsequent pages skip the `COUNT(*)` query — clients that want the total fetch it once on page 1.

`@odata.deltaLink` and `@odata.nextLink` are mutually exclusive in a single response: deltaLink describes "track changes from this snapshot onward" while nextLink describes "the rest of this page chain". Emitting both invites confusion about which to poll. The server emits `@odata.deltaLink` only on responses that are not capped by paging.

### Cross-join {#cross-join}

`/odata/$crossjoin(A,B,...)` returns a cross product of two or more exposed entities. Use `$filter` with qualified column refs to express the join condition:

```bash
curl "http://localhost:8090/odata/\$crossjoin(raw_customers,raw_orders)?\$filter=raw_customers/id%20eq%20raw_orders/customer_id"
```

```json
{
  "@odata.context": ".../$metadata#Collection(Edm.ComplexType)",
  "value": [
    {
      "raw_customers": {"id": 1, "name": "Alice"},
      "raw_orders":    {"id": 101, "customer_id": 1, "amount": 50}
    }
  ]
}
```

Each row is a flat object with one nested object per source entity-set, keyed by entity-set name.

**Why this exists.** `@expose` marks tables available; it does not predefine joins. Without `$crossjoin`, the only way to deliver joined data is to add a JOIN-containing SQL model and `@expose` it — which fixes the join shape at the operator level. `$crossjoin` lets clients pick the join condition at request time without operator action.

**Supported in `$crossjoin`.**

- `$filter` with qualified refs (`A/col eq B/col`) for the join condition and additional row filters
- `$top` / `$skip` for paging
- `$count=true` for total row count of the filtered cross product

**Not supported in `$crossjoin`.** `$select`, `$orderby`, `$expand`, `$compute`, `$apply`, `$search` return 400 Bad Request. Deep navigation paths (more than 2 segments, e.g. `A/B/col`) are also rejected.

### Positional access {#positional-access}

`/odata/{entity}/{N}` returns the entity at zero-based ordinal `N` in the default key ordering. Sugar over `$orderby=key + $skip=N + $top=1`:

```bash
curl http://localhost:8090/odata/mart_orders/0    # first row
curl http://localhost:8090/odata/mart_orders/9    # tenth row
curl http://localhost:8090/odata/mart_orders/9999 # 404 if past the end
```

Non-numeric segments (`/Entity/abc`) and negative ordinals return 404 — those paths don't match the OData ordinal form.

### Examples

**Collection and entity:**

```bash
curl "http://localhost:8090/odata/mart_revenue"
curl "http://localhost:8090/odata/mart_revenue(1)"
curl "http://localhost:8090/odata/mart_revenue?\$select=customer,amount"
```

**Filtering:**

```bash
curl "http://localhost:8090/odata/mart_revenue?\$filter=amount%20gt%20100"
curl "http://localhost:8090/odata/mart_revenue?\$filter=customer%20in%20('Alice','Bob')"
curl "http://localhost:8090/odata/mart_revenue?\$filter=contains(customer,'Ali')"
curl "http://localhost:8090/odata/mart_revenue?\$filter=year(order_date)%20eq%202026"
curl "http://localhost:8090/odata/mart_revenue?\$filter=amount%20add%2050%20gt%201000"
```

**Sorting, pagination, count:**

```bash
curl "http://localhost:8090/odata/mart_revenue?\$orderby=amount%20desc&\$top=10"
curl "http://localhost:8090/odata/mart_revenue?\$top=10&\$skip=20"
curl "http://localhost:8090/odata/mart_revenue?\$filter=amount%20gt%20100&\$count=true"
```

**Search, compute, expand:**

```bash
curl "http://localhost:8090/odata/mart_revenue?\$search=alice"
curl "http://localhost:8090/odata/mart_revenue?\$compute=amount%20mul%202%20as%20doubled"
curl "http://localhost:8090/odata/raw_customers?\$expand=raw_orders"
```

**Aggregation:**

```bash
curl "http://localhost:8090/odata/mart_revenue?\$apply=aggregate(amount%20with%20sum%20as%20total)"
curl "http://localhost:8090/odata/mart_revenue?\$apply=groupby((customer),aggregate(amount%20with%20sum%20as%20total))"
```

## Data Types

| DuckDB | OData | JSON |
|---|---|---|
| `VARCHAR` | Edm.String | `"text"` |
| `TINYINT` | Edm.Byte | `1` |
| `SMALLINT` | Edm.Int16 | `100` |
| `INTEGER` | Edm.Int32 | `42` |
| `BIGINT` | Edm.Int64 | `9999999999` |
| `DOUBLE` | Edm.Double | `3.14` |
| `FLOAT` | Edm.Single | `1.5` |
| `DECIMAL` | Edm.Decimal | `150.99` |
| `BOOLEAN` | Edm.Boolean | `true` |
| `DATE` | Edm.Date | `"2026-01-15"` |
| `TIMESTAMP` | Edm.DateTimeOffset | `"2026-01-15T08:30:00Z"` |

NULL values are JSON `null`. Empty strings are preserved as `""`.

## Security

- **Read-only**: only SELECT queries, no writes
- **Column validation**: queries only access columns that exist in the schema
- **No SQL injection**: column names validated against `information_schema`, string values escaped
- **Localhost only**: binds to `127.0.0.1` by default

## Scope

Read-only OData v4.01. Server emits `OData-Version: 4.01` and CSDL `Version="4.01"`.

Supported query options: `$filter`, `$select`, `$orderby`, `$top`, `$skip`, `$count`, `$expand` (one level), `$apply`, `$compute`, `$search`, `$format`, `$deltatoken`, `$skiptoken`. Supported routes: collection, single-entity-by-key, positional access (`/Entity/N`), `$entity?$id=`, `$count`, `$crossjoin`, `$batch` (JSON format), `$metadata`.

Not supported:

- Write operations (POST, PUT, DELETE on entity sets)
- Complex type functions or actions
- Deep `$expand` (nested navigation)
- Deep navigation paths in `$filter` (more than 2 segments)
- `$crossjoin` with `$select`, `$orderby`, `$expand`, `$compute`, `$apply`, `$search`
- Multipart `$batch` (only JSON batch is supported)
- `$format=xml` / Atom serialization
- `Prefer: respond-async`

For write access to external systems, use `@sink`.
