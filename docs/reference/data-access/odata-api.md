---
description: Complete OData v4 API reference for OndatraSQL. Endpoints, filter functions, aggregation, navigation properties, batch requests, and data types.
draft: false
title: OData API
weight: 55
---
Complete OData v4 query reference. For setup and tool connections, see [Serve Data via OData](/guides/serve-data-via-odata/).

## Endpoints

| Method | Path | Response |
|---|---|---|
| `GET` | `/odata` | Service document (lists all exposed entities) |
| `GET` | `/odata/$metadata` | CSDL XML (schema for all entities) |
| `GET` | `/odata/{entity}` | Query collection |
| `GET` | `/odata/{entity}(key)` | Single entity by key (requires `@expose <column>`) |
| `GET` | `/odata/{entity}/$count` | Row count (plain text) |
| `GET` | `/odata/$entity?$id=<url>` | Dereference a canonical entity-id URL (the `@odata.id` value) |
| `POST` | `/odata/$batch` | JSON batch (multiple requests in one call) |

Entity names use underscores: `mart.revenue` becomes `mart_revenue` in the URL.

### Response annotations

Every entity response carries OData v4.01 annotations alongside the row data:

| Annotation | When | Description |
|---|---|---|
| `@odata.context` | Always | Schema reference. Collection: `<baseURL>/odata/$metadata#<entity>`. Single entity: `…#<entity>/$entity`. Delta: `…#<entity>/$delta`. |
| `@odata.id` | Per row, when entity has `@expose <key>` | Canonical URL of the row. Dereferenceable via `/odata/$entity?$id=<url>`. |
| `@odata.count` | When `$count=true` | Total row count, independent of `$top`/`$skip`. |
| `@odata.deltaLink` | When the query has no `$filter`/`$apply`/`$compute`/`$search`/`$expand` and the entity has `@expose <key>` | Stateless, signed URL that returns only what changed since this snapshot. See [Delta tracking](#delta-tracking). |

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
| `$format` | `$format=json` | Response format. Only `json` is supported; anything else returns 406. Equivalent to `Accept: application/json`. |
| `$deltatoken` | `$deltatoken=<opaque>` | Returned in `@odata.deltaLink` URLs; opaque to the client. Asks the server for the diff since the snapshot embedded in the token. See [Delta tracking](#delta-tracking). |

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

Multiple requests in one HTTP call (JSON batch format):

```bash
curl -X POST http://localhost:8090/odata/\$batch \
  -H "Content-Type: application/json" \
  -d '{"requests":[
    {"id":"1","method":"GET","url":"mart_revenue?$top=5"},
    {"id":"2","method":"GET","url":"mart_revenue/$count"}
  ]}'
```

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

**Token format.** The `$deltatoken` is opaque to the client — it carries the snapshot id, the entity name, and a hash of the original query options, all signed with HMAC-SHA256. The client never inspects or constructs it.

**Failure modes.** A `$deltatoken` request returns:

- `410 Gone DeltaLinkInvalid` — token signature is wrong (tampered, or signed by a different server / different key)
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

**Backed by DuckLake.** The runtime wraps DuckLake's `table_changes(table, from_snapshot, to_snapshot)` — the delta is the literal CDC diff between the snapshot the client last saw and the current one.

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

Read-only OData v4. Supports `$filter`, `$select`, `$orderby`, `$top`, `$skip`, `$count`, `$expand` (one level), `$batch` (JSON format). Not supported:

- Write operations (POST, PUT, DELETE on entity sets)
- Complex type functions or actions
- Deep `$expand` (nested navigation)

For write access to external systems, use `@sink`.
