---
date: "2026-04-04T00:00:00+02:00"
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
| `POST` | `/odata/$batch` | JSON batch (multiple requests in one call) |

Entity names use underscores: `mart.revenue` becomes `mart_revenue` in the URL.

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
