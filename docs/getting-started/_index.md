---
description: Run your first OndatraSQL data pipeline in under 5 minutes. Install the binary, create a project, write SQL models, and execute with DuckDB and DuckLake.
draft: false
title: Getting Started
weight: 10
---
This guide walks through creating and running a data pipeline with OndatraSQL.

## 1. Install

```bash
curl -fsSL https://ondatra.sh/install.sh | sh
```

OndatraSQL supports Linux, macOS, and Windows via [WSL2](https://learn.microsoft.com/en-us/windows/wsl/install). The installation produces a single binary with no external dependencies.

## 2. Create a project

```bash
mkdir my-pipeline && cd my-pipeline
ondatrasql init
```

```
Initialized project "my-pipeline"
```

## 3. Ingest data

Create a blueprint in `lib/` and a SQL model:

```bash
mkdir -p lib
```

```python
# lib/rest_countries.star

API = {
    "base_url": "https://restcountries.com",
    "fetch": {
        "args": ["region"],
    },
}

def fetch(region, page):
    resp = http.get("/v3.1/region/" + region)
    rows = [{"name": c["name"]["common"], "capital": c["capital"][0], "population": c["population"]} for c in resp.json]
    return {"rows": rows, "next": None}
```

```bash
ondatrasql new raw.countries
ondatrasql edit raw.countries
```

```sql
-- @kind: table
-- @fetch

SELECT name::VARCHAR AS name, capital::VARCHAR AS capital, population::BIGINT AS population
FROM rest_countries('europe')
```

The file path determines the target table: `raw.countries`. `@fetch` declares this as a lib-backed model — every projected column is cast explicitly. See [Fetch Contract](/reference/lib-functions/fetch-contract/#sql-schema-contract).

## 4. Transform with SQL

```bash
ondatrasql new staging.countries
ondatrasql edit staging.countries
```

```sql
-- @kind: table
-- @constraint: not_null(name)
-- @constraint: compare(population, >, 0)

SELECT
    name, capital, population,
    CASE
        WHEN population > 50000000 THEN 'large'
        WHEN population > 10000000 THEN 'medium'
        ELSE 'small'
    END AS size
FROM raw.countries
```


## 5. Aggregate into a mart

```bash
ondatrasql new mart.population
ondatrasql edit mart.population
```

```sql
-- @kind: table
-- @audit: row_count(>, 0)

SELECT size, COUNT(*) AS countries, SUM(population)::BIGINT AS total_pop
FROM staging.countries
GROUP BY size
```


## 6. Run the pipeline

```bash
ondatrasql run
```

```
Running 3 models...
[OK] raw.countries      (table, backfill, 53 rows, 1.1s)
[OK] staging.countries  (table, backfill, 53 rows, 250ms, first run)
[OK] mart.population    (table, backfill, 3 rows, 200ms, first run)

Done: 3 ran, 0 skipped, 0 failed (109 rows, 1.6s)
```


## 7. Query the results

```bash
ondatrasql sql "SELECT * FROM mart.population"
```

```
| size   | countries | total_pop |
| ------ | --------- | --------- |
| large  | 5         | 424080603 |
| medium | 10        | 211169761 |
| small  | 38        | 106407558 |
```

## Additional capabilities

- **Serve to BI tools**: add [`@expose`](/guides/serve-data-via-odata/) and connect Power BI, Excel, or Grafana via OData v4
- **Outbound sync**: [push data to APIs](/guides/outbound-sync/) with `@push` and automatic change detection
- **Ingest from APIs**: [blueprints](/reference/lib-functions/api-dict/) with HTTP, OAuth, and pagination
- **Collect events**: [POST to an embedded endpoint](/guides/collect-events/) with in-memory and batch buffering
- **Validate data**: [constraints, audits, and warnings](/reference/) with atomic rollback
- **Track lineage**: [column-level lineage](/reference/data-access/lineage/) extracted from SQL AST