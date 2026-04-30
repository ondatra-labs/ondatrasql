<p align="center">
  <img src="https://ondatra.sh/images/ondatra_hex.png" alt="OndatraSQL" width="200">
</p>

<h1 align="center">OndatraSQL</h1>

<p align="center">
  <b>A data pipeline runtime for DuckDB and DuckLake</b><br>
  Ingestion, transformation, validation, and scheduling in a single binary.
</p>

<p align="center">
  <a href="https://ondatra.sh">Documentation</a> &middot;
  <a href="https://discord.gg/TQEqsa9q">Discord</a> &middot;
  <a href="https://ondatra.sh/blueprints/">Blueprints</a>
</p>

---

OndatraSQL runs data pipelines using SQL models, [DuckDB](https://duckdb.org/) for query execution, and [DuckLake](https://ducklake.select/) for catalog management, snapshots, and time-travel.

The runtime handles:

- **Dependency resolution** — extracted from SQL references
- **Change detection** — via DuckLake snapshots and `table_changes()`
- **Schema evolution** — columns added, renamed, or type-promoted automatically
- **Validation** — constraints, audits, and warnings as part of execution
- **Incremental processing** — Smart CDC rewrites queries to process only changed data

## Install

```bash
curl -fsSL https://ondatra.sh/install.sh | sh
```

Supports Linux, macOS, and Windows via [WSL2](https://learn.microsoft.com/en-us/windows/wsl/install).

## Quick Start

```bash
mkdir my-pipeline && cd my-pipeline
ondatrasql init
```

Ingest data from an API (create a blueprint in `lib/`):

```python
# lib/countries_fetch.star
API = {
    "base_url": "https://restcountries.com",
    "fetch": {
        "args": [],
    },
}

def fetch(page):
    resp = http.get("/v3.1/region/europe")
    rows = [{"name": c["name"]["common"], "capital": c["capital"][0], "population": c["population"]} for c in resp.json]
    return {"rows": rows, "next": None}
```

```sql
-- models/raw/countries.sql
-- @kind: table
SELECT name::VARCHAR AS name, capital::VARCHAR AS capital, population::BIGINT AS population
FROM countries_fetch()
```

Transform with SQL:

```bash
ondatrasql new staging.countries.sql
ondatrasql edit staging.countries.sql
```

```sql
-- @kind: table
-- @constraint: not_null(name)

SELECT
    name, capital, population,
    CASE
        WHEN population > 50000000 THEN 'large'
        WHEN population > 10000000 THEN 'medium'
        ELSE 'small'
    END AS size
FROM raw.countries
```

Run the pipeline:

```bash
ondatrasql run
```

```
Running 2 models...
[OK] raw.countries      (table, backfill, 53 rows, 1.1s)
[OK] staging.countries  (table, backfill, 53 rows, 250ms — first run)

Done: 2 ran, 0 skipped, 0 failed (106 rows, 1.4s)
```

## Model Types

**SQL** — transformations:

```sql
-- @kind: table

SELECT date, SUM(total) AS revenue
FROM staging.orders GROUP BY date
```

**Lib functions** — API ingestion and outbound sync via Starlark in `lib/`:

```sql
-- models/raw/users.sql
-- @kind: table
SELECT id::BIGINT AS id, email::VARCHAR AS email, name::VARCHAR AS name FROM my_api('users')
```

```python
# lib/my_api.star — fetch function called by the SQL model
API = {"base_url": "https://api.example.com", "auth": {"env": "API_KEY"},
       "fetch": {"args": ["resource"], "page_size": 100}}

def fetch(resource, page):
    resp = http.get("/v1/" + resource, params={"limit": page.size, "cursor": page.cursor})
    return {"rows": resp.json["items"], "next": resp.json.get("next")}
```

Models are SQL files. Starlark is used in `lib/` for API transport (HTTP, auth, pagination).

All models execute in the same pipeline and share the same dependency graph.

## Key Capabilities

| Capability | How it works |
|---|---|
| SQL transformation | SQL models with automatic materialization and CDC |
| API ingestion | Built-in HTTP, OAuth, pagination via Starlark |
| Event collection | Embedded HTTP endpoint with in-memory and durable buffering |
| Outbound sync | Push to APIs via @sink with raw DuckLake change types |
| Validation | 30 constraint macros, 18 audit macros, 14 warning macros |
| Schema evolution | Automatic via ALTER TABLE (metadata-only in DuckLake) |
| Sandbox preview | Full DAG simulation before committing |
| Scheduling | OS-native cron via systemd (Linux) or launchd (macOS) |
| OData serving | Built-in OData v4.01 server for Power BI, Excel, Grafana |
| Column lineage | Extracted from SQL AST |

## Design

OndatraSQL executes on a single machine using DuckDB. It is not a distributed system. For workloads that fit on one machine — batch ETL, reporting, analytics, internal tooling — this approach provides the full pipeline lifecycle with minimal operational overhead.

## Commands

```text
run [model]          Execute pipeline or specific model
sandbox [model]      Preview changes before committing
schedule [cron]      Install OS-native scheduler
odata <port>         Start OData v4.01 server
events <port>        Start event collection endpoint
auth [provider]      Authenticate with OAuth2 providers
new <model>          Create a model file
edit <target>        Open file in $EDITOR
sql "SELECT ..."     Query DuckLake catalog
stats                Project overview
describe <model>     Model details and schema
history [model]      Run history
lineage overview     View dependencies and column lineage
flush                Flush inlined data to Parquet
checkpoint           Run all maintenance
```

[Full CLI reference →](https://ondatra.sh/cli/)

## Documentation

**[ondatra.sh](https://ondatra.sh)**

## License

[GNU AGPL v3](LICENSE)
