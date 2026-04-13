<p align="center">
  <img src="https://ondatra.sh/images/ondatra.png" alt="OndatraSQL" width="200">
</p>

<h1 align="center">OndatraSQL</h1>

<p align="center">
  <b>A data runtime built on DuckDB and DuckLake</b><br>
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

Create `models/staging/orders.sql`:

```sql
-- @kind: merge
-- @unique_key: order_id

SELECT * FROM (VALUES
    (1, 'Alice', 100, '2026-01-15'),
    (2, 'Bob',   200, '2026-02-20'),
    (3, 'Charlie', 150, '2026-03-10')
) AS t(order_id, customer, amount, order_date)
```

Run the pipeline:

```bash
ondatrasql run
```

```
[OK] staging.orders (merge, backfill, 3 rows, 180ms)
```

OndatraSQL creates a DuckLake catalog, builds the dependency graph, executes the model, and materializes the result with snapshot metadata.

## Model Types

**SQL** — transformations:

```sql
-- @kind: table

SELECT date, SUM(total) AS revenue
FROM staging.orders GROUP BY date
```

**Starlark** — API ingestion (embedded scripting with Python-like syntax):

```python
# @kind: append
# @incremental: updated_at

resp = http.get("https://api.example.com/users")
for user in resp.json:
    save.row(user)
```

**YAML** — declarative configuration for reusable source functions:

```yaml
kind: append
source: api_fetch
config:
  base_url: https://api.example.com
```

All model types execute in the same pipeline and share the same dependency graph.

## Key Capabilities

| Capability | How it works |
|---|---|
| SQL transformation | SQL models with automatic materialization and CDC |
| API ingestion | Built-in HTTP, OAuth, pagination via Starlark |
| Event collection | Embedded HTTP endpoint with durable buffering |
| Outbound sync | Tracked models with content-hash change detection |
| Validation | 30 constraint macros, 18 audit macros, 14 warning macros |
| Schema evolution | Automatic via ALTER TABLE (metadata-only in DuckLake) |
| Sandbox preview | Full DAG simulation before committing |
| Scheduling | OS-native cron via systemd (Linux) or launchd (macOS) |
| OData serving | Built-in OData v4 server for Power BI, Excel, Grafana |
| Column lineage | Extracted from SQL AST |

## Design

OndatraSQL executes on a single machine using DuckDB. It is not a distributed system. For workloads that fit on one machine — batch ETL, reporting, analytics, internal tooling — this approach provides the full pipeline lifecycle with minimal operational overhead.

## Commands

```text
run [model]          Execute pipeline or specific model
sandbox [model]      Preview changes before committing
schedule [cron]      Install OS-native scheduler
odata <port>         Start OData v4 server
events <port>        Start event collection endpoint
auth [provider]      Authenticate with OAuth2 providers
sql "SELECT ..."     Query DuckLake catalog
lineage overview     View dependencies and column lineage
```

[Full CLI reference →](https://ondatra.sh/cli/)

## Documentation

**[ondatra.sh](https://ondatra.sh)**

## License

[GNU AGPL v3](LICENSE)
