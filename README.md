<p align="center">
  <img src="https://ondatra.sh/images/ondatra.png" alt="OndatraSQL" width="200">
</p>

<h1 align="center">OndatraSQL</h1>

<p align="center">
  <b>You don't need a data stack anymore</b><br>
  One binary replaces ingestion, transformation, validation, serving, and reverse ETL.
</p>

<p align="center">
  No setup. No services. No infrastructure.
</p>

<p align="center">
  <a href="https://ondatra.sh">Documentation</a> &middot;
  <a href="https://discord.gg/TQEqsa9q">Discord</a> &middot;
  <a href="https://ondatra.sh/blueprints/">Blueprints</a>
</p>

---

No Kafka. No Airflow. No dbt. No warehouse setup.

Just SQL files, one binary, and your data.

OndatraSQL is a data runtime that runs directly on [DuckDB](https://duckdb.org/) and [DuckLake](https://ducklake.select/). You write SQL. OndatraSQL handles:

- Execution order (no DAGs to define)
- Change detection (no incremental logic)
- Schema evolution (no migrations)
- Validation and lineage (built-in)

## Why

Most data tools assume you already have a stack. Kafka. Airflow. dbt. A warehouse.

OndatraSQL removes that assumption.

It runs on a single machine, requires no services, and works in minutes.

## Install

**Linux / macOS:**

```bash
curl -fsSL https://ondatra.sh/install.sh | sh
```

**Windows:**

```powershell
irm https://ondatra.sh/install.ps1 | iex
```

**From source:** `go install github.com/ondatra-labs/ondatrasql/cmd/ondatrasql@latest` (Go 1.25+ and gcc/clang).

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

Run it:

```bash
ondatrasql run
```

```
[OK] staging.orders (merge, backfill, 3 rows, 180ms)
```

You now have a versioned table in DuckLake with automatic change tracking and a reproducible pipeline. No setup beyond this.

## Principles

- **No DAGs** — dependencies are inferred from SQL
- **No incremental logic** — change detection is automatic
- **No infrastructure** — runs on one machine
- **No blind runs** — preview everything with sandbox

## What You Don't Have to Do

**No incremental logic** — only changed data is processed automatically.

**No migrations** — schema updates itself when your query changes.

**No blind runs** — preview every change before committing.

**No event infrastructure** — POST events directly to an HTTP endpoint.

**No separate tools for quality or lineage** — built into execution.

**No reverse ETL tool** — push data to APIs with `@kind: tracked`.

**No BI middleware** — serve data to Power BI, Excel, and Grafana via OData v4.

## Three Ways to Write Models

**SQL** — transformations:

```sql
-- @kind: table

SELECT date, SUM(total) AS revenue
FROM staging.orders GROUP BY date
```

**Starlark** — API ingestion and reverse ETL (no Python):

```python
# @kind: tracked
# @unique_key: customer_id

rows = query("SELECT * FROM mart.customers")
for row in rows:
    http.post("https://api.example.com/contacts", json=row)
    save.row(row)
```

**YAML** — declarative config:

```yaml
kind: append
source: api_fetch
config:
  base_url: https://api.example.com
```

## Mental Model

Files are tables.
SQL is the pipeline.
Runs are deterministic.

You don't build pipelines. You run data.

## Compared to the Modern Data Stack

**Traditional:**

- dbt for transforms
- Airbyte for ingestion
- Airflow for orchestration
- Kafka for events
- Snowflake for storage
- Census for reverse ETL

**OndatraSQL:**

- One binary
- One runtime
- One execution model

## When Not to Use OndatraSQL

- You need a distributed system
- You process petabytes across clusters
- You require real-time streaming

OndatraSQL is designed for simplicity over horizontal scale.

## Commands

```text
run [model]          Run pipeline or specific model
sandbox [model]      Preview changes safely
schedule [cron]      Install/show/remove OS scheduler
odata <port>         Serve data via OData v4
events <port>        Start event collection
auth [provider]      Authenticate with OAuth2 providers
sql "SELECT ..."     Query your data
lineage overview     See all dependencies
```

[Full CLI reference →](https://ondatra.sh/cli/)

## Documentation

**[ondatra.sh](https://ondatra.sh)**

## Philosophy

The best data system is the one that runs now.

Not the one you finish setting up next week.

## License

[GNU AGPL v3](LICENSE)
