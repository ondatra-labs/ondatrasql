---
description: Complete OndatraSQL CLI reference. Every command with syntax and options.
draft: false
title: CLI Reference
weight: 10
---
Single binary. All commands listed below.

## Commands

| Command | Description |
|---|---|
| [`run`](#run) | Execute the pipeline |
| [`sandbox`](#sandbox) | Preview changes before committing |
| [`schedule`](#schedule) | Install OS-native scheduler |
| [`events`](#events) | Start event collection endpoint |
| [`odata`](#odata) | Start OData v4 server |
| [`init`](#init) | Create a new project |
| [`new`](#new) | Create a model file |
| [`edit`](#edit) | Open file in $EDITOR |
| [`sql`](#sql) | Run arbitrary SQL |
| [`query`](#query) | Query a table directly |
| [`stats`](#stats) | Project overview |
| [`history`](#history) | Run history for a model |
| [`describe`](#describe) | Model details and schema |
| [`lineage`](#lineage) | Column-level lineage |
| [`auth`](#auth) | OAuth2 provider management |
| [`checkpoint`](#maintenance) | Run all maintenance |
| [`flush`](#maintenance) | Flush inlined data to Parquet |
| [`merge`](#maintenance) | Merge small Parquet files |
| [`expire`](#maintenance) | Expire old snapshots |
| [`cleanup`](#maintenance) | Delete unreferenced files |
| [`orphaned`](#maintenance) | Delete orphaned files |
| [`rewrite`](#maintenance) | Rewrite files with many deletes |
| [`--json`](#--json) | Machine-readable output |
| [`version`](#version) | Print version |

---

## run

```bash
ondatrasql run                    # All models in DAG order
ondatrasql run staging.orders     # Single model
```

## sandbox

```bash
ondatrasql sandbox                # All models
ondatrasql sandbox staging.orders # One model
```

See [Preview Changes](/guides/preview-changes/).

## schedule

```bash
ondatrasql schedule "*/5 * * * *"   # Install (auto-detects OS)
ondatrasql schedule                 # Show status
ondatrasql schedule remove          # Remove
```

| OS | Backend |
|---|---|
| Linux | systemd user timer |
| macOS | launchd plist |
| Windows | [WSL2](https://learn.microsoft.com/en-us/windows/wsl/install) |

See [Schedule Pipeline Runs](/guides/schedule-pipeline-runs/).

## events

```bash
ondatrasql events 8080            # Public on 8080, admin on 8081
```

Admin endpoint runs on `port + 1` (localhost-only).

See [Collect Events](/guides/collect-events/).

## odata

```bash
ondatrasql odata 8090
```

Serves `@expose` models at `http://127.0.0.1:<port>/odata`.

See [Serve Data via OData](/guides/serve-data-via-odata/).

---

## init

```bash
ondatrasql init
```

## new

```bash
ondatrasql new staging.orders.sql
ondatrasql new raw.api_users.sql
ondatrasql new sync.hubspot.sql
```

## edit

```bash
ondatrasql edit staging.orders      # Model
ondatrasql edit env                 # .env
ondatrasql edit catalog             # config/catalog.sql
ondatrasql edit macros/audits       # config/macros/audits.sql
ondatrasql edit macros/constraints  # config/macros/constraints.sql
ondatrasql edit macros/warnings     # config/macros/warnings.sql
ondatrasql edit macros/helpers      # config/macros/helpers.sql
ondatrasql edit macros/masking      # config/macros/masking.sql
ondatrasql edit variables/constants # config/variables/constants.sql
ondatrasql edit variables/global    # config/variables/global.sql
ondatrasql edit variables/local     # config/variables/local.sql
ondatrasql edit secrets             # config/secrets.sql
ondatrasql edit settings            # config/settings.sql
ondatrasql edit sources             # config/sources.sql
ondatrasql edit extensions          # config/extensions.sql
```

---

## sql

```bash
ondatrasql sql "SELECT COUNT(*) FROM staging.orders"
ondatrasql sql "SELECT * FROM staging.orders" --format json
```

## query

```bash
ondatrasql query staging.orders
ondatrasql query staging.orders --limit 10 --format csv
```

| Flag | Description |
|---|---|
| `--format` | `csv`, `json`, `markdown` (or `md`) |
| `--limit` | Maximum rows returned |

---

## stats

```bash
ondatrasql stats
```

## history

```bash
ondatrasql history staging.orders
ondatrasql history staging.orders --limit 5
```

## describe

```bash
ondatrasql describe staging.orders
```

## lineage

```bash
ondatrasql lineage overview              # All models
ondatrasql lineage staging.orders        # One model
ondatrasql lineage staging.orders.total  # One column
```

See [Lineage & Metadata](/reference/data-access/lineage/).

---

## auth

```bash
ondatrasql auth                   # List providers
ondatrasql auth google-sheets     # Authenticate
ondatrasql auth fortnox           # Authenticate
```

| Mode | Detected by | Description |
|---|---|---|
| Managed | `ONDATRA_KEY` in `.env` | Uses oauth2.ondatra.sh, no app registration |
| Local | `<PREFIX>_CLIENT_ID` in `.env` | Uses your own OAuth2 credentials |

Tokens are stored in `.ondatra/tokens/` and auto-refresh on every pipeline run.

See [Environment Variables](/reference/pipeline/env/) for OAuth2 variable reference.

---

## --json

Machine-readable output on stdout. Human output redirected to stderr.

```bash
ondatrasql run --json 2>/dev/null | jq -s '.'
```

| Field | Description |
|---|---|
| `model` | Target table |
| `kind` | `table`, `append`, `merge`, `scd2`, `tracked`, `events` |
| `run_type` | `skip`, `backfill`, `incremental`, `full`, `flush` |
| `run_reason` | Why this run type was chosen |
| `rows_affected` | Rows written (0 for skip) |
| `duration_ms` | Execution time |
| `status` | `ok`, `skip`, or `error` |
| `errors` | Error messages (when `status` is `error`) |
| `warnings` | Warnings (schema evolution, validation) |

## version

```bash
ondatrasql version
```

---

## Maintenance

```bash
ondatrasql checkpoint             # Run all maintenance in order
ondatrasql flush                  # Flush inlined data to Parquet
ondatrasql merge                  # Merge small Parquet files
ondatrasql expire                 # Expire old snapshots
ondatrasql cleanup                # Delete unreferenced files
ondatrasql orphaned               # Delete orphaned files
ondatrasql rewrite                # Rewrite files with many deletes
```

See [Maintain DuckLake Storage](/guides/maintain-ducklake-storage/).
