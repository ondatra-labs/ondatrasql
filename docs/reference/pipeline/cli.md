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
| [`init`](#init) | Create a new project |
| [`new`](#new) | Create a model file |
| [`edit`](#edit) | Open file in $EDITOR |
| [`sql`](#sql) | Run arbitrary SQL |
| [`query`](#query) | Query a table directly |
| [`stats`](#stats) | Project overview |
| [`history`](#history) | Run history for a model |
| [`describe`](#describe) | Model details and schema |
| [`describe blueprint`](#describe-blueprint) | Blueprint API contract introspection |
| [`validate`](#validate) | Static validation of models and blueprints |
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
ondatrasql --json sql "SELECT * FROM staging.orders"     # Versioned envelope
```

`ondatrasql --json sql` returns a versioned envelope (distinct from `--format json`, which is a row-oriented dump):

```json
{"schema_version": 1, "query": "...", "rows": [...], "warnings": []}
```

`warnings` is always emitted (`[]` when empty) so typed clients decode unconditionally — same shape contract as `history --json` and `run --json`.

## query

```bash
ondatrasql query staging.orders
ondatrasql query staging.orders --limit 10 --format csv
ondatrasql --json query staging.orders                   # Versioned envelope
```

| Flag | Description |
|---|---|
| `--format` | `csv`, `json`, `markdown` (or `md`) |
| `--limit` | Maximum rows returned |

`ondatrasql --json query` returns a versioned envelope:

```json
{"schema_version": 1, "table": "...", "limit": N, "rows": [...], "warnings": []}
```

Same contract as `--json sql` above — `warnings` is always present as an array.

---

## stats

```bash
ondatrasql stats
```

## history

```bash
ondatrasql history staging.orders
ondatrasql history staging.orders --limit 5
ondatrasql --json history staging.orders         # Versioned envelope
```

`--json history` emits a versioned envelope with snake_case keys:

```json
{
  "schema_version": 1,
  "model": "staging.orders",
  "limit": 50,
  "runs": [
    {
      "id": 12345,
      "time": "2026-01-15T08:00:00Z",
      "model": "staging.orders",
      "kind": "merge",
      "run_type": "incremental",
      "rows": 421,
      "duration_ms": 1830,
      "run_id": "dag-2026-01-15-08-00-00"
    }
  ],
  "warnings": []
}
```

`warnings` is always emitted (`[]` when empty) so typed clients can decode unconditionally. Numeric fields (`id`, `rows`, `duration_ms`) are JSON numbers, not strings — parse failures from the underlying SQL row are surfaced in `warnings` rather than silently coerced to `0`.

## describe

```bash
ondatrasql describe staging.orders
```

JSON output (`--json`) field names use snake_case (`target`, `kind`, `materialization`, `source_file`, etc.) for consistency with the rest of the machine-readable surface. v0.31 changed this from PascalCase — pipelines that parsed the previous output will need to migrate.

The output includes a `blueprint` field cross-linking to `describe blueprint <name>` when the model fetches from a registered lib function.

## describe blueprint

Static introspection of a blueprint's API contract — fetch/push args, function signatures, supported kinds, HTTP endpoints. Reads only `lib/`, never opens the catalog.

```bash
ondatrasql describe blueprint                    # List all blueprints
ondatrasql describe blueprint riksbank           # Full description (human)
ondatrasql --json describe blueprint riksbank    # Structured JSON
ondatrasql --json describe blueprint riksbank --fields=fetch.args,fetch.mode
```

JSON output is the primary form for agent use. The `--fields` flag projects through dotted paths so agents fetch only what they need; unknown paths return an error rather than empty values. Schema is versioned via `schema_version` at the top level — bump signals a breaking change.

`--fields` projection emits `null` for schema-valid paths whose underlying struct field is `omitempty`-zero (e.g. `fetch.poll_interval` on a sync blueprint). This is **not** the same shape as the unprojected detail JSON, which omits zero fields via `omitempty`. The trade-off is intentional: a null answer carries more information than an absent key (it confirms the path is valid). Clients that want the unprojected shape should call `describe blueprint <name>` without `--fields`.

## validate

Runs all static validators against models and blueprints without opening the catalog. Catches what would otherwise surface mid-execution: parser issues, strict-fetch / strict-push contract violations, LIMIT/OFFSET in pipeline models, blueprint API-dict errors, DAG cycles, and external-reference INFO findings.

```bash
ondatrasql validate                              # Full project (models/ + lib/)
ondatrasql validate models/raw/foo.sql           # One file
ondatrasql --json validate                       # Buffered JSON report
ondatrasql validate --output ndjson              # Stream one line per file
ondatrasql validate --strict                     # Promote WARN to exit 1
```

Severity levels: `BLOCKER` (would fail at `run` — includes deprecated directives, which the parser rejects outright), `WARN` (validate's own environment is degraded so downstream findings may be incomplete — extension load failed, blueprint parse error, walk skipped, etc.), `INFO` (cannot be verified without external state, e.g. external table references). Each finding carries a stable rule-ID like `parser.multi_statement` or `strict_fetch.cast_required` — these are public contract, never renamed without a `schema_version` bump.

`validate.*` WARN findings always trigger exit 1 regardless of `--strict` because they signal that validate's own analysis was incomplete — CI consumers can't treat a degraded run as a clean one.

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

Machine-readable output on stdout. Human output (status banners, progress, warning logs from internal subsystems) is routed to stderr.

```bash
ondatrasql run --json 2>/dev/null | jq -s '.'
```

**Stream separation is required.** `--json` only guarantees clean JSON on **stdout**; stderr may contain unstructured warnings (e.g. "badger: nack claim X failed"). Tooling that merges streams (`2>&1`, default systemd `StandardOutput=journal` + `StandardError=journal`) WILL produce non-JSON lines mixed into the stream. For service units, redirect stderr to a separate sink:

```ini
[Service]
StandardOutput=file:/var/log/ondatrasql.json
StandardError=file:/var/log/ondatrasql.err
```

| Field | Description |
|---|---|
| `schema_version` | Always emitted as `2`. Bump signals breaking shape change. (v2 made `errors`/`warnings` always-emit instead of `omitempty`.) |
| `model` | Target table |
| `kind` | `table`, `append`, `merge`, `scd2`, `tracked`, `events` |
| `run_type` | `skip`, `backfill`, `incremental`, `full`, `flush` |
| `run_reason` | Why this run type was chosen (omitted when empty) |
| `rows_affected` | Rows written (0 for skip) |
| `duration_ms` | Execution time |
| `status` | `ok`, `skip`, or `error` |
| `errors` | Error messages — always emitted as `[]` when none |
| `warnings` | Schema evolution / validation warnings — always emitted as `[]` when none |
| `dag_run_id` | DAG run correlation ID (omitted when empty) |
| `sandbox` | `true` for `sandbox` mode runs (omitted when false) |

## version

```bash
ondatrasql version
```

---

## Exit codes

Exit codes follow the eslint/ruff convention and apply to **every** subcommand, not just `validate`:

- `0` — clean (command succeeded, no findings, no errors)
- `1` — findings or runtime failure: any `validate` BLOCKER, any `validate.*` WARN (degraded run — extension load failed, blueprint parse error, walk skipped, etc.), any WARN with `--strict`, OR any non-invocation runtime error (DuckDB error, network failure, write error, etc.)
- `2` — invocation error: bad flag, unknown command, unexpected/extra args, file or scope outside `models/`/`lib/`, invalid CLI value (e.g. `--limit=abc`)

`validate.*` WARN findings always trigger exit 1 regardless of `--strict` because they signal validate's own analysis was incomplete — CI consumers can't treat a degraded run as a clean one.

The exit-2 contract is enforced for the following surfaces (regression-tested in `cli_contract_test.go`): `version`, `init`, `stats`, `lineage`, `history`, `query`, `sql`, `describe`, `describe blueprint`, `edit`, `new`, `events`, `auth`, `schedule`, `validate`, plus the `unknown_command` fallthrough. CI scripts that gate on exit code can rely on `1` vs `2` to distinguish "real failure" from "you typed it wrong".

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


