---
description: Run type values, triggers, and what executes in each.
draft: false
title: Run Types
weight: 8
---

## Values

| Run type | Trigger | Kinds |
|---|---|---|
| `skip` | Hash unchanged, no dep changes, no `@fetch` | non-fetch table only |
| `backfill` | Model hash changed, config hash changed, first run, or a one-time upgrade rebuild | All |
| `incremental` | Hash unchanged, source data may have changed | append, merge, scd2, tracked |
| `full` | Upstream dep changed, dep metadata missing/invalid, destructive schema evolution, OR `@kind: table @fetch` (always re-fetches) | table |

## Hash inputs

Two hashes are compared against the model's last DuckLake commit, and either one changing forces a `backfill`.

**Model hash** — the SQL body plus `@kind`, `@unique_key`, `@group_key`, `@partitioned_by`, `@incremental`, `@incremental_initial`, `@fetch` and `@push`. A mismatch reports `run_reason: sql changed`.

**Config hash** — the `config/` SQL that applies to *this* model. A mismatch reports `run_reason: config changed`.

### Upgrading to per-model config hashing

Before this change, config content was folded into `sql_hash`. Splitting it out changes `sql_hash` for every model in any project that has a `config/` directory, so **the first run after upgrading rebuilds every model once**. That rebuild reports `run_reason: hash format changed (upgrade)` rather than `sql changed`, so it is distinguishable in `--json` output and in `commit_extra_info`. Subsequent runs are stable.

Plan for it before upgrading a production lake:

- `append` and `merge` models are rebuilt with `TRUNCATE` + `INSERT`, and `scd2` models have their history rebuilt. If a model's source no longer holds the full history — a rolling API window, a truncated staging table — that history is not recoverable from the source.
- `@fetch` models re-fetch from the beginning: the incremental cursor resets to `@incremental_initial`.

Projects with no `config/` directory are unaffected: their `sql_hash` is unchanged and nothing rebuilds.

## Config hash scope

Every statement in `config/` lands in one of three buckets, and a model is rebuilt only for what actually reaches it.

| Statement | Bucket | Rebuilds |
|---|---|---|
| `CREATE MACRO` in `config/macros/*.sql` | scoped | models naming that macro — in the SQL body, in `@constraint` / `@audit` / `@warning`, or as a `@column` masking tag |
| `SET VARIABLE` in `config/variables/{constants,global,local}.sql` | scoped | models naming it, typically via `getvariable('name')` |
| `ATTACH ... AS <alias>` or `CREATE VIEW <name>` in `config/sources.sql` | scoped | models naming that alias or view — but see the search-path note below |
| `SET`/`RESET` of a DuckDB setting that cannot change a result | inert | nothing |
| `CREATE SCHEMA` | inert | nothing |
| Anything in `config/state.sql` | inert | nothing — it never reaches the model execution session |
| Everything else, anywhere in `config/` | global | every model |

Classification is per statement, not per file: one unrecognized statement in a macro file sends only itself to the global bucket instead of dragging the file's macros with it. Global is the default — a statement is scoped or dropped only when it is positively recognized.

### Sources and the search path

Scoping a source by its alias assumes a model that reads it names it. A search path breaks that assumption: with `SET search_path = 'warehouse'` in config, a model reading `FROM orders` resolves to `warehouse.orders` without the word `warehouse` appearing anywhere in it, and repointing that `ATTACH` would skip exactly the models it affects.

So if **any** statement anywhere in `config/` is a `USE`, a `SET search_path`, or a `SET schema`, source scoping is switched off and `sources.sql` is treated as session-wide. Only projects that actually use that pattern pay for it. Macros and variables stay scoped either way — they are always called by name, so a search path cannot hide the dependency.

### Which settings are inert

74 DuckDB settings cannot change what a query returns and are dropped from the hash: thread and memory limits, `temp_directory`, optimizer and join thresholds, caching and prefetching, checkpoint/WAL/write buffering, and the profiling, logging and progress-bar families. Bumping `threads` or `memory_limit` rebuilds nothing.

Everything else is treated as semantic, **including settings not on the list at all** — a knob added by a future DuckDB release keeps forcing a rebuild until it is classified. Notable settings that stay semantic despite sounding harmless:

| Setting | Why |
|---|---|
| `integer_division` | changes what the `/` operator returns |
| `scalar_subquery_error_on_multiple_rows` | returns a random row instead of erroring |
| `search_path`, `schema` | which table an unqualified name resolves to |
| `preserve_insertion_order` | row order when there is no `ORDER BY` |
| `preserve_identifier_case` | output column names |
| `binary_as_string` | how Parquet binary data is read |
| `TimeZone`, `Calendar`, `default_collation`, `default_order`, `default_null_order` | date arithmetic, comparison, ordering |
| `file_search_path`, `home_directory`, `http_proxy*` | where input data comes from |
| `secret_directory`, `default_secret_storage` | which credentials apply |
| `custom_extension_repository`, `extension_directory` | which implementation of a function loads |

### Which files stay global

`secrets.sql` — DuckDB secrets bind to a query by `SCOPE`, a path prefix, never by name. A model never mentions the secret it uses, so there is nothing to scope against. Note that credentials do not live here: `.env` is loaded into the process environment and every config file is passed through `os.ExpandEnv`, so secret values are `${VAR}` references. What the file carries is topology — `ENDPOINT`, `REGION`, `SCOPE`, `HOST` — and repointing that changes which bytes a model reads.

`catalog.sql` — its `ATTACH` accepts `SNAPSHOT_TIME` and `SNAPSHOT_VERSION`, i.e. time travel for the whole lake, plus `DATA_PATH` and `METADATA_PATH`. Models address targets as `schema.table` under `USE`, so scoping by the catalog alias would miss them.

`extensions.sql` — `INSTALL`/`LOAD` decides which functions exist and which implementation answers.

### Two further properties

- **Comments and formatting are ignored.** Config content is normalized before hashing — `--` line comments, `/* … */` block comments and whitespace all come out — so reformatting or re-commenting a config file rebuilds nothing. Case *is* significant, because config carries endpoints, paths and environment names; and text inside `'literals'` and `"quoted identifiers"` is kept verbatim, so a change there always counts.
- **Macro-to-macro references are followed.** If a model calls `outer()` and `outer()` calls `inner()`, editing `inner()` rebuilds the model.

Name matching is lexical and deliberately over-inclusive: a macro named `total` is treated as a dependency of any model containing the word `total`. An extra dependency costs a needless rebuild; a missing one would serve stale data. Two known limits: a Starlark model that assembles a macro name at runtime through string concatenation is not seen, and `.env` is not hashed — repointing an endpoint there changes results with no hashed file changing, so follow it with a full refresh.

## What executes per run type

| Step | skip | backfill | incremental | full |
|---|---|---|---|---|
| SQL execution | no | full query | CDC-filtered (append/merge) or full query (other kinds) | full query |
| Schema evolution | no | yes | yes | yes |
| Constraints | no | yes | yes | yes |
| Audits | no | yes (transactional) | yes (transactional) | yes (transactional) |
| Materialize | no | yes | yes | yes |
| Sink | backlog only | yes | yes | yes |
| Commit metadata | no | yes | yes | yes |

## Sink on skip

When a model is skipped but has `@push`, the runner still drains the pending state-store backlog from previous failed pushes. No new delta is generated.

## Incremental kwargs

On backfill: `is_backfill=True`, `last_value` reset to `@incremental_initial`.
On incremental: `is_backfill=False`, `last_value` is `MAX(cursor_column)` from target.
