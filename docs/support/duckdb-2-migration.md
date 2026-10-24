---
description: What changes for an OndatraSQL project when it moves from DuckDB 1.5 to DuckDB 2.0.
draft: false
title: Moving to DuckDB 2.0
weight: 4
---
OndatraSQL embeds DuckDB 2.0. Most projects run unchanged; these are the differences to check.

## Lambdas: `lambda x: ...`, not `x -> ...`

DuckDB 2.0 rejects the arrow form of a lambda when the query runs:

```text
Binder Error: Deprecated lambda arrow (->) detected.
```

Rewrite `list_transform(xs, x -> x + 1)` as `list_transform(xs, lambda x: x + 1)`, and `COLUMNS(c -> c LIKE 'a%')` as `COLUMNS(lambda c: c LIKE 'a%')`. `ondatrasql validate` reports every arrow lambda in a model as [`parser.lambda_arrow`](/reference/pipeline/validate-rules/). The JSON operator `j -> '$.a'` is unaffected.

Macros count too. DuckDB 2.0 binds a macro's body when it creates the macro, so an arrow lambda in a file under `config/macros/` fails session start, and with it every run. `ondatrasql validate` reports it on the macro file.

To keep the old syntax for now, put `SET lambda_syntax = 'ENABLE_SINGLE_ARROW';` in [`config/settings.sql`](/reference/configuration/config-settings/); validate then accepts the arrow too, in models and macros. DuckDB plans to remove that switch in a later release.

## Settings DuckDB 2.0 removed

A `SET` of one of these in `config/` fails on 2.0. Drop it, or use the successor:

| DuckDB 1.5 | DuckDB 2.0 |
|---|---|
| `custom_profiling_settings` | `tracked_metrics` |
| `profiling_mode` | `profiling_renderer_settings` |
| `enable_http_logging`, `http_logging_output` | `enable_logging` with `logging_storage`; `redact_http_logs` |
| `enable_caching_operators` | none |
| `enable_object_cache` | none (it did nothing on 1.5) |
| `vacuum_rebuild_indexes` | none |

## Error messages

DuckDB's error text changed in places. A caret line under a SQL error now underlines the whole token (`^^^^^^^^`) instead of marking its first character. Tooling that matches DuckDB error text needs updating.

## DuckLake catalogs and DuckDB 1.5

A catalog created under DuckDB 1.5 stays readable by 1.5: OndatraSQL on 2.0 reads and writes it without upgrading it. A new catalog, including one from `ondatrasql init`, uses the current DuckLake format, which 1.5 cannot attach. See [DuckLake version and DuckDB 1.5](/reference/configuration/config-catalog/#ducklake-version-and-duckdb-15), and never add `AUTOMATIC_MIGRATION`.

## Behaviour OndatraSQL changed alongside

- **Lib-call args are strings and bind by name.** Every argument reaches the blueprint as a string (`5` is `"5"`, `NULL` is `""`), and `name => value` binds to the parameter of that name. See [Args from SQL](/reference/lib-functions/api-dict/#args-from-sql).
- **Cast types are exact.** A typed `@fetch` or `@push` column carries the type its cast names: `double precision` is `DOUBLE`, `FLOAT(53)` is `DOUBLE`, `myschema.mytype` keeps its qualifier. A cast whose type cannot be read fails the run instead of becoming `VARCHAR`.
- **An unreadable model fails the run.** When DuckDB cannot serialize a model's SQL, the run stops with the error. It used to continue without lineage, which skipped CDC. `validate` already reported these models as BLOCKERs; now the run agrees. The ones to look for:
  - a model that is a top-level `PIVOT` statement, or uses a dynamic `PIVOT` (one without an `IN` list) anywhere: `json_serialize_sql` serializes only SELECT statements, and DuckDB rewrites both into something else. Write the `PIVOT` with an explicit `IN (...)` list inside a `SELECT`, as in `SELECT * FROM (PIVOT src ON k IN ('a', 'b') USING sum(v))`. `UNPIVOT` is unaffected.
  - an append or merge model whose column lineage cannot be read, since CDC needs it.
- **Lib-call arguments are evaluated on their own.** An argument that is not a plain literal is evaluated by DuckDB as `SELECT CAST(<arg> AS VARCHAR)`, outside the model's query. An argument that names a column, as in `FROM my_api(k)`, fails there with a binder error instead of passing the name through. Pass a literal, or compute the value in a variable (`getvariable('name')`).
- **Rule ID change:** a lib call whose arguments cannot be read from the AST is reported as `parser.other`. Before 2.0 it was `cross_ast.args_mismatch`, which now only means an argument the blueprint cannot bind. See [Validate Rule IDs](/reference/pipeline/validate-rules/).
- **`late_materialization` is off.** DuckDB 2.0.0's late-materialization optimizer can overflow a buffer on tables with deleted rows, so OndatraSQL adds it to `disabled_optimizers` in every session, alongside anything your `config/` disables. Query results do not change.
