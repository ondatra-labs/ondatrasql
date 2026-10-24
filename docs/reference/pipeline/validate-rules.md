---
description: Every rule ID ondatrasql validate reports, with its severity.
draft: false
title: Validate Rule IDs
weight: 15
---
Each finding of [`ondatrasql validate`](/reference/pipeline/cli/#validate) carries one of these rule IDs. They are public contract: a rule ID is never renamed or removed without a `schema_version` bump. `BLOCKER` fails at `run`; `WARN` means validate's own analysis was degraded; `INFO` cannot be verified without external state.

| Rule ID | Severity | Reported when |
|---|---|---|
| `parser.multi_statement` | BLOCKER | A model holds more than one SQL statement |
| `parser.invalid_directive` | BLOCKER | A `-- @` directive is unknown or malformed |
| `parser.invalid_kind` | BLOCKER | `@kind` names no model kind |
| `parser.invalid_unique_key` | BLOCKER | `@unique_key` is missing or malformed for the kind |
| `parser.invalid_incremental` | BLOCKER | `@incremental` is malformed |
| `parser.invalid_sorted_by` | BLOCKER | `@sorted_by` is malformed |
| `parser.invalid_partitioned_by` | BLOCKER | `@partitioned_by` is malformed |
| `parser.missing_directive` | BLOCKER | A directive the model needs is missing, such as `@fetch` on a model with a lib call |
| `parser.duplicate_target` | BLOCKER | Two models materialize the same target |
| `parser.lambda_arrow` | BLOCKER | A lambda is written `x -> ...`, in a model or in a macro under `config/macros/`; DuckDB 2.0 rejects it when the query runs or the macro is created. Write `lambda x: ...`. Not reported when `config/settings.sql` sets `lambda_syntax` to `ENABLE_SINGLE_ARROW` |
| `parser.unterminated_block_comment` | BLOCKER | A `/*` in the header has no closing `*/` |
| `parser.deprecated_sink` | BLOCKER | The removed `@sink` directive |
| `parser.deprecated_script` | BLOCKER | The removed `@script` directive |
| `parser.deprecated_sink_detect_deletes` | BLOCKER | The removed `@sink_detect_deletes` directive |
| `parser.deprecated_sink_delete_threshold` | BLOCKER | The removed `@sink_delete_threshold` directive |
| `parser.other` | BLOCKER | Any other parse failure: SQL DuckDB cannot serialize (such as a top-level or dynamic `PIVOT`), source tables or, for append and merge, column lineage that cannot be read, or lib-call arguments that cannot be read from the AST (reported as `cross_ast.args_mismatch` before DuckDB 2.0) |
| `strict_fetch.cast_required` | BLOCKER | A `@fetch` projection is not cast |
| `strict_fetch.alias_required` | BLOCKER | A `@fetch` cast has no alias |
| `strict_fetch.where_clause_disallowed` | BLOCKER | A `@fetch` model has a WHERE clause |
| `strict_fetch.group_by_disallowed` | BLOCKER | A `@fetch` model has GROUP BY |
| `strict_fetch.distinct_disallowed` | BLOCKER | A `@fetch` model has DISTINCT |
| `strict_fetch.order_by_disallowed` | BLOCKER | A `@fetch` model has ORDER BY |
| `strict_fetch.union_disallowed` | BLOCKER | A `@fetch` model is a set operation |
| `strict_fetch.multiple_lib_calls` | BLOCKER | A `@fetch` model calls more than one lib function |
| `strict_fetch.no_lib_call` | BLOCKER | A `@fetch` model calls no lib function |
| `strict_fetch.other` | BLOCKER | Any other `@fetch` contract violation |
| `strict_push.cast_required` | BLOCKER | A `@push` projection is not cast |
| `strict_push.alias_required` | BLOCKER | A `@push` cast has no alias |
| `strict_push.select_star_disallowed` | BLOCKER | A `@push` model projects `*` |
| `strict_push.lib_call_disallowed` | BLOCKER | A `@push` model calls a lib function in FROM |
| `strict_push.other` | BLOCKER | Any other `@push` contract violation |
| `read_only.limit_disallowed` | BLOCKER | A pipeline model has LIMIT |
| `read_only.offset_disallowed` | BLOCKER | A pipeline model has OFFSET |
| `dag.cycle_detected` | BLOCKER | Models depend on each other in a cycle |
| `dag.external_reference` | INFO | A model reads a table no model produces |
| `blueprint.invalid_api_dict` | BLOCKER | A lib file's `API` dict is invalid, or the file does not parse |
| `blueprint.helper_without_api` | INFO | A lib file has no `API` dict and no `_` prefix marking it as a helper |
| `blueprint.unchecked_status` | INFO | A blueprint calls `http.*` without checking the response status (heuristic) |
| `blueprint.other` | BLOCKER | Any other blueprint problem |
| `push.kind_not_supported` | BLOCKER | The sink does not support the model's kind |
| `push.missing_unique_key` | BLOCKER | The push needs `@unique_key` |
| `push.missing_group_key` | BLOCKER | The push needs `@group_key` |
| `push.invalid_poll_interval` | BLOCKER | A poll interval is malformed |
| `push.invalid_poll_timeout` | BLOCKER | A poll timeout is malformed |
| `push.other` | BLOCKER | Any other push compatibility problem |
| `cross_ast.args_mismatch` | BLOCKER | A lib-call argument cannot be bound: an unknown name, a parameter given twice, a positional argument after a named one, or more arguments than the blueprint declares |
| `cross_ast.unknown_lib` | BLOCKER | `FROM name(...)` is neither a blueprint nor a DuckDB table function |
| `validate.extensions_load_failed` | WARN | `extensions.sql` failed to load in validate's session |
| `validate.blueprint_load_failed` | WARN | A blueprint failed to load, so models using it are checked partially |
| `validate.builtin_introspection_failed` | WARN | DuckDB's built-in table functions could not be listed |
| `validate.walk_skipped` | WARN | Part of the project tree could not be walked |
| `validate.lineage_extract_failed` | WARN | The model's lineage or AST could not be read, so its DAG and cross-AST checks were skipped |
