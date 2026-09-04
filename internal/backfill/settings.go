// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package backfill

// inertDuckDBSettings lists DuckDB settings that cannot change what a query
// returns. A `SET` of one of these in config/ contributes nothing to any
// model's config hash, so bumping thread count or memory limit does not
// rebuild the lake.
//
// The list is an ALLOWLIST and the default is "semantic": a setting that is
// not named here — including one added by a future DuckDB release — keeps
// forcing a rebuild. Being wrong that way costs a needless rebuild; being
// wrong the other way serves stale data.
//
// The 74 names were classified against duckdb_settings(). The embedded DuckDB
// currently exposes 166 settings, so this covers a little under half of them.
// TestInertDuckDBSettings_ExistUpstream pins that every name here still exists
// in the embedded build — a rename upstream would otherwise silently un-list a
// setting and nobody would notice, because the fallback is the safe direction.
//
// Deliberately NOT here, despite sounding harmless:
//
//	integer_division                        changes what the / operator returns
//	scalar_subquery_error_on_multiple_rows  returns a random row instead of erroring
//	search_path, schema                     which table an unqualified name resolves to
//	preserve_insertion_order                row order when there is no ORDER BY
//	preserve_identifier_case                output column names
//	binary_as_string                        how Parquet binary data is read
//	TimeZone, Calendar                      date and time arithmetic
//	default_collation, default_order        comparison and ordering
//	default_null_order, null_order          ditto
//	ieee_floating_point_ops                 NaN versus error/NULL
//	old_implicit_casting                    implicit VARCHAR casts
//	disable_timestamptz_casts               cast behaviour
//	file_search_path, home_directory        where input files are found
//	http_proxy*                             where HTTP data comes from
//	secret_directory, default_secret_storage  which credentials apply
//	custom_extension_repository, extension_directory  which implementation of a function loads
//	enable_external_access, allowed_paths   whether data is reachable at all
//	every debug_* and force_*               conservative: they exist to change behaviour
var inertDuckDBSettings = map[string]bool{
	// Threads, memory and spill location: how much machine the query gets, not what it computes.
	"threads":                      true,
	"worker_threads":               true,
	"external_threads":             true,
	"pin_threads":                  true,
	"memory_limit":                 true,
	"max_memory":                   true,
	"block_allocator_memory":       true,
	"streaming_buffer_size":        true,
	"allocator_background_threads": true,
	"allocator_bulk_deallocation_flush_threshold": true,
	"allocator_flush_threshold":                   true,
	"temp_directory":                              true,
	"max_temp_directory_size":                     true,
	"scheduler_process_partial":                   true,

	// Optimizer and join thresholds: they pick a different plan for the same answer.
	"asof_loop_join_threshold":      true,
	"merge_join_threshold":          true,
	"nested_loop_join_threshold":    true,
	"perfect_ht_threshold":          true,
	"dynamic_or_filter_threshold":   true,
	"ordered_aggregate_threshold":   true,
	"pivot_filter_threshold":        true,
	"index_scan_max_count":          true,
	"index_scan_percentage":         true,
	"late_materialization_max_rows": true,
	"prefer_range_joins":            true,

	// Caching and prefetching: same bytes, fetched differently.
	"enable_external_file_cache":   true,
	"validate_external_file_cache": true,
	"parquet_metadata_cache":       true,
	"enable_http_metadata_cache":   true,
	"disable_parquet_prefetching":  true,
	"prefetch_all_parquet_files":   true,
	"storage_block_prefetch":       true,
	"enable_caching_operators":     true,
	"enable_fsst_vectors":          true,
	"enable_object_cache":          true,

	// Checkpoint, WAL and write buffering: when data is flushed, not what it is.
	"checkpoint_threshold":                true,
	"wal_autocheckpoint":                  true,
	"wal_autocheckpoint_entries":          true,
	"auto_checkpoint_skip_wal_threshold":  true,
	"max_vacuum_tasks":                    true,
	"vacuum_rebuild_indexes":              true,
	"write_buffer_row_group_count":        true,
	"write_buffer_row_group_memory_limit": true,
	"partitioned_write_flush_threshold":   true,
	"partitioned_write_max_open_files":    true,
	"experimental_metadata_reuse":         true,
	"immediate_transaction_mode":          true,

	// Profiling, logging and progress reporting: observability only.
	"enable_profiling":          true,
	"profiling_mode":            true,
	"profiling_coverage":        true,
	"profile_output":            true,
	"profiling_output":          true,
	"custom_profiling_settings": true,
	"explain_output":            true,
	"enable_progress_bar":       true,
	"enable_progress_bar_print": true,
	"progress_bar_time":         true,
	"enable_logging":            true,
	"logging_level":             true,
	"logging_mode":              true,
	"logging_storage":           true,
	"enabled_log_types":         true,
	"disabled_log_types":        true,
	"log_query_path":            true,
	"enable_http_logging":       true,
	"http_logging_output":       true,
	"duckdb_api":                true,
	"custom_user_agent":         true,
	"errors_as_json":            true,
	"catalog_error_max_schemas": true,

	// Legacy no-ops: DuckDB documents these as ignored.
	"user":                     true,
	"username":                 true,
	"password":                 true,
	"allow_unredacted_secrets": true,
}
