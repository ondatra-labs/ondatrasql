// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package backfill

import (
	"sort"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// TestInertDuckDBSettings_ExistUpstream pins every name on the inert allowlist
// against the DuckDB actually embedded in this build.
//
// The allowlist is the one place where being wrong is silent: a setting whose
// name we get wrong — a typo, or a rename in a DuckDB upgrade — simply stops
// matching, and the statement falls back to the global bucket. That direction
// is safe, but it also means the entry is dead and nobody would notice. This
// test turns that into a build failure so the list stays honest.
func TestInertDuckDBSettings_ExistUpstream(t *testing.T) {
	sess := testutil.NewSession(t)

	rows, err := sess.QueryRows("SELECT name FROM duckdb_settings()")
	if err != nil {
		t.Fatalf("query duckdb_settings(): %v", err)
	}
	upstream := make(map[string]bool, len(rows))
	for _, row := range rows {
		upstream[strings.ToLower(strings.TrimSpace(row))] = true
	}
	if len(upstream) == 0 {
		t.Fatal("duckdb_settings() returned nothing")
	}

	var stale []string
	for name := range inertDuckDBSettings {
		if !upstream[name] {
			stale = append(stale, name)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("inertDuckDBSettings names not present in this DuckDB build (renamed or misspelled — they are silently dead): %v", stale)
	}

	t.Logf("allowlist covers %d of %d DuckDB settings", len(inertDuckDBSettings), len(upstream))
}

// TestInertDuckDBSettings_ExcludesKnownSemantic guards the other direction:
// settings that demonstrably change query results must never appear on the
// allowlist, however harmless the name sounds.
func TestInertDuckDBSettings_ExcludesKnownSemantic(t *testing.T) {
	mustBeSemantic := []string{
		"TimeZone", "Calendar",
		"integer_division", "ieee_floating_point_ops", "old_implicit_casting",
		"disable_timestamptz_casts", "scalar_subquery_error_on_multiple_rows",
		"default_collation", "default_order", "default_null_order", "null_order",
		"search_path", "schema", "file_search_path", "home_directory",
		"binary_as_string", "preserve_insertion_order", "preserve_identifier_case",
		"secret_directory", "default_secret_storage",
		"custom_extension_repository", "extension_directory", "autoload_known_extensions",
		"enable_external_access", "allowed_paths", "allowed_directories", "disabled_filesystems",
		"disabled_optimizers", "http_proxy",
	}
	for _, name := range mustBeSemantic {
		if inertDuckDBSettings[strings.ToLower(name)] {
			t.Errorf("%s can change query results and must not be on the inert allowlist", name)
		}
	}
}
