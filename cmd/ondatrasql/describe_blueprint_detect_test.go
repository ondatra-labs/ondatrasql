// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
)

// writeLib drops a Starlark blueprint file at projectDir/lib/<name>.
// Mirrors the helper in internal/libregistry/registry_test.go so this
// test stays self-contained.
func writeLib(t *testing.T, projectDir, fileName, code string) {
	t.Helper()
	libDir := filepath.Join(projectDir, "lib")
	if err := os.MkdirAll(libDir, 0o755); err != nil {
		t.Fatalf("mkdir lib: %v", err)
	}
	if err := os.WriteFile(filepath.Join(libDir, fileName), []byte(code), 0o644); err != nil {
		t.Fatalf("write lib: %v", err)
	}
}

// validBlueprint is a minimal Starlark blueprint that ScanLenient can
// parse cleanly. The test only exercises the cross-link resolution
// path, not the full blueprint contract.
const validBlueprint = `
API = {
    "base_url": "https://api.example.com",
    "fetch": {
        "args": ["api_key"],
    },
}

def fetch(api_key, page):
    pass
`

// brokenBlueprint deliberately fails Starlark parsing so ScanLenient
// records it in scanErrs. Used to drive the "model references a broken
// blueprint" branch.
const brokenBlueprint = `def fetch(`

// TestDetectModelBlueprint regression-tests the cross-link resolver
// behind `describe <model>`. The four branches map 1:1 to the
// BlueprintError contract:
//   - clean lib + matching call          → ("name", "")
//   - clean lib + no call                → ("",     "")
//   - call references a broken blueprint → ("",     "blueprint ... failed to parse")
//   - registry empty due to parse errors → ("",     "blueprint registry empty ...")
//
// Plus the trivial guards (nil session, empty SQL → both empty) so the
// helper is safe to call from a test/preview context that hasn't yet
// resolved the model SQL.
func TestDetectModelBlueprint(t *testing.T) {
	sess, err := duckdb.NewSession(":memory:")
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	defer sess.Close()

	t.Run("clean_lib_matching_call", func(t *testing.T) {
		dir := t.TempDir()
		writeLib(t, dir, "example_api.star", validBlueprint)

		bp, bpErr := detectModelBlueprint(sess, dir,
			`SELECT id::BIGINT AS id FROM example_api(api_key=>'k')`)
		if bp != "example_api" {
			t.Errorf("blueprint = %q, want example_api", bp)
		}
		if bpErr != "" {
			t.Errorf("blueprint_error = %q, want empty", bpErr)
		}
	})

	t.Run("clean_lib_no_call_in_sql", func(t *testing.T) {
		dir := t.TempDir()
		writeLib(t, dir, "example_api.star", validBlueprint)

		bp, bpErr := detectModelBlueprint(sess, dir, `SELECT 1 AS id`)
		if bp != "" || bpErr != "" {
			t.Errorf("expected ('',''), got (%q, %q)", bp, bpErr)
		}
	})

	t.Run("model_references_broken_blueprint", func(t *testing.T) {
		dir := t.TempDir()
		writeLib(t, dir, "example_api.star", validBlueprint)
		writeLib(t, dir, "broken_lib.star", brokenBlueprint)

		bp, bpErr := detectModelBlueprint(sess, dir,
			`SELECT id::BIGINT AS id FROM broken_lib(x=>1)`)
		if bp != "" {
			t.Errorf("blueprint = %q, want empty (broken blueprint must not resolve)", bp)
		}
		if !strings.Contains(bpErr, "broken_lib") {
			t.Errorf("blueprint_error = %q, want reference to broken_lib", bpErr)
		}
		if !strings.Contains(bpErr, "failed to parse") {
			t.Errorf("blueprint_error = %q, want 'failed to parse' phrase", bpErr)
		}
	})

	t.Run("registry_empty_due_to_parse_errors", func(t *testing.T) {
		dir := t.TempDir()
		writeLib(t, dir, "broken_a.star", brokenBlueprint)
		writeLib(t, dir, "broken_b.star", brokenBlueprint)

		bp, bpErr := detectModelBlueprint(sess, dir,
			`SELECT id::BIGINT AS id FROM broken_a(x=>1)`)
		if bp != "" {
			t.Errorf("blueprint = %q, want empty (registry is empty)", bp)
		}
		if !strings.Contains(bpErr, "blueprint registry empty") {
			t.Errorf("blueprint_error = %q, want 'blueprint registry empty' phrase", bpErr)
		}
	})

	t.Run("nil_session_returns_empty", func(t *testing.T) {
		dir := t.TempDir()
		writeLib(t, dir, "example_api.star", validBlueprint)

		bp, bpErr := detectModelBlueprint(nil, dir, `SELECT 1 AS id`)
		if bp != "" || bpErr != "" {
			t.Errorf("expected ('',''), got (%q, %q)", bp, bpErr)
		}
	})

	t.Run("empty_sql_returns_empty", func(t *testing.T) {
		dir := t.TempDir()
		writeLib(t, dir, "example_api.star", validBlueprint)

		bp, bpErr := detectModelBlueprint(sess, dir, "")
		if bp != "" || bpErr != "" {
			t.Errorf("expected ('',''), got (%q, %q)", bp, bpErr)
		}
	})
}
