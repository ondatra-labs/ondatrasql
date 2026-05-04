// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package main

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// TestCLI_ExitCodeContract regression-tests the documented exit-code
// contract across CLI commands. The contract (per docs/reference/
// pipeline/cli.md and CLAUDE.md) is:
//
//	0 — clean (no findings, no errors)
//	1 — findings (BLOCKER, or WARN with --strict) or runtime failure
//	2 — invocation error (bad flag, file not found, scope outside
//	    models/ or lib/)
//
// This test pins exit codes for both finding-style errors and
// invocation-style errors so a future refactor can't silently drift
// the contract for one command (e.g. `describe blueprint` was
// returning 1 for invocation errors instead of 2 — Codex round 4
// caught it).
//
// Convention: if `wantExit == 2`, the returned error MUST implement
// the `exitCoder` interface (which main() reads). The test asserts
// that explicitly, since silent fallback to exit 1 is the bug we're
// guarding against.
func TestCLI_ExitCodeContract(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	if err := os.Chdir(p.Dir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(oldWd) //nolint:errcheck // test cleanup; cwd restoration on test exit is best-effort

	p.AddModel("staging/x.sql", `-- @kind: table
SELECT 1 AS id`)

	cases := []struct {
		name     string
		args     []string
		wantExit int
		// If wantExit >= 1, an error must be returned. If it's 2, the
		// error must satisfy exitCoder.ExitCode() == 2.
		wantPhrase string
	}{
		// Invocation errors → exit 2
		{"validate_unknown_flag", []string{"validate", "--bogus-flag"}, 2, "unknown flag"},
		{"validate_bad_output_value", []string{"validate", "--output=invalid"}, 2, ""},
		{"validate_path_outside_scope", []string{"validate", "/tmp/somewhere-outside.sql"}, 2, ""},

		// Strict-args errors → exit 2. The documented contract treats
		// "bad CLI invocation" (extra/unexpected args, unknown flag,
		// unknown command) as an invocation error, distinct from
		// runtime/finding errors which use exit 1.
		{"version_extra_arg", []string{"version", "extra"}, 2, "unexpected args"},
		{"stats_extra_arg", []string{"stats", "extra"}, 2, "unexpected args"},
		{"lineage_extra_arg", []string{"lineage", "staging.x", "extra"}, 2, "extra args"},
		{"unknown_command", []string{"no_such_command"}, 2, "unknown command"},

		// history/query flag-arg errors — R6 finding: previously
		// returned exit 1 instead of 2 because invocationErr wrapping
		// was missing on the new query/history dispatchers.
		{"history_unknown_flag", []string{"history", "--bogus"}, 2, "unknown flag"},
		{"history_bad_limit", []string{"history", "--limit=abc"}, 2, "invalid limit"},
		{"history_zero_limit", []string{"history", "--limit=0"}, 2, "positive integer"},
		{"query_no_target", []string{"query"}, 2, "usage:"},
		{"query_bad_target", []string{"query", "no_dot"}, 2, "schema.table"},
		{"query_unknown_flag", []string{"query", "raw.customers", "--bogus"}, 2, "unknown flag"},
		{"query_bad_format", []string{"query", "raw.customers", "--format=xml"}, 2, "invalid --format"},

		// auth/sql/edit/new/init/events/schedule strict-args.
		// (R9 #9) Pre-fix the cli.md docs claimed all these surfaces
		// were regression-tested here, but only validate/version/
		// stats/lineage/history/query had explicit cases. The wider
		// run_test.go's TestRun_StrictArguments_RejectsExtraArgs
		// table covers them via integration tag, but the contract
		// test must also pin the exit code so the public claim
		// matches the actual coverage.
		{"auth_extra_arg", []string{"auth", "google", "extra"}, 2, "extra args"},
		{"sql_no_query", []string{"sql"}, 2, "usage:"},
		{"sql_unknown_flag", []string{"sql", "SELECT 1", "--bogus"}, 2, "unknown flag"},
		{"edit_no_target", []string{"edit"}, 2, "usage:"},
		{"new_no_target", []string{"new"}, 2, "usage:"},
		{"init_extra_arg", []string{"init", "extra"}, 2, "unexpected args"},
		{"events_no_port", []string{"events"}, 2, "usage:"},
		{"schedule_extra_arg", []string{"schedule", "*/5 * * * *", "extra"}, 2, "extra args"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := run(tc.args)
			if err == nil {
				t.Fatalf("expected error for %v, got nil", tc.args)
			}
			if tc.wantPhrase != "" && !strings.Contains(err.Error(), tc.wantPhrase) {
				t.Errorf("err=%v, want phrase %q", err, tc.wantPhrase)
			}

			// Check exit code via exitCoder interface.
			gotExit := 1 // default for any error per main()
			var ec exitCoder
			if errors.As(err, &ec) {
				gotExit = ec.ExitCode()
			}
			if gotExit != tc.wantExit {
				t.Errorf("exit code = %d, want %d (err: %v)", gotExit, tc.wantExit, err)
			}
		})
	}
}

// TestCLI_DescribeBlueprintExitCodeContract pins that
// `describe blueprint` invocation errors return exit code 2,
// matching the validate-command contract. This was a Codex round-4
// finding: invalid blueprint names / unknown flags returned exit 1
// instead of 2, drifting from the documented invocation-error
// contract.
//
// Separated from the main exit-code table because describeBlueprint
// requires a project with a `lib/` directory; the test wires up an
// empty project and calls into the dispatcher directly.
func TestCLI_DescribeBlueprintExitCodeContract(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	if err := os.Chdir(p.Dir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(oldWd) //nolint:errcheck // test cleanup; cwd restoration on test exit is best-effort

	cases := []struct {
		name     string
		args     []string
		wantExit int
	}{
		{"unknown_blueprint", []string{"describe", "blueprint", "no_such_blueprint"}, 2},
		{"invalid_name_chars", []string{"describe", "blueprint", "../../etc/passwd"}, 2},
		{"unknown_flag", []string{"describe", "blueprint", "--bogus"}, 2},
		// Listing form rejects --fields (the projection only makes sense
		// for the detail shape). Round-5 finding: this previously
		// returned exit 1, drifting from the invocation-error contract.
		{"listing_fields_rejected", []string{"describe", "blueprint", "--fields=fetch.args"}, 2},
		// R11 #5: `--fields=,` and whitespace/comma-only forms must
		// be rejected explicitly — pre-fix splitFieldMask dropped
		// empties and projection produced a near-empty envelope.
		{"fields_comma_only", []string{"describe", "blueprint", "x", "--fields=,"}, 2},
		{"fields_whitespace_only", []string{"describe", "blueprint", "x", "--fields= , , "}, 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := run(tc.args)
			if err == nil {
				t.Fatalf("expected error for %v, got nil", tc.args)
			}
			gotExit := 1
			var ec exitCoder
			if errors.As(err, &ec) {
				gotExit = ec.ExitCode()
			}
			if gotExit != tc.wantExit {
				t.Errorf("exit code = %d, want %d (err: %v)", gotExit, tc.wantExit, err)
			}
		})
	}
}
