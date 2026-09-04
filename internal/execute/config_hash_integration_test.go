// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/dag"
	"github.com/ondatra-labs/ondatrasql/internal/execute"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// runModelWithProjectDir runs a model through a Runner that knows the project
// root, which is what makes the config hash participate in the run-type
// decision. The plain runModel helper deliberately leaves projectDir unset.
func runModelWithProjectDir(t *testing.T, p *testutil.Project, relPath string) *execute.Result {
	t.Helper()
	model, err := parser.ParseModel(filepath.Join(p.Dir, "models", relPath), p.Dir)
	if err != nil {
		t.Fatalf("parse %s: %v", relPath, err)
	}
	runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
	runner.SetProjectDir(p.Dir)
	result, err := runner.Run(context.Background(), model)
	if err != nil {
		t.Fatalf("run %s: %v", relPath, err)
	}
	return result
}

func rewriteConfig(t *testing.T, p *testutil.Project, rel, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(p.Dir, "config", rel), []byte(body), 0o644); err != nil {
		t.Fatalf("rewrite config/%s: %v", rel, err)
	}
}

// assertNotRebuilt pins that a run did no work at all. Asserting only
// RunReason != "config changed" would pass for a model that rebuilt for some
// other reason, which leaves the negative case unpinned — the whole point of
// these subtests is that nothing happened.
func assertNotRebuilt(t *testing.T, r *execute.Result, what string) {
	t.Helper()
	if r.RunType != "skip" {
		t.Errorf("%s must not rebuild anything: got run_type=%q reason=%q, want skip", what, r.RunType, r.RunReason)
	}
}

// TestRunType_ConfigChange_Attribution pins the three-part contract that a
// config edit gets its own run_reason, that cosmetic config edits don't
// rebuild the lake, and that non-semantic config files are out of the hash
// entirely.
//
// Before this, every config/*.sql byte was folded into the model hash, so any
// edit — a reworded comment, a rotated secret, a state-backend switch — landed
// as run_reason "sql changed" and forced a full backfill of every model.
func TestRunType_ConfigChange_Attribution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/nums.sql", `-- @kind: table
SELECT safe_divide(10, 2) AS v
`)

	const macroRel = "macros/macros_helpers.sql"
	const macroV1 = "CREATE OR REPLACE MACRO safe_divide(a, b) AS\n  CASE WHEN b = 0 THEN NULL ELSE a / b END;\n"

	rewriteConfig(t, p, macroRel, macroV1)
	if r := runModelWithProjectDir(t, p, "staging/nums.sql"); r.RunType != "backfill" {
		t.Fatalf("first run: want backfill, got %q (%s)", r.RunType, r.RunReason)
	}

	t.Run("comment-only config edit is not a config change", func(t *testing.T) {
		rewriteConfig(t, p, macroRel, "-- a freshly reworded explanation\n"+macroV1)
		assertNotRebuilt(t, runModelWithProjectDir(t, p, "staging/nums.sql"), "comment-only config edit")
	})

	t.Run("state.sql is not a config change", func(t *testing.T) {
		// state.sql never reaches the model execution session, so it cannot
		// change what a model computes. secrets.sql and catalog.sql are NOT
		// in this list: they carry topology (ENDPOINT, REGION, DATA_PATH),
		// not credentials — those live in .env.
		rewriteConfig(t, p, "state.sql", "ATTACH '"+filepath.Join(p.Dir, "state.duckdb")+"' AS state; -- touched\n")
		assertNotRebuilt(t, runModelWithProjectDir(t, p, "staging/nums.sql"), "editing config/state.sql")
	})

	t.Run("macro body edit is reported as config changed", func(t *testing.T) {
		rewriteConfig(t, p, macroRel, "CREATE OR REPLACE MACRO safe_divide(a, b) AS\n  CASE WHEN b = 0 THEN -1 ELSE a / b END;\n")
		r := runModelWithProjectDir(t, p, "staging/nums.sql")
		if r.RunType != "backfill" {
			t.Errorf("macro edit: want run_type backfill, got %q", r.RunType)
		}
		if r.RunReason != "config changed" {
			t.Errorf("macro edit: want run_reason %q, got %q", "config changed", r.RunReason)
		}
	})

	t.Run("model sql edit is still reported as sql changed", func(t *testing.T) {
		p.AddModel("staging/nums.sql", `-- @kind: table
SELECT safe_divide(20, 2) AS v
`)
		r := runModelWithProjectDir(t, p, "staging/nums.sql")
		if r.RunReason != "sql changed" {
			t.Errorf("sql edit: want run_reason %q, got %q", "sql changed", r.RunReason)
		}
	})
}

// TestRunType_ConfigChange_PerModelScope pins the per-model contract at
// runtime: editing a macro rebuilds the models that call it and leaves the
// rest alone. Before config indexing, one shared hash covered the whole
// config directory, so any edit rebuilt every model in the project.
func TestRunType_ConfigChange_PerModelScope(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	const macroRel = "macros/scoped.sql"

	rewriteConfig(t, p, macroRel,
		"CREATE OR REPLACE MACRO used_macro(x) AS x + 1;\n"+
			"CREATE OR REPLACE MACRO other_macro(x) AS x + 2;\n")
	if err := p.Sess.Exec("CREATE OR REPLACE MACRO used_macro(x) AS x + 1"); err != nil {
		t.Fatalf("define used_macro: %v", err)
	}

	p.AddModel("staging/uses_macro.sql", `-- @kind: table
SELECT used_macro(1) AS v
`)
	p.AddModel("staging/plain.sql", `-- @kind: table
SELECT 1 AS v
`)
	for _, m := range []string{"staging/uses_macro.sql", "staging/plain.sql"} {
		if r := runModelWithProjectDir(t, p, m); r.RunType != "backfill" {
			t.Fatalf("first run of %s: want backfill, got %q (%s)", m, r.RunType, r.RunReason)
		}
	}

	t.Run("editing an unused macro rebuilds nothing", func(t *testing.T) {
		rewriteConfig(t, p, macroRel,
			"CREATE OR REPLACE MACRO used_macro(x) AS x + 1;\n"+
				"CREATE OR REPLACE MACRO other_macro(x) AS x + 999;\n")
		for _, m := range []string{"staging/uses_macro.sql", "staging/plain.sql"} {
			assertNotRebuilt(t, runModelWithProjectDir(t, p, m), m+" calls neither edited macro")
		}
	})

	t.Run("editing a used macro rebuilds only its callers", func(t *testing.T) {
		rewriteConfig(t, p, macroRel,
			"CREATE OR REPLACE MACRO used_macro(x) AS x + 42;\n"+
				"CREATE OR REPLACE MACRO other_macro(x) AS x + 999;\n")

		caller := runModelWithProjectDir(t, p, "staging/uses_macro.sql")
		if caller.RunReason != "config changed" {
			t.Errorf("caller: want run_reason %q, got %q (run_type=%q)", "config changed", caller.RunReason, caller.RunType)
		}

		assertNotRebuilt(t, runModelWithProjectDir(t, p, "staging/plain.sql"),
			"a model that calls no config macro")
	})
}

// TestRunType_ConfigChange_InertSettings pins the end-to-end payoff of the
// settings allowlist: bumping a resource knob must not rebuild the lake, while
// a setting that can change a result still must.
func TestRunType_ConfigChange_InertSettings(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/nums.sql", `-- @kind: table
SELECT 1 AS v
`)
	rewriteConfig(t, p, "settings.sql", "SET threads = 4;\n")
	if r := runModelWithProjectDir(t, p, "staging/nums.sql"); r.RunType != "backfill" {
		t.Fatalf("first run: want backfill, got %q (%s)", r.RunType, r.RunReason)
	}

	t.Run("bumping threads and memory does not rebuild", func(t *testing.T) {
		rewriteConfig(t, p, "settings.sql", "SET threads = 16;\nSET memory_limit = '32GB';\n")
		assertNotRebuilt(t, runModelWithProjectDir(t, p, "staging/nums.sql"), "bumping threads and memory_limit")
	})

	t.Run("changing TimeZone does rebuild", func(t *testing.T) {
		rewriteConfig(t, p, "settings.sql", "SET threads = 16;\nSET memory_limit = '32GB';\nSET TimeZone = 'Europe/Stockholm';\n")
		r := runModelWithProjectDir(t, p, "staging/nums.sql")
		if r.RunReason != "config changed" {
			t.Errorf("TimeZone changes date arithmetic; want run_reason %q, got %q (run_type=%q)", "config changed", r.RunReason, r.RunType)
		}
	})

	t.Run("adding a schema does not rebuild", func(t *testing.T) {
		rewriteConfig(t, p, "schemas.sql", "CREATE SCHEMA IF NOT EXISTS mart;\n")
		assertNotRebuilt(t, runModelWithProjectDir(t, p, "staging/nums.sql"), "adding a schema")
	})
}

// TestRunType_UpgradeRebuild_NamesItself pins that the one-time rebuild caused
// by the hash-format change reports itself as such.
//
// Removing config from sql_hash changes it for every model in every existing
// project, so the first run after upgrading rebuilds everything. That rebuild
// is accepted, but it must not masquerade as "sql changed" — anyone reading
// --json or commit_extra_info would conclude their models had been edited.
//
// Pre-upgrade commits carry no `config_hash` key at all, which `->>` reports
// as NULL (an empty value comes back as ”). That is what distinguishes them.
func TestRunType_UpgradeRebuild_NamesItself(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	p.AddModel("staging/nums.sql", `-- @kind: table
SELECT 1 AS v
`)
	if r := runModelWithProjectDir(t, p, "staging/nums.sql"); r.RunType != "backfill" {
		t.Fatalf("first run: want backfill, got %q (%s)", r.RunType, r.RunReason)
	}

	// Simulate a pre-upgrade commit: same model, but its stored metadata has
	// no config_hash key and an sql_hash from the old composition.
	commit := `{"model":"staging.nums","sql_hash":"legacy-format-hash","depends":[],"run_type":"backfill"}`
	stage := "BEGIN;\n" +
		"INSERT INTO staging.nums SELECT 1;\n" +
		"CALL set_commit_message('upgrade-sim', 'legacy commit', extra_info => '" + commit + "');\n" +
		"COMMIT"
	if err := p.Sess.Exec(stage); err != nil {
		t.Fatalf("stage a pre-upgrade commit: %v", err)
	}

	r := runModelWithProjectDir(t, p, "staging/nums.sql")
	if r.RunType != "backfill" {
		t.Errorf("a pre-upgrade commit must force a rebuild, got run_type=%q", r.RunType)
	}
	if r.RunReason == "sql changed" {
		t.Error("the upgrade rebuild must not be reported as 'sql changed' — the model was never edited")
	}
	if r.RunReason != "hash format changed (upgrade)" {
		t.Errorf("want run_reason %q, got %q", "hash format changed (upgrade)", r.RunReason)
	}

	// The reason must survive into the lake, not just the console: the upgrade
	// is a one-time historical event, and "why did everything rebuild that
	// day" has to be answerable long after the run's output is gone.
	persisted, err := p.Sess.QueryValue(
		"SELECT commit_extra_info->>'run_reason' FROM " + p.Sess.CatalogAlias() +
			".snapshots() WHERE LOWER(commit_extra_info->>'model') = 'staging.nums' " +
			"ORDER BY snapshot_id DESC LIMIT 1")
	if err != nil {
		t.Fatalf("read run_reason from commit metadata: %v", err)
	}
	if persisted != "hash format changed (upgrade)" {
		t.Errorf("commit_extra_info run_reason = %q, want %q", persisted, "hash format changed (upgrade)")
	}
}
