// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package backfill

import (
	"os"
	"path/filepath"
	"testing"

	sqlfiles "github.com/ondatra-labs/ondatrasql/internal/sql"
)

// writeConfig writes a config file, creating parent directories as needed.
func writeConfig(t *testing.T, dir, rel, body string) {
	t.Helper()
	full := filepath.Join(dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatalf("mkdir for %s: %v", rel, err)
	}
	if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", rel, err)
	}
}

func hashFor(t *testing.T, dir, refText string) string {
	t.Helper()
	ix, err := BuildConfigIndex(dir)
	if err != nil {
		t.Fatalf("BuildConfigIndex(%s): %v", dir, err)
	}
	return ix.HashFor(refText)
}

// buildIndex asserts a clean build and returns the index (which may be nil).
func buildIndex(t *testing.T, dir string) *ConfigIndex {
	t.Helper()
	ix, err := BuildConfigIndex(dir)
	if err != nil {
		t.Fatalf("BuildConfigIndex(%s): %v", dir, err)
	}
	return ix
}

func TestBuildConfigIndex_NothingHashable(t *testing.T) {
	t.Parallel()

	t.Run("missing dir", func(t *testing.T) {
		t.Parallel()
		if ix := buildIndex(t, filepath.Join(t.TempDir(), "nope")); ix != nil {
			t.Error("missing config dir should yield a nil index")
		}
	})

	t.Run("empty dir", func(t *testing.T) {
		t.Parallel()
		if ix := buildIndex(t, t.TempDir()); ix != nil {
			t.Error("empty config dir should yield a nil index")
		}
	})

	t.Run("only excluded files", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		writeConfig(t, dir, "state.sql", "ATTACH 'state';")
		if ix := buildIndex(t, dir); ix != nil {
			t.Error("config holding only excluded files should yield a nil index")
		}
	})

	t.Run("unreadable config file is an error, not silence", func(t *testing.T) {
		t.Parallel()
		if os.Geteuid() == 0 {
			t.Skip("running as root: file mode 0 is still readable")
		}
		dir := t.TempDir()
		writeConfig(t, dir, "settings.sql", "SET TimeZone = 'UTC';")
		if err := os.Chmod(filepath.Join(dir, "settings.sql"), 0o000); err != nil {
			t.Fatalf("chmod: %v", err)
		}
		t.Cleanup(func() { _ = os.Chmod(filepath.Join(dir, "settings.sql"), 0o644) })

		// Degrading to "no config" here would hash every model as if the
		// project had no config at all, silently disabling config-change
		// detection for the rest of its life.
		ix, err := BuildConfigIndex(dir)
		if err == nil {
			t.Error("an unreadable config file must surface as an error")
		}
		if ix != nil {
			t.Error("a failed build must not return a usable index")
		}
	})

	t.Run("non-sql files are ignored", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		writeConfig(t, dir, "README.md", "not config")
		if ix := buildIndex(t, dir); ix != nil {
			t.Error("non-sql files should not produce an index")
		}
	})
}

// TestConfigIndex_Normalization pins that cosmetic config edits don't rebuild
// the lake: only the statement text matters, not the comments or whitespace
// around it. Case is deliberately significant — config carries endpoints,
// paths and environment names where folding case would hide a real change.
func TestConfigIndex_Normalization(t *testing.T) {
	t.Parallel()
	const ref = "SELECT safe_divide(a, b) FROM t"
	const body = "CREATE OR REPLACE MACRO safe_divide(a, b) AS a / b;"

	t.Run("comment edit does not bust hash", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		writeConfig(t, dir, "macros/helpers.sql", "-- old note\n"+body)
		h1 := hashFor(t, dir, ref)
		writeConfig(t, dir, "macros/helpers.sql", "-- an entirely different note\n"+body+"\n-- trailing note")
		if hashFor(t, dir, ref) != h1 {
			t.Error("comment-only edit must not change the model's config hash")
		}
	})

	t.Run("whitespace edit does not bust hash", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		writeConfig(t, dir, "macros/helpers.sql", body)
		h1 := hashFor(t, dir, ref)
		writeConfig(t, dir, "macros/helpers.sql", "\n\nCREATE OR REPLACE   MACRO safe_divide(a, b)\n\tAS a / b;\n\n")
		if hashFor(t, dir, ref) != h1 {
			t.Error("whitespace-only edit must not change the model's config hash")
		}
	})

	t.Run("case edit busts hash", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		writeConfig(t, dir, "settings.sql", "SET s3_endpoint = 'Storage.Example.COM';")
		h1 := hashFor(t, dir, ref)
		writeConfig(t, dir, "settings.sql", "SET s3_endpoint = 'storage.example.com';")
		if hashFor(t, dir, ref) == h1 {
			t.Error("config is case-significant; a literal's case change must bust the hash")
		}
	})

	t.Run("edit inside a multi-line literal busts hash", func(t *testing.T) {
		t.Parallel()
		// A `--` inside a string literal that spans lines is data, not a
		// comment. Resetting quote state per line would strip the rest of the
		// literal and let these two configs hash equal.
		dir := t.TempDir()
		writeConfig(t, dir, "settings.sql", "SET s3_endpoint = 'host-a\n-- keep\n/path-one';")
		h1 := hashFor(t, dir, ref)
		writeConfig(t, dir, "settings.sql", "SET s3_endpoint = 'host-a\n-- keep\n/path-two';")
		if hashFor(t, dir, ref) == h1 {
			t.Error("content after a -- inside a multi-line literal must still be hashed")
		}
	})

	t.Run("edit inside a quoted identifier busts hash", func(t *testing.T) {
		t.Parallel()
		// A `--` inside a "quoted identifier" is part of the name, not a
		// comment. Treating it as one would discard the rest of the statement
		// and let two different configs hash equal.
		dir := t.TempDir()
		writeConfig(t, dir, "sources.sql", `CREATE VIEW "events--v1" AS SELECT 1 AS a;`)
		h1 := hashFor(t, dir, "SELECT * FROM events")
		writeConfig(t, dir, "sources.sql", `CREATE VIEW "events--v1" AS SELECT 2 AS a;`)
		if hashFor(t, dir, "SELECT * FROM events") == h1 {
			t.Error("content after a -- inside a quoted identifier must still be hashed")
		}
	})

	t.Run("real edit busts hash", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		writeConfig(t, dir, "macros/helpers.sql", body)
		h1 := hashFor(t, dir, ref)
		writeConfig(t, dir, "macros/helpers.sql", "CREATE OR REPLACE MACRO safe_divide(a, b) AS CASE WHEN b = 0 THEN NULL ELSE a / b END;")
		if hashFor(t, dir, ref) == h1 {
			t.Error("changing a referenced macro's body must bust the hash")
		}
	})
}

// TestConfigIndex_ExcludedFiles pins which config files stay out of the hash.
//
// Only state.sql does. It never reaches the model execution session —
// state.Open runs it in its own :memory: session — so it cannot change what a
// model computes.
//
// secrets.sql and catalog.sql deliberately stay IN. They read like plumbing,
// but credentials live in .env (config is os.ExpandEnv'd, so secret values are
// ${VAR} references); what these files actually carry is topology — ENDPOINT,
// REGION, SCOPE, HOST, DATA_PATH — and editing that changes which bytes a
// model reads while its SQL is unchanged.
func TestConfigIndex_ExcludedFiles(t *testing.T) {
	t.Parallel()
	const ref = "SELECT 1"

	t.Run("state.sql is out of the hash", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		writeConfig(t, dir, "settings.sql", "SET threads = 4;")
		h1 := hashFor(t, dir, ref)
		writeConfig(t, dir, "state.sql", "-- v1\nATTACH 'a' AS state;")
		if hashFor(t, dir, ref) != h1 {
			t.Error("adding config/state.sql must not change the hash")
		}
		writeConfig(t, dir, "state.sql", "ATTACH 'somewhere-else' AS state;")
		if hashFor(t, dir, ref) != h1 {
			t.Error("editing config/state.sql must not change the hash")
		}
	})

	for _, tc := range []struct{ name, before, after string }{
		{
			"secrets.sql",
			"CREATE SECRET s (TYPE s3, ENDPOINT 'a.example.com', KEY_ID '${AWS_KEY}');",
			"CREATE SECRET s (TYPE s3, ENDPOINT 'b.example.com', KEY_ID '${AWS_KEY}');",
		},
		{
			"catalog.sql",
			"ATTACH 'ducklake:sqlite:lake.sqlite' AS lake (DATA_PATH 'files');",
			"ATTACH 'ducklake:sqlite:lake.sqlite' AS lake (DATA_PATH 's3://other/');",
		},
	} {
		t.Run(tc.name+" repointing rebuilds", func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			writeConfig(t, dir, tc.name, tc.before)
			h1 := hashFor(t, dir, ref)
			writeConfig(t, dir, tc.name, tc.after)
			if hashFor(t, dir, ref) == h1 {
				t.Errorf("config/%s carries topology, not credentials; repointing it must rebuild", tc.name)
			}
		})
	}

	t.Run("exclusion is anchored to the config root", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		writeConfig(t, dir, "macros/state.sql", "CREATE OR REPLACE MACRO m() AS 1;")
		h1 := hashFor(t, dir, "SELECT m()")
		writeConfig(t, dir, "macros/state.sql", "CREATE OR REPLACE MACRO m() AS 2;")
		if hashFor(t, dir, "SELECT m()") == h1 {
			t.Error("config/macros/state.sql is a macro file and must bust the hash")
		}
	})
}

// TestConfigIndex_PerModelScope is the core of the per-model contract: a model
// is rebuilt for the config it actually uses, and only that.
func TestConfigIndex_PerModelScope(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) string {
		t.Helper()
		dir := t.TempDir()
		writeConfig(t, dir, "macros/helpers.sql",
			"CREATE OR REPLACE MACRO used_macro(x) AS x + 1;\n"+
				"CREATE OR REPLACE MACRO other_macro(x) AS x + 2;\n")
		writeConfig(t, dir, "variables/constants.sql",
			"SET VARIABLE used_var = 1;\nSET VARIABLE other_var = 2;\n")
		return dir
	}

	t.Run("unreferenced macro edit does not rebuild", func(t *testing.T) {
		t.Parallel()
		dir := setup(t)
		const ref = "SELECT used_macro(id) FROM t"
		h1 := hashFor(t, dir, ref)
		writeConfig(t, dir, "macros/helpers.sql",
			"CREATE OR REPLACE MACRO used_macro(x) AS x + 1;\n"+
				"CREATE OR REPLACE MACRO other_macro(x) AS x + 999;\n")
		if hashFor(t, dir, ref) != h1 {
			t.Error("editing a macro the model never calls must not rebuild it")
		}
	})

	t.Run("referenced macro edit rebuilds", func(t *testing.T) {
		t.Parallel()
		dir := setup(t)
		const ref = "SELECT used_macro(id) FROM t"
		h1 := hashFor(t, dir, ref)
		writeConfig(t, dir, "macros/helpers.sql",
			"CREATE OR REPLACE MACRO used_macro(x) AS x + 999;\n"+
				"CREATE OR REPLACE MACRO other_macro(x) AS x + 2;\n")
		if hashFor(t, dir, ref) == h1 {
			t.Error("editing a macro the model calls must rebuild it")
		}
	})

	t.Run("variables are scoped through getvariable", func(t *testing.T) {
		t.Parallel()
		dir := setup(t)
		const ref = "SELECT * FROM t WHERE n > getvariable('used_var')"
		h1 := hashFor(t, dir, ref)
		writeConfig(t, dir, "variables/constants.sql",
			"SET VARIABLE used_var = 1;\nSET VARIABLE other_var = 42;\n")
		if hashFor(t, dir, ref) != h1 {
			t.Error("editing an unreferenced variable must not rebuild the model")
		}
		writeConfig(t, dir, "variables/constants.sql",
			"SET VARIABLE used_var = 7;\nSET VARIABLE other_var = 42;\n")
		if hashFor(t, dir, ref) == h1 {
			t.Error("editing the referenced variable must rebuild the model")
		}
	})

	t.Run("macro-calls-macro is followed transitively", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		writeConfig(t, dir, "macros/helpers.sql",
			"CREATE OR REPLACE MACRO inner_macro(x) AS x + 1;\n"+
				"CREATE OR REPLACE MACRO outer_macro(x) AS inner_macro(x) * 2;\n")
		const ref = "SELECT outer_macro(id) FROM t"
		h1 := hashFor(t, dir, ref)
		writeConfig(t, dir, "macros/helpers.sql",
			"CREATE OR REPLACE MACRO inner_macro(x) AS x + 999;\n"+
				"CREATE OR REPLACE MACRO outer_macro(x) AS inner_macro(x) * 2;\n")
		if hashFor(t, dir, ref) == h1 {
			t.Error("a macro reached only through another macro must still rebuild the model")
		}
	})

	t.Run("deleting a referenced macro rebuilds", func(t *testing.T) {
		t.Parallel()
		dir := setup(t)
		const ref = "SELECT used_macro(id) FROM t"
		h1 := hashFor(t, dir, ref)
		writeConfig(t, dir, "macros/helpers.sql", "CREATE OR REPLACE MACRO other_macro(x) AS x + 2;\n")
		if hashFor(t, dir, ref) == h1 {
			t.Error("deleting a macro the model calls must rebuild it")
		}
	})

	t.Run("adding an unreferenced macro does not rebuild", func(t *testing.T) {
		t.Parallel()
		dir := setup(t)
		const ref = "SELECT used_macro(id) FROM t"
		h1 := hashFor(t, dir, ref)
		writeConfig(t, dir, "macros/extra.sql", "CREATE OR REPLACE MACRO brand_new(x) AS x;\n")
		if hashFor(t, dir, ref) != h1 {
			t.Error("adding a macro nobody calls must not rebuild anything")
		}
	})

	t.Run("global settings rebuild every model", func(t *testing.T) {
		t.Parallel()
		dir := setup(t)
		refs := []string{"SELECT used_macro(id) FROM t", "SELECT 1", "SELECT other_macro(id) FROM t"}
		before := make([]string, len(refs))
		for i, ref := range refs {
			before[i] = hashFor(t, dir, ref)
		}
		writeConfig(t, dir, "settings.sql", "SET TimeZone = 'Europe/Stockholm';")
		for i, ref := range refs {
			if hashFor(t, dir, ref) == before[i] {
				t.Errorf("settings.sql applies session-wide; model %d must rebuild", i)
			}
		}
	})

	t.Run("undecomposable macro file falls back to global", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		// A CREATE TABLE among the macros: not attributable to a caller, so
		// the whole file must apply to everyone rather than be dropped.
		writeConfig(t, dir, "macros/helpers.sql",
			"CREATE OR REPLACE MACRO m(x) AS x;\nCREATE TABLE lookup AS SELECT 1 AS k;\n")
		const ref = "SELECT 1"
		h1 := hashFor(t, dir, ref)
		writeConfig(t, dir, "macros/helpers.sql",
			"CREATE OR REPLACE MACRO m(x) AS x;\nCREATE TABLE lookup AS SELECT 2 AS k;\n")
		if hashFor(t, dir, ref) == h1 {
			t.Error("a macro file that didn't decompose must apply to every model")
		}
	})

	t.Run("model touching no config and no globals hashes empty", func(t *testing.T) {
		t.Parallel()
		dir := setup(t)
		if h := hashFor(t, dir, "SELECT 1"); h != "" {
			t.Errorf("model referencing nothing, with no global config, should hash empty, got %q", h)
		}
	})

	t.Run("deterministic across calls", func(t *testing.T) {
		t.Parallel()
		dir := setup(t)
		writeConfig(t, dir, "settings.sql", "SET threads = 4;")
		const ref = "SELECT used_macro(id) FROM t"
		first := hashFor(t, dir, ref)
		second := hashFor(t, dir, ref)
		if first != second {
			t.Errorf("config hash must be stable across calls: %q != %q", first, second)
		}
	})
}

func TestDefinedName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		stmt string
		want string
	}{
		{"CREATE MACRO foo(a) AS a", "foo"},
		{"CREATE OR REPLACE MACRO foo(a) AS a", "foo"},
		{"create or replace macro Foo(a) AS a", "foo"},
		{"CREATE OR REPLACE TEMP MACRO foo(a) AS a", "foo"},
		{"CREATE TEMPORARY MACRO foo(a) AS a", "foo"},
		{"CREATE OR REPLACE FUNCTION foo(a) AS a", "foo"},
		{`CREATE MACRO "Quoted"(a) AS a`, "quoted"},
		{"CREATE MACRO memory.foo(a) AS a", "foo"},
		{"CREATE MACRO foo (a) AS a", "foo"},
		{"SET VARIABLE bar = 1", "bar"},
		{"set variable Bar=1", "bar"},
		{"CREATE TABLE t AS SELECT 1", ""},
		{"SET threads = 4", ""},
		{"SELECT 1", ""},
		{"", ""},
	}
	for _, tt := range tests {
		t.Run(tt.stmt, func(t *testing.T) {
			t.Parallel()
			if got := definedName(tt.stmt); got != tt.want {
				t.Errorf("definedName(%q) = %q, want %q", tt.stmt, got, tt.want)
			}
		})
	}
}

func TestSplitStatements_RespectsStringLiterals(t *testing.T) {
	t.Parallel()
	got := splitStatements("SET VARIABLE a = 'x;y'; SET VARIABLE b = 2;")
	want := []string{"SET VARIABLE a = 'x;y'", "SET VARIABLE b = 2"}
	if len(got) != len(want) {
		t.Fatalf("got %d statements %q, want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("statement %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestConfigIndex_ScaffoldedConfigDecomposes pins that the config `ondatrasql
// init` writes actually decomposes into named fragments. If a template ever
// grows a statement that isn't a plain CREATE MACRO / SET VARIABLE, its whole
// file silently falls back to the always-applies bucket and every model in
// every project starts rebuilding on any edit to it — the exact regression
// this indexing exists to prevent. The failure is invisible at runtime, so it
// needs a test.
func TestConfigIndex_ScaffoldedConfigDecomposes(t *testing.T) {
	t.Parallel()

	// Mirrors the config/ layout written by cmd/ondatrasql/init_cmd.go.
	scaffold := map[string]string{
		"macros/helpers.sql":      "init/macros_helpers.sql",
		"macros/masking.sql":      "init/macros_masking.sql",
		"macros/constraints.sql":  "init/macros_constraint.sql",
		"macros/audits.sql":       "init/macros_audit.sql",
		"macros/warnings.sql":     "init/macros_warning.sql",
		"variables/constants.sql": "init/variables_constants.sql",
		"variables/global.sql":    "init/variables_global.sql",
		"variables/local.sql":     "init/variables_models.sql",
	}

	dir := t.TempDir()
	for rel, src := range scaffold {
		body, err := sqlfiles.Load(src)
		if err != nil {
			t.Fatalf("load %s: %v", src, err)
		}
		writeConfig(t, dir, rel, body)
	}

	// A model that names one scaffolded macro, and one that names nothing.
	const caller = "SELECT * FROM ondatra_audit_row_count('t', '>=', 1)"
	const bystander = "SELECT 1"
	callerBefore := hashFor(t, dir, caller)
	bystanderBefore := hashFor(t, dir, bystander)

	if bystanderBefore != "" {
		t.Errorf("scaffolded config is all macros and variables, so a model naming none of them should hash empty; got %q — a template file did not decompose", bystanderBefore)
	}

	// Edit the audit macro the caller names.
	audits, err := sqlfiles.Load("init/macros_audit.sql")
	if err != nil {
		t.Fatalf("load audits: %v", err)
	}
	writeConfig(t, dir, "macros/audits.sql", audits+"\nCREATE OR REPLACE MACRO ondatra_audit_row_count(t, op, n) AS TABLE SELECT 'changed';\n")

	if hashFor(t, dir, caller) == callerBefore {
		t.Error("editing a scaffolded audit macro must rebuild the models that use it")
	}
	if hashFor(t, dir, bystander) != bystanderBefore {
		t.Error("editing a scaffolded audit macro must not rebuild models that never use it")
	}
}

// TestConfigIndex_InertStatements pins the settings allowlist at the hash
// level: bumping a resource knob must not rebuild the lake, while a setting
// that can change a result still must.
func TestConfigIndex_InertStatements(t *testing.T) {
	t.Parallel()
	const ref = "SELECT 1"

	t.Run("inert settings contribute nothing", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		writeConfig(t, dir, "settings.sql", "SET threads = 4;\nSET memory_limit = '8GB';\n")
		if h := hashFor(t, dir, ref); h != "" {
			t.Fatalf("a config of nothing but inert settings should hash empty, got %q", h)
		}
		writeConfig(t, dir, "settings.sql", "SET threads = 16;\nSET memory_limit = '64GB';\nSET temp_directory = '/tmp/duckdb';\n")
		if h := hashFor(t, dir, ref); h != "" {
			t.Errorf("bumping threads/memory/temp_directory must not rebuild anything, got %q", h)
		}
	})

	for _, tc := range []struct{ name, before, after string }{
		{"TimeZone", "SET TimeZone = 'UTC';", "SET TimeZone = 'Europe/Stockholm';"},
		{"integer_division", "SET integer_division = false;", "SET integer_division = true;"},
		{"search_path", "SET search_path = 'a';", "SET search_path = 'b';"},
		{"preserve_insertion_order", "SET preserve_insertion_order = true;", "SET preserve_insertion_order = false;"},
	} {
		t.Run("semantic setting "+tc.name+" rebuilds", func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			writeConfig(t, dir, "settings.sql", tc.before)
			h1 := hashFor(t, dir, ref)
			if h1 == "" {
				t.Fatalf("%s must not be treated as inert", tc.name)
			}
			writeConfig(t, dir, "settings.sql", tc.after)
			if hashFor(t, dir, ref) == h1 {
				t.Errorf("changing %s must rebuild every model", tc.name)
			}
		})
	}

	t.Run("unknown setting defaults to semantic", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		writeConfig(t, dir, "settings.sql", "SET some_future_duckdb_knob = 1;")
		if h := hashFor(t, dir, ref); h == "" {
			t.Error("a setting not on the allowlist must default to semantic")
		}
	})

	t.Run("SET VARIABLE is never inert", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		// `threads` is an inert *setting*; as a user variable name it must not
		// inherit that, or a referenced variable would silently stop tracking.
		writeConfig(t, dir, "variables/constants.sql", "SET VARIABLE threads = 1;")
		h1 := hashFor(t, dir, "SELECT getvariable('threads')")
		if h1 == "" {
			t.Fatal("SET VARIABLE must produce a fragment, not be dropped as inert")
		}
		writeConfig(t, dir, "variables/constants.sql", "SET VARIABLE threads = 2;")
		if hashFor(t, dir, "SELECT getvariable('threads')") == h1 {
			t.Error("editing a referenced variable must rebuild its users")
		}
	})

	t.Run("CREATE SCHEMA is inert", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		writeConfig(t, dir, "schemas.sql", "CREATE SCHEMA IF NOT EXISTS staging;")
		if h := hashFor(t, dir, ref); h != "" {
			t.Fatalf("CREATE SCHEMA cannot change a result and must be inert, got %q", h)
		}
		writeConfig(t, dir, "schemas.sql", "CREATE SCHEMA IF NOT EXISTS staging;\nCREATE SCHEMA IF NOT EXISTS mart;\n")
		if h := hashFor(t, dir, ref); h != "" {
			t.Errorf("adding a schema must not rebuild anything, got %q", h)
		}
	})
}

// TestConfigIndex_SourcesScoping pins that external sources are scoped by name
// the same way macros are, and that catalog.sql deliberately is not.
func TestConfigIndex_SourcesScoping(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) string {
		t.Helper()
		dir := t.TempDir()
		writeConfig(t, dir, "sources.sql",
			"ATTACH 'postgresql://host/warehouse' AS warehouse (READ_ONLY);\n"+
				"ATTACH 'postgresql://host/crm' AS crm (READ_ONLY);\n"+
				"CREATE VIEW raw.external_events AS SELECT * FROM read_parquet('s3://bucket/v1/*.parquet');\n")
		return dir
	}

	t.Run("model is scoped to the source it names", func(t *testing.T) {
		t.Parallel()
		dir := setup(t)
		const ref = "SELECT * FROM warehouse.public.orders"
		h1 := hashFor(t, dir, ref)
		writeConfig(t, dir, "sources.sql",
			"ATTACH 'postgresql://host/warehouse' AS warehouse (READ_ONLY);\n"+
				"ATTACH 'postgresql://other-host/crm' AS crm (READ_ONLY);\n"+
				"CREATE VIEW raw.external_events AS SELECT * FROM read_parquet('s3://bucket/v1/*.parquet');\n")
		if hashFor(t, dir, ref) != h1 {
			t.Error("repointing an unrelated ATTACH must not rebuild this model")
		}
		writeConfig(t, dir, "sources.sql",
			"ATTACH 'postgresql://elsewhere/warehouse' AS warehouse (READ_ONLY);\n"+
				"ATTACH 'postgresql://other-host/crm' AS crm (READ_ONLY);\n"+
				"CREATE VIEW raw.external_events AS SELECT * FROM read_parquet('s3://bucket/v1/*.parquet');\n")
		if hashFor(t, dir, ref) == h1 {
			t.Error("repointing the ATTACH this model reads must rebuild it")
		}
	})

	t.Run("views on external data are scoped by name", func(t *testing.T) {
		t.Parallel()
		dir := setup(t)
		const user = "SELECT * FROM raw.external_events"
		const bystander = "SELECT * FROM warehouse.public.orders"
		userBefore := hashFor(t, dir, user)
		bystanderBefore := hashFor(t, dir, bystander)

		writeConfig(t, dir, "sources.sql",
			"ATTACH 'postgresql://host/warehouse' AS warehouse (READ_ONLY);\n"+
				"ATTACH 'postgresql://host/crm' AS crm (READ_ONLY);\n"+
				"CREATE VIEW raw.external_events AS SELECT * FROM read_parquet('s3://bucket/v2/*.parquet');\n")

		if hashFor(t, dir, user) == userBefore {
			t.Error("repointing a view must rebuild the models selecting from it")
		}
		if hashFor(t, dir, bystander) != bystanderBefore {
			t.Error("repointing a view must not rebuild models that never select from it")
		}
	})

	t.Run("catalog.sql ATTACH stays global", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		writeConfig(t, dir, "catalog.sql", "ATTACH 'ducklake:sqlite:lake.sqlite' AS lake (DATA_PATH 'files');")
		const ref = "SELECT 1"
		h1 := hashFor(t, dir, ref)
		if h1 == "" {
			t.Fatal("catalog.sql must not be scoped away — its ATTACH carries DATA_PATH and SNAPSHOT_TIME")
		}
		writeConfig(t, dir, "catalog.sql", "ATTACH 'ducklake:sqlite:lake.sqlite' AS lake (DATA_PATH 'files', SNAPSHOT_VERSION 3);")
		if hashFor(t, dir, ref) == h1 {
			t.Error("pinning the lake to a past snapshot must rebuild every model")
		}
	})
}

// TestConfigIndex_PerStatementClassification pins that one unrecognized
// statement sends only itself to the global bucket, instead of dragging the
// macros in the same file along with it.
func TestConfigIndex_PerStatementClassification(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeConfig(t, dir, "macros/mixed.sql",
		"CREATE TABLE lookup AS SELECT 1 AS k;\n"+
			"CREATE OR REPLACE MACRO used(x) AS x + 1;\n"+
			"CREATE OR REPLACE MACRO unused(x) AS x + 2;\n")

	const caller = "SELECT used(id) FROM t"
	const bystander = "SELECT 1"
	bystanderBefore := hashFor(t, dir, bystander)

	if bystanderBefore == "" {
		t.Fatal("the CREATE TABLE is unrecognized and must land in the global bucket")
	}

	t.Run("the unrecognized statement stays global", func(t *testing.T) {
		writeConfig(t, dir, "macros/mixed.sql",
			"CREATE TABLE lookup AS SELECT 2 AS k;\n"+
				"CREATE OR REPLACE MACRO used(x) AS x + 1;\n"+
				"CREATE OR REPLACE MACRO unused(x) AS x + 2;\n")
		if hashFor(t, dir, bystander) == bystanderBefore {
			t.Error("editing the unrecognized statement must rebuild every model")
		}
	})

	t.Run("its neighbours are still scoped", func(t *testing.T) {
		base := hashFor(t, dir, caller)
		writeConfig(t, dir, "macros/mixed.sql",
			"CREATE TABLE lookup AS SELECT 2 AS k;\n"+
				"CREATE OR REPLACE MACRO used(x) AS x + 1;\n"+
				"CREATE OR REPLACE MACRO unused(x) AS x + 999;\n")
		if hashFor(t, dir, caller) != base {
			t.Error("a sibling macro nobody calls must not rebuild this model")
		}
	})
}

func TestSourceName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		stmt string
		want string
	}{
		{"ATTACH 'postgresql://h/db' AS warehouse (READ_ONLY)", "warehouse"},
		{"attach 'x' as Warehouse", "warehouse"},
		{"ATTACH 'ducklake:sqlite:l.sqlite' AS lake (DATA_PATH 'f')", "lake"},
		{"CREATE VIEW raw.events AS SELECT 1", "events"},
		{"CREATE OR REPLACE VIEW raw.events AS SELECT 1", "events"},
		{"CREATE TEMP VIEW events AS SELECT 1", "events"},
		{"CREATE VIEW IF NOT EXISTS raw.events AS SELECT 1", "events"},
		{"CREATE TABLE t AS SELECT 1", ""},
		{"ATTACH 'x'", ""},
		// The connection string can contain whitespace and even a bare AS.
		{"ATTACH 'my db AS backup.db' AS warehouse", "warehouse"},
		{"ATTACH 'it''s AS odd' AS warehouse", "warehouse"},
		{"ATTACH 'unterminated AS warehouse", ""},
		{"SELECT 1", ""},
	}
	for _, tt := range tests {
		t.Run(tt.stmt, func(t *testing.T) {
			t.Parallel()
			if got := sourceName(tt.stmt); got != tt.want {
				t.Errorf("sourceName(%q) = %q, want %q", tt.stmt, got, tt.want)
			}
		})
	}
}

func TestIsInertStatement(t *testing.T) {
	t.Parallel()
	tests := []struct {
		stmt string
		want bool
	}{
		{"SET threads = 4", true},
		{"set THREADS=4", true},
		{"SET GLOBAL memory_limit = '8GB'", true},
		{"RESET threads", true},
		{"SET temp_directory = '/tmp/d'", true},
		{"CREATE SCHEMA staging", true},
		{"CREATE SCHEMA IF NOT EXISTS staging", true},
		{"SET TimeZone = 'UTC'", false},
		{"SET integer_division = true", false},
		{"SET search_path = 'a'", false},
		{"SET VARIABLE threads = 4", false},
		{"SET some_unknown_knob = 1", false},
		{"CREATE TABLE t AS SELECT 1", false},
		{"SELECT 1", false},
		{"SET", false},
	}
	for _, tt := range tests {
		t.Run(tt.stmt, func(t *testing.T) {
			t.Parallel()
			if got := isInertStatement(tt.stmt); got != tt.want {
				t.Errorf("isInertStatement(%q) = %v, want %v", tt.stmt, got, tt.want)
			}
		})
	}
}

// TestConfigIndex_FragmentMoveIsVisible pins that a definition moving between
// files changes its fragment hash.
//
// The runtime loads config/macros/*.sql but not config/macros.sql, so moving a
// macro into the latter removes it from the session entirely. Hashing the
// statement text alone would call that "unchanged" and let callers skip while
// the macro they depend on has quietly vanished.
func TestConfigIndex_FragmentMoveIsVisible(t *testing.T) {
	t.Parallel()
	const body = "CREATE OR REPLACE MACRO used(x) AS x + 1;"
	const ref = "SELECT used(id) FROM t"

	dir := t.TempDir()
	writeConfig(t, dir, "macros/helpers.sql", body)
	h1 := hashFor(t, dir, ref)

	// Same statement, different file.
	if err := os.Remove(filepath.Join(dir, "macros", "helpers.sql")); err != nil {
		t.Fatalf("remove: %v", err)
	}
	writeConfig(t, dir, "macros.sql", body)
	if hashFor(t, dir, ref) == h1 {
		t.Error("a macro moved to a file the runtime does not load must rebuild its callers")
	}
}

// TestConfigIndex_SearchPathDisablesSourceScoping pins that alias scoping backs
// off when config makes unqualified names resolve into an attached catalog.
//
// With `SET search_path = 'warehouse'`, a model reading `FROM orders` gets
// `warehouse.orders` without the word "warehouse" appearing in it. Scoping the
// ATTACH by alias would then skip exactly the models it affects when it is
// repointed — stale data, the direction this package exists to prevent.
func TestConfigIndex_SearchPathDisablesSourceScoping(t *testing.T) {
	t.Parallel()

	const unqualified = "SELECT * FROM orders"
	attach := func(host string) string {
		return "ATTACH 'postgresql://" + host + "/warehouse' AS warehouse (READ_ONLY);"
	}

	t.Run("scoped while nothing widens resolution", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		writeConfig(t, dir, "sources.sql", attach("host-a"))
		h1 := hashFor(t, dir, unqualified)
		writeConfig(t, dir, "sources.sql", attach("host-b"))
		if hashFor(t, dir, unqualified) != h1 {
			t.Error("without a search path, a model that never names the alias is not affected by it")
		}
	})

	for _, widener := range []string{
		"SET search_path = 'warehouse';",
		"SET GLOBAL search_path = 'warehouse';",
		"SET schema = 'warehouse';",
		"USE warehouse;",
	} {
		t.Run("global once config has "+widener, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			writeConfig(t, dir, "sources.sql", attach("host-a")+"\n"+widener)
			h1 := hashFor(t, dir, unqualified)
			if h1 == "" {
				t.Fatal("a widening statement must itself land in the global bucket")
			}
			writeConfig(t, dir, "sources.sql", attach("host-b")+"\n"+widener)
			if hashFor(t, dir, unqualified) == h1 {
				t.Errorf("with %q in config, repointing the ATTACH must rebuild models that never name the alias", widener)
			}
		})
	}

	t.Run("a widener anywhere in config disables scoping", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		writeConfig(t, dir, "sources.sql", attach("host-a"))
		writeConfig(t, dir, "settings.sql", "SET search_path = 'warehouse';")
		h1 := hashFor(t, dir, unqualified)
		writeConfig(t, dir, "sources.sql", attach("host-b"))
		if hashFor(t, dir, unqualified) == h1 {
			t.Error("the widening statement need not sit in the same file as the ATTACH")
		}
	})

	t.Run("macros stay scoped regardless", func(t *testing.T) {
		t.Parallel()
		// Macros are always called by name, so a search path cannot hide the
		// dependency the way it can for a table reference.
		dir := t.TempDir()
		writeConfig(t, dir, "settings.sql", "SET search_path = 'warehouse';")
		writeConfig(t, dir, "macros/helpers.sql",
			"CREATE OR REPLACE MACRO used(x) AS x + 1;\nCREATE OR REPLACE MACRO unused(x) AS x + 2;\n")
		const ref = "SELECT used(id) FROM t"
		h1 := hashFor(t, dir, ref)
		writeConfig(t, dir, "macros/helpers.sql",
			"CREATE OR REPLACE MACRO used(x) AS x + 1;\nCREATE OR REPLACE MACRO unused(x) AS x + 999;\n")
		if hashFor(t, dir, ref) != h1 {
			t.Error("a search path must not widen macro scoping")
		}
	})
}

// TestConfigIndex_BlockCommentDoesNotDemote pins that a block comment ahead of
// a definition does not knock it out of its fragment.
//
// Leaving `/* ... */` in the normalized text would make the statement start
// with the comment, so definedName would not recognize it and the whole thing
// would fall to the global bucket — turning one comment edit into a rebuild of
// every model in the project.
func TestConfigIndex_BlockCommentDoesNotDemote(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeConfig(t, dir, "macros/helpers.sql",
		"/* explains the macro */\nCREATE OR REPLACE MACRO used(x) AS x + 1;\n"+
			"CREATE OR REPLACE MACRO unused(x) AS x + 2;\n")

	const caller = "SELECT used(id) FROM t"
	const bystander = "SELECT 1"

	if h := hashFor(t, dir, bystander); h != "" {
		t.Errorf("a commented macro file is still all macros; a model using none of them should hash empty, got %q", h)
	}

	h1 := hashFor(t, dir, caller)
	writeConfig(t, dir, "macros/helpers.sql",
		"/* an entirely different explanation */\nCREATE OR REPLACE MACRO used(x) AS x + 1;\n"+
			"CREATE OR REPLACE MACRO unused(x) AS x + 2;\n")
	if hashFor(t, dir, caller) != h1 {
		t.Error("editing a block comment must not rebuild anything")
	}
}
