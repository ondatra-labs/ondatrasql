// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/output"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// captureOutput redirects human output to a buffer for the duration of f.
func captureOutput(t *testing.T, f func()) string {
	t.Helper()
	old := output.Human
	defer func() { output.Human = old }()
	var buf strings.Builder
	output.Human = &buf
	f()
	return buf.String()
}

// captureAll captures both output.Human and real stdout.
func captureAll(t *testing.T, f func()) string {
	t.Helper()
	// Capture output.Human
	old := output.Human
	defer func() { output.Human = old }()
	var humanBuf strings.Builder
	output.Human = &humanBuf

	// Capture real stdout
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	f()

	w.Close()
	os.Stdout = origStdout
	var stdoutBuf strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			stdoutBuf.Write(buf[:n])
		}
		if err != nil {
			break
		}
	}

	return humanBuf.String() + stdoutBuf.String()
}

// --- Command Routing ---

func TestRun_NoArgs_ShowsHelp(t *testing.T) {
	got := captureOutput(t, func() {
		err := run(nil)
		if err != nil {
			t.Fatalf("run() error: %v", err)
		}
	})
	if !strings.Contains(got, "OndatraSQL") {
		t.Errorf("expected help output, got: %s", got)
	}
}

func TestRun_Version(t *testing.T) {
	got := captureAll(t, func() {
		err := run([]string{"version"})
		if err != nil {
			t.Fatalf("run() error: %v", err)
		}
	})
	if !strings.Contains(got, version) {
		t.Errorf("expected version %s, got: %s", version, got)
	}
}

func TestRun_Init_CreatesProject(t *testing.T) {
	dir := t.TempDir()
	oldWd, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(oldWd)

	err := run([]string{"init"})
	if err != nil {
		t.Fatalf("run init: %v", err)
	}

	// Verify project structure
	for _, path := range []string{"config/catalog.sql", "models", ".env"} {
		if _, err := os.Stat(filepath.Join(dir, path)); os.IsNotExist(err) {
			t.Errorf("missing %s after init", path)
		}
	}
}

func TestRun_Init_FailsIfExists(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "config"), 0o755)
	oldWd, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(oldWd)

	err := run([]string{"init"})
	if err == nil {
		t.Fatal("expected error for existing project")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRun_UnknownCommand(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	os.Chdir(p.Dir)
	defer os.Chdir(oldWd)

	err := run([]string{"nonexistent_cmd"})
	if err == nil {
		t.Fatal("expected error for unknown command")
	}
	if !strings.Contains(err.Error(), "unknown command") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRun_NotInProject(t *testing.T) {
	dir := t.TempDir()
	oldWd, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(oldWd)

	err := run([]string{"run"})
	if err == nil {
		t.Fatal("expected error outside project")
	}
	if !strings.Contains(err.Error(), "not in an ondatrasql project") {
		t.Errorf("unexpected error: %v", err)
	}
}

// --- Commands that need a project ---

func TestRun_RunSingleModel(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	os.Chdir(p.Dir)
	defer os.Chdir(oldWd)

	p.AddModel("staging/orders.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name`)

	got := captureOutput(t, func() {
		err := run([]string{"run", "staging.orders"})
		if err != nil {
			t.Fatalf("run model: %v", err)
		}
	})
	if !strings.Contains(got, "staging.orders") {
		t.Errorf("expected model name in output, got: %s", got)
	}
}

func TestRun_RunAll(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	os.Chdir(p.Dir)
	defer os.Chdir(oldWd)

	p.AddModel("staging/a.sql", `-- @kind: table
SELECT 1 AS id`)
	p.AddModel("staging/b.sql", `-- @kind: table
SELECT * FROM staging.a`)

	got := captureOutput(t, func() {
		err := run([]string{"run"})
		if err != nil {
			t.Fatalf("run all: %v", err)
		}
	})
	if !strings.Contains(got, "staging.a") || !strings.Contains(got, "staging.b") {
		t.Errorf("expected both models in output, got: %s", got)
	}
}

func TestRun_History(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	os.Chdir(p.Dir)
	defer os.Chdir(oldWd)

	// Run a model first to have history
	p.AddModel("staging/hist.sql", `-- @kind: table
SELECT 1 AS id`)
	run([]string{"run", "staging.hist"})

	err := run([]string{"history"})
	if err != nil {
		t.Fatalf("history: %v", err)
	}
}

func TestRun_Stats(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	os.Chdir(p.Dir)
	defer os.Chdir(oldWd)

	p.AddModel("staging/stat.sql", `-- @kind: table
SELECT 1 AS id`)
	run([]string{"run", "staging.stat"})

	got := captureOutput(t, func() {
		err := run([]string{"stats"})
		if err != nil {
			t.Fatalf("stats: %v", err)
		}
	})
	if !strings.Contains(got, "stat") {
		t.Errorf("expected model in stats output, got: %s", got)
	}
}

func TestRun_Describe(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	os.Chdir(p.Dir)
	defer os.Chdir(oldWd)

	p.AddModel("staging/desc.sql", `-- @kind: table
SELECT 1 AS id, 'hello' AS name`)
	run([]string{"run", "staging.desc"})

	got := captureOutput(t, func() {
		err := run([]string{"describe", "staging.desc"})
		if err != nil {
			t.Fatalf("describe: %v", err)
		}
	})
	if !strings.Contains(got, "staging.desc") {
		t.Errorf("expected model in describe output, got: %s", got)
	}
}

func TestRun_SQL(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	os.Chdir(p.Dir)
	defer os.Chdir(oldWd)

	got := captureAll(t, func() {
		err := run([]string{"sql", "SELECT 42 AS answer"})
		if err != nil {
			t.Fatalf("sql: %v", err)
		}
	})
	if !strings.Contains(got, "42") {
		t.Errorf("expected 42 in sql output, got: %s", got)
	}
}

func TestRun_SQL_CSV(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	os.Chdir(p.Dir)
	defer os.Chdir(oldWd)

	got := captureAll(t, func() {
		err := run([]string{"sql", "SELECT 42 AS answer", "--format", "csv"})
		if err != nil {
			t.Fatalf("sql csv: %v", err)
		}
	})
	if !strings.Contains(got, "answer") || !strings.Contains(got, "42") {
		t.Errorf("expected csv output, got: %s", got)
	}
}

func TestRun_SQL_JSON(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	os.Chdir(p.Dir)
	defer os.Chdir(oldWd)

	got := captureAll(t, func() {
		err := run([]string{"sql", "SELECT 42 AS answer", "--format", "json"})
		if err != nil {
			t.Fatalf("sql json: %v", err)
		}
	})
	if !strings.Contains(got, "42") {
		t.Errorf("expected json output with 42, got: %s", got)
	}
}

func TestRun_Lineage_MissingArgs(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	os.Chdir(p.Dir)
	defer os.Chdir(oldWd)

	err := run([]string{"lineage"})
	if err == nil {
		t.Fatal("expected error for lineage without args")
	}
	if !strings.Contains(err.Error(), "usage") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRun_Describe_MissingArgs(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	os.Chdir(p.Dir)
	defer os.Chdir(oldWd)

	err := run([]string{"describe"})
	if err == nil {
		t.Fatal("expected error for describe without args")
	}
}

func TestRun_Edit_MissingArgs(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	os.Chdir(p.Dir)
	defer os.Chdir(oldWd)

	err := run([]string{"edit"})
	if err == nil {
		t.Fatal("expected error for edit without args")
	}
}

func TestRun_New_MissingArgs(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	os.Chdir(p.Dir)
	defer os.Chdir(oldWd)

	err := run([]string{"new"})
	if err == nil {
		t.Fatal("expected error for new without args")
	}
}

func TestRun_SQL_MissingArgs(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	os.Chdir(p.Dir)
	defer os.Chdir(oldWd)

	err := run([]string{"sql"})
	if err == nil {
		t.Fatal("expected error for sql without args")
	}
}

func TestRun_JsonFlag(t *testing.T) {
	got := captureAll(t, func() {
		err := run([]string{"--json", "version"})
		if err != nil {
			t.Fatalf("run --json version: %v", err)
		}
	})
	if !strings.Contains(got, version) {
		t.Errorf("expected version %s with --json, got: %s", version, got)
	}
}

func TestRun_Sandbox_SingleModel(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	os.Chdir(p.Dir)
	defer os.Chdir(oldWd)

	p.AddModel("staging/sbox.sql", `-- @kind: table
SELECT 1 AS id`)

	got := captureOutput(t, func() {
		err := run([]string{"sandbox", "staging.sbox"})
		if err != nil {
			t.Fatalf("sandbox: %v", err)
		}
	})
	if !strings.Contains(got, "staging.sbox") {
		t.Errorf("expected model in sandbox output, got: %s", got)
	}

	// The per-pid sandbox subdir must be cleaned up on the success path —
	// pin it here so the deferred-cleanup refactor isn't silently reverted.
	// (v0.12.1+ uses .sandbox/<pid>-<rand> for per-invocation isolation;
	// the parent .sandbox/ may or may not exist after cleanup.)
	if entries, _ := filepath.Glob(filepath.Join(p.Dir, ".sandbox", "*")); len(entries) > 0 {
		t.Errorf(".sandbox/<sub> should not exist after successful sandbox run, got entries: %v", entries)
	}
}

// TestRun_Sandbox_SingleModel_FailedRun_CleansUpDir is a regression test for
// the sandbox cleanup leak fixed in cmd/ondatrasql/model.go.
//
// Bug: when a sandbox model run failed (e.g. audit failure), the cleanup of
// the .sandbox directory was gated on `sandboxMode && err == nil`, so failed
// runs left the directory behind. The next sandbox run would silently reuse
// the dirty state instead of starting fresh, hiding catalog drift.
//
// Fix: defer os.RemoveAll(sandboxDir) right after createSandbox so cleanup
// runs on every exit path. This test pins that contract.
func TestRun_Sandbox_SingleModel_FailedRun_CleansUpDir(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	os.Chdir(p.Dir)
	defer os.Chdir(oldWd)

	// Use an audit pattern that the @audit DSL parser does NOT recognize.
	// The sandbox materialize path will fail with "audit parse error",
	// returning a non-nil err from runner.Run, which under the old code
	// would skip the cleanup branch entirely.
	p.AddModel("staging/sbox_fail.sql", `-- @kind: table
-- @audit: amount > 0
SELECT 1 AS id, 100 AS amount`)

	_ = captureOutput(t, func() {
		// Either return value is acceptable — the contract under test is
		// the filesystem state, not the error propagation shape.
		_ = run([]string{"sandbox", "staging.sbox_fail"})
	})

	if entries, _ := filepath.Glob(filepath.Join(p.Dir, ".sandbox", "*")); len(entries) > 0 {
		t.Errorf(".sandbox/<sub> must be removed after failed sandbox run, got entries: %v — see model.go cleanup-leak fix", entries)
	}
}

// TestRun_Sandbox_NoParquetLeak is the regression test for Bug S16.
// Pre-v0.12.2 each sandbox session wrote ~2 new parquet files into prod's
// shared data directory (the registry-update file and the model-output
// file) and never cleaned them up. The fix is in Session.Close: query both
// catalogs' data-file manifests, compute the diff, and delete sandbox-only
// files from disk before tearing down.
func TestRun_Sandbox_NoParquetLeak(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	os.Chdir(p.Dir)
	defer os.Chdir(oldWd)

	p.AddModel("staging/leak.sql", `-- @kind: table
-- @audit: row_count >= 1
SELECT 1 AS id`)

	// Materialize prod once.
	if err := run([]string{"run", "staging.leak"}); err != nil {
		t.Fatalf("prod run: %v", err)
	}

	dataPath := filepath.Join(p.Dir, "ducklake.sqlite.files")
	countParquet := func() int {
		var n int
		_ = filepath.WalkDir(dataPath, func(_ string, d os.DirEntry, _ error) error {
			if d != nil && !d.IsDir() && strings.HasSuffix(d.Name(), ".parquet") {
				n++
			}
			return nil
		})
		return n
	}
	before := countParquet()

	// Run sandbox 5 times. Pre-v0.12.2 each run added 2 orphan parquet files.
	for i := 0; i < 5; i++ {
		_ = captureOutput(t, func() {
			_ = run([]string{"sandbox", "staging.leak"})
		})
	}

	after := countParquet()
	if after != before {
		t.Errorf("parquet count grew from %d to %d after 5 sandbox runs (Bug S16 regression)",
			before, after)
	}
}

// TestRun_Sandbox_ReapsStaleSubdirs is the regression test for Bug S17.
// Pre-v0.12.2 the .sandbox/<pid>-<rand> directory left behind by a SIGKILL'd
// or crashed sandbox session was never cleaned up by subsequent normal
// sandbox runs — disk leak that accumulates indefinitely. v0.12.2 adds a
// best-effort reap step to createSandbox: scan .sandbox/, find subdirs
// whose pid is dead AND whose mtime is older than 1 minute, delete them.
//
// We simulate the stale state by pre-creating .sandbox/<dead-pid>-<rand>
// with an old mtime, then running a normal sandbox command, and asserting
// the stale dir is gone.
func TestRun_Sandbox_ReapsStaleSubdirs(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	os.Chdir(p.Dir)
	defer os.Chdir(oldWd)

	p.AddModel("staging/x.sql", `-- @kind: table
SELECT 1 AS id`)

	// Pre-seed a stale subdir using pid 1 (init/systemd — always exists,
	// so to make it look stale we use a definitely-dead pid). Pid 999999
	// is virtually guaranteed not to exist on a typical Linux system.
	staleParent := filepath.Join(p.Dir, ".sandbox")
	if err := os.MkdirAll(staleParent, 0o755); err != nil {
		t.Fatalf("seed parent: %v", err)
	}
	staleDir := filepath.Join(staleParent, "999999-deadbeef")
	if err := os.MkdirAll(staleDir, 0o755); err != nil {
		t.Fatalf("seed stale dir: %v", err)
	}
	// Backdate so reapStaleSandboxDirs's age check accepts it as stale.
	oldTime := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(staleDir, oldTime, oldTime); err != nil {
		t.Fatalf("backdate stale dir: %v", err)
	}

	_ = captureOutput(t, func() {
		_ = run([]string{"sandbox", "staging.x"})
	})

	// The stale subdir must have been reaped during createSandbox.
	if _, err := os.Stat(staleDir); !os.IsNotExist(err) {
		t.Errorf("stale .sandbox/999999-deadbeef should have been reaped, got err=%v", err)
	}
}

// TestRun_Sandbox_DAG_InitSandboxFailure_CleansUpDir is the DAG-mode
// regression test for cmd/ondatrasql/dag.go cleanup-leak bug. With sandbox
// v2's catalog-fork architecture, the failure trigger is "prod catalog file
// missing" — `forkSqliteCatalog` fails at the read step and InitSandbox
// returns early. The deferred cleanup must still wipe the per-pid sandbox
// subdir that `createSandbox` allocated.
func TestRun_Sandbox_DAG_InitSandboxFailure_CleansUpDir(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	os.Chdir(p.Dir)
	defer os.Chdir(oldWd)

	p.AddModel("staging/dag_ok.sql", `-- @kind: table
SELECT 1 AS id`)

	// Delete the prod catalog file so forkSqliteCatalog fails inside
	// InitSandbox. (testutil.NewProject creates and initialises one;
	// we remove it here to simulate "user ran sandbox before run".)
	prodCatalog := filepath.Join(p.Dir, "ducklake.sqlite")
	if err := os.Remove(prodCatalog); err != nil && !os.IsNotExist(err) {
		t.Fatalf("remove prod catalog: %v", err)
	}

	_ = captureOutput(t, func() {
		// `sandbox` with no model name → DAG mode. Expected to fail at
		// InitSandbox; we ignore the error shape and pin the filesystem.
		_ = run([]string{"sandbox"})
	})

	// No per-pid subdir should remain under .sandbox/
	if entries, _ := filepath.Glob(filepath.Join(p.Dir, ".sandbox", "*")); len(entries) > 0 {
		t.Errorf(".sandbox/<sub> must be removed even when InitSandbox fails, got entries: %v", entries)
	}
}

func TestRun_Query(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	os.Chdir(p.Dir)
	defer os.Chdir(oldWd)

	p.AddModel("staging/qt.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name`)
	run([]string{"run", "staging.qt"})

	got := captureAll(t, func() {
		err := run([]string{"query", "staging.qt"})
		if err != nil {
			t.Fatalf("query: %v", err)
		}
	})
	if !strings.Contains(got, "Alice") {
		t.Errorf("expected data in query output, got: %s", got)
	}
}

func TestRun_Lineage_Overview(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	os.Chdir(p.Dir)
	defer os.Chdir(oldWd)

	p.AddModel("staging/lo.sql", `-- @kind: table
SELECT 1 AS id`)
	run([]string{"run", "staging.lo"})

	got := captureAll(t, func() {
		err := run([]string{"lineage", "overview"})
		if err != nil {
			t.Fatalf("lineage overview: %v", err)
		}
	})
	if !strings.Contains(got, "lo") {
		t.Errorf("expected model in lineage, got: %s", got)
	}
}
