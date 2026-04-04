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
