// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package main

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/deprecation"
	"github.com/ondatra-labs/ondatrasql/internal/output"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
	"github.com/ondatra-labs/ondatrasql/internal/validate"
)

// inProject runs the test from p's directory with the binary's version set
// to v, restoring both afterwards.
func inProject(t *testing.T, dir, v string) {
	t.Helper()
	oldWd, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	oldVersion := version
	version = v
	t.Cleanup(func() {
		version = oldVersion
		deprecation.SetVersion(oldVersion)
		deprecation.SetReporter(nil)
		deprecation.Reset()
		output.Reset()
		_ = os.Chdir(oldWd)
	})
}

// hashCommentProject has two models that use `#` header comments, the
// model.hash_comment deprecation.
func hashCommentProject(t *testing.T) *testutil.Project {
	t.Helper()
	p := testutil.NewProject(t)
	p.AddModel("staging/a.sql", "# @kind: table\nSELECT 1 AS id\n")
	p.AddModel("staging/b.sql", "// a note\n-- @kind: table\nSELECT 2 AS id\n")
	return p
}

func featureSince(t *testing.T, id deprecation.ID) deprecation.Version {
	t.Helper()
	for _, f := range deprecation.Features() {
		if f.ID == id {
			v, err := deprecation.ParseVersion(f.Since)
			if err != nil {
				t.Fatal(err)
			}
			return v
		}
	}
	t.Fatalf("%s is not registered", id)
	return deprecation.Version{}
}

func minorsAfter(v deprecation.Version, n int) string {
	return deprecation.Version{Major: v.Major, Minor: v.Minor + n}.String()
}

func TestDeprecation_RunWarnsOncePerFeature(t *testing.T) {
	p := hashCommentProject(t)
	inProject(t, p.Dir, minorsAfter(featureSince(t, deprecation.ModelHashComment), 0))

	var err error
	out := captureOutput(t, func() { err = run([]string{"run"}) })
	if err != nil {
		t.Fatalf("run in the warning phase failed: %v", err)
	}
	if n := strings.Count(out, "WARN deprecated:"); n != 1 {
		t.Fatalf("run printed %d deprecation warnings, want 1:\n%s", n, out)
	}
	if !strings.Contains(out, "replace it with `--`") || !strings.Contains(out, deprecation.DocsURL+"#hash-comments-in-model-headers") {
		t.Errorf("warning does not name the replacement and anchor:\n%s", out)
	}
	if !strings.Contains(out, "staging.a") || !strings.Contains(out, "staging.b") {
		t.Errorf("both models must still run:\n%s", out)
	}
}

func TestDeprecation_RunJSONEnvelope(t *testing.T) {
	p := hashCommentProject(t)
	inProject(t, p.Dir, minorsAfter(featureSince(t, deprecation.ModelHashComment), 1))

	var err error
	stdout := captureAll(t, func() { err = run([]string{"--json", "run"}) })
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	var found int
	for _, line := range strings.Split(stdout, "\n") {
		var env map[string]any
		if json.Unmarshal([]byte(line), &env) != nil || env["kind"] != "deprecation" {
			continue
		}
		found++
		if env["feature"] != string(deprecation.ModelHashComment) || env["level"] != "warn" || env["schema_version"] != float64(1) {
			t.Errorf("envelope = %v", env)
		}
	}
	if found != 1 {
		t.Fatalf("found %d deprecation envelopes, want 1:\n%s", found, stdout)
	}
}

func TestDeprecation_ErrorPhaseFailsRun(t *testing.T) {
	p := hashCommentProject(t)
	inProject(t, p.Dir, minorsAfter(featureSince(t, deprecation.ModelHashComment), deprecation.WarnMinors))

	var err error
	out := captureOutput(t, func() { err = run([]string{"run"}) })
	if err == nil {
		t.Fatal("run in the error phase succeeded")
	}
	if !strings.Contains(err.Error(), "no longer works") || !strings.Contains(err.Error(), "replace it with `--`") {
		t.Errorf("error does not name the replacement: %v", err)
	}
	if strings.Contains(out, "WARN deprecated:") {
		t.Errorf("an error-phase use was also printed as a warning:\n%s", out)
	}
}

// validateFindings runs `--json validate` with args and returns its
// findings by rule.
func validateFindings(t *testing.T, args ...string) (map[validate.RuleID][]validate.Finding, error) {
	t.Helper()
	var err error
	stdout := captureAll(t, func() { err = run(append([]string{"--json", "validate"}, args...)) })
	var report validate.Report
	if jerr := json.Unmarshal([]byte(stdout), &report); jerr != nil {
		t.Fatalf("validate --json output: %v\n%s", jerr, stdout)
	}
	byRule := map[validate.RuleID][]validate.Finding{}
	for _, fr := range report.Files {
		for _, f := range fr.Findings {
			byRule[f.Rule] = append(byRule[f.Rule], f)
		}
	}
	return byRule, err
}

func TestDeprecation_ValidateFindings(t *testing.T) {
	p := hashCommentProject(t)
	since := featureSince(t, deprecation.ModelHashComment)

	inProject(t, p.Dir, minorsAfter(since, 2))
	byRule, err := validateFindings(t)
	if err != nil {
		t.Fatalf("validate without --strict failed on a warning: %v", err)
	}
	warns := byRule[validate.RuleDeprecationWarn]
	if len(warns) != 2 || len(byRule) != 1 {
		t.Fatalf("want one deprecation.warn per model and nothing else, got %v", byRule)
	}
	for _, f := range warns {
		if f.Severity != validate.SeverityWarn || f.Line != 1 || !strings.Contains(f.Message, "replace it with `--`") {
			t.Errorf("finding = %+v", f)
		}
	}
	var out string
	out = captureOutput(t, func() { err = run([]string{"validate", "--strict"}) })
	if !errors.Is(err, errFindings) {
		t.Errorf("validate --strict = %v, want findings", err)
	}
	if strings.Contains(out, "WARN deprecated:") {
		t.Errorf("validate printed run-style warnings:\n%s", out)
	}

	version = minorsAfter(since, deprecation.WarnMinors)
	byRule, err = validateFindings(t)
	if !errors.Is(err, errFindings) {
		t.Fatalf("validate in the error phase = %v, want findings", err)
	}
	errs := byRule[validate.RuleDeprecationError]
	if len(errs) != 2 || len(byRule) != 1 {
		t.Fatalf("want one deprecation.error per model and nothing else, got %v", byRule)
	}
	for _, f := range errs {
		if f.Severity != validate.SeverityBlocker || !strings.Contains(f.Message, "no longer works") {
			t.Errorf("finding = %+v", f)
		}
	}
}

// asyncFetchModeLib is a valid async blueprint that turns async on with
// the deprecated `"mode": "async"` (line 5).
const asyncFetchModeLib = `API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": [],
        "mode": "async",
    },
}

def submit():
    return {}

def check(job_ref):
    return {}

def fetch_result(result_ref, page):
    return {"rows": [], "next": None}
`

func TestDeprecation_FetchModeInValidate(t *testing.T) {
	p := testutil.NewProject(t)
	testutil.WriteFile(t, p.Dir, "lib/orders.star", asyncFetchModeLib)
	since := featureSince(t, deprecation.LibFetchMode)
	inProject(t, p.Dir, minorsAfter(since, 0))
	byRule, err := validateFindings(t)
	if err != nil {
		t.Fatalf("validate: %v (%v)", err, byRule)
	}
	warns := byRule[validate.RuleDeprecationWarn]
	if len(warns) != 1 || len(byRule) != 1 {
		t.Fatalf("blueprint parsed twice must give one deprecation.warn and nothing else, got %v", byRule)
	}
	if f := warns[0]; f.Path != "lib/orders.star" || f.Line != 5 {
		t.Errorf("finding at %s:%d, want lib/orders.star:5", f.Path, f.Line)
	}

	version = minorsAfter(since, deprecation.WarnMinors)
	byRule, err = validateFindings(t)
	if !errors.Is(err, errFindings) || len(byRule[validate.RuleDeprecationError]) != 1 || len(byRule) != 1 {
		t.Fatalf("error phase: err=%v findings=%v, want one deprecation.error", err, byRule)
	}
}

// A scoped validate reports deprecated forms only in the files it covers,
// although it parses every blueprint.
func TestDeprecation_ScopedValidateIgnoresOtherFiles(t *testing.T) {
	p := hashCommentProject(t)
	p.AddModel("staging/c.sql", "-- @kind: table\nSELECT 3 AS id\n")
	testutil.WriteFile(t, p.Dir, "lib/orders.star", asyncFetchModeLib)
	inProject(t, p.Dir, minorsAfter(featureSince(t, deprecation.ModelHashComment), 0))

	var err error
	stdout := captureAll(t, func() { err = run([]string{"--json", "validate", "--strict", "models/staging/c.sql"}) })
	if err != nil {
		t.Fatalf("validate --strict of a clean file failed: %v\n%s", err, stdout)
	}
	var report validate.Report
	if jerr := json.Unmarshal([]byte(stdout), &report); jerr != nil {
		t.Fatal(jerr)
	}
	if report.Summary.Files != 1 || report.Summary.Warns != 0 || len(report.Files) != 1 || report.Files[0].Path != "models/staging/c.sql" {
		t.Errorf("scoped report = %+v, want only models/staging/c.sql, clean", report)
	}

	byRule, err := validateFindings(t, "--strict", "models/staging/a.sql")
	if !errors.Is(err, errFindings) || len(byRule[validate.RuleDeprecationWarn]) != 1 || len(byRule) != 1 {
		t.Errorf("validate of a.sql: err=%v findings=%v, want its one deprecation.warn", err, byRule)
	}
}

// captureStreams runs f with os.Stdout and os.Stderr each on a pipe and
// returns what was written to them.
func captureStreams(t *testing.T, f func()) (stdout, stderr string) {
	t.Helper()
	read := func(r *os.File, out *string, done chan<- struct{}) {
		var b strings.Builder
		_, _ = io.Copy(&b, r)
		*out = b.String()
		close(done)
	}
	oldOut, oldErr, oldHuman := os.Stdout, os.Stderr, output.Human
	outR, outW, _ := os.Pipe()
	errR, errW, _ := os.Pipe()
	outDone, errDone := make(chan struct{}), make(chan struct{})
	go read(outR, &stdout, outDone)
	go read(errR, &stderr, errDone)
	os.Stdout, os.Stderr, output.Human = outW, errW, outW
	defer func() {
		os.Stdout, os.Stderr, output.Human = oldOut, oldErr, oldHuman
	}()
	f()
	outW.Close()
	errW.Close()
	<-outDone
	<-errDone
	return stdout, stderr
}

// Commands that print one JSON document keep stdout to that document when
// the project uses deprecated forms; the notice goes to stderr.
func TestDeprecation_SingleDocumentJSONStaysParseable(t *testing.T) {
	p := hashCommentProject(t)
	testutil.WriteFile(t, p.Dir, "lib/orders.star", asyncFetchModeLib)
	inProject(t, p.Dir, minorsAfter(featureSince(t, deprecation.ModelHashComment), 0))
	if out, _ := captureStreams(t, func() {
		if err := run([]string{"run"}); err != nil {
			t.Fatalf("run: %v", err)
		}
	}); !strings.Contains(out, "WARN deprecated:") {
		t.Fatalf("run printed no deprecation warning:\n%s", out)
	}

	for _, args := range [][]string{
		{"describe", "staging.a"},
		{"describe", "blueprint", "orders"},
		{"lineage", "staging.a"},
		{"stats"},
	} {
		var err error
		stdout, stderr := captureStreams(t, func() { err = run(append([]string{"--json"}, args...)) })
		if err != nil {
			t.Errorf("%v: %v", args, err)
			continue
		}
		if args[0] == "lineage" {
			// lineage has no JSON form: under --json its view goes to
			// stderr and stdout stays empty.
			if strings.TrimSpace(stdout) != "" {
				t.Errorf("%v: stdout = %q, want empty", args, stdout)
			}
		} else {
			dec := json.NewDecoder(strings.NewReader(stdout))
			var doc any
			if derr := dec.Decode(&doc); derr != nil {
				t.Errorf("%v: stdout is not JSON: %v\n%s", args, derr, stdout)
				continue
			}
			if derr := dec.Decode(&doc); derr != io.EOF {
				t.Errorf("%v: stdout holds more than one JSON value (%v):\n%s", args, derr, stdout)
			}
		}
		if !strings.Contains(stderr, "WARN deprecated:") {
			t.Errorf("%v: no deprecation warning on stderr:\n%s", args, stderr)
		}
	}
}

func TestMinVersion_RefusesOlderBinary(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "config/settings.sql", "SET threads = 2;\nSET VARIABLE ondatrasql_min_version = '0.99.0'; -- needs the new thing\n")
	testutil.WriteFile(t, dir, "config/catalog.sql", "ATTACH 'ducklake:sqlite:"+filepath.Join(dir, "ducklake.sqlite")+"' AS lake (DATA_PATH '"+filepath.Join(dir, "data")+"');\n")
	testutil.WriteFile(t, dir, "config/state.sql", "ATTACH '"+filepath.Join(dir, "state.duckdb")+"' AS state;\n")
	testutil.WriteFile(t, dir, "models/staging/a.sql", "-- @kind: table\nSELECT 1 AS id\n")
	inProject(t, dir, "0.98.3")

	for _, args := range [][]string{{"run"}, {"validate"}, {"stats"}, {"auth"}} {
		err := run(args)
		if err == nil {
			t.Fatalf("%v with min_version 0.99.0 on 0.98.3 succeeded", args)
		}
		for _, want := range []string{"0.99.0", "0.98.3", "ondatrasql_min_version"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%v: error %q does not name %s", args, err, want)
			}
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "ducklake.sqlite")); !os.IsNotExist(err) {
		t.Errorf("the refused run touched the catalog: %v", err)
	}

	version = "0.99.0"
	if err := run([]string{"run"}); err != nil {
		t.Fatalf("run on exactly the pinned version: %v", err)
	}
}
