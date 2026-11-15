// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package deprecation

import (
	"errors"
	"strings"
	"testing"
)

func TestParseVersion(t *testing.T) {
	ok := map[string]Version{
		"0.47":       {0, 47, 0},
		"0.47.1":     {0, 47, 1},
		"v0.47.1":    {0, 47, 1},
		"0.48.0-rc1": {0, 48, 0},
		"1.2.3+abc":  {1, 2, 3},
		" 0.5.0 ":    {0, 5, 0},
	}
	for in, want := range ok {
		got, err := ParseVersion(in)
		if err != nil || got != want {
			t.Errorf("ParseVersion(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, in := range []string{"", "dev", "0", "0.x", "0.47.1.2", "-1.2", "0.-1", "0..1"} {
		if _, err := ParseVersion(in); err == nil {
			t.Errorf("ParseVersion(%q) accepted", in)
		}
	}
}

func TestLevelAt(t *testing.T) {
	f := Feature{Since: "0.47"}
	cases := map[string]Level{
		"0.46.0": LevelWarn, // registered before its release
		"0.47.0": LevelWarn,
		"0.49.9": LevelWarn,
		"0.50.0": LevelError,
		"0.52.3": LevelError,
		"0.53.0": LevelRemoved,
		"0.60.0": LevelRemoved,
		"1.0.0":  LevelRemoved,
		"dev":    LevelWarn,
		"":       LevelWarn,
	}
	for v, want := range cases {
		if got := f.LevelAt(v); got != want {
			t.Errorf("LevelAt(%q) = %v, want %v", v, got, want)
		}
	}
	if f.ErrorIn() != "0.50" || f.RemovedIn() != "0.53" {
		t.Errorf("ErrorIn %s, RemovedIn %s", f.ErrorIn(), f.RemovedIn())
	}
}

// withVersion runs fn with the running version set to v and one test
// feature registered, restoring both afterwards.
func withVersion(t *testing.T, v string, f Feature) {
	t.Helper()
	restore := Swap([]Feature{f})
	mu.Lock()
	saved := running
	mu.Unlock()
	SetVersion(v)
	Reset()
	t.Cleanup(func() {
		restore()
		SetVersion(saved)
		SetReporter(nil)
		Reset()
	})
}

var testFeature = Feature{
	ID:          "test.old_flag",
	Kind:        KindCLI,
	Old:         "`--old`",
	Replacement: "`--new`",
	Since:       "0.47",
	Anchor:      "old-flag",
}

func TestUse_WarnReportsOnceAndRecordsEvery(t *testing.T) {
	withVersion(t, "0.47.0", testFeature)
	var reports []Usage
	SetReporter(func(u Usage) { reports = append(reports, u) })
	for line := 1; line <= 3; line++ {
		if err := Use(testFeature.ID, "models/a.sql", line); err != nil {
			t.Fatalf("warn phase returned %v", err)
		}
	}
	if len(reports) != 1 || reports[0].Line != 1 {
		t.Fatalf("reports = %+v, want the first use only", reports)
	}
	if got := Drain(); len(got) != 3 {
		t.Fatalf("Drain() = %d uses, want 3", len(got))
	}
	if got := Drain(); len(got) != 0 {
		t.Fatalf("second Drain() = %d uses, want 0", len(got))
	}
	Reset()
	_ = Use(testFeature.ID, "", 0)
	if len(reports) != 2 {
		t.Fatalf("after Reset the feature is reported again; reports = %d", len(reports))
	}
}

func TestUse_ErrorPhaseFails(t *testing.T) {
	withVersion(t, "0.50.0", testFeature)
	called := false
	SetReporter(func(Usage) { called = true })
	err := Use(testFeature.ID, "", 0)
	var de *Error
	if !errors.As(err, &de) || de.Level != LevelError || !IsError(err) {
		t.Fatalf("Use = %v, want *Error at LevelError", err)
	}
	if called {
		t.Error("an error-phase use went to the reporter; the call site reports it")
	}
	for _, want := range []string{"`--old`", "`--new`", "0.50", "0.53", DocsURL + "#old-flag"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %s", err, want)
		}
	}
}

func TestUse_UnregisteredPanics(t *testing.T) {
	withVersion(t, "0.47.0", testFeature)
	defer func() {
		if recover() == nil {
			t.Fatal("Use of an unregistered ID did not panic")
		}
	}()
	_ = Use("test.nope", "", 0)
}

// TestDemo_Lifecycle walks one feature deprecated at the binary's current
// minor through its life: WARN now, ERROR three minors later, and six
// minors later the guard demands that its call site is gone.
func TestDemo_Lifecycle(t *testing.T) {
	cur, err := ParseVersion(binaryVersion(t))
	if err != nil {
		t.Fatal(err)
	}
	f := testFeature
	f.Since = cur.minorString()
	at := func(minors int) string { return Version{cur.Major, cur.Minor + minors, 0}.String() }
	site := map[ID][]string{f.ID: {"cmd/ondatrasql/main.go:1"}}

	withVersion(t, at(0), f)
	var warned string
	SetReporter(func(u Usage) { warned = "WARN deprecated: " + u.Message() })
	if err := Use(f.ID, "", 0); err != nil {
		t.Fatalf("%s: %v", at(0), err)
	}
	t.Logf("ondatrasql %s: %s", at(0), warned)
	if !strings.HasPrefix(warned, "WARN deprecated: `--old` is deprecated since "+f.Since) {
		t.Errorf("warning = %q", warned)
	}

	SetVersion(at(3))
	err = Use(f.ID, "", 0)
	if !IsError(err) {
		t.Fatalf("%s: Use = %v, want the error phase", at(3), err)
	}
	t.Logf("ondatrasql %s: ERROR %v", at(3), err)
	if problems := audit([]Feature{f}, site, at(5)); len(problems) != 0 {
		t.Errorf("%s: guard = %v, want none", at(5), problems)
	}

	problems := audit([]Feature{f}, site, at(6))
	if len(problems) != 1 || !strings.Contains(problems[0], "remove") {
		t.Fatalf("%s: guard = %v, want a demand for removal", at(6), problems)
	}
	t.Logf("ondatrasql %s: guard: %s", at(6), problems[0])
	if problems := audit([]Feature{f}, nil, at(6)); len(problems) != 0 {
		t.Errorf("%s with the call site gone: guard = %v, want none", at(6), problems)
	}
	if problems := audit([]Feature{f}, nil, at(2)); len(problems) != 1 {
		t.Errorf("%s with the call site gone early: guard = %v, want one problem", at(2), problems)
	}
}
