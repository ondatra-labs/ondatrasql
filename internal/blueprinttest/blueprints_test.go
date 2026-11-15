// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build blueprints

package blueprinttest

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// testsDir returns BLUEPRINT_TESTS_DIR as an absolute path, "" when unset.
func testsDir(t *testing.T) string {
	t.Helper()
	dir := os.Getenv(EnvTestsDir)
	if dir == "" {
		return ""
	}
	if rest, ok := strings.CutPrefix(dir, "~/"); ok {
		home, err := os.UserHomeDir()
		if err != nil {
			t.Fatal(err)
		}
		dir = filepath.Join(home, rest)
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	if st, err := os.Stat(abs); err != nil || !st.IsDir() {
		t.Fatalf("%s=%s is not a directory", EnvTestsDir, dir)
	}
	return abs
}

func repoHarness(t *testing.T) *harness {
	t.Helper()
	shared, err := filepath.Abs(filepath.Join(blueprintsRoot, "_shared"))
	if err != nil {
		t.Fatal(err)
	}
	return &harness{bin: ondatrasqlBinary(t), shared: shared, mainVersion: mainVersion(t)}
}

// binaryVersion asks the binary for its version, as vX.Y.Z.
func (h *harness) binaryVersion(t testing.TB) string {
	t.Helper()
	out, err := exec.Command(h.bin, "version").Output()
	v := "v" + strings.TrimSpace(string(out))
	if err != nil || !validVersion(v) {
		t.Fatalf("%s version: %q %v", h.bin, out, err)
	}
	return v
}

// TestBlueprints checks every blueprint offline and, with
// BLUEPRINT_TESTS_DIR set, replays its recording. With ONDATRASQL_BIN set
// (a released binary), a blueprint whose min_version is above that
// binary's version is skipped: it is tested against main only.
func TestBlueprints(t *testing.T) {
	h := repoHarness(t)
	rec := testsDir(t)
	names, err := blueprintDirs(blueprintsRoot)
	if err != nil {
		t.Fatal(err)
	}
	binVersion := h.binaryVersion(t)
	external := os.Getenv(EnvBinary) != ""
	with := fmt.Sprintf("ondatrasql %s built from this checkout", binVersion)
	if external {
		with = fmt.Sprintf("ondatrasql %s from %s=%s", binVersion, EnvBinary, os.Getenv(EnvBinary))
	}
	var report []string
	defer func() { t.Logf("blueprints and the binary each was proven with:\n%s", strings.Join(report, "\n")) }()
	for _, name := range names {
		bpDir, err := filepath.Abs(filepath.Join(blueprintsRoot, name))
		if err != nil {
			t.Fatal(err)
		}
		t.Run(name, func(t *testing.T) {
			t.Run("check", func(t *testing.T) {
				if external {
					fm, _, err := readPage(bpDir)
					if err == nil && validVersion(fm.MinVersion) && compareVersions(fm.MinVersion, binVersion) > 0 {
						report = append(report, fmt.Sprintf("  %s: main only (min_version %s is above %s)", name, fm.MinVersion, binVersion))
						t.Skipf("%s: not proven with %s: its min_version %s is above it; tested against main only", name, with, fm.MinVersion)
					}
				}
				if err := h.check(t, bpDir); err != nil {
					report = append(report, fmt.Sprintf("  %s: FAILED with %s", name, with))
					t.Errorf("%s: %v", name, err)
					return
				}
				report = append(report, fmt.Sprintf("  %s: proven with %s", name, with))
				t.Logf("%s: proven with %s", name, with)
			})
			t.Run("replay", func(t *testing.T) {
				if rec == "" {
					t.Skipf("%s: replay skipped: %s is not set", name, EnvTestsDir)
				}
				if err := h.replayBlueprint(t, name, bpDir, rec); err != nil {
					t.Error(err)
				}
			})
		})
	}
}

// TestBlueprintsSharedConfig keeps blueprints/_shared/config what
// ondatrasql init writes, so blueprints run on the config a user has.
func TestBlueprintsSharedConfig(t *testing.T) {
	h := repoHarness(t)
	p := &project{dir: t.TempDir()}
	if out, errOut, err := h.exec(p, cliEnv(nil, nil), "init"); err != nil {
		t.Fatalf("init: %v\n%s%s", err, out, errOut)
	}
	initCfg := filepath.Join(p.dir, "config")
	sharedCfg := filepath.Join(h.shared, "config")
	err := filepath.WalkDir(initCfg, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(initCfg, path) // path is under initCfg
		if rel == "secrets.sql" {
			return nil // credentials stay out of a shared config
		}
		want, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		got, err := os.ReadFile(filepath.Join(sharedCfg, rel))
		if err != nil {
			t.Errorf("blueprints/_shared/config/%s: %v (init writes it)", filepath.ToSlash(rel), err)
			return nil
		}
		if !bytes.Equal(got, want) {
			t.Errorf("blueprints/_shared/config/%s differs from what ondatrasql init writes; copy it from a fresh init", filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = filepath.WalkDir(h.shared, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(h.shared, path) // path is under h.shared
		if _, err := os.Stat(filepath.Join(p.dir, rel)); err != nil {
			t.Errorf("blueprints/_shared/%s: init does not write it", filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestRecordBlueprint records blueprint BP against its live API into
// BLUEPRINT_TESTS_DIR/BP (make record-blueprint). Skipped unless BP is set.
func TestRecordBlueprint(t *testing.T) {
	name := os.Getenv("BP")
	if name == "" {
		t.Skip("BP not set: nothing to record")
	}
	rec := testsDir(t)
	if rec == "" {
		t.Fatalf("recording needs %s: the recordings never go into this repo", EnvTestsDir)
	}
	bpDir, err := filepath.Abs(filepath.Join(blueprintsRoot, name))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(bpDir, "index.md")); err != nil {
		t.Fatalf("no blueprint %q: %v", name, err)
	}
	h := repoHarness(t)
	if err := h.check(t, bpDir); err != nil {
		t.Fatalf("%s: fix the check before recording: %v", name, err)
	}
	dir := filepath.Join(rec, name)
	draft, err := h.record(t, bpDir, dir)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	t.Logf("recorded %s into %s/cassettes/; replay produced:\n%s", name, dir, draft)
	if err := h.replayBlueprint(t, name, bpDir, rec); err != nil {
		t.Errorf("the recording does not replay to %s/expect.yaml; review it against the draft above: %v", dir, err)
	}
}
