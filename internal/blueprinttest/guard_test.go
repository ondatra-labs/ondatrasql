// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package blueprinttest checks the blueprints under blueprints/: the guard
// here runs in every test run; the harness (build tag blueprints, see
// harness_test.go) validates each blueprint and replays its recording.
package blueprinttest

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// blueprintsRoot is blueprints/ seen from this package's directory.
const blueprintsRoot = "../../blueprints"

// recordingProblems lists every file or directory under root that is a
// recording or an expected output: a cassettes/ directory, an expect.yaml
// (or .yml), or a file that is a JSON object with an "interactions" key.
// Those are kept privately by the maintainers and never in this repo.
func recordingProblems(root string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path) // path is under root
		rel = filepath.ToSlash(rel)
		name := strings.ToLower(d.Name())
		if d.IsDir() {
			if name == "cassettes" {
				out = append(out, rel+"/: a cassettes directory")
				return fs.SkipDir
			}
			return nil
		}
		if name == "expect.yaml" || name == "expect.yml" {
			out = append(out, rel+": an expected-output file")
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var obj map[string]json.RawMessage
		if json.Unmarshal(data, &obj) == nil {
			if _, ok := obj["interactions"]; ok {
				out = append(out, rel+": JSON with an interactions key (a cassette)")
			}
		}
		return nil
	})
	return out, err
}

// layoutProblems lists what in one blueprint directory is not part of the
// layout: index.md, .env.example, lib/ and models/, nothing else.
func layoutProblems(dir string) ([]string, error) {
	allowed := map[string]bool{"index.md": false, ".env.example": false, "lib": true, "models": true}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	seen := map[string]bool{}
	for _, e := range entries {
		wantDir, ok := allowed[e.Name()]
		switch {
		case !ok:
			out = append(out, e.Name()+": not part of a blueprint (index.md, .env.example, lib/, models/)")
		case wantDir != e.IsDir():
			out = append(out, e.Name()+": wrong type")
		default:
			seen[e.Name()] = true
		}
	}
	for name := range allowed {
		if !seen[name] {
			out = append(out, name+": missing")
		}
	}
	return out, nil
}

// blueprintDirs returns the blueprint directories under root: every
// subdirectory except _shared.
func blueprintDirs(root string) ([]string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() && e.Name() != "_shared" {
			out = append(out, e.Name())
		}
	}
	return out, nil
}

func TestBlueprintsGuard(t *testing.T) {
	probs, err := recordingProblems(blueprintsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range probs {
		t.Errorf("blueprints/%s: recordings and expected outputs are kept privately, never in this repo (see blueprints/README.md)", p)
	}

	names, err := blueprintDirs(blueprintsRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) == 0 {
		t.Fatal("no blueprints found under blueprints/")
	}
	for _, name := range names {
		probs, err := layoutProblems(filepath.Join(blueprintsRoot, name))
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range probs {
			t.Errorf("blueprints/%s/%s", name, p)
		}
	}
	for _, f := range []string{"LICENSE", "README.md", "_shared/config/catalog.sql"} {
		if _, err := os.Stat(filepath.Join(blueprintsRoot, f)); err != nil {
			t.Errorf("blueprints/%s: %v", f, err)
		}
	}
}

// The guard itself: each kind of recording is caught, code is not.
func TestBlueprintsGuard_Detects(t *testing.T) {
	root := t.TempDir()
	write := func(rel, content string) {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("bp/lib/bp.star", "API = {}\n")
	write("bp/models/raw/x.sql", "SELECT 1\n")
	write("bp/lib/schema.json", `{"type": "object"}`)
	if probs, err := recordingProblems(root); err != nil || len(probs) != 0 {
		t.Fatalf("clean tree: %v %v", probs, err)
	}
	write("bp/cassettes/raw.x/bp.json", `{"interactions": []}`)
	write("bp/expect.yaml", "runs: []\n")
	write("bp/models/raw/recorded.txt", `{"version": 1, "interactions": [{}]}`)
	probs, err := recordingProblems(root)
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(probs, "\n")
	for _, want := range []string{"bp/cassettes/", "bp/expect.yaml", "bp/models/raw/recorded.txt"} {
		if !strings.Contains(got, want) {
			t.Errorf("guard missed %s; found:\n%s", want, got)
		}
	}
	if len(probs) != 3 {
		t.Errorf("guard found %d problems, want 3:\n%s", len(probs), got)
	}

	write("bp/notes.txt", "x")
	lp, err := layoutProblems(filepath.Join(root, "bp"))
	if err != nil {
		t.Fatal(err)
	}
	if s := fmt.Sprint(lp); !strings.Contains(s, "notes.txt") || !strings.Contains(s, "index.md: missing") || !strings.Contains(s, "cassettes") {
		t.Errorf("layout problems = %v", lp)
	}
}
