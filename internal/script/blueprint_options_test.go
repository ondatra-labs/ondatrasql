// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.starlark.net/starlark"
)

// v0.30.0 fas 11: blueprints (lib/*.star) load with strict syntax
// options — top-level control flow and global reassignment are
// rejected. Tests use the load() path so they exercise the same
// code path as the runtime's blueprint loader.

func writeBlueprint(t *testing.T, dir, name, body string) {
	t.Helper()
	libDir := filepath.Join(dir, "lib")
	if err := os.MkdirAll(libDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	path := filepath.Join(libDir, name+".star")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// loadBlueprint exercises Runtime.makeLoadFunc — the same path the
// runtime uses to load lib/*.star — so we test the strict options
// in their actual usage context.
func loadBlueprint(t *testing.T, dir, name string) error {
	t.Helper()
	rt := NewRuntime(nil, nil, dir)
	ctx := context.Background()
	predeclared := rt.libraryPredeclared(ctx)
	loadFunc := rt.makeLoadFunc(ctx, predeclared, map[string]starlark.StringDict{})
	thread := &starlark.Thread{Name: "test", Load: loadFunc}
	thread.SetLocal("ctx", ctx)
	thread.SetLocal("sess", rt.sess)
	_, err := loadFunc(thread, "lib/"+name+".star")
	return err
}

func TestBlueprintOptions_RejectsTopLevelIf(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeBlueprint(t, dir, "bad_if", `
API = {"fetch": {"args": []}}

if True:
    EXTRA = "this is conditional, illegal at module top level"

def fetch(page):
    return {"rows": [], "next": None}
`)
	err := loadBlueprint(t, dir, "bad_if")
	if err == nil {
		t.Fatal("top-level if should be rejected in blueprints")
	}
	if !strings.Contains(err.Error(), "if statement not within a function") {
		t.Errorf("expected top-level-if rejection, got: %v", err)
	}
}

func TestBlueprintOptions_RejectsTopLevelFor(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeBlueprint(t, dir, "bad_for", `
API = {"fetch": {"args": []}}

ITEMS = []
for i in range(3):
    ITEMS.append(i)

def fetch(page):
    return {"rows": [], "next": None}
`)
	err := loadBlueprint(t, dir, "bad_for")
	if err == nil {
		t.Fatal("top-level for should be rejected")
	}
	if !strings.Contains(err.Error(), "for") {
		t.Errorf("expected for-related rejection, got: %v", err)
	}
}

func TestBlueprintOptions_RejectsTopLevelWhile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeBlueprint(t, dir, "bad_while", `
API = {"fetch": {"args": []}}

n = 0
while n < 3:
    n = n + 1

def fetch(page):
    return {"rows": [], "next": None}
`)
	err := loadBlueprint(t, dir, "bad_while")
	if err == nil {
		t.Fatal("top-level while should be rejected")
	}
	if !strings.Contains(err.Error(), "while") {
		t.Errorf("expected while-related rejection, got: %v", err)
	}
}

func TestBlueprintOptions_RejectsGlobalReassign(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeBlueprint(t, dir, "bad_reassign", `
API = {"fetch": {"args": []}}

# Reassigning a top-level name silently overwrites the API dict the
# runtime introspects. Reject with a clear parser error.
API = {"fetch": {"args": ["resource"]}}

def fetch(page):
    return {"rows": [], "next": None}
`)
	err := loadBlueprint(t, dir, "bad_reassign")
	if err == nil {
		t.Fatal("top-level reassign should be rejected")
	}
	if !strings.Contains(err.Error(), "reassigned") && !strings.Contains(err.Error(), "API") {
		t.Errorf("expected reassign rejection mentioning the name, got: %v", err)
	}
}

// Recursion / While / Set inside function bodies are still allowed —
// these are widely used in fetch/push function bodies (cursor parsing,
// retry loops, dedup sets). Make sure the strict toggles don't reject
// them by accident.
func TestBlueprintOptions_AllowsRecursionWhileSetInFunction(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeBlueprint(t, dir, "ok_advanced", `
API = {"fetch": {"args": []}}

def factorial(n):
    if n <= 1:
        return 1
    return n * factorial(n - 1)

def fetch(page):
    seen = set()
    i = 0
    while i < 3:
        seen.add(i)
        i = i + 1
    return {"rows": [{"n": factorial(5), "count": len(seen)}], "next": None}
`)
	if err := loadBlueprint(t, dir, "ok_advanced"); err != nil {
		t.Fatalf("recursion + while + set inside function bodies should be allowed: %v", err)
	}
}

// The canonical blueprint shape — pure declarations + function defs —
// must continue to load cleanly under the strict toggles.
func TestBlueprintOptions_AllowsCanonicalShape(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeBlueprint(t, dir, "ok_canonical", `
API = {
    "base_url": "https://example.com",
    "fetch": {
        "args": ["resource"],
    },
}

def fetch(resource, page):
    return {"rows": [{"id": 1, "name": "alice"}], "next": None}
`)
	if err := loadBlueprint(t, dir, "ok_canonical"); err != nil {
		t.Fatalf("canonical blueprint should load: %v", err)
	}
}
