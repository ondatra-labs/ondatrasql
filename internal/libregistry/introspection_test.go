// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package libregistry

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"go.starlark.net/syntax"
)

func TestRegistry_List_NilReceiver(t *testing.T) {
	var r *Registry
	if got := r.List(); got != nil {
		t.Errorf("List() on nil registry = %v, want nil", got)
	}
}

func TestRegistry_List_Empty(t *testing.T) {
	r := NewRegistryForTest(map[string]*LibFunc{})
	if got := r.List(); len(got) != 0 {
		t.Errorf("List() on empty registry returned %d entries, want 0", len(got))
	}
}

func TestRegistry_List_SortedByName(t *testing.T) {
	r := NewRegistryForTest(map[string]*LibFunc{
		"stripe":      {Name: "stripe"},
		"gam_fetch":   {Name: "gam_fetch"},
		"riksbank":    {Name: "riksbank"},
		"mistral_ocr": {Name: "mistral_ocr"},
	})

	got := r.List()
	want := []string{"gam_fetch", "mistral_ocr", "riksbank", "stripe"}

	if len(got) != len(want) {
		t.Fatalf("List() returned %d entries, want %d", len(got), len(want))
	}
	for i, lf := range got {
		if lf.Name != want[i] {
			t.Errorf("List()[%d].Name = %q, want %q", i, lf.Name, want[i])
		}
	}
}

func TestRegistry_List_DeterministicAcrossCalls(t *testing.T) {
	r := NewRegistryForTest(map[string]*LibFunc{
		"a": {Name: "a"},
		"b": {Name: "b"},
		"c": {Name: "c"},
		"d": {Name: "d"},
	})

	first := r.List()
	for i := range 10 {
		got := r.List()
		if len(got) != len(first) {
			t.Fatalf("call %d: len differs", i)
		}
		for j := range got {
			if got[j].Name != first[j].Name {
				t.Errorf("call %d: order changed at index %d (%q vs %q)",
					i, j, got[j].Name, first[j].Name)
			}
		}
	}
}

func parseStarlark(t *testing.T, code string) *syntax.File {
	t.Helper()
	opts := &syntax.FileOptions{
		Set:             true,
		While:           true,
		TopLevelControl: true,
		GlobalReassign:  true,
		Recursion:       true,
	}
	f, err := opts.Parse("test.star", code, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return f
}

func TestExtractSignatures_NilFile(t *testing.T) {
	if got := ExtractSignatures(nil); got != nil {
		t.Errorf("ExtractSignatures(nil) = %v, want nil", got)
	}
}

func TestExtractSignatures_EmptyFile(t *testing.T) {
	f := parseStarlark(t, ``)
	got := ExtractSignatures(f)
	if len(got) != 0 {
		t.Errorf("ExtractSignatures(empty) = %v, want empty map", got)
	}
}

func TestExtractSignatures_BareIdentifiers(t *testing.T) {
	f := parseStarlark(t, `
def fetch(network_code, key_file, page):
    pass
`)
	got := ExtractSignatures(f)
	want := map[string][]string{
		"fetch": {"network_code", "key_file", "page"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ExtractSignatures() = %v, want %v", got, want)
	}
}

func TestExtractSignatures_DefaultValues(t *testing.T) {
	f := parseStarlark(t, `
def fetch(pattern, page=0, target=""):
    pass
`)
	got := ExtractSignatures(f)
	want := map[string][]string{
		"fetch": {"pattern", "page", "target"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ExtractSignatures() = %v, want %v", got, want)
	}
}

func TestExtractSignatures_KwargsSkipped(t *testing.T) {
	f := parseStarlark(t, `
def fetch(pattern, **kwargs):
    pass
`)
	got := ExtractSignatures(f)
	want := map[string][]string{
		"fetch": {"pattern"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ExtractSignatures() = %v, want %v", got, want)
	}
}

func TestExtractSignatures_VarargsSkipped(t *testing.T) {
	f := parseStarlark(t, `
def helper(name, *args):
    pass
`)
	got := ExtractSignatures(f)
	want := map[string][]string{
		"helper": {"name"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ExtractSignatures() = %v, want %v", got, want)
	}
}

func TestExtractSignatures_MultipleFunctions(t *testing.T) {
	f := parseStarlark(t, `
def submit(options, columns, is_backfill):
    pass

def check(job_ref):
    pass

def fetch_result(result_ref, page):
    pass

def _helper(x):
    pass
`)
	got := ExtractSignatures(f)
	want := map[string][]string{
		"submit":       {"options", "columns", "is_backfill"},
		"check":        {"job_ref"},
		"fetch_result": {"result_ref", "page"},
		"_helper":      {"x"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ExtractSignatures() = %v, want %v", got, want)
	}
}

func TestExtractSignatures_NoParams(t *testing.T) {
	f := parseStarlark(t, `
def noop():
    pass
`)
	got := ExtractSignatures(f)
	want := map[string][]string{
		"noop": {},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ExtractSignatures() = %v, want %v", got, want)
	}
}

func TestExtractSignatures_IgnoresNonDefStatements(t *testing.T) {
	f := parseStarlark(t, `
API = {"fetch": {"args": ["x"]}}

CONSTANT = 42

def fetch(x):
    pass
`)
	got := ExtractSignatures(f)
	want := map[string][]string{
		"fetch": {"x"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ExtractSignatures() = %v, want %v", got, want)
	}
}

// writeStarlarkFile is a test helper for ScanLenient regression tests.
func writeStarlarkFile(t *testing.T, dir, name, code string) {
	t.Helper()
	libDir := filepath.Join(dir, "lib")
	if err := os.MkdirAll(libDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(libDir, name), []byte(code), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestScanLenient_PartialRegistryOnError pins that a single broken
// blueprint doesn't strip the entire registry. Critical regression
// target — Codex round 5 flagged that strict Scan() abort caused
// false-positive cross_ast.unknown_lib for healthy models.
func TestScanLenient_PartialRegistryOnError(t *testing.T) {
	dir := t.TempDir()
	writeStarlarkFile(t, dir, "good.star", `
API = {
    "fetch": {"args": ["x"]},
}

def fetch(x, page):
    pass
`)
	writeStarlarkFile(t, dir, "broken.star", `broken syntax (((`)

	reg, errs := ScanLenient(dir)

	if reg.Empty() {
		t.Fatal("registry is empty — broken sibling should not strip the partial registry")
	}
	if reg.Get("good") == nil {
		t.Error("good blueprint missing from partial registry")
	}
	if reg.Get("broken") != nil {
		t.Error("broken blueprint should not be in registry")
	}
	if errs == nil || errs["broken.star"] == nil {
		t.Errorf("expected per-file error for broken.star, got: %v", errs)
	}
}

// TestScanLenient_NoLibDir pins that absence of lib/ is not an error.
func TestScanLenient_NoLibDir(t *testing.T) {
	dir := t.TempDir()
	reg, errs := ScanLenient(dir)
	if reg == nil {
		t.Fatal("nil registry on missing lib/")
	}
	if !reg.Empty() {
		t.Error("expected empty registry when lib/ doesn't exist")
	}
	if errs != nil {
		t.Errorf("unexpected errors on missing lib/: %v", errs)
	}
}

// TestScanLenient_AllBroken pins behavior when every lib file is broken:
// empty registry + per-file errors, no panic.
func TestScanLenient_AllBroken(t *testing.T) {
	dir := t.TempDir()
	writeStarlarkFile(t, dir, "a.star", `garbage 1`)
	writeStarlarkFile(t, dir, "b.star", `garbage 2`)
	reg, errs := ScanLenient(dir)
	if !reg.Empty() {
		t.Error("registry should be empty when all blueprints are broken")
	}
	if len(errs) != 2 {
		t.Errorf("expected 2 errors, got %d: %v", len(errs), errs)
	}
}
