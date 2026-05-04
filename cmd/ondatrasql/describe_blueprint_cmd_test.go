// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"go.starlark.net/syntax"
)

func TestIsValidBlueprintName(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"riksbank", true},
		{"gam_report", true},
		{"a", true},
		{"_helper", true},
		{"a1", true},
		{"", false},
		{"1foo", false},
		{"a-b", false},
		{"a.b", false},
		{"a/b", false},
		{"../etc/passwd", false},
		{"foo bar", false},
	}
	for _, tc := range cases {
		if got := isValidBlueprintName(tc.in); got != tc.want {
			t.Errorf("isValidBlueprintName(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestSplitFieldMask(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", []string{}},
		{"a", []string{"a"}},
		{"a,b", []string{"a", "b"}},
		{"a , b", []string{"a", "b"}},
		{"a.b,c.d", []string{"a.b", "c.d"}},
		{",,", []string{}},
	}
	for _, tc := range cases {
		got := splitFieldMask(tc.in)
		if len(got) == 0 && len(tc.want) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("splitFieldMask(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestProjectFields_Happy(t *testing.T) {
	src := map[string]any{
		"name": "x",
		"fetch": map[string]any{
			"args": []any{"a", "b"},
			"mode": "sync",
		},
	}
	got, err := projectFields(src, []string{"fetch.args", "fetch.mode"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"fetch": map[string]any{
			"args": []any{"a", "b"},
			"mode": "sync",
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("projectFields = %v, want %v", got, want)
	}
}

func TestProjectFields_UnknownPath(t *testing.T) {
	src := map[string]any{
		"name": "x",
	}
	if _, err := projectFields(src, []string{"missing.path"}); err == nil {
		t.Error("expected error for unknown field path")
	}
}

func TestProjectFields_TopLevelField(t *testing.T) {
	src := map[string]any{
		"name": "x",
		"path": "lib/x.star",
	}
	got, err := projectFields(src, []string{"name"})
	if err != nil {
		t.Fatal(err)
	}
	if got["name"] != "x" {
		t.Errorf("name = %v, want x", got["name"])
	}
	if _, ok := got["path"]; ok {
		t.Error("path should not be in projected output")
	}
}

func TestLookupPath_NestedNull(t *testing.T) {
	src := map[string]any{
		"a": map[string]any{
			"b": nil,
		},
	}
	v, ok := lookupPath(src, "a.b")
	if !ok {
		t.Error("expected ok=true for explicit null leaf")
	}
	if v != nil {
		t.Errorf("v = %v, want nil", v)
	}
}

func TestLookupPath_Missing(t *testing.T) {
	src := map[string]any{"a": "x"}
	_, ok := lookupPath(src, "a.b.c")
	if ok {
		t.Error("expected ok=false for traversal through non-map")
	}
	_, ok = lookupPath(src, "missing")
	if ok {
		t.Error("expected ok=false for missing key")
	}
}

func TestSetPath_CreatesIntermediateMaps(t *testing.T) {
	m := map[string]any{}
	setPath(m, "a.b.c", 42)
	got, ok := lookupPath(m, "a.b.c")
	if !ok {
		t.Fatal("path not set")
	}
	if got != 42 {
		t.Errorf("got %v, want 42", got)
	}
}

// TestExtractEndpoints_TopLevelIfAndNestedDef pins the R6 finding
// fix: pre-fix, http.* calls outside a top-level def (e.g. inside a
// top-level `if`) were dropped, and calls inside a nested def were
// misattributed to the outer def. New behaviour: top-level non-def
// calls are recorded with empty Function, nested-def calls are
// attributed to the smallest enclosing def.
func TestExtractEndpoints_TopLevelIfAndNestedDef(t *testing.T) {
	src := `
def outer():
    http.get("/outer")
    def inner():
        http.post("/inner")
    inner()

if True:
    http.put("/toplevel")
`
	opts := &syntax.FileOptions{Set: true, While: true, TopLevelControl: true, GlobalReassign: true, Recursion: true}
	f, err := opts.Parse("blueprint.star", src, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	got := extractEndpoints(f)
	want := map[string]string{
		"GET /outer":     "outer",
		"POST /inner":    "inner",     // attributed to inner, NOT outer
		"PUT /toplevel":  "",          // top-level, not inside any def
	}
	if len(got) != len(want) {
		t.Fatalf("got %d endpoints, want %d: %+v", len(got), len(want), got)
	}
	for _, ep := range got {
		key := ep.Method + " " + ep.Path
		expectFn, ok := want[key]
		if !ok {
			t.Errorf("unexpected endpoint %s (fn=%q)", key, ep.Function)
			continue
		}
		if ep.Function != expectFn {
			t.Errorf("endpoint %s: Function=%q, want %q", key, ep.Function, expectFn)
		}
	}
}

// TestOrderedProjection_TopLevelOrder pins the fix for the R6
// finding that --fields output drifted from the unprojected struct
// order: encoding/json sorts map keys alphabetically, so the same
// envelope shipped two different shapes depending on whether
// `--fields` was passed. orderedProjection now emits top-level keys
// in the canonical struct-declaration order.
func TestOrderedProjection_TopLevelOrder(t *testing.T) {
	desc := &BlueprintDescription{
		SchemaVersion: 1,
		Kind:          "detail",
		Name:          "x",
		Path:          "lib/x.star",
		Auth:          map[string]any{"env": "X_TOKEN"},
		BaseURL:       "https://example.com",
		Fetch:         &fetchSection{Args: []string{"a"}, Mode: "sync"},
	}
	all := descToMap(desc)
	projected, err := projectFields(all, []string{"name", "fetch.args", "auth.env", "base_url"})
	if err != nil {
		t.Fatalf("projectFields: %v", err)
	}
	projected["schema_version"] = describeBlueprintDetailSchemaVersion
	projected["kind"] = "detail"
	out, err := json.Marshal(orderedProjection{m: projected, order: blueprintSchemaTopLevelOrder})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// schema_version must come first — typed clients use its position
	// as a stable seek anchor regardless of which fields were projected.
	if !strings.HasPrefix(string(out), `{"schema_version":`) {
		t.Errorf("output must start with schema_version, got: %s", out)
	}
	// kind must immediately follow schema_version.
	if !strings.HasPrefix(string(out), `{"schema_version":1,"kind":"detail"`) {
		t.Errorf("kind must follow schema_version, got: %s", out)
	}
	// Spot-check that name/base_url/auth/fetch appear in struct
	// declaration order: name → fetch → auth → base_url.
	idxName := strings.Index(string(out), `"name":`)
	idxFetch := strings.Index(string(out), `"fetch":`)
	idxAuth := strings.Index(string(out), `"auth":`)
	idxBaseURL := strings.Index(string(out), `"base_url":`)
	if !(idxName < idxFetch && idxFetch < idxAuth && idxAuth < idxBaseURL) {
		t.Errorf("top-level keys not in struct order (name<fetch<auth<base_url), got positions name=%d fetch=%d auth=%d base_url=%d in: %s",
			idxName, idxFetch, idxAuth, idxBaseURL, out)
	}
}

// TestBlueprintSchemaPaths_ContainsExpected pins the reflection-based
// schema enumeration. If new fields are added to BlueprintDescription
// without JSON tags, they won't appear here — the test fails until tags
// are added (Codex round 4 finding 3 about omitempty handling).
func TestBlueprintSchemaPaths_ContainsExpected(t *testing.T) {
	mustExist := []string{
		"schema_version",
		"name",
		"path",
		"is_sink",
		"fetch.args",
		"fetch.page_size",
		"fetch.mode",
		"fetch.poll_interval",
		"fetch.poll_timeout",
		"fetch.poll_backoff",
		"push.args",
		"push.batch_size",
		"push.batch_mode",
		"endpoints",
		"rate_limit.requests",
		"rate_limit.per",
		"base_url",
		"timeout",
		"retry",
		"backoff",
	}
	for _, p := range mustExist {
		if !blueprintSchemaPaths[p] {
			t.Errorf("blueprintSchemaPaths missing %q (struct tag drift?)", p)
		}
	}
}

// TestIsValidBlueprintFieldPath_OmitemptyFields pins the fix for
// Codex round 4 finding 3. Fields with omitempty in JSON tags must
// still be recognized as valid paths even when their runtime value
// is zero (so projection returns null instead of "unknown path").
func TestIsValidBlueprintFieldPath_OmitemptyFields(t *testing.T) {
	cases := []struct {
		path string
		want bool
	}{
		{"is_sink", true},                // bool with omitempty
		{"fetch.poll_interval", true},    // string with omitempty
		{"fetch.poll_timeout", true},
		{"fetch.poll_backoff", true},
		{"push.max_concurrent", true},
		{"endpoints", true},
		{"rate_limit", true},
		{"rate_limit.requests", true},
		{"auth", true},
		{"auth.env", true}, // dynamic map sub-key
		{"headers.X-Custom-Header", true},
		// Truly unknown paths must still error
		{"truly.unknown", false},
		{"fetch.bogus", false},
		{"push.bogus", false},
		// R6 finding: only ONE level of nesting under map-typed parents
		// is valid (auth/headers values are string/any scalars). Two
		// or more dots after the prefix means the user is trying to
		// descend into a leaf — reject so lookupPath doesn't fabricate
		// nested objects.
		{"auth.provider.foo", false},
		{"auth..", false},
		{"auth.", false},
		{"headers.X-Foo.bar", false},
		{"headers.", false},
	}
	for _, tc := range cases {
		if got := isValidBlueprintFieldPath(tc.path); got != tc.want {
			t.Errorf("isValidBlueprintFieldPath(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}

// TestProjectFields_OmitemptyReturnsNull pins that valid-but-empty
// schema paths return null in the projection rather than erroring out
// (Codex round 4 finding 3 — sync blueprint querying fetch.poll_interval
// should get null, not "unknown field path").
func TestProjectFields_OmitemptyReturnsNull(t *testing.T) {
	src := map[string]any{
		"name":  "riksbank",
		"fetch": map[string]any{"args": []any{"x"}, "mode": "sync"},
		// poll_interval intentionally absent (omitempty struck on serialization)
	}
	got, err := projectFields(src, []string{"fetch.poll_interval"})
	if err != nil {
		t.Fatalf("projectFields returned error for valid omitempty path: %v", err)
	}
	v, ok := lookupPath(got, "fetch.poll_interval")
	if !ok {
		t.Error("expected fetch.poll_interval in projection")
	}
	if v != nil {
		t.Errorf("expected null for empty omitempty field, got %v", v)
	}
}
