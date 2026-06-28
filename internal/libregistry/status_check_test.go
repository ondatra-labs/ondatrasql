// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package libregistry

import (
	"strings"
	"testing"

	"go.starlark.net/syntax"
)

func parseStar(t *testing.T, code string) *syntax.File {
	t.Helper()
	opts := &syntax.FileOptions{Set: true, While: true, GlobalReassign: false, Recursion: true}
	f, err := opts.Parse("test.star", code, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return f
}

func TestCheckFetchStatusHandling(t *testing.T) {
	cases := []struct {
		name      string
		code      string
		wantWarns int
	}{
		{
			name: "unguarded http fetch is flagged",
			code: `
def fetch(resource, page):
    resp = http.get("/v1/" + resource)
    return {"rows": resp.json["items"], "next": None}
`,
			wantWarns: 1,
		},
		{
			name: "resp.ok check clears it",
			code: `
def fetch(resource, page):
    resp = http.get("/v1/" + resource)
    if not resp.ok:
        fail("API error: " + str(resp.status_code))
    return {"rows": resp.json["items"], "next": None}
`,
			wantWarns: 0,
		},
		{
			name: "status_code reference alone clears it",
			code: `
def fetch(resource, page):
    resp = http.get("/v1/" + resource)
    if resp.status_code != 200:
        return {"rows": [], "next": None}
    return {"rows": resp.json, "next": None}
`,
			wantWarns: 0,
		},
		{
			name: "no http call is never flagged",
			code: `
def fetch(resource, page):
    return {"rows": [{"id": 1}], "next": None}
`,
			wantWarns: 0,
		},
		{
			name: "abort counts as a guard",
			code: `
def fetch(resource, page):
    resp = http.get("/v1/" + resource)
    if resp.status_code == 404:
        abort()
    return {"rows": resp.json, "next": None}
`,
			wantWarns: 0,
		},
		{
			name: "each unguarded async func flagged",
			code: `
def submit(resource, page):
    resp = http.post("/reports", json={})
    return {"job_id": resp.json["id"]}

def fetch_result(result_ref, page):
    resp = http.get(result_ref["url"])
    return {"rows": resp.json["data"], "next": None}
`,
			wantWarns: 2,
		},
		{
			name: "file-level bypass with reason silences",
			code: `
# ondatracheck:allow-unchecked-status this API returns 404 for empty collections
def fetch(resource, page):
    resp = http.get("/v1/" + resource)
    return {"rows": resp.json["items"], "next": None}
`,
			wantWarns: 0,
		},
		{
			name: "bare bypass (no reason) does NOT silence",
			code: `
# ondatracheck:allow-unchecked-status
def fetch(resource, page):
    resp = http.get("/v1/" + resource)
    return {"rows": resp.json["items"], "next": None}
`,
			wantWarns: 1,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := parseStar(t, c.code)
			got := checkFetchStatusHandling("testapi", f, c.code)
			if len(got) != c.wantWarns {
				t.Errorf("got %d warnings, want %d: %v", len(got), c.wantWarns, got)
			}
			for _, w := range got {
				if !strings.Contains(w, "resp.ok/status_code") {
					t.Errorf("warning missing guidance text: %q", w)
				}
			}
		})
	}
}
