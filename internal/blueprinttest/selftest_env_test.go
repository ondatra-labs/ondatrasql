// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build blueprints || e2e

package blueprinttest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// The env: map of expect.yaml, proven against a fake API: a blueprint that
// takes a sheet id from env.get() when the model passes '' is recorded
// from a seed expect.yaml (env: and the runs, no assertions), and replays with the
// variable unset in the shell, because the harness sets it in both.

const fakeSheetID = "sheet-selftest-7c41"

const fakeSheetLib = `API = {
    "base_url": %q,
    "fetch": {"args": ["sheet_id"]},
}

def fetch(sheet_id):
    sheet_id = sheet_id or env.get("FAKESHEET_ID")
    if not sheet_id:
        fail("fakesheet: no sheet id")
    resp = http.get("/sheets/" + sheet_id + "/rows")
    if not resp.ok:
        fail("fakesheet: " + str(resp.status_code))
    return {"rows": resp.json, "next": None}
`

const fakeSheetModel = `-- @kind: table
-- @fetch
SELECT
    id::BIGINT   AS id,
    name::VARCHAR AS name
FROM fakesheet('')
`

const fakeSheetPage = `---
title: Fake Sheet
description: Rows from a fake sheet API the self-test starts.
vendor: Self-test
tags: [test]
auth_method: none
source_type: fetch
model_kind: table
args: [sheet_id]
min_version: v0.47.0
---
{{< bp-file "lib/fakesheet.star" >}}

{{< bp-file "models/raw/rows.sql" >}}
`

func TestHarness_ExpectEnv(t *testing.T) {
	shared, err := filepath.Abs(filepath.Join(blueprintsRoot, "_shared"))
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{bin: ondatrasqlBinary(t), shared: shared, mainVersion: mainVersion(t)}

	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.URL.Path != "/sheets/"+fakeSheetID+"/rows" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 1, "name": "Ada"}, {"id": 2, "name": "Bo"}}) // test server
	}))
	defer srv.Close()

	bp := filepath.Join(t.TempDir(), "fakesheet")
	writeFiles(t, bp, map[string]string{
		"index.md":            fakeSheetPage,
		".env.example":        "# the sheet's id, when the model passes ''\nFAKESHEET_ID=\n",
		"lib/fakesheet.star":  fmt.Sprintf(fakeSheetLib, srv.URL),
		"models/raw/rows.sql": fakeSheetModel,
	})
	if err := h.check(t, bp); err != nil {
		t.Fatalf("check: %v", err)
	}

	// Record from a seed expect.yaml (env: and runs that assert nothing),
	// the variable unset in the shell.
	tests := t.TempDir()
	rec := filepath.Join(tests, "fakesheet")
	// A table model fetches on every run, so run 2 records into run2/.
	writeFiles(t, tests, map[string]string{"fakesheet/expect.yaml": "env:\n  FAKESHEET_ID: " + fakeSheetID + "\n" +
		"runs:\n  - now: \"2026-03-03T12:00:00Z\"\n  - now: \"2026-03-03T12:00:00Z\"\n    cassettes: run2\n"})
	t.Setenv("FAKESHEET_ID", "")
	draft, err := h.record(t, bp, rec)
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	if !strings.Contains(string(draft), "FAKESHEET_ID: "+fakeSheetID) || !strings.Contains(string(draft), "rows: 2") {
		t.Errorf("draft keeps neither the env nor the rows:\n%s", draft)
	}
	written, err := os.ReadFile(filepath.Join(rec, "expect.yaml"))
	if err != nil || string(written) != string(draft) {
		t.Fatalf("the seed expect.yaml was not replaced by the draft: %v\n%s", err, written)
	}
	cas, err := os.ReadFile(filepath.Join(rec, "cassettes", "raw.rows", "fakesheet.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(cas), "/sheets/"+fakeSheetID+"/rows") {
		t.Fatalf("the env value is not in the recorded URL as is:\n%s", cas)
	}

	// Replay with the server gone and the variable unset: env: sets it.
	srv.Close()
	before := hits.Load()
	if err := h.replayBlueprint(t, "fakesheet", bp, tests); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if hits.Load() != before {
		t.Fatalf("replay reached the server")
	}

	t.Run("without env the replay misses", func(t *testing.T) {
		dir := t.TempDir()
		if err := copyTree(rec, filepath.Join(dir, "fakesheet")); err != nil {
			t.Fatal(err)
		}
		exp := strings.Replace(string(written), "FAKESHEET_ID: "+fakeSheetID, "FAKESHEET_ID: sheet-other", 1)
		writeFiles(t, dir, map[string]string{"fakesheet/expect.yaml": exp})
		wantErr(t, "env changed", h.replayBlueprint(t, "fakesheet", bp, dir), "no recorded interaction for GET", "sheet-other")
	})
	t.Run("harness variables are refused", func(t *testing.T) {
		dir := t.TempDir()
		if err := copyTree(rec, filepath.Join(dir, "fakesheet")); err != nil {
			t.Fatal(err)
		}
		exp := strings.Replace(string(written), "env:\n", "env:\n  ONDATRA_NOW: \"2030-01-01T00:00:00Z\"\n", 1)
		writeFiles(t, dir, map[string]string{"fakesheet/expect.yaml": exp})
		wantErr(t, "ONDATRA_NOW", h.replayBlueprint(t, "fakesheet", bp, dir), `env "ONDATRA_NOW"`)
	})
}
