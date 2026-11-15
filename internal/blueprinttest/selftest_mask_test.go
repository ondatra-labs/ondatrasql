// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build blueprints || e2e

package blueprinttest

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/httpcassette"
)

// The mask: list of expect.yaml, proven against a fake API: people with a
// nested email (a string, the same one twice), a rate (a number) and an
// admin flag (a boolean) are recorded with those three masked, and the
// recording replays to the draft made from it.

const fakePeopleBody = `{"data":{"people":[` +
	`{"id":1,"name":"Ada","contact":{"email":"ada.selftest@example.com"},"rate_cents":987651,"admin":true,"manager":null},` +
	`{"id":2,"name":"Bo","contact":{"email":"bo.selftest@example.com"},"rate_cents":456782,"admin":true,"manager":{"email":"ada.selftest@example.com"}}` +
	`]}}`

const fakePeopleLib = `API = {
    "base_url": %q,
    "fetch": {"args": ["resource"]},
}

def fetch(resource):
    notice = http.get("/notice")
    if not notice.ok:
        fail("fakepeople: notice " + str(notice.status_code))
    resp = http.get("/" + resource)
    if not resp.ok:
        fail("fakepeople: " + str(resp.status_code))
    rows = []
    for p in resp.json["data"]["people"]:
        rows.append({
            "id": p["id"],
            "name": p["name"],
            "email": p["contact"]["email"],
            "manager_email": p["manager"]["email"] if p["manager"] else None,
            "rate_cents": p["rate_cents"],
            "admin": p["admin"],
        })
    return {"rows": rows, "next": None}
`

const fakePeopleModel = `-- @kind: table
-- @fetch
SELECT
    id::BIGINT             AS id,
    name::VARCHAR          AS name,
    email::VARCHAR         AS email,
    manager_email::VARCHAR AS manager_email,
    rate_cents::BIGINT     AS rate_cents,
    admin::BOOLEAN         AS admin
FROM fakepeople('people')
`

const fakePeoplePage = `---
title: Fake People
description: People from a fake API the self-test starts.
vendor: Self-test
tags: [test]
auth_method: none
source_type: fetch
model_kind: table
args: [resource]
min_version: v0.47.0
---
{{< bp-file "lib/fakepeople.star" >}}

{{< bp-file "models/raw/people.sql" >}}
`

func TestHarness_ExpectMask(t *testing.T) {
	shared, err := filepath.Abs(filepath.Join(blueprintsRoot, "_shared"))
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{bin: ondatrasqlBinary(t), shared: shared, mainVersion: mainVersion(t)}

	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		switch r.URL.Path {
		case "/people":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, fakePeopleBody)
		case "/notice":
			w.Header().Set("Content-Type", "text/plain")
			fmt.Fprint(w, "ok")
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	bp := filepath.Join(t.TempDir(), "fakepeople")
	writeFiles(t, bp, map[string]string{
		"index.md":              fakePeoplePage,
		".env.example":          "",
		"lib/fakepeople.star":   fmt.Sprintf(fakePeopleLib, srv.URL),
		"models/raw/people.sql": fakePeopleModel,
	})
	if err := h.check(t, bp); err != nil {
		t.Fatalf("check: %v", err)
	}

	// A seed with mask: and two runs; a table model fetches every run.
	tests := t.TempDir()
	rec := filepath.Join(tests, "fakepeople")
	writeFiles(t, tests, map[string]string{"fakepeople/expect.yaml": "mask: [email, rate_cents, admin]\n" +
		"runs:\n  - now: \"2026-03-03T12:00:00Z\"\n  - now: \"2026-03-03T12:00:00Z\"\n    cassettes: run2\n"})
	draft, err := h.record(t, bp, rec)
	if err != nil {
		t.Fatalf("record: %v", err)
	}

	ada, bo := httpcassette.MaskString("ada.selftest@example.com"), httpcassette.MaskString("bo.selftest@example.com")
	originals := []string{"ada.selftest@example.com", "bo.selftest@example.com", "987651", "456782"}
	for _, cas := range []string{"cassettes/raw.people/fakepeople.json", "cassettes/run2/raw.people/fakepeople.json"} {
		data, err := os.ReadFile(filepath.Join(rec, cas))
		if err != nil {
			t.Fatal(err)
		}
		s := string(data)
		for _, v := range append(originals, `"admin": true`) {
			if strings.Contains(s, v) {
				t.Errorf("%s keeps %s:\n%s", cas, v, s)
			}
		}
		// Ada's email as her own and as Bo's manager's: one placeholder.
		if n := strings.Count(s, `"email": "`+ada+`"`); n != 2 {
			t.Errorf("%s: %d placeholders for Ada's two emails, want 2:\n%s", cas, n, s)
		}
		for _, v := range []string{`"email": "` + bo + `"`, `"rate_cents": 0`, `"admin": false`, `"name": "Ada"`, `"body": "ok"`} {
			if !strings.Contains(s, v) {
				t.Errorf("%s lacks %s:\n%s", cas, v, s)
			}
		}
	}

	// The draft is what the masked recording replays to, and keeps mask:.
	d := string(draft)
	for _, v := range originals {
		if strings.Contains(d, v) {
			t.Errorf("draft keeps %s:\n%s", v, d)
		}
	}
	if !strings.Contains(d, "mask:\n  - email\n  - rate_cents\n  - admin\n") || strings.Count(d, ada) != 2 || !strings.Contains(d, "rows: 2") {
		t.Errorf("draft lacks mask: or Ada's placeholder as email and manager_email:\n%s", d)
	}
	written, err := os.ReadFile(filepath.Join(rec, "expect.yaml"))
	if err != nil || string(written) != d {
		t.Fatalf("the seed expect.yaml was not replaced by the draft: %v\n%s", err, written)
	}

	// The recorder names the response mask: could not reach.
	notJSON, err := nonJSONResponses(filepath.Join(rec, "cassettes"))
	if err != nil {
		t.Fatal(err)
	}
	if len(notJSON) != 2 || !strings.Contains(notJSON[0], "/notice") || !strings.Contains(notJSON[1], "/notice") {
		t.Errorf("non-JSON responses: %q, want the two /notice ones", notJSON)
	}

	// Replay with the server gone.
	srv.Close()
	before := hits.Load()
	if err := h.replayBlueprint(t, "fakepeople", bp, tests); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if hits.Load() != before {
		t.Fatalf("replay reached the server")
	}

	t.Run("a mask entry that is no field name is refused", func(t *testing.T) {
		for _, bad := range [][]string{{""}, {"a,b"}, {" "}} {
			if _, err := maskVar(bad); err == nil {
				t.Errorf("mask %q accepted", bad)
			}
		}
		if v, err := maskVar(nil); v != nil || err != nil {
			t.Errorf("no mask: %q, %v; want nothing set", v, err)
		}
	})
}
