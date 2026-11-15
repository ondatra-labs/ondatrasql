// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build blueprints || e2e

package blueprinttest

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

// The harness proves itself against a fake API this test starts, with
// data it makes up: a fetch blueprint (API key, incremental, paged, three
// runs on two days) and a push blueprint. It records them, replays them
// with the server gone and the keys unset, and shows that each kind of
// breakage the harness is for is caught.

const fakeShopKey = "fk_live_selftest_5b0c1d2e3f"

// fakeOrders is the shop's data: order i on 2026-03-0i, amount i*10 + 0.25
// (never whole: a whole number in JSON would type the column as BIGINT).
func fakeOrders() []map[string]any {
	var out []map[string]any
	for i := 1; i <= 5; i++ {
		out = append(out, map[string]any{"id": i, "day": fmt.Sprintf("2026-03-%02d", i), "amount": float64(i)*10 + 0.25})
	}
	return out
}

// fakeShop answers GET /orders?since=&until=&page=&limit= with the orders
// in [since, until], one page at a time, to requests with the key.
func fakeShop(hits *atomic.Int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get("X-Api-Key") != fakeShopKey {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		q := r.URL.Query()
		page, _ := strconv.Atoi(q.Get("page"))
		limit, _ := strconv.Atoi(q.Get("limit"))
		if r.URL.Path != "/orders" || page < 1 || limit < 1 || q.Get("since") == "" || q.Get("until") == "" {
			http.Error(w, "bad request "+r.URL.String(), http.StatusBadRequest)
			return
		}
		var match []map[string]any
		for _, o := range fakeOrders() {
			if d := o["day"].(string); d >= q.Get("since") && d <= q.Get("until") {
				match = append(match, o)
			}
		}
		lo, hi := min((page-1)*limit, len(match)), min(page*limit, len(match))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"orders": match[lo:hi], "more": hi < len(match)}) // test server
	}))
}

const fakeShopLib = `API = {
    "base_url": %q,
    "auth": {"env": "FAKESHOP_KEY", "header": "X-Api-Key"},
    "fetch": {"args": ["resource"], "page_size": 2},
}

DAY = time.parse_duration("24h")

def fetch(resource, page, is_backfill=True, last_value="", initial_value=""):
    if page.cursor:
        cur = page.cursor
    else:
        since = initial_value
        if not is_backfill and last_value:
            since = (time.parse_time(last_value[:10] + "T00:00:00Z") + DAY).format("2006-01-02")
        cur = {"since": since, "until": time.now().format("2006-01-02"), "page": 1}
    if cur["since"] > cur["until"]:
        return {"rows": [], "next": None}
    resp = http.get("/" + resource, params={"since": cur["since"], "until": cur["until"], "page": str(cur["page"]), "limit": str(page.size)})
    if not resp.ok:
        fail("fakeshop: " + str(resp.status_code) + " " + resp.text)
    nxt = None
    if resp.json["more"]:
        nxt = {"since": cur["since"], "until": cur["until"], "page": cur["page"] + 1}
    return {"rows": resp.json["orders"], "next": nxt}
`

const fakeShopModel = `-- @kind: merge
-- @unique_key: id
-- @fetch
-- @incremental: day
-- @incremental_initial: 2026-03-01
SELECT
    id::BIGINT            AS id,
    day::DATE             AS day,
    amount::DECIMAL(10, 2) AS amount
FROM fakeshop('orders')
`

const fakeShopPage = `---
title: Fake Shop
description: Orders from a fake shop API the self-test starts.
vendor: Self-test
tags: [test]
auth_method: api_key
source_type: fetch
model_kind: merge
args: [resource]
---
Orders, incrementally by day.

{{< bp-file "lib/fakeshop.star" >}}

{{< bp-file "models/raw/orders.sql" >}}
`

// fakeShopExpect: run 1 on 2026-03-03 backfills orders 1-3 in two pages;
// run 2 the same day is up to date and makes no request; run 3 on
// 2026-03-05 fetches orders 4-5 in one page, from its own cassettes.
const fakeShopExpect = `runs:
  - now: "2026-03-03T12:00:00Z"
    requests: 2
    models:
      raw.orders: {run_type: backfill, rows: 3}
    tables:
      raw.orders:
        columns:
          - id BIGINT
          - day DATE
          - amount DECIMAL(10,2)
        rows: 3
        sample:
          - {id: "1", day: "2026-03-01", amount: "10.25"}
          - {id: "3", day: "2026-03-03", amount: "30.25"}
  - now: "2026-03-03T12:00:00Z"
    requests: 0
    models:
      raw.orders: {run_type: incremental, rows: 0}
    tables:
      raw.orders: {rows: 3}
  - now: "2026-03-05T09:00:00Z"
    cassettes: run3
    requests: 1
    models:
      raw.orders: {run_type: incremental, rows: 2}
    tables:
      raw.orders:
        rows: 5
        sample:
          - {id: "5", day: "2026-03-05", amount: "50.25"}
`

const fakeCRMKey = "crm_selftest_8a7b6c5d4e"

const fakeCRMLib = `API = {
    "base_url": %q,
    "auth": {"env": "FAKECRM_KEY"},
    "push": {"batch_size": 2},
}

def push(rows):
    resp = http.post("/contacts", json=[{"id": r["id"], "name": r["name"]} for r in rows])
    if not resp.ok:
        fail("fakecrm: " + str(resp.status_code))
    out = {}
    for r in rows:
        out[str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]] = "ok"
    return out
`

const fakeCRMSource = `-- @kind: table
SELECT * FROM (VALUES (1, 'Ada'), (2, 'Bo'), (3, 'Cy')) AS t(id, name)
`

const fakeCRMSync = `-- @kind: merge
-- @unique_key: id
-- @push: fakecrm
SELECT id::BIGINT AS id, name::VARCHAR AS name FROM raw.contacts
`

const fakeCRMPage = `---
title: Fake CRM
description: Contacts pushed to a fake CRM API the self-test starts.
vendor: Self-test
tags: [test]
auth_method: api_key
source_type: push
model_kind: merge
args: []
---
{{< bp-file "lib/fakecrm.star" >}}

{{< bp-file "models/raw/contacts.sql" >}}

{{< bp-file "models/sync/contacts.sql" >}}
`

// fakeCRMExpect: three contacts go out in two batches; the second run has
// nothing new and pushes nothing.
const fakeCRMExpect = `runs:
  - now: "2026-03-03T12:00:00Z"
    requests: 2
    models:
      sync.contacts: {rows: 3}
    tables:
      raw.contacts:
        columns:
          - id INTEGER
          - name VARCHAR
        rows: 3
      sync.contacts:
        columns:
          - id BIGINT
          - name VARCHAR
        rows: 3
        sample:
          - {id: "2", name: "Bo"}
  - now: "2026-03-03T12:00:00Z"
    requests: 0
    tables:
      raw.contacts: {rows: 3}
      sync.contacts: {rows: 3}
`

const fakeDocsKey = "fd_selftest_1c2d3e4f5a"

// fakeDocs answers POST /docs, a multipart upload of one text file, with
// the number of words in it, to requests with the key.
func fakeDocs(hits *atomic.Int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get("Authorization") != "Bearer "+fakeDocsKey {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		f, _, err := r.FormFile("file")
		if r.URL.Path != "/docs" || err != nil {
			http.Error(w, fmt.Sprintf("bad request %s: %v", r.URL, err), http.StatusBadRequest)
			return
		}
		defer f.Close()
		var buf strings.Builder
		if _, err := io.Copy(&buf, f); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"words": len(strings.Fields(buf.String()))}) // test server
	}))
}

// fakeDocsLib reads its input from the project, as the mistral-ocr
// blueprint does: every file the glob matches is hashed and uploaded.
const fakeDocsLib = `API = {
    "base_url": %q,
    "auth": {"env": "FAKEDOCS_KEY"},
    "fetch": {"args": ["pattern"]},
}

def fetch(pattern, page):
    rows = []
    for f in glob(pattern):
        digest = md5_file(f)
        resp = http.upload("/docs", file=f, fields={"md5": digest})
        if not resp.ok:
            fail("fakedocs " + f + ": " + str(resp.status_code) + " " + resp.text)
        rows.append({"file": f, "md5": digest, "words": resp.json["words"]})
    return {"rows": rows, "next": None}
`

const fakeDocsModel = `-- @kind: table
-- @fetch
SELECT
    file::VARCHAR  AS file,
    md5::VARCHAR   AS md5,
    words::BIGINT  AS words
FROM fakedocs('data/*.txt')
`

const fakeDocsPage = `---
title: Fake Docs
description: Word counts of the text files in data/, from a fake API the self-test starts.
vendor: Self-test
tags: [test]
auth_method: api_key
source_type: fetch
model_kind: table
args: [pattern]
---
{{< bp-file "lib/fakedocs.star" >}}

{{< bp-file "models/raw/docs.sql" >}}
`

// fakeDocsData is the one input file, kept with the recording in data/.
const fakeDocsData = "four words of text\n"

// fakeDocsExpect: the one file in data/ is uploaded once, and its words
// counted, which only happens when record and replay both have data/.
const fakeDocsExpect = `runs:
  - now: "2026-03-03T12:00:00Z"
    requests: 1
    models:
      raw.docs: {run_type: backfill, rows: 1}
    tables:
      raw.docs:
        columns:
          - file VARCHAR
          - md5 VARCHAR
          - words BIGINT
        rows: 1
        sample:
          - {file: "data/note.txt", words: "4"}
`

func writeFiles(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for rel, content := range files {
		p := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// copyBlueprint copies a blueprint to a new directory and applies edit
// (file -> new content) to it, for a variant that must fail.
func copyBlueprint(t *testing.T, src string, edit map[string]string) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), filepath.Base(src))
	if err := copyTree(src, dst); err != nil {
		t.Fatal(err)
	}
	writeFiles(t, dst, edit)
	return dst
}

func wantErr(t *testing.T, what string, err error, substrs ...string) {
	t.Helper()
	if err == nil {
		t.Errorf("%s: harness passed, want a failure", what)
		return
	}
	for _, s := range substrs {
		if !strings.Contains(err.Error(), s) {
			t.Errorf("%s: failure does not mention %q:\n%v", what, s, err)
		}
	}
}

func TestHarness_SelfTest(t *testing.T) {
	shared, err := filepath.Abs(filepath.Join(blueprintsRoot, "_shared"))
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{bin: ondatrasqlBinary(t), shared: shared}

	var shopHits, crmHits, docsHits atomic.Int32
	shop := fakeShop(&shopHits)
	defer shop.Close()
	docs := fakeDocs(&docsHits)
	defer docs.Close()
	var pushed atomic.Int32
	crm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		crmHits.Add(1)
		if r.Header.Get("Authorization") != "Bearer "+fakeCRMKey {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var rows []map[string]any
		if err := json.NewDecoder(r.Body).Decode(&rows); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		pushed.Add(int32(len(rows)))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer crm.Close()

	root := t.TempDir()
	shopDir := filepath.Join(root, "fakeshop")
	writeFiles(t, shopDir, map[string]string{
		"index.md":              fakeShopPage,
		".env.example":          "# the shop's API key\nFAKESHOP_KEY=\n",
		"lib/fakeshop.star":     fmt.Sprintf(fakeShopLib, shop.URL),
		"models/raw/orders.sql": fakeShopModel,
	})
	crmDir := filepath.Join(root, "fakecrm")
	writeFiles(t, crmDir, map[string]string{
		"index.md":                 fakeCRMPage,
		".env.example":             "FAKECRM_KEY=\n",
		"lib/fakecrm.star":         fmt.Sprintf(fakeCRMLib, crm.URL),
		"models/raw/contacts.sql":  fakeCRMSource,
		"models/sync/contacts.sql": fakeCRMSync,
	})
	docsDir := filepath.Join(root, "fakedocs")
	writeFiles(t, docsDir, map[string]string{
		"index.md":            fakeDocsPage,
		".env.example":        "FAKEDOCS_KEY=\n",
		"lib/fakedocs.star":   fmt.Sprintf(fakeDocsLib, docs.URL),
		"models/raw/docs.sql": fakeDocsModel,
	})
	tests := t.TempDir()
	writeFiles(t, tests, map[string]string{
		"fakeshop/expect.yaml":   fakeShopExpect,
		"fakecrm/expect.yaml":    fakeCRMExpect,
		"fakedocs/expect.yaml":   fakeDocsExpect,
		"fakedocs/data/note.txt": fakeDocsData,
	})

	// The offline check passes for all three; fakedocs has no data/ there.
	for _, dir := range []string{shopDir, crmDir, docsDir} {
		if err := h.check(t, dir); err != nil {
			t.Fatalf("check %s: %v", filepath.Base(dir), err)
		}
	}

	// Recording without the key fails before any request.
	t.Setenv("FAKESHOP_KEY", "")
	_, err = h.record(t, shopDir, filepath.Join(tests, "fakeshop"))
	wantErr(t, "record without key", err, "FAKESHOP_KEY")
	if shopHits.Load() != 0 {
		t.Fatalf("record without key reached the server")
	}

	// Record both against the fake servers, with the keys.
	t.Setenv("FAKESHOP_KEY", fakeShopKey)
	t.Setenv("FAKECRM_KEY", fakeCRMKey)
	t.Setenv("FAKEDOCS_KEY", fakeDocsKey)
	draft, err := h.record(t, shopDir, filepath.Join(tests, "fakeshop"))
	if err != nil {
		t.Fatalf("record fakeshop: %v", err)
	}
	if !strings.Contains(string(draft), "rows: 5") || !strings.Contains(string(draft), "cassettes: run3") {
		t.Errorf("fakeshop draft expect.yaml:\n%s", draft)
	}
	if _, err := h.record(t, crmDir, filepath.Join(tests, "fakecrm")); err != nil {
		t.Fatalf("record fakecrm: %v", err)
	}
	if pushed.Load() != 3 {
		t.Fatalf("record fakecrm pushed %d rows, want 3", pushed.Load())
	}
	// fakedocs records with data/ copied in: the upload reaches the server.
	if _, err := h.record(t, docsDir, filepath.Join(tests, "fakedocs")); err != nil {
		t.Fatalf("record fakedocs: %v", err)
	}
	if docsHits.Load() != 1 {
		t.Fatalf("record fakedocs made %d requests, want 1 (data/note.txt uploaded)", docsHits.Load())
	}
	for _, f := range []string{"fakeshop/cassettes/raw.orders/fakeshop.json", "fakeshop/cassettes/run3/raw.orders/fakeshop.json", "fakecrm/cassettes/sync.contacts/fakecrm.json", "fakedocs/cassettes/raw.docs/fakedocs.json"} {
		data, err := os.ReadFile(filepath.Join(tests, f))
		if err != nil {
			t.Fatalf("recording %s: %v", f, err)
		}
		if strings.Contains(string(data), fakeShopKey) || strings.Contains(string(data), fakeCRMKey) || strings.Contains(string(data), fakeDocsKey) {
			t.Fatalf("recording %s contains a key", f)
		}
	}

	// Replay: servers closed, keys unset (replay drops them as well).
	shop.Close()
	crm.Close()
	docs.Close()
	before := shopHits.Load() + crmHits.Load() + docsHits.Load()
	t.Setenv("FAKESHOP_KEY", "")
	t.Setenv("FAKECRM_KEY", "")
	t.Setenv("FAKEDOCS_KEY", "")
	for _, name := range []string{"fakeshop", "fakecrm", "fakedocs"} {
		if err := h.replayBlueprint(t, name, filepath.Join(root, name), tests); err != nil {
			t.Fatalf("replay: %v", err)
		}
	}
	if shopHits.Load()+crmHits.Load()+docsHits.Load() != before {
		t.Fatalf("replay reached a server")
	}

	// What the harness must catch.
	t.Run("missing recording", func(t *testing.T) {
		wantErr(t, "missing recording", h.replayBlueprint(t, "fakeshop", shopDir, t.TempDir()),
			"fakeshop: no recording in BLUEPRINT_TESTS_DIR")
	})
	t.Run("column dropped from the lib's model", func(t *testing.T) {
		bp := copyBlueprint(t, shopDir, map[string]string{"models/raw/orders.sql": strings.Replace(fakeShopModel,
			",\n    amount::DECIMAL(10, 2) AS amount\n", "\n", 1)})
		wantErr(t, "column dropped", h.replayBlueprint(t, "fakeshop", bp, tests), "table raw.orders: columns")
	})
	t.Run("wrong row count and missing sample", func(t *testing.T) {
		rec := t.TempDir()
		if err := copyTree(filepath.Join(tests, "fakeshop"), filepath.Join(rec, "fakeshop")); err != nil {
			t.Fatal(err)
		}
		exp := strings.Replace(fakeShopExpect, "rows: 5", "rows: 6", 1)
		exp = strings.Replace(exp, `amount: "50.25"`, `amount: "51.00"`, 1)
		writeFiles(t, rec, map[string]string{"fakeshop/expect.yaml": exp})
		wantErr(t, "row count", h.replayBlueprint(t, "fakeshop", shopDir, rec),
			"run 3 (now 2026-03-05T09:00:00Z): table raw.orders: 5 rows, want 6", "no row matches sample")
	})
	t.Run("request count differs", func(t *testing.T) {
		rec := t.TempDir()
		if err := copyTree(filepath.Join(tests, "fakeshop"), filepath.Join(rec, "fakeshop")); err != nil {
			t.Fatal(err)
		}
		exp := strings.Replace(fakeShopExpect, "    requests: 0\n", "    requests: 1\n", 1)
		writeFiles(t, rec, map[string]string{"fakeshop/expect.yaml": exp})
		wantErr(t, "request count", h.replayBlueprint(t, "fakeshop", shopDir, rec), "0 requests replayed, want 1")
	})
	t.Run("clock moved", func(t *testing.T) {
		rec := t.TempDir()
		if err := copyTree(filepath.Join(tests, "fakeshop"), filepath.Join(rec, "fakeshop")); err != nil {
			t.Fatal(err)
		}
		writeFiles(t, rec, map[string]string{"fakeshop/expect.yaml": strings.ReplaceAll(fakeShopExpect, "2026-03-03T12:00:00Z", "2026-03-04T12:00:00Z")})
		wantErr(t, "clock moved", h.replayBlueprint(t, "fakeshop", shopDir, rec), "no recorded interaction for GET", "until=2026-03-04")
	})
	t.Run("push body changed", func(t *testing.T) {
		bp := copyBlueprint(t, crmDir, map[string]string{"models/raw/contacts.sql": strings.Replace(fakeCRMSource, "'Bo'", "'Bob'", 1)})
		// The batch whose body differs gets no recorded response; the
		// sink reports it as a warning, which fails the replay.
		wantErr(t, "push body", h.replayBlueprint(t, "fakecrm", bp, tests),
			"model sync.contacts: warning: sink fakecrm", "1 requests replayed, want 2")
	})
	t.Run("data file differs at replay", func(t *testing.T) {
		rec := t.TempDir()
		if err := copyTree(filepath.Join(tests, "fakedocs"), filepath.Join(rec, "fakedocs")); err != nil {
			t.Fatal(err)
		}
		writeFiles(t, rec, map[string]string{"fakedocs/data/note.txt": "five words of other text\n"})
		wantErr(t, "data differs", h.replayBlueprint(t, "fakedocs", docsDir, rec), "no recorded interaction for POST", "/docs")
	})
	t.Run("data missing at replay", func(t *testing.T) {
		rec := t.TempDir()
		if err := copyTree(filepath.Join(tests, "fakedocs"), filepath.Join(rec, "fakedocs")); err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(filepath.Join(rec, "fakedocs", "data")); err != nil {
			t.Fatal(err)
		}
		wantErr(t, "data missing", h.replayBlueprint(t, "fakedocs", docsDir, rec), "0 requests replayed, want 1")
	})
	t.Run("page out of step with describe blueprint", func(t *testing.T) {
		bp := copyBlueprint(t, shopDir, map[string]string{"index.md": strings.NewReplacer(
			"auth_method: api_key", "auth_method: none",
			"args: [resource]", "args: [resource, since]",
			"model_kind: merge", "model_kind: append",
			`{{< bp-file "models/raw/orders.sql" >}}`, `{{< bp-file "models/raw/order.sql" >}}`,
		).Replace(fakeShopPage)})
		wantErr(t, "page", h.check(t, bp),
			`auth_method "none"`, `args ["resource" "since"]`, `model_kind "append", models/raw/orders.sql is @kind: merge`,
			`bp-file "models/raw/order.sql" does not exist`, "models/raw/orders.sql is not shown")
	})
	t.Run("env example", func(t *testing.T) {
		bp := copyBlueprint(t, shopDir, map[string]string{".env.example": "OTHER=\n"})
		wantErr(t, "env missing", h.check(t, bp), "FAKESHOP_KEY (read by the API dict's auth) is missing")
		bp = copyBlueprint(t, shopDir, map[string]string{".env.example": "FAKESHOP_KEY=abc\n"})
		wantErr(t, "env value", h.check(t, bp), "FAKESHOP_KEY has a value")
	})
	t.Run("validate warning", func(t *testing.T) {
		// A fetch model whose column is not cast breaks the strict fetch contract.
		bp := copyBlueprint(t, shopDir, map[string]string{"models/raw/orders.sql": strings.Replace(fakeShopModel,
			"amount::DECIMAL(10, 2) AS amount", "amount", 1)})
		wantErr(t, "validate", h.check(t, bp), "validate --strict")
	})
}
