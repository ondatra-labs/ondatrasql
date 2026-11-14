// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/clock"
	"github.com/ondatra-labs/ondatrasql/internal/httpcassette"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// HTTP record/replay end to end: a fetch and a push blueprint run once
// against an httptest server with ONDATRA_HTTP_MODE=record, then in a
// fresh project with the server shut down, the credentials unset and
// ONDATRA_HTTP_MODE=replay, and must produce the same result.

const cassetteAPIKey = "sk_live_e2e_9f8e7d6c5b4a"

func setCassetteEnv(t *testing.T, dir, mode string) {
	t.Helper()
	t.Setenv(httpcassette.EnvDir, dir)
	t.Setenv(httpcassette.EnvMode, mode)
	httpcassette.Reset()
	t.Cleanup(httpcassette.Reset)
}

// ordersServer pages through 5 orders, 2 per page, and only answers a
// request with the API key and a since= date one week before ONDATRA_NOW.
func ordersServer(t *testing.T, hits *atomic.Int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get("Authorization") != "Bearer "+cassetteAPIKey {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if got := r.URL.Query().Get("since"); got != "2026-02-22" {
			http.Error(w, "bad since "+got, http.StatusBadRequest)
			return
		}
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		if page == 0 {
			page = 1
		}
		var data []map[string]any
		for id := page*2 - 1; id <= page*2 && id <= 5; id++ {
			data = append(data, map[string]any{"id": id, "amount": float64(id) * 10.5, "status": "paid"})
		}
		next := any(nil)
		if page*2 < 5 {
			next = page + 1
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data, "next": next}) // test server
	}))
}

func writeOrdersProject(t *testing.T, p *testutil.Project, baseURL string) {
	testutil.WriteFile(t, p.Dir, "lib/orders_api.star", fmt.Sprintf(`
API = {
    "base_url": %q,
    "auth": {"env": "E2E_ORDERS_KEY"},
    "retry": 1,
    "fetch": {"args": [], "page_size": 2},
}

def fetch(page):
    since = (time.now() - time.parse_duration("168h")).format("2006-01-02")
    resp = http.get("/orders", params={"since": since, "page": str(int(page.cursor or 1))})
    if not resp.ok:
        fail("orders: HTTP %%d %%s" %% (resp.status_code, resp.text))
    return {"rows": resp.json["data"], "next": resp.json["next"]}
`, baseURL))
	p.AddModel("raw/orders.sql", `-- @kind: table
-- @fetch
SELECT id::BIGINT AS id, amount::DOUBLE AS amount, status::VARCHAR AS status FROM orders_api()
`)
}

func orderRows(t *testing.T, p *testutil.Project) []string {
	t.Helper()
	rows, err := p.Sess.QueryRows("SELECT id || '|' || amount || '|' || status FROM raw.orders ORDER BY id")
	if err != nil {
		t.Fatalf("query raw.orders: %v", err)
	}
	return rows
}

func TestCassette_FetchRecordReplay(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(clock.EnvNow, "2026-03-01T08:30:00Z")
	var hits atomic.Int32
	srv := ordersServer(t, &hits)

	// Record, with the key set.
	t.Setenv("E2E_ORDERS_KEY", cassetteAPIKey)
	setCassetteEnv(t, dir, "record")
	rec := testutil.NewProject(t)
	writeOrdersProject(t, rec, srv.URL)
	r1 := runModelWithSinkE2E(t, rec, "raw/orders.sql")
	if len(r1.Errors) > 0 {
		t.Fatalf("record run: %v", r1.Errors)
	}
	recorded := orderRows(t, rec)
	if len(recorded) != 5 {
		t.Fatalf("record: %d rows, want 5: %v", len(recorded), recorded)
	}

	path := filepath.Join(dir, "raw.orders", "orders_api.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("cassette not written: %v", err)
	}
	if strings.Contains(string(data), cassetteAPIKey) {
		t.Fatalf("cassette contains the API key:\n%s", data)
	}
	if n := strings.Count(string(data), `"method": "GET"`); n != 3 {
		t.Fatalf("cassette has %d GETs, want 3 pages:\n%s", n, data)
	}

	// Replay: server gone, key unset, fresh project.
	srv.Close()
	before := hits.Load()
	t.Setenv("E2E_ORDERS_KEY", "")
	setCassetteEnv(t, dir, "replay")
	rep := testutil.NewProject(t)
	writeOrdersProject(t, rep, srv.URL)
	r2 := runModelWithSinkE2E(t, rep, "raw/orders.sql")
	if len(r2.Errors) > 0 {
		t.Fatalf("replay run: %v", r2.Errors)
	}
	replayed := orderRows(t, rep)
	if strings.Join(recorded, "\n") != strings.Join(replayed, "\n") {
		t.Fatalf("recorded rows:\n%s\nreplayed rows:\n%s", strings.Join(recorded, "\n"), strings.Join(replayed, "\n"))
	}
	if hits.Load() != before {
		t.Fatalf("replay reached the server")
	}

	// A different frozen clock changes the since= date, so the request no
	// longer matches and the run fails naming it.
	t.Setenv(clock.EnvNow, "2026-04-01T08:30:00Z")
	setCassetteEnv(t, dir, "replay")
	other := testutil.NewProject(t)
	writeOrdersProject(t, other, srv.URL)
	r3 := runModelWithSinkE2E(t, other, "raw/orders.sql")
	msg := strings.Join(r3.Errors, "\n")
	if !strings.Contains(msg, "no recorded interaction for GET") || !strings.Contains(msg, "since=2026-03-25") || !strings.Contains(msg, "ONDATRA_NOW") {
		t.Fatalf("replay under another clock: errors = %q", msg)
	}
}

func TestCassette_PushRecordReplay(t *testing.T) {
	dir := t.TempDir()
	var hits atomic.Int32
	var received atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get("X-Api-Key") != cassetteAPIKey {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var rows []map[string]any
		if err := json.NewDecoder(r.Body).Decode(&rows); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		received.Add(int32(len(rows)))
		res := map[string]string{}
		for _, row := range rows {
			res[fmt.Sprint(row["id"])] = "created"
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"results": res}) // test server
	}))

	writePush := func(p *testutil.Project) {
		testutil.WriteFile(t, p.Dir, "lib/crm_push.star", fmt.Sprintf(`
API = {
    "base_url": %q,
    "auth": {"env": "E2E_CRM_KEY", "header": "X-Api-Key"},
    "push": {"batch_size": 2, "rate_limit": {"requests": 1, "per": "10s"}},
}

def push(rows):
    resp = http.post("/contacts", json=[{"id": r["id"], "name": r["name"]} for r in rows])
    if not resp.ok:
        fail("crm: HTTP %%d" %% resp.status_code)
    out = {}
    for r in rows:
        if resp.json["results"].get(str(int(r["id"]))) == "created":
            out[str(r["__ondatra_rowid"]) + ":" + r["__ondatra_change_type"]] = "ok"
    return out
`, srv.URL))
		p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
		p.Sess.Exec("CREATE TABLE raw.contacts AS SELECT * FROM (VALUES (1,'Ada'),(2,'Bo'),(3,'Cy')) AS t(id,name)")
		p.AddModel("sync/contacts.sql", `-- @kind: merge
-- @unique_key: id
-- @push: crm_push

SELECT id::BIGINT AS id, name::VARCHAR AS name FROM raw.contacts
`)
	}

	t.Setenv("E2E_CRM_KEY", cassetteAPIKey)
	setCassetteEnv(t, dir, "record")
	rec := testutil.NewProject(t)
	writePush(rec)
	r1 := runModelWithSink(t, rec, "sync/contacts.sql")
	if r1.SyncSucceeded != 3 || r1.SyncFailed != 0 || received.Load() != 3 {
		t.Fatalf("record: ok=%d failed=%d received=%d warnings=%v errors=%v", r1.SyncSucceeded, r1.SyncFailed, received.Load(), r1.Warnings, r1.Errors)
	}
	data, err := os.ReadFile(filepath.Join(dir, "sync.contacts", "crm_push.json"))
	if err != nil {
		t.Fatalf("cassette not written: %v", err)
	}
	if strings.Contains(string(data), cassetteAPIKey) {
		t.Fatalf("cassette contains the API key:\n%s", data)
	}
	if os.Getenv("CASSETTE_DUMP") != "" {
		t.Logf("%s", data)
	}

	srv.Close()
	before := hits.Load()
	t.Setenv("E2E_CRM_KEY", "")
	setCassetteEnv(t, dir, "replay")
	rep := testutil.NewProject(t)
	writePush(rep)
	start := time.Now()
	r2 := runModelWithSink(t, rep, "sync/contacts.sql")
	// Two batches under a 1-per-10s rate limit: recording waited 10s,
	// replay must not.
	if d := time.Since(start); d > 8*time.Second {
		t.Fatalf("replay took %v: rate_limit was not skipped", d)
	}
	if r2.SyncSucceeded != r1.SyncSucceeded || r2.SyncFailed != 0 {
		t.Fatalf("replay: ok=%d failed=%d warnings=%v errors=%v", r2.SyncSucceeded, r2.SyncFailed, r2.Warnings, r2.Errors)
	}
	if hits.Load() != before {
		t.Fatal("replay reached the server")
	}
}

// TestCassette_OffIsLive: with none of the variables set the same fetch
// blueprint talks to the server and nothing is written anywhere.
func TestCassette_OffIsLive(t *testing.T) {
	t.Setenv(clock.EnvNow, "2026-03-01T08:30:00Z") // the server checks the date
	setCassetteEnv(t, "", "")
	var hits atomic.Int32
	srv := ordersServer(t, &hits)
	defer srv.Close()
	t.Setenv("E2E_ORDERS_KEY", cassetteAPIKey)
	p := testutil.NewProject(t)
	writeOrdersProject(t, p, srv.URL)
	r := runModelWithSinkE2E(t, p, "raw/orders.sql")
	if len(r.Errors) > 0 {
		t.Fatalf("run: %v", r.Errors)
	}
	if got := len(orderRows(t, p)); got != 5 {
		t.Fatalf("%d rows, want 5", got)
	}
	if hits.Load() != 3 {
		t.Fatalf("server hits = %d, want 3", hits.Load())
	}
	if httpcassette.Current() != httpcassette.Off {
		t.Fatal("cassette mode not off")
	}
}
