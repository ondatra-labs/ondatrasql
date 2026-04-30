// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/odata"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// odataDeltaSetup creates a project with a tracked-kind model that we can
// mutate between runs to produce DuckLake snapshot history. Returns the
// httptest server URL and the project so the test can re-run the model
// with new data.
func odataDeltaSetup(t *testing.T) (string, *testutil.Project) {
	t.Helper()
	// Use a deterministic delta key so deltaLinks survive across handler
	// reconstructions in this test (we never reconstruct, but the env var
	// keeps the key stable in case future test changes do).
	t.Setenv("ONDATRA_ODATA_DELTA_KEY",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")

	p := testutil.NewProject(t)

	// Use a regular table model and mutate via direct SQL between snapshots.
	// Tracked or merge would also work, but direct SQL keeps the test
	// focused on @odata.deltaLink semantics rather than CDC particulars.
	p.AddModel("mart/widgets.sql", `-- @kind: table
-- @expose id
SELECT * FROM (VALUES
    (1, 'Apple',  10),
    (2, 'Banana', 20),
    (3, 'Cherry', 30)
) AS t(id, name, qty)
`)
	runModel(t, p, "mart/widgets.sql")

	schemas, err := odata.DiscoverSchemas(p.Sess, []odata.ExposeTarget{
		{Target: "mart.widgets", KeyColumn: "id"},
	})
	if err != nil {
		t.Fatalf("DiscoverSchemas: %v", err)
	}

	handler, err := odata.NewServer(p.Sess, schemas, "http://testhost")
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv := newTestServer(t, handler)
	t.Cleanup(func() { srv.Close() })

	return srv.URL, p
}

// rewriteToTestServer takes a deltaLink that the server emitted with the
// "http://testhost" baseURL and rewrites it to use the actual httptest
// listener URL so the test can issue the request.
func rewriteToTestServer(deltaLink, srvURL string) string {
	if !strings.HasPrefix(deltaLink, "http://testhost") {
		return deltaLink
	}
	return srvURL + strings.TrimPrefix(deltaLink, "http://testhost")
}

// TestODataDelta_EmitsLinkAndDereferences pins the full delta cycle:
//  1. A normal collection request includes @odata.deltaLink.
//  2. Following the deltaLink immediately (no changes since) returns
//     an empty value array with a fresh deltaLink.
//  3. After mutating the underlying table (insert + update + delete),
//     following the deltaLink returns the new rows, the updated row,
//     and an @removed entry for the deletion.
func TestODataDelta_EmitsLinkAndDereferences(t *testing.T) {
	srvURL, p := odataDeltaSetup(t)

	// 1. Initial query — must include @odata.deltaLink.
	resp1 := odataGet(t, srvURL+"/odata/mart_widgets")
	var initial map[string]any
	if err := json.Unmarshal([]byte(resp1), &initial); err != nil {
		t.Fatalf("unmarshal initial: %v", err)
	}
	deltaLink, ok := initial["@odata.deltaLink"].(string)
	if !ok || deltaLink == "" {
		t.Fatalf("initial response missing @odata.deltaLink (got: %v)", initial["@odata.deltaLink"])
	}
	if values, _ := initial["value"].([]any); len(values) != 3 {
		t.Errorf("initial value count = %d, want 3", len(values))
	}

	// 2. Follow the deltaLink immediately. Snapshot is unchanged, so the
	// delta should be empty but still carry a fresh link.
	deltaURL := rewriteToTestServer(deltaLink, srvURL)
	resp2 := odataGet(t, deltaURL)
	var empty map[string]any
	if err := json.Unmarshal([]byte(resp2), &empty); err != nil {
		t.Fatalf("unmarshal empty delta: %v", err)
	}
	if values, _ := empty["value"].([]any); len(values) != 0 {
		t.Errorf("expected empty value array, got %d entries (body: %s)", len(values), resp2)
	}
	if _, ok := empty["@odata.deltaLink"].(string); !ok {
		t.Errorf("empty delta response missing fresh @odata.deltaLink")
	}
	if ctx, _ := empty["@odata.context"].(string); !strings.HasSuffix(ctx, "/$delta") {
		t.Errorf("delta context = %q, expected suffix /$delta", ctx)
	}

	// 3. Mutate the underlying table to produce real changes.
	//    - INSERT id=4
	//    - UPDATE id=2 (qty 20 -> 200)
	//    - DELETE id=3
	if err := p.Sess.Exec("INSERT INTO mart.widgets VALUES (4, 'Date', 40)"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := p.Sess.Exec("UPDATE mart.widgets SET qty = 200 WHERE id = 2"); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := p.Sess.Exec("DELETE FROM mart.widgets WHERE id = 3"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Follow the (still-original) deltaLink — it should now report the
	// changes between the original snapshot and the current one.
	resp3 := odataGet(t, deltaURL)
	var delta map[string]any
	if err := json.Unmarshal([]byte(resp3), &delta); err != nil {
		t.Fatalf("unmarshal delta: %v", err)
	}
	values, _ := delta["value"].([]any)

	var inserts, updates, removes int
	for _, v := range values {
		row, ok := v.(map[string]any)
		if !ok {
			continue
		}
		if _, isRemoved := row["@removed"]; isRemoved {
			removes++
			// Removed rows must carry @odata.id so clients can match
			// against their local cache.
			if id, _ := row["@odata.id"].(string); !strings.Contains(id, "mart_widgets(3)") {
				t.Errorf("@removed row missing or wrong @odata.id: %v", row)
			}
			continue
		}
		// Distinguish new from updated by primary key — id=4 is the new
		// row, id=2 is the updated one. Both look like normal entities
		// in OData delta format.
		switch fmt.Sprintf("%v", row["id"]) {
		case "4":
			inserts++
			if name, _ := row["name"].(string); name != "Date" {
				t.Errorf("insert row name = %q, want Date", name)
			}
		case "2":
			updates++
			if qty := fmt.Sprintf("%v", row["qty"]); qty != "200" {
				t.Errorf("update row qty = %q, want 200", qty)
			}
		}
	}
	if inserts != 1 {
		t.Errorf("inserts = %d, want 1 (body: %s)", inserts, resp3)
	}
	if updates != 1 {
		t.Errorf("updates = %d, want 1 (body: %s)", updates, resp3)
	}
	if removes != 1 {
		t.Errorf("removes = %d, want 1 (body: %s)", removes, resp3)
	}
}

// TestODataDelta_RejectsTokenWithChangedFilter pins that deltaLink's
// filterHash check refuses follow-ups that have different query options
// from the original. Otherwise a client could swap $filter in mid-stream
// and get an inconsistent change set.
func TestODataDelta_RejectsTokenWithChangedFilter(t *testing.T) {
	srvURL, _ := odataDeltaSetup(t)

	// Original query has no $filter — capture the deltaLink.
	resp := odataGet(t, srvURL+"/odata/mart_widgets")
	var body map[string]any
	json.Unmarshal([]byte(resp), &body)
	deltaLink, _ := body["@odata.deltaLink"].(string)
	if deltaLink == "" {
		t.Fatal("setup: expected @odata.deltaLink on initial unfiltered query")
	}

	// Follow the deltaLink WITH a $filter that wasn't on the original.
	tampered := rewriteToTestServer(deltaLink, srvURL) + "&$filter=qty%20gt%20100"
	httpResp, err := http.Get(tampered)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode != 410 {
		t.Errorf("expected 410 on filter mismatch, got %d", httpResp.StatusCode)
	}
	bodyBytes, _ := io.ReadAll(httpResp.Body)
	if !strings.Contains(string(bodyBytes), "DeltaLinkFilterChanged") &&
		!strings.Contains(string(bodyBytes), "BadRequest") {
		t.Errorf("error body should mention filter change or bad request: %s", bodyBytes)
	}
}

// TestODataDelta_NoLinkForFilteredQueries pins the documented v0.26.0
// limitation: queries with $filter, $apply, $compute, $search, or
// $expand do not get a deltaLink, because we can't apply those options
// to the table_changes() result without a more involved design.
func TestODataDelta_NoLinkForFilteredQueries(t *testing.T) {
	srvURL, _ := odataDeltaSetup(t)

	// $filter — should not get a deltaLink.
	resp := odataGet(t, srvURL+"/odata/mart_widgets?$filter=qty%20gt%2015")
	var body map[string]any
	json.Unmarshal([]byte(resp), &body)
	if link, ok := body["@odata.deltaLink"]; ok {
		t.Errorf("expected no @odata.deltaLink with $filter, got %v", link)
	}
}

// TestODataDelta_ConcurrentRequestsAreSerialised pins that the delta-handler
// mutex serialises SET search_path + table_changes() + RESET across
// concurrent HTTP requests sharing the OData server's session. Without
// the mutex, two delta requests could race and run their SELECT against
// the wrong search_path (cross-schema data leak in multi-schema deployments).
//
// Run with `-race` to catch any unprotected data access in addition to
// the correctness check.
func TestODataDelta_ConcurrentRequestsAreSerialised(t *testing.T) {
	srvURL, p := odataDeltaSetup(t)

	// Capture the deltaLink once.
	resp := odataGet(t, srvURL+"/odata/mart_widgets")
	var body map[string]any
	json.Unmarshal([]byte(resp), &body)
	deltaLink, _ := body["@odata.deltaLink"].(string)
	if deltaLink == "" {
		t.Fatal("setup: no deltaLink")
	}
	deltaURL := rewriteToTestServer(deltaLink, srvURL)

	// Mutate to produce real changes that the deltaLink should report.
	if err := p.Sess.Exec("INSERT INTO mart.widgets VALUES (4, 'Date', 40)"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Fire many concurrent delta requests. Without the mutex, the
	// race detector almost always trips because runTableChanges
	// mutates session state (search_path) outside the per-statement
	// mutex DuckDB Session provides.
	const concurrency = 16
	errCh := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			httpResp, err := http.Get(deltaURL)
			if err != nil {
				errCh <- err
				return
			}
			defer httpResp.Body.Close()
			if httpResp.StatusCode != 200 {
				errCh <- fmt.Errorf("status %d", httpResp.StatusCode)
				return
			}
			body, _ := io.ReadAll(httpResp.Body)
			var resp map[string]any
			if err := json.Unmarshal(body, &resp); err != nil {
				errCh <- err
				return
			}
			values, _ := resp["value"].([]any)
			// Each response must contain exactly the insert we just did.
			// Counting matters: a path-race would surface as either no rows
			// (search_path pointed elsewhere) or a different table's rows.
			if len(values) != 1 {
				errCh <- fmt.Errorf("expected 1 change row, got %d (body: %s)", len(values), body)
				return
			}
			errCh <- nil
		}()
	}
	for i := 0; i < concurrency; i++ {
		if err := <-errCh; err != nil {
			t.Errorf("concurrent delta request: %v", err)
		}
	}
}

// TestODataDelta_RespectsSelectProjection pins that the delta response
// honors the original $select. The deltaLink carries $select forward and
// the token's filterHash binds it, so the follow-up rows must be
// projected to the same columns. Otherwise a client that asked for
// {id,name} would see {id,name,qty} on delta — different shape than the
// original collection response.
func TestODataDelta_RespectsSelectProjection(t *testing.T) {
	srvURL, p := odataDeltaSetup(t)

	// Initial query with $select=id,name — capture the deltaLink.
	resp := odataGet(t, srvURL+"/odata/mart_widgets?$select=id,name")
	var body map[string]any
	json.Unmarshal([]byte(resp), &body)
	deltaLink, _ := body["@odata.deltaLink"].(string)
	if deltaLink == "" {
		t.Fatal("setup: no deltaLink on $select query")
	}
	// The link must carry $select forward — that's what makes the
	// hash check round-trip.
	if !strings.Contains(deltaLink, "%24select=id%2Cname") &&
		!strings.Contains(deltaLink, "$select=id,name") {
		t.Errorf("deltaLink missing $select: %s", deltaLink)
	}

	// Mutate the table — insert a row with all three columns.
	if err := p.Sess.Exec("INSERT INTO mart.widgets VALUES (4, 'Date', 40)"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Follow the deltaLink. Insert row should expose only id and name —
	// not qty, even though qty exists in the underlying table.
	deltaResp := odataGet(t, rewriteToTestServer(deltaLink, srvURL))
	var delta map[string]any
	if err := json.Unmarshal([]byte(deltaResp), &delta); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	values, _ := delta["value"].([]any)
	if len(values) != 1 {
		t.Fatalf("expected 1 change row, got %d (body: %s)", len(values), deltaResp)
	}
	row, _ := values[0].(map[string]any)
	if _, hasQty := row["qty"]; hasQty {
		t.Errorf("delta row leaked qty despite $select=id,name (row: %v)", row)
	}
	if _, hasName := row["name"]; !hasName {
		t.Errorf("delta row missing name (row: %v)", row)
	}
	if _, hasID := row["id"]; !hasID {
		t.Errorf("delta row missing id (row: %v)", row)
	}
}

// TestODataDelta_MaxAgeReturnsExpired pins the end-to-end max-age path:
// boot a server with ONDATRA_ODATA_DELTA_MAX_AGE=1ms, issue a deltaLink,
// wait, and verify the next call returns 410 DeltaLinkExpired (not
// DeltaLinkInvalid). Catches regressions where the env wiring or HTTP
// classification breaks even though the unit-level decode works.
func TestODataDelta_MaxAgeReturnsExpired(t *testing.T) {
	t.Setenv("ONDATRA_ODATA_DELTA_KEY",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	t.Setenv("ONDATRA_ODATA_DELTA_MAX_AGE", "1ms")
	srvURL, _ := odataDeltaSetup(t)

	resp := odataGet(t, srvURL+"/odata/mart_widgets")
	var body map[string]any
	json.Unmarshal([]byte(resp), &body)
	deltaLink, _ := body["@odata.deltaLink"].(string)
	if deltaLink == "" {
		t.Fatal("setup: expected @odata.deltaLink on initial response")
	}
	deltaURL := rewriteToTestServer(deltaLink, srvURL)

	// Wait past the configured max-age.
	time.Sleep(50 * time.Millisecond)

	httpResp, err := http.Get(deltaURL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode != 410 {
		t.Errorf("expected 410 on expired token, got %d", httpResp.StatusCode)
	}
	bodyBytes, _ := io.ReadAll(httpResp.Body)
	if !strings.Contains(string(bodyBytes), "DeltaLinkExpired") {
		t.Errorf("expected DeltaLinkExpired error code, got body: %s", bodyBytes)
	}
}

// TestODataDelta_KidRotationE2E pins the rotation contract through HTTP:
// 1. Server boots with keyset (k1 only) and issues a deltaLink.
// 2. We mint a new server with keyset (k2,k1) — same project, fresh
//    handler — and verify the old k1-signed deltaLink still works.
// This catches regressions where a server restart with a rotated env
// silently invalidates outstanding tokens.
//
// Built without odataDeltaSetup because that helper overwrites
// ONDATRA_ODATA_DELTA_KEY with its own value, defeating the rotation we
// want to exercise.
func TestODataDelta_KidRotationE2E(t *testing.T) {
	const k1 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	const k2 = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"

	p := testutil.NewProject(t)
	p.AddModel("mart/widgets.sql", `-- @kind: table
-- @expose id
SELECT * FROM (VALUES (1, 'A'), (2, 'B')) AS t(id, name)
`)
	runModel(t, p, "mart/widgets.sql")
	schemas, err := odata.DiscoverSchemas(p.Sess, []odata.ExposeTarget{
		{Target: "mart.widgets", KeyColumn: "id"},
	})
	if err != nil {
		t.Fatalf("DiscoverSchemas: %v", err)
	}

	// Step 1: server with k1 only, capture a deltaLink.
	t.Setenv("ONDATRA_ODATA_DELTA_KEY", "k1:"+k1)
	handler1, err := odata.NewServer(p.Sess, schemas, "http://testhost")
	if err != nil {
		t.Fatalf("NewServer (k1): %v", err)
	}
	srv1 := newTestServer(t, handler1)
	t.Cleanup(func() { srv1.Close() })

	resp := odataGet(t, srv1.URL+"/odata/mart_widgets")
	var body map[string]any
	json.Unmarshal([]byte(resp), &body)
	deltaLink, _ := body["@odata.deltaLink"].(string)
	if deltaLink == "" {
		t.Fatal("setup: expected @odata.deltaLink on initial response")
	}

	// Step 2: rotate. Build a fresh handler on the same project with
	// keyset (k2,k1). New tokens sign with k2; old tokens (signed under
	// k1) must still verify.
	//
	// CRITICAL: each NewServer call re-reads the env, so the order is
	// (set k1) -> NewServer -> (set k2,k1) -> NewServer.
	t.Setenv("ONDATRA_ODATA_DELTA_KEY", "k2:"+k2+",k1:"+k1)
	handler2, err := odata.NewServer(p.Sess, schemas, "http://testhost")
	if err != nil {
		t.Fatalf("NewServer (rotated): %v", err)
	}
	srv2 := newTestServer(t, handler2)
	t.Cleanup(func() { srv2.Close() })

	// Replay the original deltaLink against the rotated server.
	rotatedURL := strings.Replace(deltaLink, "http://testhost", srv2.URL, 1)
	httpResp, err := http.Get(rotatedURL)
	if err != nil {
		t.Fatalf("get rotated: %v", err)
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode != 200 {
		bodyBytes, _ := io.ReadAll(httpResp.Body)
		t.Errorf("k1 token should verify under rotated keyset; got %d body=%s",
			httpResp.StatusCode, bodyBytes)
	}
}

// TestODataDelta_MaxAgeBadEnvFailsClosed pins that an unparseable
// ONDATRA_ODATA_DELTA_MAX_AGE refuses to construct the server rather
// than silently disabling expiry. Operators who typoed the env are
// asking for security; running without it is a regression.
func TestODataDelta_MaxAgeBadEnvFailsClosed(t *testing.T) {
	t.Setenv("ONDATRA_ODATA_DELTA_KEY",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	t.Setenv("ONDATRA_ODATA_DELTA_MAX_AGE", "garbage")

	p := testutil.NewProject(t)
	p.AddModel("mart/widgets.sql", `-- @kind: table
-- @expose id
SELECT * FROM (VALUES (1, 'A')) AS t(id, name)
`)
	runModel(t, p, "mart/widgets.sql")

	schemas, err := odata.DiscoverSchemas(p.Sess, []odata.ExposeTarget{
		{Target: "mart.widgets", KeyColumn: "id"},
	})
	if err != nil {
		t.Fatalf("DiscoverSchemas: %v", err)
	}
	_, err = odata.NewServer(p.Sess, schemas, "http://testhost")
	if err == nil {
		t.Fatal("NewServer should refuse to start with unparseable MAX_AGE; got nil error")
	}
	if !strings.Contains(err.Error(), "ONDATRA_ODATA_DELTA_MAX_AGE") {
		t.Errorf("error should mention the env var, got: %v", err)
	}
}

// TestODataServer_PoolSizeBadEnvFailsClosed pins that an unparseable
// ONDATRA_ODATA_POOL_SIZE refuses to construct the server, mirroring
// the MAX_AGE fail-closed contract. Catches regressions where the env
// is silently ignored.
func TestODataServer_PoolSizeBadEnvFailsClosed(t *testing.T) {
	t.Setenv("ONDATRA_ODATA_POOL_SIZE", "garbage")

	p := testutil.NewProject(t)
	p.AddModel("mart/widgets.sql", `-- @kind: table
-- @expose id
SELECT * FROM (VALUES (1, 'A')) AS t(id, name)
`)
	runModel(t, p, "mart/widgets.sql")

	schemas, err := odata.DiscoverSchemas(p.Sess, []odata.ExposeTarget{
		{Target: "mart.widgets", KeyColumn: "id"},
	})
	if err != nil {
		t.Fatalf("DiscoverSchemas: %v", err)
	}
	_, err = odata.NewServer(p.Sess, schemas, "http://testhost")
	if err == nil {
		t.Fatal("NewServer should refuse to start with unparseable POOL_SIZE; got nil error")
	}
	if !strings.Contains(err.Error(), "ONDATRA_ODATA_POOL_SIZE") {
		t.Errorf("error should mention env var, got: %v", err)
	}
}

// TestODataDelta_RejectsTamperedToken pins that mutating a deltaLink token
// returns 410 (signature mismatch) rather than silently accepting it.
func TestODataDelta_RejectsTamperedToken(t *testing.T) {
	srvURL, _ := odataDeltaSetup(t)

	resp := odataGet(t, srvURL+"/odata/mart_widgets")
	var body map[string]any
	json.Unmarshal([]byte(resp), &body)
	deltaLink, _ := body["@odata.deltaLink"].(string)
	deltaURL := rewriteToTestServer(deltaLink, srvURL)

	// Mutate one character of the token. Parse the URL, swap a token char,
	// re-encode.
	u, _ := url.Parse(deltaURL)
	q := u.Query()
	tok := q.Get("$deltatoken")
	if tok == "" {
		t.Fatal("setup: empty $deltatoken")
	}
	// Flip the first character of the signature half (after the dot).
	dot := strings.LastIndex(tok, ".")
	tampered := tok[:dot+1] + string("X"[0]) + tok[dot+2:]
	q.Set("$deltatoken", tampered)
	u.RawQuery = q.Encode()

	httpResp, err := http.Get(u.String())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode != 410 {
		t.Errorf("expected 410 on tampered token, got %d", httpResp.StatusCode)
	}
}

