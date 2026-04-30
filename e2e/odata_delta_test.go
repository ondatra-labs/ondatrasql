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
			// Spec (JSON Format §15) — tombstones carry exactly two
			// fields: @removed and @odata.id (or v4.01 short-form @id,
			// which we don't emit yet to stay consistent with the rest
			// of our long-form annotation set). Earlier drafts also
			// injected the bare key column; that was non-spec. Pin the
			// "exactly two fields" shape so a regression re-adding
			// extra fields fails this test.
			if len(row) != 2 {
				t.Errorf("@removed tombstone should have exactly 2 fields (@removed, @odata.id), got %d: %v",
					len(row), row)
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

// TestODataSkipToken_Pagination pins server-driven paging end-to-end:
// boot a server with PAGE_SIZE=5, query a 12-row collection, walk the
// nextLink chain, verify all rows arrive in stable order with no
// duplicates and no skips.
func TestODataSkipToken_Pagination(t *testing.T) {
	t.Setenv("ONDATRA_ODATA_DELTA_KEY",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	t.Setenv("ONDATRA_ODATA_PAGE_SIZE", "5")

	p := testutil.NewProject(t)
	p.AddModel("mart/widgets.sql", `-- @kind: table
-- @expose id
SELECT id::BIGINT AS id, name::VARCHAR AS name FROM (VALUES
    (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), (5, 'E'),
    (6, 'F'), (7, 'G'), (8, 'H'), (9, 'I'), (10, 'J'),
    (11, 'K'), (12, 'L')
) AS t(id, name)
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

	collected := make([]int, 0, 12)
	// Start with $count=true so we can also pin the
	// "@odata.count is first-page-only" invariant: count present on
	// page 1, absent on page 2+, even though the original request
	// asked for count.
	url := srv.URL + "/odata/mart_widgets?$orderby=id&$count=true"
	pageCount := 0
	for url != "" {
		pageCount++
		if pageCount > 5 {
			t.Fatalf("too many pages — paging not terminating; collected so far: %v", collected)
		}
		body := odataGet(t, url)
		var resp map[string]any
		if err := json.Unmarshal([]byte(body), &resp); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		values, _ := resp["value"].([]any)
		for _, v := range values {
			row, _ := v.(map[string]any)
			id, _ := row["id"].(float64)
			collected = append(collected, int(id))
		}

		// Page-aware annotation invariants:
		//   1. @odata.count: present on first page (with the original
		//      $count=true), absent on follow-up pages — server
		//      drops $count from emitted nextLink to avoid recomputing
		//      COUNT(*) per page.
		//   2. @odata.deltaLink and @odata.nextLink are mutually
		//      exclusive within one response. deltaLink describes
		//      "track changes from this snapshot"; nextLink describes
		//      "rest of this page chain". Emitting both would invite
		//      confusion about which to poll.
		_, hasCount := resp["@odata.count"]
		_, hasNext := resp["@odata.nextLink"]
		_, hasDelta := resp["@odata.deltaLink"]
		if pageCount == 1 {
			if !hasCount {
				t.Errorf("page 1 missing @odata.count despite $count=true: %s", body)
			}
			if cnt, _ := resp["@odata.count"].(float64); int(cnt) != 12 {
				t.Errorf("page 1 @odata.count = %v, want 12", resp["@odata.count"])
			}
		} else {
			if hasCount {
				t.Errorf("page %d unexpectedly carries @odata.count = %v (should be first-page-only)",
					pageCount, resp["@odata.count"])
			}
		}
		if hasNext && hasDelta {
			t.Errorf("page %d emits BOTH @odata.nextLink and @odata.deltaLink — they must be mutually exclusive",
				pageCount)
		}

		next, _ := resp["@odata.nextLink"].(string)
		if next == "" {
			url = ""
			break
		}
		url = strings.Replace(next, "http://testhost", srv.URL, 1)
	}

	if len(collected) != 12 {
		t.Fatalf("collected %d rows, want 12: %v", len(collected), collected)
	}
	for i, id := range collected {
		if id != i+1 {
			t.Errorf("row %d = %d, want %d (paging produced gaps or duplicates: %v)",
				i, id, i+1, collected)
			break
		}
	}
	if pageCount < 3 {
		t.Errorf("expected at least 3 pages with PAGE_SIZE=5 and 12 rows, got %d", pageCount)
	}
}

// TestODataSkipToken_ClientTopSuppressesNextLink pins that a client $top
// smaller than PAGE_SIZE is honored as a hard cap and no nextLink is
// emitted. Without this, a `$top=3` request against a 12-row collection
// would return 3 rows + a nextLink to the rest, which contradicts the
// client's "I want at most 3" intent.
func TestODataSkipToken_ClientTopSuppressesNextLink(t *testing.T) {
	t.Setenv("ONDATRA_ODATA_DELTA_KEY",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	t.Setenv("ONDATRA_ODATA_PAGE_SIZE", "10")

	p := testutil.NewProject(t)
	p.AddModel("mart/widgets.sql", `-- @kind: table
-- @expose id
SELECT id::BIGINT AS id, name::VARCHAR AS name FROM (VALUES
    (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), (5, 'E'),
    (6, 'F'), (7, 'G'), (8, 'H'), (9, 'I'), (10, 'J'),
    (11, 'K'), (12, 'L')
) AS t(id, name)
`)
	runModel(t, p, "mart/widgets.sql")
	schemas, _ := odata.DiscoverSchemas(p.Sess, []odata.ExposeTarget{
		{Target: "mart.widgets", KeyColumn: "id"},
	})
	handler, err := odata.NewServer(p.Sess, schemas, "http://testhost")
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv := newTestServer(t, handler)
	t.Cleanup(func() { srv.Close() })

	body := odataGet(t, srv.URL+"/odata/mart_widgets?$top=3&$orderby=id")
	var resp map[string]any
	json.Unmarshal([]byte(body), &resp)

	values, _ := resp["value"].([]any)
	if len(values) != 3 {
		t.Errorf("got %d rows, want 3", len(values))
	}
	if next, ok := resp["@odata.nextLink"].(string); ok && next != "" {
		t.Errorf("expected no @odata.nextLink (client cap respected), got %q", next)
	}
}

// TestODataSkipToken_FilterChangedRejected pins that swapping query
// options between the original page and a nextLink follow-up returns
// 410 SkipTokenFilterChanged. Mirrors the delta-token contract.
func TestODataSkipToken_FilterChangedRejected(t *testing.T) {
	t.Setenv("ONDATRA_ODATA_DELTA_KEY",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	t.Setenv("ONDATRA_ODATA_PAGE_SIZE", "3")

	p := testutil.NewProject(t)
	p.AddModel("mart/widgets.sql", `-- @kind: table
-- @expose id
SELECT id::BIGINT AS id, name::VARCHAR AS name FROM (VALUES
    (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), (5, 'E')
) AS t(id, name)
`)
	runModel(t, p, "mart/widgets.sql")
	schemas, _ := odata.DiscoverSchemas(p.Sess, []odata.ExposeTarget{
		{Target: "mart.widgets", KeyColumn: "id"},
	})
	handler, err := odata.NewServer(p.Sess, schemas, "http://testhost")
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv := newTestServer(t, handler)
	t.Cleanup(func() { srv.Close() })

	// Page 1 — capture nextLink.
	body := odataGet(t, srv.URL+"/odata/mart_widgets?$orderby=id")
	var resp map[string]any
	json.Unmarshal([]byte(body), &resp)
	next, _ := resp["@odata.nextLink"].(string)
	if next == "" {
		t.Fatal("expected nextLink on first page (5 rows, page size 3)")
	}

	// Tamper: add a $select that wasn't on the original query — the
	// filter hash includes $select, so any change here breaks the hash
	// check. Appending another $orderby would NOT be a change since
	// url.Values.Get returns the first value (the original $orderby
	// remains active).
	tampered := strings.Replace(next, "http://testhost", srv.URL, 1) + "&$select=id"
	httpResp, err := http.Get(tampered)
	if err != nil {
		t.Fatalf("get tampered: %v", err)
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode != 410 {
		t.Errorf("expected 410, got %d", httpResp.StatusCode)
	}
	bodyBytes, _ := io.ReadAll(httpResp.Body)
	if !strings.Contains(string(bodyBytes), "SkipTokenFilterChanged") {
		t.Errorf("expected SkipTokenFilterChanged, got body: %s", bodyBytes)
	}
}

// TestODataCrossJoin_BasicEqualityJoin pins the main use case: client
// joins two exposed entities via $filter qualified refs. Without
// $crossjoin this would require the operator to predefine a JOIN model
// and re-expose it — $crossjoin lets clients do ad-hoc joins between
// any two exposed tables.
func TestODataCrossJoin_BasicEqualityJoin(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("raw/customers.sql", `-- @kind: table
-- @expose id
SELECT id::BIGINT AS id, name::VARCHAR AS name FROM (VALUES
    (1, 'Alice'), (2, 'Bob'), (3, 'Carol')
) AS t(id, name)
`)
	p.AddModel("raw/orders.sql", `-- @kind: table
-- @expose id
SELECT id::BIGINT AS id, customer_id::BIGINT AS customer_id, amount::BIGINT AS amount FROM (VALUES
    (101, 1, 50), (102, 2, 75), (103, 1, 30), (104, 99, 999)
) AS t(id, customer_id, amount)
`)
	runModel(t, p, "raw/customers.sql")
	runModel(t, p, "raw/orders.sql")
	schemas, _ := odata.DiscoverSchemas(p.Sess, []odata.ExposeTarget{
		{Target: "raw.customers", KeyColumn: "id"},
		{Target: "raw.orders", KeyColumn: "id"},
	})
	handler, err := odata.NewServer(p.Sess, schemas, "http://testhost")
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv := newTestServer(t, handler)
	t.Cleanup(func() { srv.Close() })

	// Equality-join: orders matched against customers via FK.
	body := odataGet(t, srv.URL+"/odata/$crossjoin(raw_customers,raw_orders)?"+
		url.QueryEscape("$filter")+"="+url.QueryEscape("raw_customers/id eq raw_orders/customer_id"))
	var resp map[string]any
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("unmarshal: %v (body: %s)", err, body)
	}
	values, _ := resp["value"].([]any)
	if len(values) != 3 {
		t.Errorf("expected 3 matched rows (orders 101,102,103; orphan 104 excluded), got %d (body: %s)",
			len(values), body)
	}
	for _, v := range values {
		row, _ := v.(map[string]any)
		cust, _ := row["raw_customers"].(map[string]any)
		ord, _ := row["raw_orders"].(map[string]any)
		if cust["id"] != ord["customer_id"] {
			t.Errorf("join key mismatch: customers.id=%v, orders.customer_id=%v",
				cust["id"], ord["customer_id"])
		}
	}
}

// TestODataCrossJoin_RejectsUnknownEntity pins that bad entity-set
// names return 404 (not 500 or silent empty result).
func TestODataCrossJoin_RejectsUnknownEntity(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("raw/a.sql", `-- @kind: table
-- @expose id
SELECT id::BIGINT AS id FROM (VALUES (1)) AS t(id)
`)
	runModel(t, p, "raw/a.sql")
	schemas, _ := odata.DiscoverSchemas(p.Sess, []odata.ExposeTarget{
		{Target: "raw.a", KeyColumn: "id"},
	})
	handler, _ := odata.NewServer(p.Sess, schemas, "http://testhost")
	srv := newTestServer(t, handler)
	t.Cleanup(func() { srv.Close() })

	httpResp, err := http.Get(srv.URL + "/odata/$crossjoin(raw_a,nope)")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode != 404 {
		t.Errorf("status = %d, want 404", httpResp.StatusCode)
	}
}

// TestODataCrossJoin_RejectsTooFewEntities pins that a single-entity
// $crossjoin (which is degenerate — just a SELECT from one table)
// returns 400.
func TestODataCrossJoin_RejectsTooFewEntities(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("raw/a.sql", `-- @kind: table
-- @expose id
SELECT id::BIGINT AS id FROM (VALUES (1)) AS t(id)
`)
	runModel(t, p, "raw/a.sql")
	schemas, _ := odata.DiscoverSchemas(p.Sess, []odata.ExposeTarget{
		{Target: "raw.a", KeyColumn: "id"},
	})
	handler, _ := odata.NewServer(p.Sess, schemas, "http://testhost")
	srv := newTestServer(t, handler)
	t.Cleanup(func() { srv.Close() })

	httpResp, err := http.Get(srv.URL + "/odata/$crossjoin(raw_a)")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode != 400 {
		t.Errorf("status = %d, want 400 for single-entity $crossjoin", httpResp.StatusCode)
	}
}

// TestODataBatch_RejectsAtomicityGroup pins JSON Format §19.1.4 — the
// atomicityGroup field requires transactional rollback semantics that
// don't apply to a read-only server. We reject with 501 Not Implemented
// so clients see the contract mismatch rather than silently processing
// the request as if the group didn't exist.
func TestODataBatch_RejectsAtomicityGroup(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("mart/widgets.sql", `-- @kind: table
-- @expose id
SELECT id::BIGINT AS id, name::VARCHAR AS name FROM (VALUES (1,'A')) AS t(id, name)
`)
	runModel(t, p, "mart/widgets.sql")
	schemas, _ := odata.DiscoverSchemas(p.Sess, []odata.ExposeTarget{
		{Target: "mart.widgets", KeyColumn: "id"},
	})
	handler, _ := odata.NewServer(p.Sess, schemas, "http://testhost")
	srv := newTestServer(t, handler)
	t.Cleanup(func() { srv.Close() })

	body := `{"requests":[{"id":"1","method":"GET","url":"mart_widgets","atomicityGroup":"g1"}]}`
	httpResp, err := http.Post(srv.URL+"/odata/$batch", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer httpResp.Body.Close()
	respBytes, _ := io.ReadAll(httpResp.Body)
	if !strings.Contains(string(respBytes), `"status":501`) {
		t.Errorf("expected sub-request status 501 for atomicityGroup, got: %s", respBytes)
	}
	if !strings.Contains(string(respBytes), "atomicityGroup is not supported") {
		t.Errorf("expected explanatory error message, got: %s", respBytes)
	}
}

// TestODataSkipToken_RejectsTamperedSignature pins the HTTP plumbing
// of skip-token integrity. Mirror of TestODataDelta_RejectsTamperedToken
// — unit tests already cover the algorithm in skiptoken_test.go, this
// confirms wiring through the full handler returns 400 SkipTokenInvalid.
func TestODataSkipToken_RejectsTamperedSignature(t *testing.T) {
	t.Setenv("ONDATRA_ODATA_DELTA_KEY",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	t.Setenv("ONDATRA_ODATA_PAGE_SIZE", "3")

	p := testutil.NewProject(t)
	p.AddModel("mart/widgets.sql", `-- @kind: table
-- @expose id
SELECT id::BIGINT AS id, name::VARCHAR AS name FROM (VALUES
    (1,'A'),(2,'B'),(3,'C'),(4,'D'),(5,'E')
) AS t(id, name)
`)
	runModel(t, p, "mart/widgets.sql")
	schemas, _ := odata.DiscoverSchemas(p.Sess, []odata.ExposeTarget{
		{Target: "mart.widgets", KeyColumn: "id"},
	})
	handler, _ := odata.NewServer(p.Sess, schemas, "http://testhost")
	srv := newTestServer(t, handler)
	t.Cleanup(func() { srv.Close() })

	// Capture a real nextLink.
	body := odataGet(t, srv.URL+"/odata/mart_widgets?$orderby=id")
	var resp map[string]any
	json.Unmarshal([]byte(body), &resp)
	next, _ := resp["@odata.nextLink"].(string)
	if next == "" {
		t.Fatal("setup: expected nextLink on first page")
	}

	// Mutate the signature half of the token (after the ".").
	u, _ := url.Parse(strings.Replace(next, "http://testhost", srv.URL, 1))
	q := u.Query()
	tok := q.Get("$skiptoken")
	dot := strings.LastIndex(tok, ".")
	if dot < 0 {
		t.Fatal("malformed skiptoken in emitted nextLink")
	}
	tampered := tok[:dot+1] + "X" + tok[dot+2:]
	q.Set("$skiptoken", tampered)
	u.RawQuery = q.Encode()

	httpResp, err := http.Get(u.String())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer httpResp.Body.Close()
	// Spec Part 1 §11.4 — tampered tokens are 400 (client error), not
	// 410 (gone); 410 is reserved for snapshot-no-longer-available
	// or filter-changed cases.
	if httpResp.StatusCode != 400 {
		t.Errorf("expected 400 on tampered skiptoken, got %d", httpResp.StatusCode)
	}
	bodyBytes, _ := io.ReadAll(httpResp.Body)
	if !strings.Contains(string(bodyBytes), "SkipTokenInvalid") {
		t.Errorf("expected SkipTokenInvalid error code, got: %s", bodyBytes)
	}
}

// TestODataIndex_PositionalAccess pins /odata/Entity/N returns the row
// at that position in the default key ordering. Per OData Part 2 §4.10
// the canonical form is a path-segment ordinal, not the parenthesised
// `Entity($index=N)` we briefly used pre-release.
func TestODataIndex_PositionalAccess(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("mart/widgets.sql", `-- @kind: table
-- @expose id
SELECT id::BIGINT AS id, name::VARCHAR AS name FROM (VALUES
    (10, 'A'), (20, 'B'), (30, 'C'), (40, 'D')
) AS t(id, name)
`)
	runModel(t, p, "mart/widgets.sql")
	schemas, _ := odata.DiscoverSchemas(p.Sess, []odata.ExposeTarget{
		{Target: "mart.widgets", KeyColumn: "id"},
	})
	handler, err := odata.NewServer(p.Sess, schemas, "http://testhost")
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv := newTestServer(t, handler)
	t.Cleanup(func() { srv.Close() })

	cases := []struct {
		ordinal  string
		wantID   float64
		wantHTTP int
	}{
		{"0", 10, 200}, // first row
		{"1", 20, 200},
		{"3", 40, 200}, // last row
		{"4", 0, 404},  // out of bounds
	}
	for _, tc := range cases {
		t.Run("ordinal_"+tc.ordinal, func(t *testing.T) {
			httpResp, err := http.Get(srv.URL + "/odata/mart_widgets/" + tc.ordinal)
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			defer httpResp.Body.Close()
			if httpResp.StatusCode != tc.wantHTTP {
				body, _ := io.ReadAll(httpResp.Body)
				t.Errorf("status = %d, want %d (body: %s)", httpResp.StatusCode, tc.wantHTTP, body)
				return
			}
			if tc.wantHTTP != 200 {
				return
			}
			body, _ := io.ReadAll(httpResp.Body)
			var resp map[string]any
			json.Unmarshal(body, &resp)
			id, _ := resp["id"].(float64)
			if id != tc.wantID {
				t.Errorf("id = %v, want %v", id, tc.wantID)
			}
		})
	}

	// Negative / non-numeric → 404 (path doesn't match the ordinal form).
	for _, bad := range []string{"-1", "abc"} {
		t.Run("invalid_"+bad, func(t *testing.T) {
			httpResp, err := http.Get(srv.URL + "/odata/mart_widgets/" + bad)
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			defer httpResp.Body.Close()
			if httpResp.StatusCode != 404 {
				body, _ := io.ReadAll(httpResp.Body)
				t.Errorf("status = %d, want 404 for /%s (body: %s)",
					httpResp.StatusCode, bad, body)
			}
		})
	}
}

// TestODataServer_PageSizeBadEnvFailsClosed pins that an unparseable
// ONDATRA_ODATA_PAGE_SIZE refuses to construct the server.
func TestODataServer_PageSizeBadEnvFailsClosed(t *testing.T) {
	t.Setenv("ONDATRA_ODATA_PAGE_SIZE", "garbage")
	p := testutil.NewProject(t)
	p.AddModel("mart/widgets.sql", `-- @kind: table
-- @expose id
SELECT * FROM (VALUES (1, 'A')) AS t(id, name)
`)
	runModel(t, p, "mart/widgets.sql")
	schemas, _ := odata.DiscoverSchemas(p.Sess, []odata.ExposeTarget{
		{Target: "mart.widgets", KeyColumn: "id"},
	})
	_, err := odata.NewServer(p.Sess, schemas, "http://testhost")
	if err == nil {
		t.Fatal("NewServer should refuse to start with unparseable PAGE_SIZE; got nil error")
	}
	if !strings.Contains(err.Error(), "ONDATRA_ODATA_PAGE_SIZE") {
		t.Errorf("error should mention env var, got: %v", err)
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
	// 400 Bad Request: tampered/invalid signature is a client error,
	// not a "resource gone" situation. Spec Part 1 §11.4 reserves 410
	// for snapshot-no-longer-available / expired tokens.
	if httpResp.StatusCode != 400 {
		t.Errorf("expected 400 on tampered token, got %d", httpResp.StatusCode)
	}
}

