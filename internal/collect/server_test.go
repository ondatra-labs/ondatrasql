// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package collect

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/parser"
)

func testModel() *parser.Model {
	return &parser.Model{
		Target: "raw.events",
		Kind:   "events",
		Columns: []parser.ColumnDef{
			{Name: "event_name", Type: "VARCHAR", NotNull: true},
			{Name: "page_url", Type: "VARCHAR"},
			{Name: "received_at", Type: "TIMESTAMPTZ"},
		},
	}
}

func TestServer_CollectAndClaim(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	model := testModel()
	srv := NewServer(store, []*parser.Model{model}, "0", "0")

	// POST /collect/raw/events
	body := `{"event_name":"test","page_url":"/home"}`
	req := httptest.NewRequest("POST", "/collect/raw/events", bytes.NewBufferString(body))
	req.SetPathValue("schema", "raw")
	req.SetPathValue("table", "events")
	w := httptest.NewRecorder()
	srv.handleCollect(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("collect: got %d, want 202. Body: %s", w.Code, w.Body.String())
	}

	// POST /flush/raw/events/claim
	claimReq := httptest.NewRequest("POST", "/flush/raw/events/claim?limit=100", nil)
	claimReq.SetPathValue("schema", "raw")
	claimReq.SetPathValue("table", "events")
	claimW := httptest.NewRecorder()
	srv.handleClaim(claimW, claimReq)

	if claimW.Code != http.StatusOK {
		t.Fatalf("claim: got %d, want 200", claimW.Code)
	}

	var resp struct {
		ClaimID string           `json:"claim_id"`
		Events  []map[string]any `json:"events"`
	}
	if err := json.Unmarshal(claimW.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal claim response: %v", err)
	}
	if len(resp.Events) != 1 {
		t.Fatalf("got %d events, want 1", len(resp.Events))
	}
	if resp.Events[0]["event_name"] != "test" {
		t.Fatalf("got event_name=%v, want test", resp.Events[0]["event_name"])
	}

	// Ack
	ackBody, _ := json.Marshal(map[string]string{"claim_id": resp.ClaimID})
	ackReq := httptest.NewRequest("POST", "/flush/raw/events/ack", bytes.NewBuffer(ackBody))
	ackReq.SetPathValue("schema", "raw")
	ackReq.SetPathValue("table", "events")
	ackW := httptest.NewRecorder()
	srv.handleAck(ackW, ackReq)

	if ackW.Code != http.StatusOK {
		t.Fatalf("ack: got %d, want 200", ackW.Code)
	}
}

// TestServer_ErrorResponse_AlwaysJSON pins R8 #12: every non-2xx
// response from the collect server is `application/json` with a
// `{"error": "..."}` envelope. Pre-fix /claim returned text/plain
// via http.Error on 4xx/5xx while emitting JSON on 2xx — clients
// had to branch on Content-Type to parse.
func TestServer_ErrorResponse_AlwaysJSON(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	model := testModel()
	srv := NewServer(store, []*parser.Model{model}, "0", "0")

	// Trigger 400 via invalid limit on /claim.
	req := httptest.NewRequest("POST", "/flush/raw/events/claim?limit=abc", nil)
	req.SetPathValue("schema", "raw")
	req.SetPathValue("table", "events")
	w := httptest.NewRecorder()
	srv.handleClaim(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
	if got := w.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("response body must be JSON, got %q: %v", w.Body.String(), err)
	}
	if body["error"] == "" {
		t.Errorf("response must have non-empty 'error' key, got: %v", body)
	}
}

// TestServer_HandleAck_RejectsCrossTarget pins R11 #6: an ack
// request whose URL path doesn't match the claim's actual target
// is rejected with 400. Pre-fix the path's schema/table was
// ignored, so a typoed dispatcher could ack another target's
// claim and silently succeed.
func TestServer_HandleAck_RejectsCrossTarget(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	rawEvents := testModel()
	auditEvents := &parser.Model{Target: "raw.audit", Kind: "events", Columns: rawEvents.Columns}
	srv := NewServer(store, []*parser.Model{rawEvents, auditEvents}, "0", "0")

	// Write + claim against raw.events.
	if err := store.Write("raw.events", map[string]any{"event_name": "x", "page_url": "/"}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := store.FlushWrites(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	claimID, _, err := store.Claim("raw.events", 100)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	// Ack against raw.audit (wrong target) — must 400.
	body, _ := json.Marshal(map[string]string{"claim_id": claimID})
	req := httptest.NewRequest("POST", "/flush/raw/audit/ack", bytes.NewReader(body))
	req.SetPathValue("schema", "raw")
	req.SetPathValue("table", "audit")
	w := httptest.NewRecorder()
	srv.handleAck(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("cross-target ack: got %d, want 400. Body: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "URL path mismatch") {
		t.Errorf("response should mention URL path mismatch, got: %s", w.Body.String())
	}

	// Same body against the correct target — must 200.
	req2 := httptest.NewRequest("POST", "/flush/raw/events/ack", bytes.NewReader(body))
	req2.SetPathValue("schema", "raw")
	req2.SetPathValue("table", "events")
	w2 := httptest.NewRecorder()
	srv.handleAck(w2, req2)
	if w2.Code != http.StatusOK {
		t.Errorf("matching-target ack: got %d, want 200. Body: %s", w2.Code, w2.Body.String())
	}
}

// TestServer_HandleClaim_RejectsInvalidLimit pins the R7 #3 fix:
// pre-fix, limit=abc / 0 / -5 silently fell back to defaultLimit and
// claimed events as if the caller had passed nothing. Now the
// handler returns 400 so the caller sees the typo.
func TestServer_HandleClaim_RejectsInvalidLimit(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	model := testModel()
	srv := NewServer(store, []*parser.Model{model}, "0", "0")

	cases := []struct {
		name     string
		query    string
		wantCode int
	}{
		{"non_numeric", "limit=abc", http.StatusBadRequest},
		{"zero", "limit=0", http.StatusBadRequest},
		{"negative", "limit=-5", http.StatusBadRequest},
		// R8 #10: explicit `?limit=` (empty value) is also rejected
		// — pre-fix it bypassed the validator because Query.Get
		// returned "" identically for "no param" and "explicit empty".
		{"explicit_empty", "limit=", http.StatusBadRequest},
		// No-limit-param at all is legitimate, falls through to defaultLimit.
		{"no_param", "", http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			url := "/flush/raw/events/claim"
			if tc.query != "" {
				url += "?" + tc.query
			}
			req := httptest.NewRequest("POST", url, nil)
			req.SetPathValue("schema", "raw")
			req.SetPathValue("table", "events")
			w := httptest.NewRecorder()
			srv.handleClaim(w, req)

			if w.Code != tc.wantCode {
				t.Errorf("query %q: got %d, want %d", tc.query, w.Code, tc.wantCode)
			}
		})
	}
}

func TestServer_CollectBatch(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	model := testModel()
	srv := NewServer(store, []*parser.Model{model}, "0", "0")

	body := `[{"event_name":"a","page_url":"/a"},{"event_name":"b","page_url":"/b"}]`
	req := httptest.NewRequest("POST", "/collect/raw/events/batch", bytes.NewBufferString(body))
	req.SetPathValue("schema", "raw")
	req.SetPathValue("table", "events")
	w := httptest.NewRecorder()
	srv.handleCollectBatch(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("batch: got %d, want 202", w.Code)
	}

	// Claim should return 2 events
	claimReq := httptest.NewRequest("POST", "/flush/raw/events/claim", nil)
	claimReq.SetPathValue("schema", "raw")
	claimReq.SetPathValue("table", "events")
	claimW := httptest.NewRecorder()
	srv.handleClaim(claimW, claimReq)

	var resp struct {
		Events []map[string]any `json:"events"`
	}
	json.Unmarshal(claimW.Body.Bytes(), &resp)
	if len(resp.Events) != 2 {
		t.Fatalf("got %d events, want 2", len(resp.Events))
	}
}

func TestServer_CollectMissingRequired(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	model := testModel()
	srv := NewServer(store, []*parser.Model{model}, "0", "0")

	// Missing event_name (required)
	body := `{"page_url":"/home"}`
	req := httptest.NewRequest("POST", "/collect/raw/events", bytes.NewBufferString(body))
	req.SetPathValue("schema", "raw")
	req.SetPathValue("table", "events")
	w := httptest.NewRecorder()
	srv.handleCollect(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400", w.Code)
	}
}

func TestServer_UnknownTarget(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	srv := NewServer(store, nil, "0", "0")

	req := httptest.NewRequest("POST", "/collect/raw/unknown", bytes.NewBufferString(`{}`))
	req.SetPathValue("schema", "raw")
	req.SetPathValue("table", "unknown")
	w := httptest.NewRecorder()
	srv.handleCollect(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("got %d, want 404", w.Code)
	}
}

func TestServer_Health(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	srv := NewServer(store, nil, "0", "0")

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	srv.handleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("got %d, want 200", w.Code)
	}
}

func TestServer_Nack(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	model := testModel()
	srv := NewServer(store, []*parser.Model{model}, "0", "0")

	// Write event
	store.Write("raw.events", map[string]any{"event_name": "test"})

	// Claim
	claimReq := httptest.NewRequest("POST", "/flush/raw/events/claim", nil)
	claimReq.SetPathValue("schema", "raw")
	claimReq.SetPathValue("table", "events")
	claimW := httptest.NewRecorder()
	srv.handleClaim(claimW, claimReq)

	var resp struct {
		ClaimID string `json:"claim_id"`
	}
	json.Unmarshal(claimW.Body.Bytes(), &resp)

	// Nack
	nackBody, _ := json.Marshal(map[string]string{"claim_id": resp.ClaimID})
	nackReq := httptest.NewRequest("POST", "/flush/raw/events/nack", bytes.NewBuffer(nackBody))
	nackReq.SetPathValue("schema", "raw")
	nackReq.SetPathValue("table", "events")
	nackW := httptest.NewRecorder()
	srv.handleNack(nackW, nackReq)

	if nackW.Code != http.StatusOK {
		t.Fatalf("nack: got %d, want 200", nackW.Code)
	}

	// Claim again should get the event back
	claimReq2 := httptest.NewRequest("POST", "/flush/raw/events/claim", nil)
	claimReq2.SetPathValue("schema", "raw")
	claimReq2.SetPathValue("table", "events")
	claimW2 := httptest.NewRecorder()
	srv.handleClaim(claimW2, claimReq2)

	var resp2 struct {
		Events []map[string]any `json:"events"`
	}
	json.Unmarshal(claimW2.Body.Bytes(), &resp2)
	if len(resp2.Events) != 1 {
		t.Fatalf("got %d events after nack, want 1", len(resp2.Events))
	}
}

func TestServer_StartShutdown(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	srv := NewServer(store, nil, "0", "0")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Should return without error
	srv.Shutdown(ctx)
}

func TestServer_ReceivedAtAutoPopulated(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	model := testModel()
	srv := NewServer(store, []*parser.Model{model}, "0", "0")

	// Send event without received_at
	body := `{"event_name":"test"}`
	req := httptest.NewRequest("POST", "/collect/raw/events", bytes.NewBufferString(body))
	req.SetPathValue("schema", "raw")
	req.SetPathValue("table", "events")
	w := httptest.NewRecorder()
	srv.handleCollect(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("got %d, want 202", w.Code)
	}

	// Claim and check received_at is populated
	claimReq := httptest.NewRequest("POST", "/flush/raw/events/claim", nil)
	claimReq.SetPathValue("schema", "raw")
	claimReq.SetPathValue("table", "events")
	claimW := httptest.NewRecorder()
	srv.handleClaim(claimW, claimReq)

	var resp struct {
		Events []map[string]any `json:"events"`
	}
	json.Unmarshal(claimW.Body.Bytes(), &resp)
	if len(resp.Events) != 1 {
		t.Fatalf("got %d events, want 1", len(resp.Events))
	}
	if resp.Events[0]["received_at"] == nil {
		t.Fatal("received_at not auto-populated")
	}
}
