// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package collect

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
