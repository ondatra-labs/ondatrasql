// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package odata

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestHandleBatch_HeadersForwarded pins JSON Format §19.1.3 — per-request
// `headers` in a JSON batch entry MUST be copied onto the inner request
// the route handler sees. Without this, clients sending Accept,
// OData-MaxVersion, etc. per sub-request get them silently dropped.
//
// Test approach: pass handleBatch a stub http.Handler that records the
// headers of any request it receives, then send a batch with a custom
// header in one entry. Assert the stub observed it.
func TestHandleBatch_HeadersForwarded(t *testing.T) {
	t.Parallel()

	var observed http.Header
	stub := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed = r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{}`))
	})

	body := strings.NewReader(`{"requests":[{"id":"1","method":"GET","url":"x","headers":{"X-Test":"hello","Prefer":"odata.maxpagesize=1"}}]}`)
	req := httptest.NewRequest("POST", "/odata/$batch", body)
	rec := httptest.NewRecorder()

	handleBatch(rec, req, stub)

	if observed == nil {
		t.Fatal("stub handler was never invoked")
	}
	if got := observed.Get("X-Test"); got != "hello" {
		t.Errorf("inner request X-Test = %q, want %q (per-request headers not copied)", got, "hello")
	}
	if got := observed.Get("Prefer"); got != "odata.maxpagesize=1" {
		t.Errorf("inner request Prefer = %q, want %q", got, "odata.maxpagesize=1")
	}
}

// TestHandleBatch_AtomicityGroupRejected pins that atomicityGroup
// returns 501 NotImplemented per the read-only-server contract. We
// already have an e2e test, but this unit-level pin is faster and
// catches regressions in handleBatch directly without a full session
// setup.
func TestHandleBatch_AtomicityGroupRejected(t *testing.T) {
	t.Parallel()

	stub := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("inner handler should NOT be called for atomicityGroup-flagged request")
	})

	body := strings.NewReader(`{"requests":[{"id":"1","method":"GET","url":"x","atomicityGroup":"g1"}]}`)
	req := httptest.NewRequest("POST", "/odata/$batch", body)
	rec := httptest.NewRecorder()

	handleBatch(rec, req, stub)

	if !strings.Contains(rec.Body.String(), `"status":501`) {
		t.Errorf("expected sub-request status 501, got body: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "NotImplemented") {
		t.Errorf("expected NotImplemented in error code, got: %s", rec.Body.String())
	}
}
