// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package collect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/parser"
)

// writeClaimError translates a typed Store error from
// AckForTarget/NackForTarget into the appropriate HTTP response.
//
// ErrClaimNotFound and ErrClaimWrongTarget are client errors (400)
// — the URL path doesn't match the claim's actual state. Anything
// else is a backend failure (500).
func writeClaimError(w http.ResponseWriter, err error) {
	var wrong *ErrClaimWrongTarget
	switch {
	case errors.Is(err, ErrClaimNotFound):
		writeJSONError(w, http.StatusBadRequest, err.Error())
	case errors.As(err, &wrong):
		writeJSONError(w, http.StatusBadRequest, err.Error())
	default:
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("ack/nack failed: %v", err))
	}
}

// writeJSONError emits a structured error response with stable shape:
//
//	{"error": "<message>"}
//
// Both admin and public endpoints route ALL non-2xx responses through
// this helper so callers see one Content-Type and one envelope shape
// regardless of error class. (R8 #12 — pre-fix /claim returned JSON
// on success but text/plain via http.Error on 4xx/5xx, mixing two
// undocumented response contracts in the same endpoint.)
func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg}) // HTTP write — client disconnect not actionable
}

// Server handles event collection (public) and flush operations (admin).
// Two separate HTTP listeners:
//   - Public: receives events from browsers/clients (port set via `ondatrasql events <port>`)
//   - Admin: localhost-bound, used by runner for flush (always public+1)
type Server struct {
	store     *Store
	schemas   map[string]*parser.Model // target → model (for validation)
	publicSrv *http.Server
	adminSrv  *http.Server
}

// NewServer creates a collect server with public and admin listeners.
func NewServer(store *Store, models []*parser.Model, publicPort, adminPort string) *Server {
	s := &Server{
		store:   store,
		schemas: make(map[string]*parser.Model),
	}

	for _, m := range models {
		if m.Kind == "events" {
			s.schemas[m.Target] = m
		}
	}

	// Public routes (can be exposed to the internet)
	pubMux := http.NewServeMux()
	pubMux.HandleFunc("POST /collect/{schema}/{table}", s.handleCollect)
	pubMux.HandleFunc("POST /collect/{schema}/{table}/batch", s.handleCollectBatch)
	pubMux.HandleFunc("GET /health", s.handleHealth)

	// Admin routes (localhost-bound, internal access only)
	adminMux := http.NewServeMux()
	adminMux.HandleFunc("POST /flush/{schema}/{table}/claim", s.handleClaim)
	adminMux.HandleFunc("POST /flush/{schema}/{table}/ack", s.handleAck)
	adminMux.HandleFunc("POST /flush/{schema}/{table}/nack", s.handleNack)
	adminMux.HandleFunc("GET /flush/{schema}/{table}/inflight", s.handleInflight)
	adminMux.HandleFunc("GET /health", s.handleHealth)

	s.publicSrv = &http.Server{
		Addr:         ":" + publicPort,
		Handler:      pubMux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	s.adminSrv = &http.Server{
		Addr:         "127.0.0.1:" + adminPort,
		Handler:      adminMux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	return s
}

// Start starts both public and admin HTTP servers.
// Blocks until context is cancelled or a server fails.
// When the context is cancelled, both servers are shut down gracefully.
func (s *Server) Start(ctx context.Context) error {
	errCh := make(chan error, 2)

	go func() {
		if err := s.publicSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("public server: %w", err)
		}
	}()

	go func() {
		if err := s.adminSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("admin server: %w", err)
		}
	}()

	// Periodically flush buffered writes to Badger
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := s.store.FlushWrites(); err != nil {
					fmt.Fprintf(os.Stderr, "event flush: %v\n", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	var serverErr error
	select {
	case serverErr = <-errCh:
		// One server failed — shut down the other
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdownErr := s.Shutdown(shutdownCtx)

	// Drain second error if available
	select {
	case err2 := <-errCh:
		serverErr = errors.Join(serverErr, err2)
	default:
	}

	return errors.Join(serverErr, shutdownErr)
}

// Shutdown gracefully shuts down both servers.
func (s *Server) Shutdown(ctx context.Context) error {
	err1 := s.publicSrv.Shutdown(ctx)
	err2 := s.adminSrv.Shutdown(ctx)
	if err1 != nil {
		return err1
	}
	return err2
}

// resolveTarget extracts and validates the target from URL path params.
func (s *Server) resolveTarget(r *http.Request) (string, *parser.Model, error) {
	schema := r.PathValue("schema")
	table := r.PathValue("table")
	target := schema + "." + table

	model, ok := s.schemas[target]
	if !ok {
		return "", nil, fmt.Errorf("unknown event target: %s", target)
	}
	return target, model, nil
}

// handleCollect receives a single event.
// POST /collect/{schema}/{table}
// Events are buffered in memory and flushed every 100ms for throughput.
// Returns 202 Accepted immediately — a crash before flush loses buffered events.
// Use /batch endpoint for durable writes.
func (s *Server) handleCollect(w http.ResponseWriter, r *http.Request) {
	target, model, err := s.resolveTarget(r)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err.Error())
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1MB limit
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "read body failed")
		return
	}

	var event map[string]any
	if err := json.Unmarshal(body, &event); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	if err := validateEvent(event, model); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Add received_at if the model has it and the event doesn't
	addReceivedAt(event, model)

	if err := s.store.Write(target, event); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "store failed")
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

// handleCollectBatch receives an array of events.
// POST /collect/{schema}/{table}/batch
func (s *Server) handleCollectBatch(w http.ResponseWriter, r *http.Request) {
	target, model, err := s.resolveTarget(r)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err.Error())
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 10<<20)) // 10MB limit
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "read body failed")
		return
	}

	var events []map[string]any
	if err := json.Unmarshal(body, &events); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON array")
		return
	}

	// Validate all events before writing any (avoid partial writes)
	for _, event := range events {
		if err := validateEvent(event, model); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		addReceivedAt(event, model)
	}

	// Write all validated events
	if err := s.store.WriteBatch(target, events); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "store failed")
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

// handleHealth returns 200 OK.
func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok")) // health response — client disconnect not actionable
}

// handleClaim claims events for flushing to DuckLake.
// POST /flush/{schema}/{table}/claim?limit=10000
func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	target, _, err := s.resolveTarget(r)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err.Error())
		return
	}

	limit := defaultLimit
	// Distinguish `?limit=` (explicit empty value, programmer error) from
	// no `?limit` parameter at all (legitimate, falls through to default).
	// `URL.Query().Get` returns "" for both cases, so check the underlying
	// map for presence. (R8 #10 — pre-fix `?limit=` bypassed the validator
	// because Get returned "" and the empty-string short-circuit fell
	// through to defaultLimit alongside the "no param" case.)
	if l, present := r.URL.Query()["limit"]; present {
		if len(l) == 0 || l[0] == "" {
			writeJSONError(w, http.StatusBadRequest, "invalid limit \"\" (must be positive integer)")
			return
		}
		// Reject `limit=abc`, `limit=0`, `limit=-5` etc. with 400.
		// Pre-R7 these silently fell back to defaultLimit, hiding
		// client bugs. The admin endpoint is a closed input contract.
		parsed, err := strconv.Atoi(l[0])
		if err != nil || parsed <= 0 {
			writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid limit %q (must be positive integer)", l[0]))
			return
		}
		limit = parsed
	}

	claimID, events, err := s.store.Claim(target, limit)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("claim failed: %v", err))
		return
	}

	resp := map[string]any{
		"claim_id": claimID,
		"events":   events,
	}
	if events == nil {
		resp["events"] = []map[string]any{}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp) // HTTP write — client disconnect not actionable
}

// handleAck acknowledges a successful flush.
// POST /flush/{schema}/{table}/ack  body: {"claim_id": "..."}
//
// Validation + delete are done atomically by Store.AckForTarget so a
// reap between "does this claim exist" and "delete it" can't leave
// the request returning 200 with no actual mutation. (R12 #3.)
func (s *Server) handleAck(w http.ResponseWriter, r *http.Request) {
	target, _, err := s.resolveTarget(r)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err.Error())
		return
	}
	var body struct {
		ClaimID string `json:"claim_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.ClaimID == "" {
		writeJSONError(w, http.StatusBadRequest, "missing claim_id")
		return
	}
	if err := s.store.AckForTarget(body.ClaimID, target); err != nil {
		writeClaimError(w, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleNack returns events to the queue after a failed flush.
// POST /flush/{schema}/{table}/nack  body: {"claim_id": "..."}
func (s *Server) handleNack(w http.ResponseWriter, r *http.Request) {
	target, _, err := s.resolveTarget(r)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err.Error())
		return
	}
	var body struct {
		ClaimID string `json:"claim_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.ClaimID == "" {
		writeJSONError(w, http.StatusBadRequest, "missing claim_id")
		return
	}
	if err := s.store.NackForTarget(body.ClaimID, target); err != nil {
		writeClaimError(w, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleInflight returns inflight claim IDs for a target.
// GET /flush/{schema}/{table}/inflight
func (s *Server) handleInflight(w http.ResponseWriter, r *http.Request) {
	target, _, err := s.resolveTarget(r)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err.Error())
		return
	}

	claimIDs, err := s.store.FindInflightClaims(target)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("find inflight: %v", err))
		return
	}

	resp := map[string]any{"claim_ids": claimIDs}
	if claimIDs == nil {
		resp["claim_ids"] = []string{}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp) // HTTP write — client disconnect not actionable
}

// validateEvent checks that required (NOT NULL) columns are present.
func validateEvent(event map[string]any, model *parser.Model) error {
	for _, col := range model.Columns {
		if col.NotNull && col.Name != "received_at" {
			if _, ok := event[col.Name]; !ok {
				return fmt.Errorf("missing required field: %s", col.Name)
			}
		}
	}
	return nil
}

// addReceivedAt adds a received_at timestamp if the model defines it and
// the event doesn't already include one.
func addReceivedAt(event map[string]any, model *parser.Model) {
	for _, col := range model.Columns {
		if col.Name == "received_at" {
			if _, ok := event["received_at"]; !ok {
				event["received_at"] = time.Now().UTC().Format(time.RFC3339Nano)
			}
			break
		}
	}
}

