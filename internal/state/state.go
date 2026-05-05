// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package state provides a local DuckDB-backed store for operational
// state — fetch staging buffers and (later) push delta tracking.
//
// The state file lives at .ondatra/state.duckdb under the project root.
// It is transient: contents can be rebuilt from scratch by re-running the
// pipeline. It must NOT contain any user data — only operational metadata
// and in-flight rows that haven't yet been materialized into DuckLake.
//
// The package replaces the previous `internal/collect`-based stores
// (fetch's `Store` and push's `SyncStore`). Crash-recovery semantics are
// preserved: inflight claims from a crashed run are detected at startup
// and either discarded (if the claim already committed, per the
// `_ondatra_acks` table in the main catalog) or reset for re-processing.
package state

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "github.com/duckdb/duckdb-go/v2" // duckdb driver registration
)

// State owns a connection to .ondatra/state.duckdb. Independent from the
// main DuckLake catalog session — the two run in separate processes only
// in a single-OS-process model, but logically they're separate stores.
type State struct {
	db   *sql.DB
	path string
}

// Open creates or opens the state.duckdb file under projectDir/.ondatra/
// and prepares it for use. The directory is created if it doesn't exist.
func Open(projectDir string) (*State, error) {
	dir := filepath.Join(projectDir, ".ondatra")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	path := filepath.Join(dir, "state.duckdb")
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, fmt.Errorf("open state.duckdb: %w", err)
	}
	// Single connection — state writes are serialized per-process.
	db.SetMaxOpenConns(1)
	return &State{db: db, path: path}, nil
}

// DB returns the underlying database handle. Callers use this to register
// Appenders or run prepared statements; they MUST NOT close the handle —
// use State.Close() instead.
func (s *State) DB() *sql.DB {
	return s.db
}

// Path returns the absolute filesystem path to state.duckdb.
func (s *State) Path() string {
	return s.path
}

// Close closes the underlying connection.
func (s *State) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

// GC runs periodic cleanup on operational state. Idempotent and cheap to
// call repeatedly. The intended invocation point is once at the start of
// each pipeline run (RunDAG) so stale events and orphaned claims don't
// accumulate over time.
//
// Today this only delegates to SyncStore.RunGC() (sync_evt TTL +
// orphan-inflight recovery). Fetch staging tables don't need TTL cleanup —
// stateCollector handles per-target recovery at open time.
//
// Returns the first error encountered; partial progress is rolled back
// via the underlying transaction. Callers should surface this so a
// degraded state-store doesn't fail silently.
func GC(st *State) error {
	if st == nil {
		return nil
	}
	sync, err := NewSyncStore(st)
	if err != nil {
		return fmt.Errorf("open sync store for gc: %w", err)
	}
	return sync.RunGC()
}

