// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package state provides a local DuckDB-backed store for operational
// state — push queue, fetch staging buffer, OAuth tokens.
//
// Backend selection lives in `config/state.sql`, which is executed
// against a per-process `:memory:` DuckDB session at startup. The file
// is expected to ATTACH a single catalog AS `state`, after which
// `state.Open` runs `USE state` so unqualified table references in the
// rest of the code resolve into the state catalog.
//
// The default `config/state.sql` (created by `ondatrasql init`) attaches
// a local file `state.duckdb` in the project root with DuckDB's native
// file-level AES-GCM encryption (ENCRYPTION_KEY from env). Switching to
// a Quack server is a single-line edit in `state.sql` — Go-side code is
// backend-agnostic.
package state

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	duckdb "github.com/duckdb/duckdb-go/v2"

	sqlfiles "github.com/ondatra-labs/ondatrasql/internal/sql"
)

// State owns a connection to the state catalog. The underlying DB is
// always a process-local `:memory:` DuckDB session whose default
// catalog (via USE state) is whatever was ATTACHed by config/state.sql.
// All sync_store / state_collector queries run unqualified and resolve
// through the USE pointer.
type State struct {
	db *sql.DB
}

// Open initializes a state session from config/state.sql at the given
// configPath. Fails hard if the file is missing — there is no implicit
// fallback to .ondatra/state.duckdb.
//
// Environment variables in state.sql are expanded via os.ExpandEnv
// before execution, so a default like
//
//	ATTACH 'state.duckdb' AS state (ENCRYPTION_KEY '${ONDATRA_STATE_KEY}');
//
// picks up the key from .env (already loaded by config.Load).
func Open(configPath string) (*State, error) {
	statePath := filepath.Join(configPath, "state.sql")
	content, err := os.ReadFile(statePath)
	if err != nil {
		return nil, fmt.Errorf("config/state.sql required: %w", err)
	}

	// Env vars in state.sql (e.g. ${ONDATRA_STATE_KEY}) are expanded
	// first so the SQL DuckDB sees has concrete values, not literal
	// `${...}` placeholders.
	attachSQL := os.ExpandEnv(string(content))
	initSQL, err := sqlfiles.Load("state/init.sql")
	if err != nil {
		return nil, fmt.Errorf("load state/init.sql: %w", err)
	}

	// A connection-init hook runs on every connection the pool opens.
	// ATTACH and the schema DDL are instance-global, so they run exactly
	// once (guarded by sync.Once). `USE state` is session-local, so it
	// MUST run on every connection: if database/sql ever discards the
	// pooled connection (e.g. a driver.ErrBadConn) and opens a fresh
	// one, that connection still resolves unqualified table names into
	// the state catalog instead of silently falling back to memory.main.
	var bootstrapOnce sync.Once
	var bootstrapErr error
	connInitFn := func(execer driver.ExecerContext) error {
		bootstrapOnce.Do(func() {
			if _, err := execer.ExecContext(context.Background(), attachSQL, nil); err != nil {
				bootstrapErr = fmt.Errorf("execute state.sql: %w", err)
				return
			}
			// USE state before the DDL so the unqualified CREATE TABLEs
			// in init.sql land in the state catalog, not memory.main.
			if _, err := execer.ExecContext(context.Background(), "USE state", nil); err != nil {
				bootstrapErr = fmt.Errorf("use state catalog: %w", err)
				return
			}
			// Create tables idempotently. Quack-backed state runs this
			// against the remote server, local-backed state runs it
			// against the attached file — same DDL, same behavior.
			if _, err := execer.ExecContext(context.Background(), initSQL, nil); err != nil {
				bootstrapErr = fmt.Errorf("init state schema: %w", err)
			}
		})
		if bootstrapErr != nil {
			return bootstrapErr
		}
		// Every connection (including ones the pool re-creates after the
		// one-time bootstrap) must point at the state catalog.
		if _, err := execer.ExecContext(context.Background(), "USE state", nil); err != nil {
			return fmt.Errorf("use state catalog: %w", err)
		}
		return nil
	}

	connector, err := duckdb.NewConnector(":memory:", connInitFn)
	if err != nil {
		return nil, fmt.Errorf("open in-memory duckdb: %w", err)
	}

	db := sql.OpenDB(connector)
	// Serialize state writes per-process. The state catalog itself
	// handles cross-process concurrency (DuckDB file lock for local,
	// MVCC on the server for Quack).
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Force the first connection open now so ATTACH/init errors surface
	// here rather than on the first query.
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &State{db: db}, nil
}

// DB returns the underlying handle. Callers run SQL through it as
// against any sql.DB; the USE state ensures unqualified references
// resolve into the state catalog.
func (s *State) DB() *sql.DB {
	return s.db
}

// Close closes the in-memory session. The attached state catalog (a
// file or a Quack connection) is released by DuckDB as part of session
// teardown.
func (s *State) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

// GC runs periodic cleanup on operational state. Idempotent and cheap
// to call repeatedly. Intended invocation point is once at the start
// of each pipeline run so stale events and orphaned claims don't
// accumulate over time.
//
// Delegates to SyncStore.RunGC (sync_evt TTL + orphan-inflight
// recovery). Fetch staging tables don't need TTL cleanup —
// stateCollector handles per-target recovery at open time.
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
