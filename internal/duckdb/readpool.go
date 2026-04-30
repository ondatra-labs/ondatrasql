// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// ReadPool is a fixed-size pool of pre-initialized read connections that
// share the writer session's catalog state. OData and any other parallel
// read path acquires from the pool, runs its query on the conn, and
// releases. Acquire blocks until a conn is free.
//
// Why a separate pool, not just bumping MaxOpenConns:
//   - The writer's `s.conn` carries persistent session state (variables,
//     macros, search_path edits during run). Sharing it with parallel
//     readers would race that state.
//   - Each read conn is initialized once with `LOAD ducklake`, `USE
//     <catalog>`, and `SET search_path = ...` so OData queries resolve
//     unqualified table refs the same way the writer does.
//   - DuckLake's `table_changes()` requires `SET search_path = catalog.schema`
//     for the duration of the call. With per-request conns each delta handler
//     mutates its own session state — no cross-handler bleed, no mutex needed.
type ReadPool struct {
	conns chan *sql.Conn
	// mu guards the conns channel against concurrent Close + Release.
	// Release holds RLock for the duration of the channel send; Close
	// holds Lock so it waits for in-flight sends before close(p.conns).
	// Without this, a Release racing with Close panics on send-on-closed.
	mu     sync.RWMutex
	closed bool // guarded by mu
}

// InitReadPool creates a pool of `size` read conns on the session's *sql.DB.
// Must be called AFTER InitWithCatalog has run — the catalog needs to be
// ATTACHed on the DuckDB instance before pool conns can USE it.
//
// Bumps `db.SetMaxOpenConns` to `1 + size` so the writer's conn isn't
// competing with the pool for slots.
//
// `size` must be > 0.  Returns an error if the pool was already initialised.
// Each conn replays a minimal init: LOAD ducklake, USE <catalog>,
// SET search_path. Catalog ATTACHes are at the DuckDB instance level — once
// the writer has ATTACHed, all conns on the same *sql.DB see the catalog.
func (s *Session) InitReadPool(size int) error {
	if size <= 0 {
		return fmt.Errorf("pool size must be > 0, got %d", size)
	}
	// Hold s.mu for the entire init: the alternative (release between
	// nil-check and assignment) creates a TOCTOU where two concurrent
	// InitReadPool calls both pass the nil-check, both create N conns,
	// and the loser orphans its conns when the winner overwrites
	// s.readPool. initReadConn uses *sql.Conn directly and never reaches
	// back into s.mu, so holding the lock here is deadlock-free.
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.readPool != nil {
		return errors.New("read pool already initialized")
	}
	if s.catalogAlias == "" {
		return errors.New("catalog not initialized; call InitWithCatalog first")
	}

	// Bump conn limit to accommodate writer + pool. SetMax* are idempotent.
	s.db.SetMaxOpenConns(1 + size)
	s.db.SetMaxIdleConns(1 + size)

	pool := &ReadPool{conns: make(chan *sql.Conn, size)}

	for i := 0; i < size; i++ {
		c, err := s.db.Conn(context.Background())
		if err != nil {
			_ = pool.Close()
			return fmt.Errorf("acquire pool conn %d: %w", i, err)
		}
		if err := s.initReadConn(context.Background(), c); err != nil {
			_ = c.Close()
			_ = pool.Close()
			return fmt.Errorf("init pool conn %d: %w", i, err)
		}
		pool.conns <- c
	}

	s.readPool = pool
	return nil
}

// initReadConn replays the minimal session setup the writer's conn already
// has, on a fresh pool conn. The catalog ATTACH itself is at the DuckDB
// instance level (writer's catalog.sql already ran), so this conn just needs
// to LOAD ducklake (per-conn function visibility), USE the catalog (so
// `current_snapshot()` resolves without prefix), and set search_path (so
// unqualified table refs find tables in the catalog's default schema).
func (s *Session) initReadConn(ctx context.Context, c *sql.Conn) error {
	if _, err := c.ExecContext(ctx, "LOAD ducklake"); err != nil {
		return fmt.Errorf("load ducklake: %w", err)
	}
	if _, err := c.ExecContext(ctx, fmt.Sprintf("USE %s;", QuoteIdentifier(s.CatalogAlias()))); err != nil {
		return fmt.Errorf("use catalog: %w", err)
	}
	if _, err := c.ExecContext(ctx, fmt.Sprintf("SET search_path = '%s';", EscapeSQL(s.DefaultSearchPath()))); err != nil {
		return fmt.Errorf("set search_path: %w", err)
	}
	return nil
}

// ReadPool returns the session's read pool, or nil if InitReadPool was
// never called.
func (s *Session) ReadPool() *ReadPool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readPool
}

// Acquire blocks until a conn is free or ctx is cancelled. Caller MUST
// call Release on the returned conn. Receiving on a channel that's been
// closed by Close returns ok=false; we map that to an error so the caller
// can't accidentally use a zero conn.
func (p *ReadPool) Acquire(ctx context.Context) (*sql.Conn, error) {
	select {
	case c, ok := <-p.conns:
		if !ok {
			return nil, errors.New("read pool closed")
		}
		return c, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Release returns a conn to the pool. Safe to call after Close — the
// conn is closed instead of being re-pooled. Holds an RLock for the
// duration of the channel send so Close can't close(p.conns) mid-send
// (which would panic).
func (p *ReadPool) Release(c *sql.Conn) {
	if c == nil {
		return
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		_ = c.Close()
		return
	}
	// Non-blocking send: if the channel is full something has gone wrong
	// (more releases than acquires); close the extra conn rather than
	// blocking the caller forever.
	select {
	case p.conns <- c:
	default:
		_ = c.Close()
	}
}

// Close drains and closes every conn in the pool. Idempotent.
func (p *ReadPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	close(p.conns)
	var firstErr error
	for c := range p.conns {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// QueryRowsAnyOnConn is the per-conn variant of Session.QueryRowsAny. It
// runs the query on the supplied conn and returns rows as native-typed maps.
//
// Mirrors QueryRowsAnyContext on Session (s.conn path). Both must keep the
// same scan/decode behaviour — if you fix one, fix the other.
func QueryRowsAnyOnConn(ctx context.Context, c *sql.Conn, sqlStr string) ([]map[string]any, error) {
	sqlStr = strings.TrimSpace(sqlStr)
	if sqlStr == "" {
		return nil, nil
	}
	rows, err := c.QueryContext(ctx, sqlStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			row[col] = vals[i]
		}
		results = append(results, row)
	}
	return results, rows.Err()
}

// ExecOnConn runs a non-query SQL statement on the supplied conn. Used by
// the OData delta handler to set search_path before `table_changes()`.
func ExecOnConn(ctx context.Context, c *sql.Conn, sqlStr string) error {
	_, err := c.ExecContext(ctx, sqlStr)
	return err
}

// QueryValueOnConn runs the query on the supplied conn and returns the first
// column of the first row as a string. Mirrors Session.QueryValue. Returns
// "" if the query has no rows.
func QueryValueOnConn(ctx context.Context, c *sql.Conn, sqlStr string) (string, error) {
	sqlStr = strings.TrimSpace(sqlStr)
	if sqlStr == "" {
		return "", nil
	}
	rows, err := c.QueryContext(ctx, sqlStr)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	if !rows.Next() {
		return "", rows.Err()
	}
	var v any
	if err := rows.Scan(&v); err != nil {
		return "", err
	}
	return anyToString(v), nil
}

// QueryRowsMapOnConn mirrors Session.QueryRowsMap on an arbitrary conn —
// every value coerced to string. Used by metadata generation; OData
// hot-path queries should use QueryRowsAnyOnConn for native types.
func QueryRowsMapOnConn(ctx context.Context, c *sql.Conn, sqlStr string) ([]map[string]string, error) {
	sqlStr = strings.TrimSpace(sqlStr)
	if sqlStr == "" {
		return nil, nil
	}
	rows, err := c.QueryContext(ctx, sqlStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]string
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]string, len(cols))
		for i, col := range cols {
			row[col] = anyToString(vals[i])
		}
		results = append(results, row)
	}
	return results, rows.Err()
}

// CurrentSnapshotOnConn returns the active DuckLake snapshot id for the
// catalog the conn is USE'd into. Returns 0 if there are no snapshots yet.
//
// Independent of the writer's `curr_snapshot` session variable — pool conns
// don't share that variable, so this reads `current_snapshot()` directly.
func CurrentSnapshotOnConn(ctx context.Context, c *sql.Conn) (int64, error) {
	var id int64
	err := c.QueryRowContext(
		ctx,
		"SELECT COALESCE((SELECT id FROM current_snapshot()), 0)::BIGINT",
	).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}
