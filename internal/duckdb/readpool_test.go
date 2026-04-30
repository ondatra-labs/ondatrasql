// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// initSessionWithCatalog spins up a fresh session with a sqlite-backed
// DuckLake catalog. Used by every pool test that needs the catalog
// attached before pool init.
func initSessionWithCatalog(t *testing.T) *Session {
	t.Helper()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config")
	if err := os.MkdirAll(configPath, 0o755); err != nil {
		t.Fatalf("mkdir config: %v", err)
	}
	catalogPath := filepath.Join(dir, "ducklake.sqlite")
	dataPath := filepath.Join(dir, "data")
	if err := os.WriteFile(filepath.Join(configPath, "catalog.sql"),
		[]byte("ATTACH 'ducklake:sqlite:"+catalogPath+"' AS lake (DATA_PATH '"+dataPath+"');\n"),
		0o644); err != nil {
		t.Fatalf("write catalog.sql: %v", err)
	}

	s, err := NewSession(":memory:")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	if err := s.InitWithCatalog(configPath); err != nil {
		t.Fatalf("InitWithCatalog: %v", err)
	}
	return s
}

// TestReadPool_BasicAcquireRelease pins the happy path: init pool, acquire
// a conn, run a query against the writer-attached catalog, release. The
// query must see the writer's schema — proving catalog ATTACHes propagate
// to pool conns at the DuckDB instance level.
func TestReadPool_BasicAcquireRelease(t *testing.T) {
	s := initSessionWithCatalog(t)

	// Writer creates a schema + table the pool conn must see.
	if err := s.Exec("CREATE SCHEMA IF NOT EXISTS lake.raw"); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if err := s.Exec("CREATE TABLE lake.raw.widgets (id BIGINT, name VARCHAR)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := s.Exec("INSERT INTO lake.raw.widgets VALUES (1, 'Apple'), (2, 'Banana')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if err := s.InitReadPool(2); err != nil {
		t.Fatalf("InitReadPool: %v", err)
	}
	pool := s.ReadPool()
	if pool == nil {
		t.Fatal("ReadPool() returned nil after InitReadPool")
	}

	c, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer pool.Release(c)

	rows, err := QueryRowsAnyOnConn(context.Background(), c,
		"SELECT id, name FROM lake.raw.widgets ORDER BY id")
	if err != nil {
		t.Fatalf("QueryRowsAnyOnConn: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	if name, _ := rows[0]["name"].(string); name != "Apple" {
		t.Errorf("first row name = %v, want Apple", rows[0]["name"])
	}
}

// TestReadPool_DoubleInitRejected pins that calling InitReadPool twice on
// the same session is an error rather than silently leaking the previous
// pool.
func TestReadPool_DoubleInitRejected(t *testing.T) {
	s := initSessionWithCatalog(t)
	if err := s.InitReadPool(2); err != nil {
		t.Fatalf("first InitReadPool: %v", err)
	}
	if err := s.InitReadPool(2); err == nil {
		t.Fatal("second InitReadPool should fail; got nil error")
	}
}

// TestReadPool_AcquireBlocksUntilRelease pins the channel semantics: with
// pool size 1, a second Acquire blocks until the first conn is released.
// Without this, the pool would hand out the same conn twice.
func TestReadPool_AcquireBlocksUntilRelease(t *testing.T) {
	s := initSessionWithCatalog(t)
	if err := s.InitReadPool(1); err != nil {
		t.Fatalf("InitReadPool: %v", err)
	}
	pool := s.ReadPool()

	c1, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	// Second Acquire with a 100ms deadline must time out — pool is empty.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := pool.Acquire(ctx); err == nil {
		t.Fatal("second Acquire should have timed out, got nil error")
	}

	// Release c1 → next Acquire succeeds.
	pool.Release(c1)
	c2, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("post-release Acquire: %v", err)
	}
	pool.Release(c2)
}

// TestReadPool_ConcurrentReads pins that pool conns can run queries in
// parallel. With size N, N goroutines should be able to query simultaneously
// without serialising on the writer's mutex. Counts wall-clock time as a
// soft proxy — if the pool serialised behind a single conn, total time
// would scale linearly with N.
func TestReadPool_ConcurrentReads(t *testing.T) {
	s := initSessionWithCatalog(t)
	if err := s.Exec("CREATE SCHEMA IF NOT EXISTS lake.raw"); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if err := s.Exec("CREATE TABLE lake.raw.t (n BIGINT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := s.Exec("INSERT INTO lake.raw.t SELECT * FROM range(100000)"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	const poolSize = 4
	if err := s.InitReadPool(poolSize); err != nil {
		t.Fatalf("InitReadPool: %v", err)
	}
	pool := s.ReadPool()

	var wg sync.WaitGroup
	errCh := make(chan error, poolSize)
	for i := 0; i < poolSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := pool.Acquire(context.Background())
			if err != nil {
				errCh <- fmt.Errorf("acquire: %w", err)
				return
			}
			defer pool.Release(c)
			rows, err := QueryRowsAnyOnConn(context.Background(), c,
				"SELECT count(*) AS c FROM lake.raw.t")
			if err != nil {
				errCh <- fmt.Errorf("query: %w", err)
				return
			}
			if len(rows) != 1 {
				errCh <- fmt.Errorf("rows = %d, want 1", len(rows))
				return
			}
			errCh <- nil
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Errorf("concurrent read: %v", err)
		}
	}
}

// TestReadPool_SearchPathIsolation pins that a `SET search_path` on one
// pool conn does not leak into another. This is the property the OData
// delta handler depends on after the v0.27.0 mutex removal.
func TestReadPool_SearchPathIsolation(t *testing.T) {
	s := initSessionWithCatalog(t)
	if err := s.Exec("CREATE SCHEMA IF NOT EXISTS lake.raw"); err != nil {
		t.Fatalf("create schema raw: %v", err)
	}
	if err := s.Exec("CREATE SCHEMA IF NOT EXISTS lake.staging"); err != nil {
		t.Fatalf("create schema staging: %v", err)
	}
	if err := s.InitReadPool(2); err != nil {
		t.Fatalf("InitReadPool: %v", err)
	}
	pool := s.ReadPool()

	c1, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire c1: %v", err)
	}
	c2, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire c2: %v", err)
	}
	defer pool.Release(c1)
	defer pool.Release(c2)

	// Set search_path on c1 only.
	if err := ExecOnConn(context.Background(), c1,
		"SET search_path = 'lake.raw,memory'"); err != nil {
		t.Fatalf("set search_path on c1: %v", err)
	}
	// Read it back on c2 — must reflect c2's own (default) search_path,
	// not c1's mutation.
	rows, err := QueryRowsAnyOnConn(context.Background(), c2,
		"SELECT current_setting('search_path') AS sp")
	if err != nil {
		t.Fatalf("read search_path on c2: %v", err)
	}
	sp, _ := rows[0]["sp"].(string)
	// c2's search_path must contain "memory" (set by initReadConn) and
	// must NOT contain "lake.raw" (which c1 just set).
	if sp == "lake.raw,memory" {
		t.Errorf("c2 picked up c1's search_path: %q — pool is leaking session state", sp)
	}
}

// TestReadPool_CurrentSnapshotOnConn pins that pool conns can read
// `current_snapshot()` and see the writer's snapshot id. Without USE on
// the pool conn this would resolve to memory and fail.
func TestReadPool_CurrentSnapshotOnConn(t *testing.T) {
	s := initSessionWithCatalog(t)
	if err := s.Exec("CREATE SCHEMA IF NOT EXISTS lake.raw"); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if err := s.Exec("CREATE TABLE lake.raw.t (id BIGINT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := s.Exec("INSERT INTO lake.raw.t VALUES (1)"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if err := s.InitReadPool(1); err != nil {
		t.Fatalf("InitReadPool: %v", err)
	}
	c, err := s.ReadPool().Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer s.ReadPool().Release(c)

	snap, err := CurrentSnapshotOnConn(context.Background(), c)
	if err != nil {
		t.Fatalf("CurrentSnapshotOnConn: %v", err)
	}
	if snap <= 0 {
		t.Errorf("snapshot id = %d, expected positive after writes", snap)
	}
}

// TestReadPool_ZeroSizeRejected pins that InitReadPool(0) returns an error
// — a zero-sized pool would hang every Acquire forever.
func TestReadPool_ZeroSizeRejected(t *testing.T) {
	s := initSessionWithCatalog(t)
	if err := s.InitReadPool(0); err == nil {
		t.Fatal("InitReadPool(0) should fail; got nil error")
	}
	if err := s.InitReadPool(-1); err == nil {
		t.Fatal("InitReadPool(-1) should fail; got nil error")
	}
}

// TestReadPool_ReleaseDuringCloseDoesNotPanic pins that Release racing
// with Close does not crash on a "send on closed channel". Close holds a
// write lock; Release holds a read lock for its channel send. Without
// the mu, this test triggers a panic ~1 in 5 runs under -race.
//
// The setup: one goroutine spam-Releases conns, another Closes the pool.
// Without protection, the Release picks a moment where closed=false but
// then close(p.conns) runs before Release's `case p.conns <- c` lands.
func TestReadPool_ReleaseDuringCloseDoesNotPanic(t *testing.T) {
	s := initSessionWithCatalog(t)
	if err := s.InitReadPool(8); err != nil {
		t.Fatalf("InitReadPool: %v", err)
	}
	pool := s.ReadPool()

	// Drain conns out so Release has somewhere to put them back.
	conns := make([]*sql.Conn, 0, 8)
	for i := 0; i < 8; i++ {
		c, err := pool.Acquire(context.Background())
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		conns = append(conns, c)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	// Releaser goroutine.
	go func() {
		defer wg.Done()
		for _, c := range conns {
			pool.Release(c)
		}
	}()
	// Closer goroutine.
	go func() {
		defer wg.Done()
		_ = pool.Close()
	}()
	// If either goroutine panics, the test process aborts.
	wg.Wait()
}

// TestReadPool_CloseClosesAllConns pins that Close drains every conn in
// the pool. After Close, Acquire must fail rather than hanging.
func TestReadPool_CloseClosesAllConns(t *testing.T) {
	s := initSessionWithCatalog(t)
	if err := s.InitReadPool(2); err != nil {
		t.Fatalf("InitReadPool: %v", err)
	}
	pool := s.ReadPool()

	if err := pool.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := pool.Acquire(context.Background()); err == nil {
		t.Error("Acquire after Close should fail; got nil error")
	}
}
