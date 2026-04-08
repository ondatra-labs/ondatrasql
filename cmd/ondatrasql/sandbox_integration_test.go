// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package main

import (
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
)

// TestTableExistsIn_RealSession verifies tableExistsIn against a live
// in-memory DuckDB session. Covers all three meaningful return shapes:
//
//   - existing table  → (true, nil)
//   - missing table   → (false, nil)
//   - lookup failure  → (false, error)  ← critical: must NOT collapse to false
//
// The earlier helper returned a single bool and lost the lookup-failure
// signal entirely, mis-classifying network/permission/syntax errors as
// "table doesn't exist" and feeding them to the downstream "new table"
// diff path. This test pins the (bool, error) contract.
func TestTableExistsIn_RealSession(t *testing.T) {
	sess, err := duckdb.NewSession(":memory:?threads=1&memory_limit=256MB")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer sess.Close()

	// Set up two schemas with tables sharing the same name to also exercise
	// the schema-qualification half of the fix (see Bug 2 follow-up).
	if err := sess.Exec("CREATE SCHEMA raw"); err != nil {
		t.Fatalf("create schema raw: %v", err)
	}
	if err := sess.Exec("CREATE SCHEMA staging"); err != nil {
		t.Fatalf("create schema staging: %v", err)
	}
	if err := sess.Exec("CREATE TABLE raw.orders (id INT, region VARCHAR)"); err != nil {
		t.Fatalf("create raw.orders: %v", err)
	}
	if err := sess.Exec("CREATE TABLE staging.events (event_id INT)"); err != nil {
		t.Fatalf("create staging.events: %v", err)
	}

	// In-memory DuckDB exposes a single catalog called "memory".
	const cat = "memory"

	t.Run("existing table returns (true, nil)", func(t *testing.T) {
		exists, err := tableExistsIn(sess, cat, "raw.orders")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !exists {
			t.Error("expected raw.orders to exist")
		}
	})

	t.Run("existing table in other schema returns (true, nil)", func(t *testing.T) {
		exists, err := tableExistsIn(sess, cat, "staging.events")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !exists {
			t.Error("expected staging.events to exist")
		}
	})

	t.Run("missing table returns (false, nil)", func(t *testing.T) {
		exists, err := tableExistsIn(sess, cat, "raw.does_not_exist")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if exists {
			t.Error("expected raw.does_not_exist to NOT exist")
		}
	})

	t.Run("missing schema returns (false, nil)", func(t *testing.T) {
		exists, err := tableExistsIn(sess, cat, "ghost.orders")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if exists {
			t.Error("expected ghost.orders to NOT exist")
		}
	})

	t.Run("schema disambiguation: same name in different schema is missing", func(t *testing.T) {
		// raw.orders exists, staging.orders doesn't — must return false
		// for staging.orders, NOT mix in raw.orders just because the
		// table_name matches. This is the Bug 2 cross-schema collision.
		exists, err := tableExistsIn(sess, cat, "staging.orders")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if exists {
			t.Error("staging.orders must NOT match raw.orders (cross-schema collision)")
		}
	})

	t.Run("missing catalog returns (false, nil)", func(t *testing.T) {
		// Catalog filter is case-sensitive in information_schema; an
		// unknown catalog yields zero rows, not an error.
		exists, err := tableExistsIn(sess, "nonexistent_catalog", "raw.orders")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if exists {
			t.Error("expected zero rows for unknown catalog")
		}
	})

	t.Run("invalid target shape returns error", func(t *testing.T) {
		_, err := tableExistsIn(sess, cat, "no_separator")
		if err == nil {
			t.Fatal("expected error for missing schema.table separator")
		}
		if !strings.Contains(err.Error(), "schema.table") {
			t.Errorf("error should mention expected format, got: %v", err)
		}
	})
}

// TestTableExistsIn_ClosedSession verifies the lookup-failure path.
// A closed session must surface the error rather than collapse to
// (false, nil) which would mis-classify the failure as "table missing".
func TestTableExistsIn_ClosedSession(t *testing.T) {
	sess, err := duckdb.NewSession(":memory:?threads=1&memory_limit=256MB")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if err := sess.Exec("CREATE SCHEMA raw"); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if err := sess.Exec("CREATE TABLE raw.t (a INT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	sess.Close()

	exists, err := tableExistsIn(sess, "memory", "raw.t")
	if err == nil {
		t.Fatal("expected error from closed session, got nil")
	}
	if exists {
		t.Errorf("expected exists=false on error, got true")
	}
}
