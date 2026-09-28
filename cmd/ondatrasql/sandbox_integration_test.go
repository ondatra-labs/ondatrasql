// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package main

import (
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
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

// TestShowDagSandboxSummary_SCD2ClosedVersionIsChange pins that the DAG
// summary counts an scd2 model as changed when the sandbox only closed a
// current version. Closing adds no row, so the total matches prod and the
// sandbox-minus-prod diff of current versions is empty; only the
// prod-minus-sandbox direction sees it.
func TestShowDagSandboxSummary_SCD2ClosedVersionIsChange(t *testing.T) {
	prod := testutil.NewProject(t)
	for _, q := range []string{
		"CREATE SCHEMA IF NOT EXISTS staging",
		"CREATE TABLE staging.hist (id INTEGER, name VARCHAR, valid_from_snapshot BIGINT, valid_to_snapshot BIGINT, is_current BOOLEAN)",
		"INSERT INTO staging.hist VALUES (1, 'a', 1, NULL, true), (2, 'b', 1, NULL, true)",
	} {
		if err := prod.Sess.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	sbox := testutil.NewSandboxProject(t, prod)
	if err := sbox.Sess.Exec("UPDATE staging.hist SET valid_to_snapshot = 2, is_current = false WHERE id = 2"); err != nil {
		t.Fatalf("close version in sandbox: %v", err)
	}

	out := captureOutput(t, func() {
		showDagSandboxSummary(sbox.Sess, []*parser.Model{{Target: "staging.hist", Kind: "scd2"}}, nil)
	})
	if !strings.Contains(out, "Changed:   1 models") {
		t.Errorf("closed scd2 version not counted as a change:\n%s", out)
	}
}
