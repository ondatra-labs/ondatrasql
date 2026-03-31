// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package testutil

import (
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/sql"
)

// NewSession creates an in-memory DuckDB session with t.Cleanup.
func NewSession(t *testing.T) *duckdb.Session {
	t.Helper()
	sess, err := duckdb.NewSession(":memory:")
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	t.Cleanup(func() { sess.Close() })
	return sess
}

// NewSessionWithMacros creates an in-memory DuckDB session with schema macros loaded.
func NewSessionWithMacros(t *testing.T) *duckdb.Session {
	t.Helper()
	sess := NewSession(t)

	macros, err := sql.Load("macros/schema.sql")
	if err != nil {
		t.Fatalf("load schema macros: %v", err)
	}
	if err := sess.Exec(macros); err != nil {
		t.Fatalf("exec schema macros: %v", err)
	}
	return sess
}
