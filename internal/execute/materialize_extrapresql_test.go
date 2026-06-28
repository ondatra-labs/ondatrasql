// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute

import (
	"testing"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// extraPreSQL carries statements that MUST run inside the same commit
// transaction as the write — chiefly the ack INSERTs for lib-call state-store
// claims (script.AckSQL → _ondatra_acks), so a claim is acked atomically with
// the data it produced. The table/append/merge path, the SCD2-incremental
// path, and the tracked-backfill/incremental paths all prepend extraPreSQL;
// the SCD2 create, SCD2 backfill, and tracked create branches historically did
// NOT, silently dropping the ack on those paths. A lib-call SCD2/tracked model
// that backfilled (first run, or a kind/SQL-hash-forced backfill) therefore
// never acked its claims, and the next run re-delivered them (dedup drift).
//
// These tests call the materialize functions directly with a marker statement
// as extraPreSQL and assert it executed — pinning that every backfill/create
// branch threads extraPreSQL into its transaction. They fail without the fix.

// markerCount returns the row count of the stand-in ack table that extraPreSQL
// writes into.
func markerCount(t *testing.T, p *testutil.Project) string {
	t.Helper()
	got, err := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.acks_marker")
	if err != nil {
		t.Fatalf("query marker: %v", err)
	}
	return got
}

// setupExtraPreSQLProbe creates the schema, a marker table (stand-in for
// _ondatra_acks), and a source temp table with one row.
func setupExtraPreSQLProbe(t *testing.T, p *testutil.Project) {
	t.Helper()
	if err := p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS staging"); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if err := p.Sess.Exec("CREATE TABLE staging.acks_marker (claim VARCHAR)"); err != nil {
		t.Fatalf("create marker: %v", err)
	}
	if err := p.Sess.Exec("CREATE TEMP TABLE tmp_src AS SELECT 1::BIGINT AS id, 100::BIGINT AS val"); err != nil {
		t.Fatalf("create tmp_src: %v", err)
	}
}

func TestMaterializeSCD2_ThreadsExtraPreSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	setupExtraPreSQLProbe(t, p)
	runner := NewRunner(p.Sess, ModeRun, "test")
	model := &parser.Model{Target: "staging.dim", Kind: "scd2", UniqueKey: "id", SQL: "SELECT 1 AS id"}

	// First call → SCD2 create path (target does not exist yet).
	res1 := &Result{Target: model.Target}
	if _, err := runner.materializeSCD2(model, "tmp_src", true, "", "", "hash", "backfill", res1, time.Now(),
		"INSERT INTO staging.acks_marker VALUES ('create')"); err != nil {
		t.Fatalf("scd2 create: %v", err)
	}
	if got := markerCount(t, p); got != "1" {
		t.Errorf("SCD2 create path dropped extraPreSQL: marker count = %s, want 1", got)
	}

	// Second call → SCD2 backfill path (target now exists, isBackfill=true).
	res2 := &Result{Target: model.Target}
	if _, err := runner.materializeSCD2(model, "tmp_src", true, "", "", "hash", "backfill", res2, time.Now(),
		"INSERT INTO staging.acks_marker VALUES ('backfill')"); err != nil {
		t.Fatalf("scd2 backfill: %v", err)
	}
	if got := markerCount(t, p); got != "2" {
		t.Errorf("SCD2 backfill path dropped extraPreSQL: marker count = %s, want 2", got)
	}
}

func TestMaterializeTracked_ThreadsExtraPreSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	setupExtraPreSQLProbe(t, p)
	runner := NewRunner(p.Sess, ModeRun, "test")
	model := &parser.Model{Target: "staging.trk", Kind: "tracked", GroupKey: "id", SQL: "SELECT 1 AS id"}

	// Tracked create path (target does not exist yet).
	res := &Result{Target: model.Target}
	if _, err := runner.materializeTracked(model, "tmp_src", true, "", "", "hash", "backfill", res, time.Now(), trackedRunOpts{},
		"INSERT INTO staging.acks_marker VALUES ('create')"); err != nil {
		t.Fatalf("tracked create: %v", err)
	}
	if got := markerCount(t, p); got != "1" {
		t.Errorf("tracked create path dropped extraPreSQL: marker count = %s, want 1", got)
	}
}
