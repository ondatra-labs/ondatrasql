// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build e2e

package e2e

import (
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// TestE2E_CDC_MergeToAppend tests that CDC correctly detects changes
// when upstream is merge (INSERT/UPDATE/DELETE) and downstream is append.
func TestE2E_CDC_MergeToAppend(t *testing.T) {
	p := testutil.NewProject(t)

	// Create source table via merge
	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.source AS SELECT * FROM (VALUES (1,'Alice',100),(2,'Bob',200)) AS t(id,customer,amount)")

	p.AddModel("raw/orders.sql", `-- @kind: merge
-- @unique_key: id
SELECT * FROM raw.source
`)

	p.AddModel("mart/summary.sql", `-- @kind: append
SELECT customer, SUM(amount) AS total FROM raw.orders GROUP BY customer
`)

	// Run 1: backfill both
	r1_raw := runModel(t, p, "raw/orders.sql")
	r1_mart := runModel(t, p, "mart/summary.sql")

	// Update source: Bob 200→250, add Carol
	p.Sess.Exec("UPDATE raw.source SET amount=250 WHERE customer='Bob'")
	p.Sess.Exec("INSERT INTO raw.source VALUES (3, 'Carol', 300)")

	// Run 2: merge detects changes, append gets CDC
	r2_raw := runModel(t, p, "raw/orders.sql")
	r2_mart := runModel(t, p, "mart/summary.sql")

	// Run 3: no changes
	r3_mart := runModel(t, p, "mart/summary.sql")

	snap := newSnapshot()
	snap.addLine("--- run 1 (backfill) ---")
	snap.addResult(r1_raw)
	snap.addResult(r1_mart)
	snap.addLine("--- run 2 (CDC: Bob changed + Carol new) ---")
	snap.addResult(r2_raw)
	snap.addResult(r2_mart)
	snap.addLine("--- run 3 (no changes) ---")
	snap.addResult(r3_mart)
	snap.addQuery(p.Sess, "total_rows", "SELECT COUNT(*) FROM mart.summary")
	snap.addQueryRows(p.Sess, "data", "SELECT customer, total FROM mart.summary ORDER BY customer, total")
	assertGolden(t, "cdc_merge_to_append", snap)
}

// TestE2E_CDC_MergeTableAppend tests CDC through a table layer:
// merge → table → append
func TestE2E_CDC_MergeTableAppend(t *testing.T) {
	p := testutil.NewProject(t)

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.source AS SELECT * FROM (VALUES (1,'Alice',100),(2,'Bob',200)) AS t(id,customer,amount)")

	p.AddModel("raw/orders.sql", `-- @kind: merge
-- @unique_key: id
SELECT * FROM raw.source
`)

	p.AddModel("staging/clean.sql", `-- @kind: table
SELECT id, LOWER(customer) AS customer, amount FROM raw.orders
`)

	p.AddModel("mart/revenue.sql", `-- @kind: append
SELECT customer, SUM(amount) AS total FROM staging.clean GROUP BY customer
`)

	r1_raw := runModel(t, p, "raw/orders.sql")
	runModel(t, p, "staging/clean.sql")
	r1_mart := runModel(t, p, "mart/revenue.sql")

	// Update source
	p.Sess.Exec("UPDATE raw.source SET amount=250 WHERE customer='Bob'")
	p.Sess.Exec("INSERT INTO raw.source VALUES (3, 'Carol', 300)")

	r2_raw := runModel(t, p, "raw/orders.sql")
	runModel(t, p, "staging/clean.sql")
	r2_mart := runModel(t, p, "mart/revenue.sql")

	snap := newSnapshot()
	snap.addLine("--- run 1 (backfill) ---")
	snap.addResult(r1_raw)
	snap.addResult(r1_mart)
	snap.addLine("--- run 2 (CDC through table) ---")
	snap.addResult(r2_raw)
	snap.addResult(r2_mart)
	snap.addQuery(p.Sess, "total_rows", "SELECT COUNT(*) FROM mart.revenue")
	snap.addQueryRows(p.Sess, "data", "SELECT customer, total FROM mart.revenue ORDER BY customer, total")
	assertGolden(t, "cdc_merge_table_append", snap)
}

// TestE2E_CDC_TrackedToAppend tests CDC when upstream is tracked kind.
func TestE2E_CDC_TrackedToAppend(t *testing.T) {
	p := testutil.NewProject(t)

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.source AS SELECT * FROM (VALUES (1,'Alice',100),(2,'Bob',200)) AS t(id,customer,amount)")

	p.AddModel("raw/orders.sql", `-- @kind: tracked
-- @unique_key: id
SELECT * FROM raw.source
`)

	p.AddModel("mart/totals.sql", `-- @kind: append
SELECT customer, SUM(amount) AS total FROM raw.orders GROUP BY customer
`)

	r1_raw := runModel(t, p, "raw/orders.sql")
	r1_mart := runModel(t, p, "mart/totals.sql")

	// Update source
	p.Sess.Exec("UPDATE raw.source SET amount=250 WHERE customer='Bob'")
	p.Sess.Exec("INSERT INTO raw.source VALUES (3, 'Carol', 300)")

	r2_raw := runModel(t, p, "raw/orders.sql")
	r2_mart := runModel(t, p, "mart/totals.sql")

	// No changes
	r3_mart := runModel(t, p, "mart/totals.sql")

	snap := newSnapshot()
	snap.addLine("--- run 1 (backfill) ---")
	snap.addResult(r1_raw)
	snap.addResult(r1_mart)
	snap.addLine("--- run 2 (CDC: Bob changed + Carol new) ---")
	snap.addResult(r2_raw)
	snap.addResult(r2_mart)
	snap.addLine("--- run 3 (no changes) ---")
	snap.addResult(r3_mart)
	snap.addQuery(p.Sess, "total_rows", "SELECT COUNT(*) FROM mart.totals")
	snap.addQueryRows(p.Sess, "data", "SELECT customer, total FROM mart.totals ORDER BY customer, total")
	assertGolden(t, "cdc_tracked_to_append", snap)
}

// TestE2E_CDC_NoChangesSkip tests that CDC correctly skips when nothing changed.
func TestE2E_CDC_NoChangesSkip(t *testing.T) {
	p := testutil.NewProject(t)

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.source AS SELECT * FROM (VALUES (1,'Alice',100)) AS t(id,customer,amount)")

	p.AddModel("raw/orders.sql", `-- @kind: merge
-- @unique_key: id
SELECT * FROM raw.source
`)

	p.AddModel("mart/agg.sql", `-- @kind: append
SELECT customer, SUM(amount) AS total FROM raw.orders GROUP BY customer
`)

	runModel(t, p, "raw/orders.sql")
	runModel(t, p, "mart/agg.sql")

	// Run again — no data changes, no SQL changes
	runModel(t, p, "raw/orders.sql")
	r2 := runModel(t, p, "mart/agg.sql")

	snap := newSnapshot()
	snap.addLine("--- run 2 (no upstream changes) ---")
	snap.addResult(r2)
	snap.addQuery(p.Sess, "total_rows", "SELECT COUNT(*) FROM mart.agg")
	assertGolden(t, "cdc_no_changes_skip", snap)
}
