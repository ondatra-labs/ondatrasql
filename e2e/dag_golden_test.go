// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build e2e

package e2e

import (
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// TestE2E_DAG_AllKinds_MultiRun tests a full DAG with all kinds across 6 runs:
// backfill → no changes → data change → add column → drop column → stability
func TestE2E_DAG_AllKinds_MultiRun(t *testing.T) {
	p := testutil.NewProject(t)

	addAllKindModels(t, p,
		`SELECT 1 AS id, 'Alice' AS customer, 'SE' AS region, 100 AS amount
UNION ALL SELECT 2, 'Bob', 'US', 200
UNION ALL SELECT 3, 'Charlie', 'SE', 150
UNION ALL SELECT 4, 'Diana', 'US', 300`)

	snap := newSnapshot()

	// RUN 1: backfill
	snap.addLine("=== RUN 1: backfill ===")
	runAllKinds(t, p, snap)
	verifyAllKindCounts(t, p, snap, "run1")

	// RUN 2: no changes
	snap.addLine("=== RUN 2: no changes ===")
	runAllKinds(t, p, snap)
	verifyAllKindCounts(t, p, snap, "run2")

	// RUN 3: data change (id=1 amount 100→150, id=3,4 deleted, id=5 new)
	snap.addLine("=== RUN 3: data change ===")
	p.AddModel("raw/orders.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS customer, 'SE' AS region, 150 AS amount
UNION ALL SELECT 2, 'Bob', 'US', 200
UNION ALL SELECT 5, 'Eve', 'NO', 500
`)
	runAllKinds(t, p, snap)
	verifyAllKindCounts(t, p, snap, "run3")
	verifyAllKindData(t, p, snap, "run3")

	// RUN 4: add column
	snap.addLine("=== RUN 4: add column ===")
	p.AddModel("raw/orders.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS customer, 'SE' AS region, 150 AS amount, 'web' AS channel
UNION ALL SELECT 2, 'Bob', 'US', 200, 'store'
UNION ALL SELECT 5, 'Eve', 'NO', 500, 'web'
`)
	runAllKinds(t, p, snap)
	verifyAllKindCounts(t, p, snap, "run4")
	snap.addQuery(p.Sess, "run4_raw_channel", "SELECT COUNT(*) FROM raw.orders WHERE channel IS NOT NULL")

	// RUN 5: drop column
	snap.addLine("=== RUN 5: drop column ===")
	p.AddModel("raw/orders.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS customer, 'SE' AS region, 150 AS amount
UNION ALL SELECT 2, 'Bob', 'US', 200
UNION ALL SELECT 5, 'Eve', 'NO', 500
`)
	runAllKinds(t, p, snap)
	verifyAllKindCounts(t, p, snap, "run5")

	// RUN 6: stability
	snap.addLine("=== RUN 6: stability ===")
	runAllKinds(t, p, snap)
	verifyAllKindCounts(t, p, snap, "run6")

	assertGolden(t, "dag_all_kinds_multi_run", snap)
}

// TestE2E_DAG_SchemaEvolution_SCD2History tests that SCD2 correctly maintains
// history across schema evolution (add + drop column).
func TestE2E_DAG_SchemaEvolution_SCD2History(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/users.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name, 100 AS score
UNION ALL SELECT 2, 'Bob', 200
`)
	p.AddModel("staging/v.sql", `-- @kind: table
SELECT * FROM raw.users
`)
	p.AddModel("mart/history.sql", `-- @kind: scd2
-- @unique_key: id
SELECT * FROM staging.v
`)

	snap := newSnapshot()

	// RUN 1: backfill
	snap.addLine("=== RUN 1: backfill ===")
	runModel(t, p, "raw/users.sql")
	runModel(t, p, "staging/v.sql")
	r1 := runModel(t, p, "mart/history.sql")
	snap.addResult(r1)

	// RUN 2: change data
	snap.addLine("=== RUN 2: data change ===")
	p.AddModel("raw/users.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name, 150 AS score
UNION ALL SELECT 2, 'Bob', 200
`)
	runModel(t, p, "raw/users.sql")
	r2 := runModel(t, p, "mart/history.sql")
	snap.addResult(r2)

	// RUN 3: add column
	snap.addLine("=== RUN 3: add column ===")
	p.AddModel("raw/users.sql", `-- @kind: table
SELECT 1 AS id, 'Alice' AS name, 150 AS score, 'SE' AS country
UNION ALL SELECT 2, 'Bob', 200, 'US'
`)
	runModel(t, p, "raw/users.sql")
	r3 := runModel(t, p, "mart/history.sql")
	snap.addResult(r3)

	// RUN 4: no changes
	snap.addLine("=== RUN 4: no changes ===")
	r4 := runModel(t, p, "mart/history.sql")
	snap.addResult(r4)

	// Verify SCD2 history (sort by is_current DESC so current row is always first per id)
	snap.addQueryRows(p.Sess, "scd2_history",
		"SELECT id, name, score, country, is_current FROM mart.history ORDER BY id, is_current DESC, score")
	snap.addQuery(p.Sess, "total_rows", "SELECT COUNT(*) FROM mart.history")
	snap.addQuery(p.Sess, "current_rows", "SELECT COUNT(*) FROM mart.history WHERE is_current IS true")

	assertGolden(t, "dag_schema_evolution_scd2", snap)
}

// TestE2E_DAG_SchemaEvolution_TrackedConsistency tests that tracked kind
// correctly handles schema evolution while maintaining content hashes.
func TestE2E_DAG_SchemaEvolution_TrackedConsistency(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/items.sql", `-- @kind: table
SELECT 1 AS id, 'Widget' AS name, 10 AS qty
UNION ALL SELECT 2, 'Gadget', 20
`)
	p.AddModel("mart/inventory.sql", `-- @kind: tracked
-- @unique_key: id
SELECT * FROM raw.items
`)

	snap := newSnapshot()

	// RUN 1: backfill
	snap.addLine("=== RUN 1: backfill ===")
	runModel(t, p, "raw/items.sql")
	r1 := runModel(t, p, "mart/inventory.sql")
	snap.addResult(r1)

	// RUN 2: no changes
	snap.addLine("=== RUN 2: no changes ===")
	r2 := runModel(t, p, "mart/inventory.sql")
	snap.addResult(r2)

	// RUN 3: add column
	snap.addLine("=== RUN 3: add column ===")
	p.AddModel("raw/items.sql", `-- @kind: table
SELECT 1 AS id, 'Widget' AS name, 10 AS qty, 'A' AS warehouse
UNION ALL SELECT 2, 'Gadget', 20, 'B'
`)
	runModel(t, p, "raw/items.sql")
	r3 := runModel(t, p, "mart/inventory.sql")
	snap.addResult(r3)

	// RUN 4: change data
	snap.addLine("=== RUN 4: data change ===")
	p.AddModel("raw/items.sql", `-- @kind: table
SELECT 1 AS id, 'Widget' AS name, 15 AS qty, 'A' AS warehouse
UNION ALL SELECT 2, 'Gadget', 20, 'B'
UNION ALL SELECT 3, 'Doohickey', 5, 'A'
`)
	runModel(t, p, "raw/items.sql")
	r4 := runModel(t, p, "mart/inventory.sql")
	snap.addResult(r4)

	// RUN 5: no changes
	snap.addLine("=== RUN 5: no changes ===")
	r5 := runModel(t, p, "mart/inventory.sql")
	snap.addResult(r5)

	snap.addQueryRows(p.Sess, "tracked_data",
		"SELECT id, name, qty, warehouse FROM mart.inventory ORDER BY id")
	snap.addQuery(p.Sess, "row_count", "SELECT COUNT(*) FROM mart.inventory")

	assertGolden(t, "dag_schema_evolution_tracked", snap)
}

// --- helpers ---

func addAllKindModels(t *testing.T, p *testutil.Project, rawSQL string) {
	t.Helper()
	p.AddModel("raw/orders.sql", "-- @kind: table\n"+rawSQL+"\n")
	p.AddModel("staging/orders_v.sql", "-- @kind: table\nSELECT * FROM raw.orders\n")
	p.AddModel("mart/orders_append.sql", "-- @kind: append\nSELECT * FROM staging.orders_v\n")
	p.AddModel("mart/orders_merge.sql", "-- @kind: merge\n-- @unique_key: id\nSELECT * FROM staging.orders_v\n")
	p.AddModel("mart/orders_scd2.sql", "-- @kind: scd2\n-- @unique_key: id\nSELECT * FROM staging.orders_v\n")
	p.AddModel("mart/orders_partition.sql", "-- @kind: partition\n-- @unique_key: region\nSELECT * FROM staging.orders_v\n")
	p.AddModel("mart/orders_tracked.sql", "-- @kind: tracked\n-- @unique_key: id\nSELECT * FROM staging.orders_v\n")
}

func runAllKinds(t *testing.T, p *testutil.Project, snap *goldenSnapshot) {
	t.Helper()
	for _, m := range []string{
		"raw/orders.sql", "staging/orders_v.sql",
		"mart/orders_append.sql", "mart/orders_merge.sql",
		"mart/orders_scd2.sql", "mart/orders_partition.sql",
		"mart/orders_tracked.sql",
	} {
		r := runModel(t, p, m)
		snap.addResult(r)
	}
}

func verifyAllKindCounts(t *testing.T, p *testutil.Project, snap *goldenSnapshot, prefix string) {
	t.Helper()
	snap.addQuery(p.Sess, prefix+"_raw", "SELECT COUNT(*) FROM raw.orders")
	snap.addQuery(p.Sess, prefix+"_append", "SELECT COUNT(*) FROM mart.orders_append")
	snap.addQuery(p.Sess, prefix+"_merge", "SELECT COUNT(*) FROM mart.orders_merge")
	snap.addQuery(p.Sess, prefix+"_partition", "SELECT COUNT(*) FROM mart.orders_partition")
	snap.addQuery(p.Sess, prefix+"_tracked", "SELECT COUNT(*) FROM mart.orders_tracked")
	snap.addQuery(p.Sess, prefix+"_scd2_total", "SELECT COUNT(*) FROM mart.orders_scd2")
	snap.addQuery(p.Sess, prefix+"_scd2_current", "SELECT COUNT(*) FROM mart.orders_scd2 WHERE is_current IS true")
}

func verifyAllKindData(t *testing.T, p *testutil.Project, snap *goldenSnapshot, prefix string) {
	t.Helper()
	snap.addQueryRows(p.Sess, prefix+"_raw_data", "SELECT id, customer, amount FROM raw.orders ORDER BY id")
	snap.addQueryRows(p.Sess, prefix+"_merge_data", "SELECT id, customer, amount FROM mart.orders_merge ORDER BY id")
	snap.addQueryRows(p.Sess, prefix+"_tracked_data", "SELECT id, customer, amount FROM mart.orders_tracked ORDER BY id")
	snap.addQueryRows(p.Sess, prefix+"_scd2_current", "SELECT id, customer, amount FROM mart.orders_scd2 WHERE is_current IS true ORDER BY id")
}
