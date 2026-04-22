// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build e2e

package e2e

import (
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

func TestE2E_TrackedKind(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/invoices.sql", `-- @kind: tracked
-- @unique_key: source_file

SELECT * FROM (VALUES
    ('inv_001.pdf', 'INV-001', 'Widget A', 10, 50.0),
    ('inv_001.pdf', 'INV-001', 'Widget B', 5, 120.0),
    ('inv_002.pdf', 'INV-002', 'Consulting', 8, 150.0)
) AS t(source_file, invoice_number, item, qty, price)
`)

	result := runModel(t, p, "raw/invoices.sql")

	snap := newSnapshot()
	snap.addResult(result)
	snap.addQuery(p.Sess, "row_count", "SELECT COUNT(*) FROM raw.invoices")
	snap.addQuery(p.Sess, "has_content_hash",
		"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='raw' AND table_name='invoices' AND column_name='_content_hash'")
	snap.addQuery(p.Sess, "group_count", "SELECT COUNT(DISTINCT _content_hash) FROM raw.invoices")
	// Verify same hash within group
	snap.addQuery(p.Sess, "inv001_hashes", "SELECT COUNT(DISTINCT _content_hash) FROM raw.invoices WHERE source_file='inv_001.pdf'")
	assertGolden(t, "tracked_kind", snap)
}

func TestE2E_TrackedKind_IncrementalSkip(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/data.sql", `-- @kind: tracked
-- @unique_key: id

SELECT * FROM (VALUES
    (1, 'Alice', 100),
    (2, 'Bob', 200)
) AS t(id, name, amount)
`)

	r1 := runModel(t, p, "raw/data.sql")
	r2 := runModel(t, p, "raw/data.sql")

	snap := newSnapshot()
	snap.addLine("--- run 1 (backfill) ---")
	snap.addResult(r1)
	snap.addLine("--- run 2 (skip) ---")
	snap.addResult(r2)
	snap.addQuery(p.Sess, "row_count", "SELECT COUNT(*) FROM raw.data")
	assertGolden(t, "tracked_incremental_skip", snap)
}

func TestE2E_TrackedKind_IncrementalChange(t *testing.T) {
	p := testutil.NewProject(t)

	// Create source table in DuckLake to simulate changing data
	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.source AS SELECT * FROM (VALUES (1,'Alice',100),(2,'Bob',200),(3,'Carol',300)) AS t(id,name,amount)")

	p.AddModel("raw/data.sql", `-- @kind: tracked
-- @unique_key: id

SELECT * FROM raw.source
`)

	r1 := runModel(t, p, "raw/data.sql")

	// Change Bob's amount in source
	p.Sess.Exec("UPDATE raw.source SET amount=999 WHERE name='Bob'")

	r2 := runModel(t, p, "raw/data.sql")

	snap := newSnapshot()
	snap.addLine("--- run 1 (backfill) ---")
	snap.addResult(r1)
	snap.addLine("--- run 2 (one change) ---")
	snap.addResult(r2)
	snap.addQuery(p.Sess, "row_count", "SELECT COUNT(*) FROM raw.data")
	snap.addQuery(p.Sess, "bob_amount", "SELECT amount FROM raw.data WHERE name='Bob'")
	snap.addQuery(p.Sess, "alice_amount", "SELECT amount FROM raw.data WHERE name='Alice'")
	assertGolden(t, "tracked_incremental_change", snap)
}

func TestE2E_TrackedKind_GroupTracking(t *testing.T) {
	p := testutil.NewProject(t)

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.source_items AS SELECT * FROM (VALUES ('A.pdf','Widget',50),('A.pdf','Service',100),('B.pdf','License',500)) AS t(file,item,price)")

	p.AddModel("raw/items.sql", `-- @kind: tracked
-- @unique_key: file

SELECT * FROM raw.source_items
`)

	r1 := runModel(t, p, "raw/items.sql")

	// Change one row in group A — both rows in group should be replaced
	p.Sess.Exec("UPDATE raw.source_items SET price=75 WHERE item='Widget'")

	r2 := runModel(t, p, "raw/items.sql")

	snap := newSnapshot()
	snap.addLine("--- run 1 (backfill 3 rows) ---")
	snap.addResult(r1)
	snap.addLine("--- run 2 (group A changed, 2 rows replaced) ---")
	snap.addResult(r2)
	snap.addQuery(p.Sess, "total_rows", "SELECT COUNT(*) FROM raw.items")
	snap.addQuery(p.Sess, "widget_price", "SELECT price FROM raw.items WHERE item='Widget'")
	snap.addQuery(p.Sess, "license_price", "SELECT price FROM raw.items WHERE item='License'")
	assertGolden(t, "tracked_group_tracking", snap)
}

func TestE2E_TrackedKind_NewGroup(t *testing.T) {
	p := testutil.NewProject(t)

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.source_users AS SELECT * FROM (VALUES (1,'Alice'),(2,'Bob')) AS t(id,name)")

	p.AddModel("raw/users.sql", `-- @kind: tracked
-- @unique_key: id

SELECT * FROM raw.source_users
`)

	r1 := runModel(t, p, "raw/users.sql")

	// Add new user, keep existing unchanged
	p.Sess.Exec("INSERT INTO raw.source_users VALUES (3, 'Carol')")

	r2 := runModel(t, p, "raw/users.sql")

	snap := newSnapshot()
	snap.addLine("--- run 1 (backfill) ---")
	snap.addResult(r1)
	snap.addLine("--- run 2 (new group) ---")
	snap.addResult(r2)
	snap.addQuery(p.Sess, "total_rows", "SELECT COUNT(*) FROM raw.users")
	assertGolden(t, "tracked_new_group", snap)
}

func TestE2E_TrackedKind_DeletedGroup(t *testing.T) {
	p := testutil.NewProject(t)

	p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS raw")
	p.Sess.Exec("CREATE TABLE raw.source_del AS SELECT * FROM (VALUES (1,'Alice',100),(2,'Bob',200),(3,'Carol',300)) AS t(id,name,amount)")

	p.AddModel("raw/data.sql", `-- @kind: tracked
-- @unique_key: id

SELECT * FROM raw.source_del
`)

	r1 := runModel(t, p, "raw/data.sql")

	// Delete Bob from source
	p.Sess.Exec("DELETE FROM raw.source_del WHERE name='Bob'")

	r2 := runModel(t, p, "raw/data.sql")

	snap := newSnapshot()
	snap.addLine("--- run 1 (backfill 3 rows) ---")
	snap.addResult(r1)
	snap.addLine("--- run 2 (Bob deleted) ---")
	snap.addResult(r2)
	snap.addQuery(p.Sess, "total_rows", "SELECT COUNT(*) FROM raw.data")
	snap.addQuery(p.Sess, "names", "SELECT string_agg(name, ',' ORDER BY name) FROM raw.data")
	assertGolden(t, "tracked_deleted_group", snap)
}

func TestE2E_TrackedKind_UniqueKeyOnly(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/keys.sql", `-- @kind: tracked
-- @unique_key: id

SELECT * FROM (VALUES (1), (2), (3)) AS t(id)
`)

	r1 := runModel(t, p, "raw/keys.sql")
	r2 := runModel(t, p, "raw/keys.sql")

	snap := newSnapshot()
	snap.addLine("--- run 1 (backfill) ---")
	snap.addResult(r1)
	snap.addLine("--- run 2 (skip) ---")
	snap.addResult(r2)
	snap.addQuery(p.Sess, "row_count", "SELECT COUNT(*) FROM raw.keys")
	assertGolden(t, "tracked_unique_key_only", snap)
}

