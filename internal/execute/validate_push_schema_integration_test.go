// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute_test

import (
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// --- v0.30.0 strict-push validator (fas 6) ---
//
// Push is asymmetric to fetch: SQL is the shape authority, so JOIN /
// WHERE / GROUP BY / DISTINCT / ORDER BY / derived expressions are all
// allowed. Only the outer projection is validated (cast + alias, no
// SELECT *, no lib calls in FROM).

func setupPushLib(t *testing.T) *testutil.Project {
	t.Helper()
	p := testutil.NewProject(t)
	writeLib(t, p, "psink", `
API = {
    "push": {"batch_size": 100, "batch_mode": "atomic"},
}

def push(rows=[], batch_number=1, kind="", key_columns=[], columns=[]):
    pass
`)
	if err := p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS staging"); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if err := p.Sess.Exec("CREATE TABLE staging.contacts AS SELECT 1::BIGINT AS id, 'Alice' AS name, 'a@example.com' AS email, 'Acme' AS company"); err != nil {
		t.Fatalf("create staging.contacts: %v", err)
	}
	return p
}

func TestStrictPush_RejectsSelectStar(t *testing.T) {
	p := setupPushLib(t)

	p.AddModel("sync/star.sql", `-- @kind: merge
-- @unique_key: id
-- @push: psink

SELECT * FROM staging.contacts
`)

	_, err := runModelWithLibErr(t, p, "sync/star.sql")
	if err == nil {
		t.Fatal("@push with SELECT * should be rejected")
	}
	if !strings.Contains(err.Error(), "SELECT *") {
		t.Errorf("error should mention SELECT *, got: %v", err)
	}
}

func TestStrictPush_RejectsBareProjection(t *testing.T) {
	p := setupPushLib(t)

	p.AddModel("sync/bare.sql", `-- @kind: merge
-- @unique_key: id
-- @push: psink

SELECT id, name FROM staging.contacts
`)

	_, err := runModelWithLibErr(t, p, "sync/bare.sql")
	if err == nil {
		t.Fatal("@push with bare projection should be rejected")
	}
	if !strings.Contains(err.Error(), "not cast") {
		t.Errorf("error should mention missing cast, got: %v", err)
	}
}

func TestStrictPush_RejectsCastWithoutAlias(t *testing.T) {
	p := setupPushLib(t)

	p.AddModel("sync/noalias.sql", `-- @kind: merge
-- @unique_key: id
-- @push: psink

SELECT id::BIGINT, name::VARCHAR FROM staging.contacts
`)

	_, err := runModelWithLibErr(t, p, "sync/noalias.sql")
	if err == nil {
		t.Fatal("@push CAST without AS alias should be rejected")
	}
	if !strings.Contains(err.Error(), "no explicit alias") {
		t.Errorf("error should mention missing alias, got: %v", err)
	}
}

// Push allows derived expressions inside CAST — different from @fetch.
func TestStrictPush_AcceptsConcatInCast(t *testing.T) {
	p := setupPushLib(t)
	if err := p.Sess.Exec("CREATE TABLE staging.subs AS SELECT 1::BIGINT AS id, 'Alice' AS first_name, 'Smith' AS last_name"); err != nil {
		t.Fatalf("create staging.subs: %v", err)
	}

	p.AddModel("sync/derived.sql", `-- @kind: append
-- @push: psink

SELECT
    id::BIGINT AS subscriber_id,
    CONCAT(first_name, ' ', last_name)::VARCHAR AS display_name
FROM staging.subs
`)

	r, err := runModelWithLibErr(t, p, "sync/derived.sql")
	if err != nil {
		t.Fatalf("@push with derived expression should be accepted: %v", err)
	}
	if r.RowsAffected != 1 {
		t.Errorf("expected 1 row, got %d", r.RowsAffected)
	}
}

// Push allows JOIN — different from @fetch.
func TestStrictPush_AcceptsJoin(t *testing.T) {
	p := setupPushLib(t)
	if err := p.Sess.Exec("CREATE TABLE staging.orders AS SELECT 1::BIGINT AS customer_id, 100::BIGINT AS total"); err != nil {
		t.Fatalf("create staging.orders: %v", err)
	}

	p.AddModel("sync/join.sql", `-- @kind: table
-- @push: psink

SELECT
    c.id::BIGINT AS customer_id,
    c.email::VARCHAR AS email,
    SUM(o.total)::DECIMAL(18,2) AS lifetime_value
FROM staging.contacts c
LEFT JOIN staging.orders o ON c.id = o.customer_id
GROUP BY c.id, c.email
`)

	r, err := runModelWithLibErr(t, p, "sync/join.sql")
	if err != nil {
		t.Fatalf("@push with JOIN + GROUP BY should be accepted: %v", err)
	}
	if r.RowsAffected != 1 {
		t.Errorf("expected 1 row, got %d", r.RowsAffected)
	}
}

// Push allows WHERE — different from @fetch.
func TestStrictPush_AcceptsWhere(t *testing.T) {
	p := setupPushLib(t)

	p.AddModel("sync/filtered.sql", `-- @kind: append
-- @push: psink

SELECT id::BIGINT AS id, name::VARCHAR AS name
FROM staging.contacts
WHERE id > 0
`)

	r, err := runModelWithLibErr(t, p, "sync/filtered.sql")
	if err != nil {
		t.Fatalf("@push with WHERE should be accepted: %v", err)
	}
	if r.RowsAffected != 1 {
		t.Errorf("expected 1 row, got %d", r.RowsAffected)
	}
}
