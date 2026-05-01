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

// --- v0.30.0 strict-fetch validator (fas 6) ---
//
// Each negative test below pins one of the new rules added to the
// strict-fetch validator. Positive coverage is in the unchanged
// canonical-form fixtures throughout libcall_badger_integration_test.go.

func setupFetchLib(t *testing.T) *testutil.Project {
	t.Helper()
	p := testutil.NewProject(t)
	writeLib(t, p, "fapi", `
API = {
    "base_url": "https://example.com",
    "fetch": {"args": ["resource"]},
}

def fetch(resource, page):
    return {"rows": [{"id": 1, "name": "a"}], "next": None}
`)
	return p
}

func TestStrictFetch_RejectsCastWithFunctionCall(t *testing.T) {
	p := setupFetchLib(t)

	p.AddModel("raw/computed.sql", `-- @kind: table
-- @fetch
SELECT UPPER(name)::VARCHAR AS name FROM fapi('items')
`)

	_, err := runModelWithLibErr(t, p, "raw/computed.sql")
	if err == nil {
		t.Fatal("@fetch projection with function-call cast child should be rejected")
	}
	if !strings.Contains(err.Error(), "must be `<col>::TYPE AS alias`") {
		t.Errorf("error should explain the bare COLUMN_REF rule, got: %v", err)
	}
}

func TestStrictFetch_RejectsCastWithArithmetic(t *testing.T) {
	p := setupFetchLib(t)

	p.AddModel("raw/arith.sql", `-- @kind: table
-- @fetch
SELECT (id * 2)::BIGINT AS id FROM fapi('items')
`)

	_, err := runModelWithLibErr(t, p, "raw/arith.sql")
	if err == nil {
		t.Fatal("@fetch projection with arithmetic in cast should be rejected")
	}
	if !strings.Contains(err.Error(), "derived expressions") {
		t.Errorf("error should mention derived expressions, got: %v", err)
	}
}

func TestStrictFetch_AcceptsQualifiedColumnRefInCast(t *testing.T) {
	p := setupFetchLib(t)

	// `f.name` is a qualified COLUMN_REF — still a COLUMN_REF, must pass.
	p.AddModel("raw/qualified.sql", `-- @kind: table
-- @fetch
SELECT f.id::BIGINT AS id, f.name::VARCHAR AS name FROM fapi('items') f
`)

	r := runModelWithLib(t, p, "raw/qualified.sql")
	if r.RowsAffected != 1 {
		t.Errorf("expected 1 row, got %d", r.RowsAffected)
	}
}

func TestStrictFetch_RejectsJoin(t *testing.T) {
	p := setupFetchLib(t)
	writeLib(t, p, "fapi2", `
API = {
    "base_url": "https://example.com",
    "fetch": {"args": ["resource"]},
}

def fetch(resource, page):
    return {"rows": [{"id": 1, "extra": "x"}], "next": None}
`)

	p.AddModel("raw/joined.sql", `-- @kind: table
-- @fetch
SELECT a.id::BIGINT AS id, a.name::VARCHAR AS name, b.extra::VARCHAR AS extra
FROM fapi('items') a
JOIN fapi2('extras') b ON a.id = b.id
`)

	_, err := runModelWithLibErr(t, p, "raw/joined.sql")
	if err == nil {
		t.Fatal("@fetch with JOIN should be rejected")
	}
	if !strings.Contains(err.Error(), "JOIN is not allowed") {
		t.Errorf("error should mention JOIN restriction, got: %v", err)
	}
}

func TestStrictFetch_RejectsBaseTableInFrom(t *testing.T) {
	p := testutil.NewProject(t)
	if err := p.Sess.Exec("CREATE TABLE plain.t AS SELECT 1::BIGINT AS id, 'a' AS name"); err != nil {
		t.Skipf("create table failed (might need schema): %v", err)
	}

	p.AddModel("raw/notlib.sql", `-- @kind: table
-- @fetch
SELECT id::BIGINT AS id FROM plain.t
`)

	_, err := runModelWithLibErr(t, p, "raw/notlib.sql")
	if err == nil {
		t.Fatal("@fetch with non-lib FROM should be rejected by relationship rule or strict-fetch")
	}
	// Could be rejected by either the @fetch-needs-lib relationship rule
	// (phase 5) or the FROM-must-be-TABLE_FUNCTION rule (phase 6).
	// Both are valid v0.30.0 rejections.
	if !strings.Contains(err.Error(), "lib") {
		t.Errorf("error should mention lib (either no-lib-call or non-lib FROM), got: %v", err)
	}
}

func TestStrictFetch_RejectsWhere(t *testing.T) {
	p := setupFetchLib(t)

	p.AddModel("raw/filtered.sql", `-- @kind: table
-- @fetch
SELECT id::BIGINT AS id FROM fapi('items') WHERE id > 0
`)

	_, err := runModelWithLibErr(t, p, "raw/filtered.sql")
	if err == nil {
		t.Fatal("@fetch with WHERE should be rejected")
	}
	if !strings.Contains(err.Error(), "WHERE") {
		t.Errorf("error should mention WHERE, got: %v", err)
	}
}

func TestStrictFetch_RejectsGroupBy(t *testing.T) {
	p := setupFetchLib(t)

	p.AddModel("raw/grouped.sql", `-- @kind: table
-- @fetch
SELECT id::BIGINT AS id FROM fapi('items') GROUP BY id
`)

	_, err := runModelWithLibErr(t, p, "raw/grouped.sql")
	if err == nil {
		t.Fatal("@fetch with GROUP BY should be rejected")
	}
	if !strings.Contains(err.Error(), "GROUP BY") {
		t.Errorf("error should mention GROUP BY, got: %v", err)
	}
}

func TestStrictFetch_RejectsDistinct(t *testing.T) {
	p := setupFetchLib(t)

	p.AddModel("raw/distinct.sql", `-- @kind: table
-- @fetch
SELECT DISTINCT id::BIGINT AS id FROM fapi('items')
`)

	_, err := runModelWithLibErr(t, p, "raw/distinct.sql")
	if err == nil {
		t.Fatal("@fetch with DISTINCT should be rejected")
	}
	if !strings.Contains(err.Error(), "DISTINCT") {
		t.Errorf("error should mention DISTINCT, got: %v", err)
	}
}

func TestStrictFetch_RejectsOrderBy(t *testing.T) {
	p := setupFetchLib(t)

	p.AddModel("raw/ordered.sql", `-- @kind: table
-- @fetch
SELECT id::BIGINT AS id FROM fapi('items') ORDER BY id
`)

	_, err := runModelWithLibErr(t, p, "raw/ordered.sql")
	if err == nil {
		t.Fatal("@fetch with ORDER BY should be rejected")
	}
	if !strings.Contains(err.Error(), "ORDER BY") {
		t.Errorf("error should mention ORDER BY, got: %v", err)
	}
}

func TestStrictFetch_RejectsUnion(t *testing.T) {
	p := setupFetchLib(t)

	p.AddModel("raw/union.sql", `-- @kind: table
-- @fetch
SELECT id::BIGINT AS id FROM fapi('items')
UNION ALL
SELECT id::BIGINT AS id FROM fapi('more')
`)

	_, err := runModelWithLibErr(t, p, "raw/union.sql")
	if err == nil {
		t.Fatal("@fetch with UNION should be rejected")
	}
	if !strings.Contains(err.Error(), "UNION") {
		t.Errorf("error should mention UNION, got: %v", err)
	}
}
