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

// --- @fetch ↔ lib-call relationship rules (v0.30.0 fas 5) ---

// A @fetch model with no lib call in FROM is rejected at runtime —
// the directive declares a lib-backed model but the SQL has no lib.
func TestFetch_NoLibCall_Rejected(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/dangling.sql", `-- @kind: table
-- @fetch
SELECT 1::BIGINT AS id, 'a'::VARCHAR AS name
`)

	_, err := runModelWithLibErr(t, p, "raw/dangling.sql")
	if err == nil {
		t.Fatal("@fetch model with no lib call should be rejected")
	}
	if !strings.Contains(err.Error(), "no lib calls") {
		t.Errorf("error should mention 'no lib calls', got: %v", err)
	}
}

// A model with a lib call in FROM but no @fetch directive is rejected —
// the inverse of the above. Lib calls need an explicit @fetch declaration
// in v0.30.0.
func TestLibCall_WithoutFetch_Rejected(t *testing.T) {
	p := testutil.NewProject(t)

	writeLib(t, p, "orphan_lib", `
API = {
    "base_url": "https://example.com",
    "fetch": {"args": ["resource"]},
}

def fetch(resource, page):
    return {"rows": [{"id": 1, "name": "x"}], "next": None}
`)

	p.AddModel("raw/orphan.sql", `-- @kind: table
SELECT id::BIGINT AS id, name::VARCHAR AS name FROM orphan_lib('items')
`)

	_, err := runModelWithLibErr(t, p, "raw/orphan.sql")
	if err == nil {
		t.Fatal("lib call without @fetch should be rejected")
	}
	if !strings.Contains(err.Error(), "lacks @fetch") {
		t.Errorf("error should mention 'lacks @fetch', got: %v", err)
	}
	if !strings.Contains(err.Error(), "orphan_lib") {
		t.Errorf("error should name the offending lib call, got: %v", err)
	}
}

// Validator runs whenever @fetch is present — the rules apply to the AST
// shape, not to whether lib calls were detected. This pins the activation
// flip from lib-call-driven (v0.29) to directive-driven (v0.30).
func TestFetch_ActivatesValidator(t *testing.T) {
	p := testutil.NewProject(t)

	writeLib(t, p, "v_api", `
API = {
    "base_url": "https://example.com",
    "fetch": {"args": ["resource"]},
}

def fetch(resource, page):
    return {"rows": [{"id": 1, "name": "a"}], "next": None}
`)

	// Bare COLUMN_REF (no CAST) — strict-fetch must reject this.
	p.AddModel("raw/loose.sql", `-- @kind: table
-- @fetch
SELECT id, name FROM v_api('items')
`)

	_, err := runModelWithLibErr(t, p, "raw/loose.sql")
	if err == nil {
		t.Fatal("@fetch model with non-CAST projection should be rejected by strict-fetch validator")
	}
	if !strings.Contains(err.Error(), "cast") && !strings.Contains(err.Error(), "CAST") {
		t.Errorf("error should mention CAST requirement, got: %v", err)
	}
}
