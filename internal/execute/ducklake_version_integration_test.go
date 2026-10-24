// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute_test

import (
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// TestDuckLakeCatalog_StaysReadableBy15 pins that running models against a
// catalog at DuckLake version 1.0 — what DuckDB 1.5 creates and the newest it
// can read — leaves it at 1.0. DuckDB 2.0's ducklake opens such a catalog as
// is; only AUTOMATIC_MIGRATION upgrades it, after which 1.5 refuses to
// attach it at all.
func TestDuckLakeCatalog_StaysReadableBy15(t *testing.T) {
	p := testutil.NewProjectOnDuckLake10(t)

	version := func() string {
		return queryVal(t, p, "SELECT value FROM __ducklake_metadata_lake.ducklake_metadata WHERE key = 'version'")
	}
	if got := version(); got != "1.0" {
		t.Fatalf("catalog version before any run = %s, want 1.0", got)
	}

	p.AddModel("raw/src.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(id, name)
`)
	p.AddModel("dim/item.sql", `-- @kind: scd2
-- @unique_key: id
SELECT id, name FROM raw.src
`)
	p.AddModel("mart/item.sql", `-- @kind: merge
-- @unique_key: id
SELECT id, upper(name) AS name FROM raw.src
`)
	for _, m := range []string{"raw/src.sql", "dim/item.sql", "mart/item.sql"} {
		runModel(t, p, m)
	}
	// A schema change and a second round of writes.
	p.AddModel("raw/src.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 'a', 10), (3, 'c', 30)) t(id, name, price)
`)
	for _, m := range []string{"raw/src.sql", "dim/item.sql", "mart/item.sql"} {
		runModel(t, p, m)
	}

	if got := version(); got != "1.0" {
		t.Errorf("catalog version after runs = %s, want 1.0 (DuckDB 1.5 cannot read anything newer)", got)
	}
}
