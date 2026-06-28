// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute_test

import (
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// TestRun_KindConversion_AddsSyntheticColumns pins the schema-evolution gap on
// kind conversion: converting an EXISTING table (kind=table) into a kind whose
// runner persists synthetic columns must add those columns to the target before
// the materialize INSERT … BY NAME runs.
//
// The gap: schema-evolution detection compares the model's SELECT columns
// against the stored target schema. The synthetic columns (SCD2's
// valid_from_snapshot/valid_to_snapshot/is_current; tracked's _content_hash)
// are produced by the INSERT literal/join, never by the SELECT, so detection
// never proposes an ALTER TABLE ADD for them. On conversion the materialize
// step then issues `INSERT INTO target BY NAME SELECT …, <synthetic>` against a
// target that lacks the synthetic column, and DuckDB rejects the unmatched
// column.
//
// Both sub-tests assert the DESIRED behavior (conversion succeeds and the
// synthetic columns exist), so they fail today and pass once conversion adds
// the synthetic columns. Safe conversion targets (table/append/merge) have no
// synthetic columns and are unaffected; only scd2 and tracked are at risk.
func TestRun_KindConversion_AddsSyntheticColumns(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	t.Run("table_to_scd2", func(t *testing.T) {
		p := testutil.NewProject(t)

		// Run 1: materialize a plain table — target has base columns only.
		p.AddModel("staging/widgets.sql", `-- @kind: table
SELECT 1 AS id, 'alpha' AS name
`)
		runModel(t, p, "staging/widgets.sql")

		// Run 2: convert the same model to scd2. The runner must add the
		// synthetic SCD2 columns to the existing target before INSERT BY NAME.
		p.AddModel("staging/widgets.sql", `-- @kind: scd2
-- @unique_key: id
SELECT 1 AS id, 'alpha' AS name
`)
		if _, err := runModelErr(t, p, "staging/widgets.sql"); err != nil {
			t.Fatalf("table→scd2 conversion should succeed, got: %v", err)
		}

		for _, col := range []string{"valid_from_snapshot", "valid_to_snapshot", "is_current"} {
			if !hasColumn(t, p, "widgets", col) {
				t.Errorf("scd2 target missing synthetic column %q after conversion", col)
			}
		}
		// The pre-existing row must survive the conversion as a current version.
		if got := queryVal(t, p, "SELECT COUNT(*) FROM staging.widgets WHERE is_current"); got != "1" {
			t.Errorf("scd2 conversion: want 1 current row, got %s", got)
		}
	})

	t.Run("table_to_tracked", func(t *testing.T) {
		p := testutil.NewProject(t)

		// Run 1: materialize a plain table — target has base columns only.
		p.AddModel("staging/gadgets.sql", `-- @kind: table
SELECT 1 AS id, 'alpha' AS name
`)
		runModel(t, p, "staging/gadgets.sql")

		// Run 2: convert to tracked. The runner must add the synthetic
		// _content_hash column to the existing target before INSERT BY NAME.
		p.AddModel("staging/gadgets.sql", `-- @kind: tracked
-- @group_key: id
SELECT 1 AS id, 'alpha' AS name
`)
		if _, err := runModelErr(t, p, "staging/gadgets.sql"); err != nil {
			t.Fatalf("table→tracked conversion should succeed, got: %v", err)
		}

		if !hasColumn(t, p, "gadgets", "_content_hash") {
			t.Error("tracked target missing synthetic column \"_content_hash\" after conversion")
		}
		if got := queryVal(t, p, "SELECT COUNT(*) FROM staging.gadgets"); got != "1" {
			t.Errorf("tracked conversion: want 1 row, got %s", got)
		}
	})

	t.Run("table_to_tracked_all_columns_grouped", func(t *testing.T) {
		p := testutil.NewProject(t)

		// Run 1: materialize a plain table — target has base columns only.
		p.AddModel("staging/sprockets.sql", `-- @kind: table
SELECT 1 AS id, 'alpha' AS name
`)
		runModel(t, p, "staging/sprockets.sql")

		// Run 2: convert to tracked with a @group_key covering EVERY column.
		// With no non-key columns, the content hash is `0::HUGEINT` rather than
		// the VARCHAR sum(hash(row(...))) form, so the synthetic _content_hash
		// must be added as HUGEINT. This pins the otherwise-untested HUGEINT
		// branch of contentHashType.
		p.AddModel("staging/sprockets.sql", `-- @kind: tracked
-- @group_key: id, name
SELECT 1 AS id, 'alpha' AS name
`)
		if _, err := runModelErr(t, p, "staging/sprockets.sql"); err != nil {
			t.Fatalf("table→tracked (all-column group_key) conversion should succeed, got: %v", err)
		}

		if !hasColumn(t, p, "sprockets", "_content_hash") {
			t.Fatal("tracked target missing synthetic column \"_content_hash\" after conversion")
		}
		// Assert the column type so this case can't silently pass on the VARCHAR
		// branch — the zero-hash-column path must type _content_hash as HUGEINT.
		if got := queryVal(t, p, "SELECT data_type FROM information_schema.columns "+
			"WHERE table_name = 'sprockets' AND column_name = '_content_hash'"); got != "HUGEINT" {
			t.Errorf("all-column-grouped tracked: want _content_hash HUGEINT, got %q", got)
		}
		if got := queryVal(t, p, "SELECT COUNT(*) FROM staging.sprockets"); got != "1" {
			t.Errorf("tracked conversion: want 1 row, got %s", got)
		}
	})
}

// hasColumn reports whether the named column exists on a table (unqualified
// table name, matched in information_schema).
func hasColumn(t *testing.T, p *testutil.Project, table, column string) bool {
	t.Helper()
	got := queryVal(t, p, "SELECT COUNT(*) FROM information_schema.columns "+
		"WHERE table_name = '"+table+"' AND column_name = '"+column+"'")
	return got != "0" && got != ""
}

// queryVal runs a single-value query and fails the test on error.
func queryVal(t *testing.T, p *testutil.Project, query string) string {
	t.Helper()
	val, err := p.Sess.QueryValue(query)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return val
}
