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

func hasHistoryResetWarning(warnings []string) bool {
	for _, w := range warnings {
		if strings.Contains(w, "scd2 history reset") {
			return true
		}
	}
	return false
}

// TestSCD2_Rebuild_KeepsHistory pins that a changed scd2 model keeps its
// history: the rebuild diffs the new result against the current versions, so
// the versions closed by earlier runs survive and the changed rows get a new
// version at the snapshot of the change. It used to TRUNCATE the target.
func TestSCD2_Rebuild_KeepsHistory(t *testing.T) {
	p := testutil.NewProject(t)

	p.AddModel("raw/src.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)) t(id, name, price)
`)
	p.AddModel("dim/item.sql", `-- @kind: scd2
-- @unique_key: id
SELECT id, name, price FROM raw.src
`)
	runModel(t, p, "raw/src.sql")
	runModel(t, p, "dim/item.sql")

	// Data change: id 1 changes price, id 3 disappears.
	p.AddModel("raw/src.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 'a', 11), (2, 'b', 20)) t(id, name, price)
`)
	runModel(t, p, "raw/src.sql")
	runModel(t, p, "dim/item.sql")

	// Logic change on the scd2 model itself.
	p.AddModel("dim/item.sql", `-- @kind: scd2
-- @unique_key: id
SELECT id, upper(name) AS name, price FROM raw.src
`)
	r := runModel(t, p, "dim/item.sql")
	if r.RunType != "backfill" || r.RunReason != "sql changed" {
		t.Fatalf("logic change: run_type=%s run_reason=%s, want backfill/sql changed", r.RunType, r.RunReason)
	}
	if hasHistoryResetWarning(r.Warnings) {
		t.Fatalf("logic change reset history: %v", r.Warnings)
	}
	if r.RowsAffected != 2 {
		t.Errorf("logic change rows_affected=%d, want 2 (ids 1 and 2 get a new version)", r.RowsAffected)
	}

	// 1 a 10 (closed), 1 a 11 (closed), 1 A 11, 2 b 20 (closed), 2 B 20, 3 c 30 (closed).
	if got := queryVal(t, p, "SELECT COUNT(*) FROM dim.item"); got != "6" {
		t.Errorf("total versions=%s, want 6", got)
	}
	if got := queryVal(t, p, "SELECT string_agg(id || name || price, ',' ORDER BY id) FROM dim.item WHERE is_current"); got != "1A11,2B20" {
		t.Errorf("current versions=%s, want 1A11,2B20", got)
	}
	if got := queryVal(t, p, "SELECT COUNT(*) FROM dim.item WHERE id = 3 AND NOT is_current"); got != "1" {
		t.Errorf("deleted id 3 closed versions=%s, want 1 (kept from before the change)", got)
	}

	// The rebuild committed the new model hash: the next run is incremental.
	r = runModel(t, p, "dim/item.sql")
	if r.RunType != "incremental" || r.RowsAffected != 0 {
		t.Errorf("rerun: run_type=%s rows_affected=%d, want incremental/0", r.RunType, r.RowsAffected)
	}
}

// TestSCD2_Rebuild_ResetsWhenVersionsCannotBeDiffed pins the cases where a
// rebuild still drops the history: the diff joins on unique_key against the
// current versions, which is meaningless when the key's identity or type
// changed, when the target was built under another kind, or when the current
// versions are not unique on the key. Each reset reports a warning.
func TestSCD2_Rebuild_ResetsWhenVersionsCannotBeDiffed(t *testing.T) {
	const std = `-- @kind: scd2
-- @unique_key: id
SELECT id, name, price FROM raw.src
`
	cases := []struct {
		name   string
		src    string // raw.src body; empty keeps the default
		before string // model before the change
		after  string
		reason string
	}{
		{
			name:   "unique_key type changed",
			before: std,
			after: `-- @kind: scd2
-- @unique_key: id
SELECT id::VARCHAR AS id, name, price FROM raw.src
`,
			reason: "changed type",
		},
		{
			name:   "unique_key changed",
			before: std,
			after: `-- @kind: scd2
-- @unique_key: name
SELECT id, name, price FROM raw.src
`,
			reason: "unique_key changed from id to name",
		},
		{
			name: "kind changed",
			before: `-- @kind: table
SELECT id, name, price FROM raw.src
`,
			after:  std,
			reason: "target was not built as scd2",
		},
		{
			// A table rebuild keeps the scd2 columns it inherited, so only
			// the previous commit's kind tells the target apart.
			name:   "kind changed back from table",
			before: std,
			after:  std,
			reason: "kind changed from table to scd2",
		},
		{
			name:   "duplicate current keys",
			src:    `SELECT * FROM (VALUES (1, 'a', 10), (1, 'x', 99), (2, 'b', 20)) t(id, name, price)`,
			before: std,
			after: `-- @kind: scd2
-- @unique_key: id
SELECT id, upper(name) AS name, price FROM raw.src
`,
			reason: "not unique and non-null",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := testutil.NewProject(t)
			src := tc.src
			if src == "" {
				src = `SELECT * FROM (VALUES (1, 'a', 10), (2, 'b', 20)) t(id, name, price)`
			}
			p.AddModel("raw/src.sql", "-- @kind: table\n"+src+"\n")
			runModel(t, p, "raw/src.sql")
			p.AddModel("dim/item.sql", tc.before)
			runModel(t, p, "dim/item.sql")
			if strings.Contains(tc.before, "scd2") {
				// Leave a closed version behind so a reset is visible.
				p.AddModel("raw/src.sql", "-- @kind: table\n"+strings.Replace(src, "'b', 20", "'b', 21", 1)+"\n")
				runModel(t, p, "raw/src.sql")
				runModel(t, p, "dim/item.sql")
			}

			if tc.name == "kind changed back from table" {
				p.AddModel("dim/item.sql", "-- @kind: table\nSELECT id, name, price FROM raw.src\n")
				runModel(t, p, "dim/item.sql")
			}

			p.AddModel("dim/item.sql", tc.after)
			r := runModel(t, p, "dim/item.sql")
			found := false
			for _, w := range r.Warnings {
				if strings.Contains(w, "scd2 history reset") && strings.Contains(w, tc.reason) {
					found = true
				}
			}
			if !found {
				t.Fatalf("want a history reset warning containing %q, got %v", tc.reason, r.Warnings)
			}
			if got := queryVal(t, p, "SELECT COUNT(*) FROM dim.item WHERE is_current IS NOT true"); got != "0" {
				t.Errorf("closed versions after reset=%s, want 0", got)
			}
		})
	}
}

// scd2Project builds raw.src and dim.item, runs both, then changes id 1's
// price so one closed version exists before the model changes.
func scd2Project(t *testing.T, model string) *testutil.Project {
	t.Helper()
	p := testutil.NewProject(t)
	p.AddModel("raw/src.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 'a', 10), (2, 'b', 20)) t(id, name, price)
`)
	p.AddModel("dim/item.sql", model)
	runModel(t, p, "raw/src.sql")
	runModel(t, p, "dim/item.sql")
	p.AddModel("raw/src.sql", `-- @kind: table
SELECT * FROM (VALUES (1, 'a', 11), (2, 'b', 20)) t(id, name, price)
`)
	runModel(t, p, "raw/src.sql")
	runModel(t, p, "dim/item.sql")
	return p
}

// TestSCD2_Rebuild_KeyCaseInsensitive pins that @unique_key matches its
// column case-insensitively, as DuckDB and the unique_key validation do. A
// case-sensitive lookup reported the key column as new and reset the history
// on every rebuild.
func TestSCD2_Rebuild_KeyCaseInsensitive(t *testing.T) {
	p := scd2Project(t, `-- @kind: scd2
-- @unique_key: ID
SELECT id, name, price FROM raw.src
`)
	p.AddModel("dim/item.sql", `-- @kind: scd2
-- @unique_key: ID
SELECT id, upper(name) AS name, price FROM raw.src
`)
	r := runModel(t, p, "dim/item.sql")
	if hasHistoryResetWarning(r.Warnings) {
		t.Fatalf("key differing only in case reset history: %v", r.Warnings)
	}
	if got := queryVal(t, p, "SELECT COUNT(*) FROM dim.item WHERE NOT is_current"); got != "3" {
		t.Errorf("closed versions=%s, want 3 (1a10, 1a11, 2b20)", got)
	}
}

// TestSCD2_Rebuild_SchemaChangesReachHistory pins what a kept history looks
// like after a schema change, as documented under SCD2 rebuilds: an added
// column is NULL in older versions, a dropped column leaves the whole
// history, and a type change (drop and re-add) empties the column in every
// closed version.
func TestSCD2_Rebuild_SchemaChangesReachHistory(t *testing.T) {
	const std = `-- @kind: scd2
-- @unique_key: id
SELECT id, name, price FROM raw.src
`
	t.Run("added column", func(t *testing.T) {
		p := scd2Project(t, std)
		p.AddModel("dim/item.sql", `-- @kind: scd2
-- @unique_key: id
SELECT id, name, price, price * 2 AS dbl FROM raw.src
`)
		runModel(t, p, "dim/item.sql")
		if got := queryVal(t, p, "SELECT COUNT(*) FILTER (WHERE dbl IS NULL) || '/' || COUNT(*) FROM dim.item WHERE NOT is_current"); got != "3/3" {
			t.Errorf("closed versions with NULL dbl=%s, want 3/3", got)
		}
		if got := queryVal(t, p, "SELECT COUNT(*) FROM dim.item WHERE is_current AND dbl IS NOT NULL"); got != "2" {
			t.Errorf("current versions with dbl=%s, want 2", got)
		}
	})
	t.Run("dropped column", func(t *testing.T) {
		p := scd2Project(t, std)
		p.AddModel("dim/item.sql", `-- @kind: scd2
-- @unique_key: id
SELECT id, name FROM raw.src
`)
		runModel(t, p, "dim/item.sql")
		if hasColumn(t, p, "item", "price") {
			t.Error("dropped column price still in dim.item")
		}
		if got := queryVal(t, p, "SELECT COUNT(*) FROM dim.item WHERE NOT is_current"); got != "1" {
			t.Errorf("closed versions=%s, want 1 (history kept, nothing else changed)", got)
		}
	})
	t.Run("type change", func(t *testing.T) {
		p := scd2Project(t, std)
		p.AddModel("dim/item.sql", `-- @kind: scd2
-- @unique_key: id
SELECT id, name, price::VARCHAR AS price FROM raw.src
`)
		runModel(t, p, "dim/item.sql")
		if got := queryVal(t, p, "SELECT COUNT(*) FILTER (WHERE price IS NULL) || '/' || COUNT(*) FROM dim.item WHERE NOT is_current"); got != "3/3" {
			t.Errorf("closed versions with NULL price=%s, want 3/3", got)
		}
		if got := queryVal(t, p, "SELECT string_agg(id || ':' || price, ',' ORDER BY id) FROM dim.item WHERE is_current"); got != "1:11,2:20" {
			t.Errorf("current versions=%s, want 1:11,2:20", got)
		}
	})
}
