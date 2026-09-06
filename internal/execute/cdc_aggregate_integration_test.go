// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute_test

import (
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// TestCDC_ChangedAggregateSourceFallsBackToFullQuery pins that delta CDC is
// abandoned when a source the model aggregates over has changed.
//
// applySmartCDC rewrites every CDC source to `(SELECT * FROM t) EXCEPT
// (SELECT * FROM t AT (VERSION => N))`. An aggregate computed over that delta
// is only correct when the affected group is entirely new: for an existing
// group it either comes back empty, because the unchanged side of the JOIN has
// an empty delta and the target silently keeps a stale total, or it sums the
// new rows alone and a merge on unique_key writes that partial over a correct
// value. Whether the group is new cannot be determined cheaply, so a changed
// aggregated source disqualifies the delta path for the whole model.
func TestCDC_ChangedAggregateSourceFallsBackToFullQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	newProject := func(t *testing.T) *testutil.Project {
		t.Helper()
		p := testutil.NewProject(t)
		p.AddModel("raw/base.sql", "-- @kind: table\nSELECT 1 AS id UNION ALL SELECT 2\n")
		runModel(t, p, "raw/base.sql")
		p.AddModel("raw/events.sql", "-- @kind: table\nSELECT 1 AS id, 10 AS amount UNION ALL SELECT 2, 20\n")
		runModel(t, p, "raw/events.sql")
		p.AddModel("staging/totals.sql", `-- @kind: merge
-- @unique_key: id
SELECT l.id::BIGINT AS id, SUM(f.amount)::BIGINT AS total
FROM raw.base l JOIN raw.events f ON f.id = l.id GROUP BY l.id
`)
		runModel(t, p, "staging/totals.sql")
		return p
	}

	rerunAndRead := func(t *testing.T, p *testutil.Project) []string {
		t.Helper()
		r := runModel(t, p, "staging/totals.sql")
		if len(r.Errors) > 0 {
			t.Fatalf("rerun failed: %v", r.Errors)
		}
		got, err := p.Sess.QueryRows("SELECT id || '=' || total FROM staging.totals ORDER BY id")
		if err != nil {
			t.Fatalf("read totals: %v", err)
		}
		return got
	}

	equal := func(got, want []string) bool {
		if len(got) != len(want) {
			return false
		}
		for i := range got {
			if got[i] != want[i] {
				return false
			}
		}
		return true
	}

	tests := []struct {
		name    string
		mutate  []string
		want    []string
		comment string
	}{
		{
			name:    "existing group gains a row",
			mutate:  []string{"INSERT INTO raw.events VALUES (1, 5)"},
			want:    []string{"1=15", "2=20"},
			comment: "the primary is unchanged, so its delta is empty and the delta-join returns nothing",
		},
		{
			name:    "two existing groups gain rows",
			mutate:  []string{"INSERT INTO raw.events VALUES (1, 5), (2, 7)"},
			want:    []string{"1=15", "2=27"},
			comment: "both totals must be recomputed in full, not replaced by the delta's partial sums",
		},
		{
			name: "an entirely new group",
			mutate: []string{
				"INSERT INTO raw.base VALUES (3)",
				"INSERT INTO raw.events VALUES (3, 30)",
			},
			want:    []string{"1=10", "2=20", "3=30"},
			comment: "the case the delta path already handled; it must keep working",
		},
		{
			name:    "primary gains a row with no matching event",
			mutate:  []string{"INSERT INTO raw.base VALUES (9)"},
			want:    []string{"1=10", "2=20"},
			comment: "the inner join yields nothing for the new key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newProject(t)
			for _, stmt := range tt.mutate {
				if err := p.Sess.Exec(stmt); err != nil {
					t.Fatalf("mutate (%s): %v", stmt, err)
				}
			}
			if got := rerunAndRead(t, p); !equal(got, tt.want) {
				t.Errorf("got %v, want %v — %s", got, tt.want, tt.comment)
			}
		})
	}
}

// TestCDC_ScalarSubqueryAggregateIsDetected pins that an aggregate hidden in a
// scalar subquery still disqualifies the delta path.
//
// traceExprWithType had no SUBQUERY case, so `(SELECT SUM(x) FROM raw.events)`
// contributed no lineage at all. The runtime never learned that raw.events is
// aggregated here, left it out of the CDC gate entirely, and served a stale
// total on every run after that source changed.
func TestCDC_ScalarSubqueryAggregateIsDetected(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("raw/base.sql", "-- @kind: table\nSELECT 1 AS id UNION ALL SELECT 2\n")
	runModel(t, p, "raw/base.sql")
	p.AddModel("raw/events.sql", "-- @kind: table\nSELECT 1 AS id, 10 AS amount UNION ALL SELECT 2, 20\n")
	runModel(t, p, "raw/events.sql")

	p.AddModel("staging/scalar_total.sql", `-- @kind: merge
-- @unique_key: id
SELECT b.id::BIGINT AS id, (SELECT SUM(f.amount) FROM raw.events f)::BIGINT AS total
FROM raw.base b
`)
	runModel(t, p, "staging/scalar_total.sql")

	read := func(when string) []string {
		t.Helper()
		got, err := p.Sess.QueryRows("SELECT id || '=' || total FROM staging.scalar_total ORDER BY id")
		if err != nil {
			t.Fatalf("read %s: %v", when, err)
		}
		return got
	}
	if got := read("after first run"); len(got) != 2 || got[0] != "1=30" {
		t.Fatalf("first run should total 30, got %v", got)
	}

	// Only the subquery's source changes. The outer FROM is untouched, so a
	// gate that never learned about raw.events reports no work to do.
	if err := p.Sess.Exec("INSERT INTO raw.events VALUES (1, 5)"); err != nil {
		t.Fatalf("change the aggregated source: %v", err)
	}

	r := runModel(t, p, "staging/scalar_total.sql")
	if len(r.Errors) > 0 {
		t.Fatalf("rerun failed: %v", r.Errors)
	}
	got := read("after rerun")
	if len(got) != 2 || got[0] != "1=35" || got[1] != "2=35" {
		t.Errorf("a change inside a scalar aggregate subquery must reach the model, got %v, want [1=35 2=35]", got)
	}
}

// TestCDC_SubqueryLocalCTEAggregateIsDetected pins the end-to-end effect of
// resolving a CTE declared inside a scalar subquery.
//
// Only the top-level statement's CTEs are collected when the extractor is
// built, so a `WITH` inside the subquery was unknown: the lineage recorded the
// CTE alias as the source table, the aggregation under it was never seen, and
// a change to the real table was served stale on every run.
func TestCDC_SubqueryLocalCTEAggregateIsDetected(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("raw/base.sql", "-- @kind: table\nSELECT 1 AS id UNION ALL SELECT 2\n")
	runModel(t, p, "raw/base.sql")
	p.AddModel("raw/events.sql", "-- @kind: table\nSELECT 1 AS id, 10 AS amount UNION ALL SELECT 2, 20\n")
	runModel(t, p, "raw/events.sql")

	p.AddModel("staging/inner_cte_total.sql", `-- @kind: merge
-- @unique_key: id
SELECT b.id::BIGINT AS id,
       (WITH x AS (SELECT SUM(f.amount) AS t FROM raw.events f) SELECT x.t FROM x)::BIGINT AS total
FROM raw.base b
`)
	runModel(t, p, "staging/inner_cte_total.sql")

	read := func(when string) []string {
		t.Helper()
		got, err := p.Sess.QueryRows("SELECT id || '=' || total FROM staging.inner_cte_total ORDER BY id")
		if err != nil {
			t.Fatalf("read %s: %v", when, err)
		}
		return got
	}
	if got := read("after first run"); len(got) != 2 || got[0] != "1=30" {
		t.Fatalf("first run should total 30, got %v", got)
	}

	if err := p.Sess.Exec("INSERT INTO raw.events VALUES (1, 5)"); err != nil {
		t.Fatalf("change the aggregated source: %v", err)
	}

	r := runModel(t, p, "staging/inner_cte_total.sql")
	if len(r.Errors) > 0 {
		t.Fatalf("rerun failed: %v", r.Errors)
	}
	if got := read("after rerun"); len(got) != 2 || got[0] != "1=35" || got[1] != "2=35" {
		t.Errorf("a change under a subquery-local CTE must reach the model, got %v, want [1=35 2=35]", got)
	}
}

// TestCDC_UnclassifiableAggregateSourceFallsBackToFullQuery pins the gate's
// error branches, not its happy path.
//
// Five places set hasChanges without classifying a single table — an unreadable
// snapshot, a name splitCDCTableName cannot parse, a failed search_path, a
// failed table_changes(). Each falls back to "assume changes exist", and for an
// aggregated source that assumption has to reach the delta decision too: a
// partial sum merged over a correct total corrupts it rather than leaving it
// stale.
//
// The reachable branch here is the name split. A model at the project root
// targets `main`, which is on the search path, so a sibling model can read it
// with a bare one-part name — a shape splitCDCTableName reports as unparseable.
func TestCDC_UnclassifiableAggregateSourceFallsBackToFullQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("dim.sql", "-- @kind: table\nSELECT 1 AS id, 10 AS amount\n")
	runModel(t, p, "dim.sql")

	p.AddModel("staging/bare_total.sql", `-- @kind: merge
-- @unique_key: id
SELECT d.id::BIGINT AS id, SUM(d.amount)::BIGINT AS total FROM dim d GROUP BY d.id
`)
	runModel(t, p, "staging/bare_total.sql")

	read := func(when string) []string {
		t.Helper()
		got, err := p.Sess.QueryRows("SELECT id || '=' || total FROM staging.bare_total ORDER BY id")
		if err != nil {
			t.Fatalf("read %s: %v", when, err)
		}
		return got
	}
	if got := read("after first run"); len(got) != 1 || got[0] != "1=10" {
		t.Fatalf("first run should total 10, got %v", got)
	}

	if err := p.Sess.Exec("INSERT INTO main.dim VALUES (1, 5)"); err != nil {
		t.Fatalf("change the aggregated source: %v", err)
	}

	r := runModel(t, p, "staging/bare_total.sql")
	if len(r.Errors) > 0 {
		t.Fatalf("rerun failed: %v", r.Errors)
	}
	// Without the guard the delta alone is aggregated and the merge writes 5
	// over the correct 10 — corruption, not staleness.
	if got := read("after rerun"); len(got) != 1 || got[0] != "1=15" {
		t.Errorf("an unclassified aggregate must be recomputed in full, got %v, want [1=15]", got)
	}
}
