// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// A rebuild cancelled because every lib reported no change and the result is
// empty must write nothing. Kind table ignored the cancellation and truncated
// the target anyway: without an audit the rows were lost under a warning that
// said they were kept, and with one the audit caught the empty target and
// failed the run. Nothing is written, so no audit runs either, and the
// rebuild stays owed until a run has data to build from.
func TestLibCall_CancelledEmptyRebuild_SkipsAuditsKeepsRowsAndDefersRebuild(t *testing.T) {
	for _, tc := range []struct {
		kind  string
		audit bool
	}{
		{"table", true}, {"table", false},
		{"merge", true}, {"append", true},
	} {
		kind := tc.kind
		name := kind
		if !tc.audit {
			name += "_no_audit"
		}
		t.Run(name, func(t *testing.T) {
			p := testutil.NewProject(t)
			lib := `
API = {
    "base_url": "https://example.com",
    "fetch": {"args": ["resource"], "supported_kinds": ["table", "merge", "append"]},
}

def fetch(resource, page, is_backfill=True, last_value=""):
    return {"rows": ROWS, "next": None}
`
			writeLib(t, p, "floorapi", strings.Replace(lib, "ROWS",
				`[{"id": 1, "val": 10}, {"id": 2, "val": 20}]`, 1))

			header := "-- @kind: " + kind + "\n-- @fetch\n"
			if kind == "merge" {
				header += "-- @unique_key: id\n"
			}
			if tc.audit {
				header += "-- @audit: row_count(>, 1)\n"
			}
			p.AddModel("raw/floor.sql", header+
				"SELECT id::BIGINT AS id, val::BIGINT AS val FROM floorapi('items')\n")

			if r := runModelWithLib(t, p, "raw/floor.sql"); r.RowsAffected != 2 {
				t.Fatalf("run 1: expected 2 rows, got %d", r.RowsAffected)
			}

			// The API goes dry and the projection is reordered, so the hash
			// changes and the run decides to rebuild from an empty result.
			writeLib(t, p, "floorapi", strings.Replace(lib, "ROWS", "[]", 1))
			if err := os.WriteFile(filepath.Join(p.Dir, "models", "raw/floor.sql"), []byte(header+
				"SELECT val::BIGINT AS val, id::BIGINT AS id FROM floorapi('items')\n"), 0o644); err != nil {
				t.Fatal(err)
			}

			r2, err := runModelWithLibErr(t, p, "raw/floor.sql")
			if err != nil {
				t.Fatalf("run 2: cancelled rebuild must not fail, got: %v", err)
			}
			if !strings.Contains(strings.Join(r2.Warnings, "\n"), "keeping existing rows") {
				t.Fatalf("run 2: expected the keep-rows warning, got %v", r2.Warnings)
			}
			if r2.RowsAffected != 0 {
				t.Fatalf("run 2: expected 0 rows affected, got %d", r2.RowsAffected)
			}
			cnt, qerr := p.Sess.QueryValue("SELECT count(*) FROM raw.floor")
			if qerr != nil {
				t.Fatal(qerr)
			}
			if cnt != "2" {
				t.Fatalf("run 2: target must keep its 2 rows, has %s", cnt)
			}

			// The API answers again. The model is unchanged since run 2, but
			// run 2 committed its hash without rebuilding, so the rebuild is
			// still owed: the kept rows 1 and 2 must give way to row 3.
			writeLib(t, p, "floorapi", strings.Replace(lib, "ROWS",
				`[{"id": 3, "val": 30}, {"id": 4, "val": 40}]`, 1))
			r3 := runModelWithLib(t, p, "raw/floor.sql")
			if r3.RunType != "backfill" || r3.RunReason != "rebuild pending" {
				t.Fatalf("run 3: expected backfill/rebuild pending, got %s/%s", r3.RunType, r3.RunReason)
			}
			ids, qerr := p.Sess.QueryValue("SELECT string_agg(id::VARCHAR, ',' ORDER BY id) FROM raw.floor")
			if qerr != nil {
				t.Fatal(qerr)
			}
			if ids != "3,4" {
				t.Fatalf("run 3: expected the rebuild to leave ids 3,4, got %s", ids)
			}

			// The rebuild is done, so the next run is an ordinary one.
			r4 := runModelWithLib(t, p, "raw/floor.sql")
			if r4.RunReason == "rebuild pending" {
				t.Fatalf("run 4: rebuild still pending after run 3 rebuilt")
			}
		})
	}
}

// A unique_key type change can only be applied by a rebuild, and schema
// evolution demands one on its own. With an empty result that rebuild would
// truncate the target just like the one the runner cancelled, so it is
// deferred together with it and the rows stay.
func TestLibCall_CancelledEmptyRebuild_UniqueKeyTypeChangeKeepsRows(t *testing.T) {
	p := testutil.NewProject(t)
	lib := `
API = {
    "base_url": "https://example.com",
    "fetch": {"args": ["resource"], "supported_kinds": ["merge"]},
}

def fetch(resource, page, is_backfill=True, last_value=""):
    return {"rows": ROWS, "next": None}
`
	writeLib(t, p, "keyapi", strings.Replace(lib, "ROWS",
		`[{"id": 1, "val": 10}, {"id": 2, "val": 20}]`, 1))
	header := "-- @kind: merge\n-- @fetch\n-- @unique_key: id\n"
	p.AddModel("raw/keyed.sql", header+
		"SELECT id::BIGINT AS id, val::BIGINT AS val FROM keyapi('items')\n")
	if r := runModelWithLib(t, p, "raw/keyed.sql"); r.RowsAffected != 2 {
		t.Fatalf("run 1: expected 2 rows, got %d", r.RowsAffected)
	}

	writeLib(t, p, "keyapi", strings.Replace(lib, "ROWS", "[]", 1))
	if err := os.WriteFile(filepath.Join(p.Dir, "models", "raw/keyed.sql"), []byte(header+
		"SELECT id::VARCHAR AS id, val::BIGINT AS val FROM keyapi('items')\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	r2, err := runModelWithLibErr(t, p, "raw/keyed.sql")
	if err != nil {
		t.Fatalf("run 2: %v", err)
	}
	if !strings.Contains(strings.Join(r2.Warnings, "\n"), "deferring it with the rebuild") {
		t.Fatalf("run 2: expected the deferral warning, got %v", r2.Warnings)
	}
	ids, qerr := p.Sess.QueryValue("SELECT string_agg(id::VARCHAR, ',' ORDER BY id) FROM raw.keyed")
	if qerr != nil {
		t.Fatal(qerr)
	}
	if ids != "1,2" {
		t.Fatalf("run 2: target must keep ids 1,2, got %q", ids)
	}

	// Data returns: the deferred rebuild applies the key type change.
	writeLib(t, p, "keyapi", strings.Replace(lib, "ROWS", `[{"id": 3, "val": 30}]`, 1))
	r3 := runModelWithLib(t, p, "raw/keyed.sql")
	if r3.RunType != "backfill" {
		t.Fatalf("run 3: expected backfill, got %s/%s", r3.RunType, r3.RunReason)
	}
	typ, qerr := p.Sess.QueryValue("SELECT data_type FROM information_schema.columns WHERE table_schema='raw' AND table_name='keyed' AND column_name='id'")
	if qerr != nil {
		t.Fatal(qerr)
	}
	ids, _ = p.Sess.QueryValue("SELECT string_agg(id, ',' ORDER BY id) FROM raw.keyed")
	if typ != "VARCHAR" || ids != "3" {
		t.Fatalf("run 3: expected VARCHAR key with only id 3, got %s / %q", typ, ids)
	}
}

// A type change on an ordinary column is applied as DROP + ADD, which nulls
// the column. On a kept target that would wipe the values of rows the run
// promised to keep, so the change is deferred with the rebuild instead.
func TestLibCall_CancelledEmptyRebuild_ColumnTypeChangeKeepsValues(t *testing.T) {
	// The second case pairs a promotable type change (INTEGER → BIGINT)
	// with an added column, which the classifier calls additive as a whole.
	for _, tc := range []struct{ name, oldVal, newSelect string }{
		{"type_change", "val::BIGINT AS val", "val::VARCHAR AS val"},
		{"promotable_with_added_column", "val::INTEGER AS val", "val::BIGINT AS val, val::VARCHAR AS note"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testKeptValuesSurviveTypeChange(t, tc.oldVal, tc.newSelect)
		})
	}
}

func testKeptValuesSurviveTypeChange(t *testing.T, oldVal, newSelect string) {
	p := testutil.NewProject(t)
	lib := `
API = {
    "base_url": "https://example.com",
    "fetch": {"args": ["resource"], "supported_kinds": ["merge"]},
}

def fetch(resource, page, is_backfill=True, last_value=""):
    return {"rows": ROWS, "next": None}
`
	writeLib(t, p, "colapi", strings.Replace(lib, "ROWS",
		`[{"id": 1, "val": 10}, {"id": 2, "val": 20}]`, 1))
	header := "-- @kind: merge\n-- @fetch\n-- @unique_key: id\n"
	p.AddModel("raw/typed.sql", header+
		"SELECT id::BIGINT AS id, "+oldVal+" FROM colapi('items')\n")
	if r := runModelWithLib(t, p, "raw/typed.sql"); r.RowsAffected != 2 {
		t.Fatalf("run 1: expected 2 rows, got %d", r.RowsAffected)
	}

	writeLib(t, p, "colapi", strings.Replace(lib, "ROWS", "[]", 1))
	if err := os.WriteFile(filepath.Join(p.Dir, "models", "raw/typed.sql"), []byte(header+
		"SELECT id::BIGINT AS id, "+newSelect+" FROM colapi('items')\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	r2, err := runModelWithLibErr(t, p, "raw/typed.sql")
	if err != nil {
		t.Fatalf("run 2: %v", err)
	}
	if !strings.Contains(strings.Join(r2.Warnings, "\n"), "deferring it with the rebuild") {
		t.Fatalf("run 2: expected the deferral warning, got %v", r2.Warnings)
	}
	vals, qerr := p.Sess.QueryValue("SELECT string_agg(val::VARCHAR, ',' ORDER BY id) FROM raw.typed")
	if qerr != nil {
		t.Fatal(qerr)
	}
	if vals != "10,20" {
		t.Fatalf("run 2: kept rows must keep their values, got %q", vals)
	}
}

// A kept target skips constraint violations, since an empty result violates
// at_least_one by definition, but a constraint the model cannot execute is a
// defect in the model and still fails the run.
func TestLibCall_CancelledEmptyRebuild_Constraints(t *testing.T) {
	lib := `
API = {
    "base_url": "https://example.com",
    "fetch": {"args": ["resource"], "supported_kinds": ["merge"]},
}

def fetch(resource, page, is_backfill=True, last_value=""):
    return {"rows": ROWS, "next": None}
`
	for _, tc := range []struct {
		constraint string
		wantErr    bool
	}{
		{"at_least_one(id)", false},
		{"not_null(nonexistent_col)", true},
	} {
		t.Run(tc.constraint, func(t *testing.T) {
			p := testutil.NewProject(t)
			writeLib(t, p, "conapi", strings.Replace(lib, "ROWS",
				`[{"id": 1, "val": 10}, {"id": 2, "val": 20}]`, 1))
			header := "-- @kind: merge\n-- @fetch\n-- @unique_key: id\n"
			p.AddModel("raw/con.sql", header+
				"SELECT id::BIGINT AS id, val::BIGINT AS val FROM conapi('items')\n")
			runModelWithLib(t, p, "raw/con.sql")

			writeLib(t, p, "conapi", strings.Replace(lib, "ROWS", "[]", 1))
			if err := os.WriteFile(filepath.Join(p.Dir, "models", "raw/con.sql"), []byte(header+
				"-- @constraint: "+tc.constraint+"\n"+
				"SELECT val::BIGINT AS val, id::BIGINT AS id FROM conapi('items')\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := runModelWithLibErr(t, p, "raw/con.sql")
			if tc.wantErr && err == nil {
				t.Fatal("expected the broken constraint to fail the run")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected the violation to be skipped, got: %v", err)
			}
			cnt, qerr := p.Sess.QueryValue("SELECT count(*) FROM raw.con")
			if qerr != nil {
				t.Fatal(qerr)
			}
			if cnt != "2" {
				t.Fatalf("target must keep its 2 rows, has %s", cnt)
			}
		})
	}
}
