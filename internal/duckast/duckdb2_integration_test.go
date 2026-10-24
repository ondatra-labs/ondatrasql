// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package duckast

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// duckdb2Fixtures maps each file in testdata/duckdb2 to the query DuckDB 2.0's
// json_serialize_sql produced it from. The files are DuckDB's own output, not
// hand-written: a hand-written fixture can agree with a wrong accessor, which
// is how 1.5-shaped fixtures kept the unit tests green while every 2.0 query
// lost its function arguments. TestDuckDB2Fixtures_MatchEmbeddedDuckDB
// regenerates them against the embedded DuckDB, so an upgrade that changes the
// shape again fails there first.
var duckdb2Fixtures = map[string]string{
	"union_chain":         "SELECT a FROM t UNION ALL SELECT b FROM u UNION ALL SELECT c FROM v",
	"cte":                 "WITH c(x) AS (SELECT a FROM t) SELECT * FROM c",
	"table_function_args": "SELECT * FROM f(api_key=>'k', 2)",
	"constants":           "SELECT 'it''s', 1_000, -5, true, NULL, 1.5, 99999999999999999999999, x'ab', 010",
	"cast_types":          "SELECT a::DECIMAL(10,2), b::STRUCT(x INT, y VARCHAR[]), c::MAP(VARCHAR, INT), d::int4, e::INT[3]",
	"base_table":          "SELECT * FROM s.t",
	"recursive_cte":       "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r",
	"mixed_setop":         "SELECT a FROM t UNION BY NAME SELECT b FROM u UNION ALL SELECT c FROM v",
	"type_spellings":      "SELECT a::double precision, b::character varying(5), c::timestamp without time zone, d::varint, e::geometry('OGC:CRS84'), f::myschema.mytype, g::cat.sch.typ, h::varchar('it''s')",
	"lambdas":             "SELECT list_transform(l, x -> x + 1), list_transform(l, lambda x: x + 1), j -> 'a' FROM t",
}

var updateDuckDB2 = flag.Bool("update-duckdb2", false, "rewrite testdata/duckdb2 from the embedded DuckDB")

// The testdata/duckdb2 fixtures must be what the embedded DuckDB actually
// emits. When an upgrade changes the AST shape this fails, naming the
// fixture, before any accessor test can pass against a stale shape.
// Regenerate the fixtures with -update-duckdb2.
func TestDuckDB2Fixtures_MatchEmbeddedDuckDB(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for name, query := range duckdb2Fixtures {
		var out string
		if err := db.QueryRow("SELECT CAST(json_serialize_sql(?::VARCHAR) AS VARCHAR)", query).Scan(&out); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var compact bytes.Buffer
		if err := json.Compact(&compact, []byte(out)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		path := filepath.Join("testdata", "duckdb2", name+".json")
		if *updateDuckDB2 {
			if err := os.WriteFile(path, append(compact.Bytes(), '\n'), 0o644); err != nil {
				t.Fatal(err)
			}
			continue
		}
		want, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := compact.String(); got != string(bytes.TrimSpace(want)) {
			t.Errorf("%s: embedded DuckDB serializes %q differently from the fixture\ngot:  %s\nwant: %s", name, query, got, want)
		}
	}
}
