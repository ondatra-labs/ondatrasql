// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute_test

import (
	"fmt"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// TestSchemaEvolution_KeepsCallerBackfillDecision pins that reporting a schema
// change cannot cancel a rebuild the runner already decided on.
//
// detectSchemaEvolution takes needsBackfill as an argument and two of its
// branches returned a bare false, discarding it. Adding a column to an `append`
// model changes its SQL hash, so the run is a backfill — and the downgrade made
// it append the full source on top of the rows already there instead of
// replacing them. Two rows became four.
func TestSchemaEvolution_KeepsCallerBackfillDecision(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	p.AddModel("raw/src.sql", "-- @kind: table\nSELECT 1 AS id UNION ALL SELECT 2\n")
	runModel(t, p, "raw/src.sql")

	p.AddModel("staging/evolving.sql", `-- @kind: append
SELECT s.id::BIGINT AS id, 'a'::VARCHAR AS v FROM raw.src s
`)
	runModel(t, p, "staging/evolving.sql")

	count := func(when string) int64 {
		t.Helper()
		rows, err := p.Sess.QueryRows("SELECT count(*)::BIGINT FROM staging.evolving")
		if err != nil {
			t.Fatalf("count %s: %v", when, err)
		}
		var n int64
		if _, err := fmt.Sscanf(rows[0], "%d", &n); err != nil {
			t.Fatalf("parse count %s (%q): %v", when, rows[0], err)
		}
		return n
	}
	if got := count("after first run"); got != 2 {
		t.Fatalf("first run should load 2 rows, got %d", got)
	}

	// Adding a column is both an additive schema change and a hash change, so
	// the run type is backfill and the schema-evolution path runs.
	p.AddModel("staging/evolving.sql", `-- @kind: append
SELECT s.id::BIGINT AS id, 'a'::VARCHAR AS v, 1::BIGINT AS w FROM raw.src s
`)
	r := runModel(t, p, "staging/evolving.sql")
	if len(r.Errors) > 0 {
		t.Fatalf("rerun failed: %v", r.Errors)
	}

	if got := count("after adding a column"); got != 2 {
		t.Errorf("a backfill must replace rows, not append to them, got %d rows", got)
	}
	// run_type stays "incremental" here on purpose: no late escalation to a
	// rebuild relabels the run, so this asserts the data, not the label.
	if r.RunType != "incremental" {
		t.Errorf("run_type should be unchanged by this fix, got %q", r.RunType)
	}
}
