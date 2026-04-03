// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute_test

import (
	"testing"

	"pgregory.net/rapid"

	"github.com/ondatra-labs/ondatrasql/internal/execute"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// --- Generators ---

func genModelKind() *rapid.Generator[string] {
	return rapid.SampledFrom([]string{"table", "append", "merge", "scd2", "partition"})
}

func genTarget() *rapid.Generator[string] {
	schema := rapid.SampledFrom([]string{"raw", "staging", "mart", "analytics"})
	table := rapid.StringMatching(`^[a-z][a-z0-9_]{1,8}$`)
	return rapid.Custom(func(t *rapid.T) string {
		return schema.Draw(t, "schema") + "." + table.Draw(t, "table")
	})
}

func genSQL() *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		base := rapid.SampledFrom([]string{
			"SELECT 1 AS id",
			"SELECT 1 AS id, 'hello' AS name",
			"SELECT 42 AS val, true AS flag",
			"SELECT CAST(1 AS INTEGER) AS id, CAST('test' AS VARCHAR) AS name",
			"SELECT a.id FROM (SELECT 1 AS id) a",
			"SELECT * FROM (VALUES (1, 'x'), (2, 'y')) AS t(id, name)",
			"SELECT 1 AS id UNION ALL SELECT 2",
		}).Draw(t, "base")
		return base
	})
}

// --- Property: ComputeSingleRunType always returns a valid RunType ---

func TestRapid_ComputeSingleRunType_AlwaysValid(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	validRunTypes := map[string]bool{
		"incremental": true,
		"backfill":    true,
		"full":        true,
	}

	rapid.Check(t, func(rt *rapid.T) {
		kind := genModelKind().Draw(rt, "kind")
		target := genTarget().Draw(rt, "target")
		sql := genSQL().Draw(rt, "sql")

		model := &parser.Model{
			Target: target,
			Kind:   kind,
			SQL:    sql,
		}

		decision, err := execute.ComputeSingleRunType(p.Sess, model)
		if err != nil {
			rt.Fatalf("ComputeSingleRunType returned error: %v", err)
		}
		if decision == nil {
			rt.Fatal("ComputeSingleRunType returned nil decision")
		}
		if !validRunTypes[decision.RunType] {
			rt.Fatalf("invalid RunType %q for kind=%q target=%q", decision.RunType, kind, target)
		}
	})
}

// --- Property: ComputeRunTypeDecisions is consistent with ComputeSingleRunType ---

func TestRapid_ComputeRunTypeDecisions_ConsistentWithSingle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Generate 1-5 unique models
		n := rapid.IntRange(1, 5).Draw(rt, "n_models")
		seen := make(map[string]bool)
		var models []*parser.Model

		for len(models) < n {
			target := genTarget().Draw(rt, "target")
			if seen[target] {
				continue
			}
			seen[target] = true

			kind := genModelKind().Draw(rt, "kind")
			sql := genSQL().Draw(rt, "sql")
			models = append(models, &parser.Model{
				Target: target,
				Kind:   kind,
				SQL:    sql,
			})
		}

		// Batch computation
		batchDecisions, err := execute.ComputeRunTypeDecisions(p.Sess, models)
		if err != nil {
			rt.Fatalf("ComputeRunTypeDecisions error: %v", err)
		}

		// Verify each model's decision matches single computation
		for _, m := range models {
			singleDecision, err := execute.ComputeSingleRunType(p.Sess, m)
			if err != nil {
				rt.Fatalf("ComputeSingleRunType error for %s: %v", m.Target, err)
			}

			batchDec := batchDecisions.GetDecision(m.Target)
			if batchDec == nil {
				rt.Fatalf("batch missing decision for %s", m.Target)
			}

			if batchDec.RunType != singleDecision.RunType {
				rt.Fatalf("inconsistent RunType for %s: batch=%q, single=%q",
					m.Target, batchDec.RunType, singleDecision.RunType)
			}
		}
	})
}

// --- Property: ComputeRunTypeDecisions with empty models returns empty map ---

func TestRapid_ComputeRunTypeDecisions_EmptyModels(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)

	decisions, err := execute.ComputeRunTypeDecisions(p.Sess, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(decisions) != 0 {
		t.Fatalf("expected empty map, got %d entries", len(decisions))
	}
}

// --- Property: GetAST never panics on arbitrary SQL ---

func TestRapid_GetAST_NoPanics(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	runner := execute.NewRunner(p.Sess, execute.ModeRun, "test-rapid")

	rapid.Check(t, func(rt *rapid.T) {
		sql := rapid.OneOf(
			// Valid SQL
			genSQL(),
			// Garbage strings
			rapid.String(),
			// SQL-like fragments
			rapid.SampledFrom([]string{
				"SELECT",
				"FROM",
				"WHERE 1=1",
				";;;",
				"SELECT * FROM",
				"DROP TABLE IF EXISTS foo",
				"CREATE TABLE t (id INT)",
				"WITH cte AS (SELECT 1) SELECT * FROM cte",
				"SELECT '",
				`SELECT "`,
				"SELECT 1; SELECT 2",
			}),
		).Draw(rt, "sql")

		// Should never panic — errors are fine
		_, _ = runner.GetAST(sql)
	})
}

// --- Property: extractLineage never panics on arbitrary SQL ---

func TestRapid_ExtractLineage_NoPanics(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	p := testutil.NewProject(t)
	runner := execute.NewRunner(p.Sess, execute.ModeRun, "test-rapid")

	rapid.Check(t, func(rt *rapid.T) {
		sql := rapid.OneOf(
			genSQL(),
			rapid.String(),
			rapid.SampledFrom([]string{
				"SELECT 1",
				"SELECT a, b, c FROM t",
				"SELECT * FROM (SELECT 1 AS x) sub",
				"INVALID SQL HERE",
				"",
				"SELECT 1 UNION ALL SELECT 2",
			}),
		).Draw(rt, "sql")

		// Should never panic
		_, _, _ = runner.ExtractLineage(sql)
	})
}
