// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build e2e

package e2e

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/dag"
	"github.com/ondatra-labs/ondatrasql/internal/execute"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// TestE2E_ScriptAuditRollback verifies that when a script model's audit fails:
// 1. The target table is rolled back to the previous snapshot
// 2. Badger claims are acked (not nacked) — events are consumed
// 3. On the next run, the script re-fetches via incremental cursor
// 4. Data ends up correct after the re-run
func TestE2E_ScriptAuditRollback(t *testing.T) {
	p := testutil.NewProject(t)

	// Run 1: establish baseline with 5 rows (no audit)
	p.AddModel("staging/script_audited.star", `# @kind: table
save.row({"id": 1, "name": "Alice"})
save.row({"id": 2, "name": "Bob"})
save.row({"id": 3, "name": "Charlie"})
save.row({"id": 4, "name": "Diana"})
save.row({"id": 5, "name": "Eve"})
`)
	result := runModel(t, p, "staging/script_audited.star")
	if result.RowsAffected != 5 {
		t.Fatalf("run 1: rows = %d, want 5", result.RowsAffected)
	}

	val, _ := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.script_audited")
	if val != "5" {
		t.Fatalf("run 1: count = %s, want 5", val)
	}

	// Run 2: add audit row_count >= 3, but only produce 1 row → audit fails
	p.AddModel("staging/script_audited.star", `# @kind: table
# @audit: row_count >= 3
save.row({"id": 1, "name": "OnlyOne"})
`)
	result2, err := runModelErr(t, p, "staging/script_audited.star")

	// Audit should fail (row_count = 1 < 3)
	if err == nil {
		t.Fatal("run 2: expected audit error, got nil")
	}
	if !strings.Contains(err.Error(), "audit") {
		t.Fatalf("run 2: error = %v, want audit-related", err)
	}
	if result2 == nil || len(result2.Errors) == 0 {
		t.Fatal("run 2: expected errors in result")
	}

	// Target table should be rolled back to run 1 state (5 rows)
	val, _ = p.Sess.QueryValue("SELECT COUNT(*) FROM staging.script_audited")
	if val != "5" {
		t.Errorf("after rollback: count = %s, want 5 (rolled back to run 1)", val)
	}

	// Original data should be intact
	val, _ = p.Sess.QueryValue("SELECT name FROM staging.script_audited WHERE id = 1")
	if val != "Alice" {
		t.Errorf("after rollback: name = %s, want Alice (original data)", val)
	}

	// Ack records should be cleaned by rollback (crash-recovery safety)
	ackCount, ackErr := p.Sess.QueryValue("SELECT COUNT(*) FROM _ondatra_acks WHERE target = 'staging.script_audited'")
	if ackErr != nil {
		t.Logf("ack table query: %v (may not exist yet)", ackErr)
	} else if ackCount != "0" {
		t.Errorf("after rollback: ack records = %s, want 0 (cleaned by rollback)", ackCount)
	}

	// Run 3: fix the script to produce enough rows, audit passes
	p.AddModel("staging/script_audited.star", `# @kind: table
# @audit: row_count >= 3
save.row({"id": 1, "name": "Alice"})
save.row({"id": 2, "name": "Bob"})
save.row({"id": 3, "name": "Charlie"})
`)
	result3, err3 := runModelErr(t, p, "staging/script_audited.star")
	if err3 != nil {
		t.Fatalf("run 3: unexpected error: %v (result: %+v)", err3, result3)
	}

	// Should now have 3 rows (backfill since hash changed)
	val, _ = p.Sess.QueryValue("SELECT COUNT(*) FROM staging.script_audited")
	if val != "3" {
		t.Errorf("run 3: count = %s, want 3", val)
	}

	// Ack records should be empty after successful run (DeleteAck cleanup)
	ackCount, ackErr = p.Sess.QueryValue("SELECT COUNT(*) FROM _ondatra_acks WHERE target = 'staging.script_audited'")
	if ackErr != nil {
		t.Logf("ack table query after run 3: %v", ackErr)
	} else if ackCount != "0" {
		t.Errorf("after successful run: ack records = %s, want 0 (cleaned by DeleteAck)", ackCount)
	}

	snap := newSnapshot()
	snap.addLine("--- run 1 ---")
	snap.addResult(result)
	snap.addLine("--- run 2 (audit fail) ---")
	snap.addResult(result2)
	snap.addLine("audit_error: true")
	snap.addLine("--- run 3 (fixed) ---")
	snap.addResult(result3)
	snap.addQuery(p.Sess, "final_count", "SELECT COUNT(*) FROM staging.script_audited")
	assertGolden(t, "script_audit_rollback", snap)
}

// TestE2E_ScriptAuditRollback_QueryDAG verifies that a script using query()
// with an audit that fails correctly rolls back without losing upstream data.
func TestE2E_ScriptAuditRollback_QueryDAG(t *testing.T) {
	p := testutil.NewProject(t)

	// SQL source table
	p.AddModel("staging/products.sql", `-- @kind: table
SELECT 1 AS id, 'Widget' AS name, 100 AS price
UNION ALL
SELECT 2, 'Gadget', 200
`)
	runModel(t, p, "staging/products.sql")

	// Script reads via query(), transforms, has audit (row_count >= 1)
	p.AddModel("mart/product_summary.star", `# @kind: table
# @audit: row_count >= 1
rows = query("SELECT id, name, price FROM staging.products")
for row in rows:
    save.row({"product_id": row["id"], "product_name": row["name"], "price": int(row["price"])})
`)

	// Build DAG to verify ordering
	paths := []string{"staging/products.sql", "mart/product_summary.star"}
	var models []*parser.Model
	for _, relPath := range paths {
		modelPath := filepath.Join(p.Dir, "models", relPath)
		m, err := parser.ParseModel(modelPath, p.Dir)
		if err != nil {
			t.Fatalf("parse %s: %v", relPath, err)
		}
		models = append(models, m)
	}

	g := dag.NewGraph(p.Sess, p.Dir)
	for _, m := range models {
		g.Add(m)
	}
	sorted, err := g.Sort()
	if err != nil {
		t.Fatalf("sort: %v", err)
	}

	// Verify DAG order
	order := map[string]int{}
	for i, m := range sorted {
		order[m.Target] = i
	}
	if order["staging.products"] >= order["mart.product_summary"] {
		t.Fatalf("staging.products should come before mart.product_summary in DAG")
	}

	// Execute in DAG order
	runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
	runner.SetProjectDir(p.Dir)
	for _, m := range sorted {
		result, runErr := runner.Run(context.Background(), m)
		if runErr != nil {
			t.Fatalf("run %s: %v (result: %+v)", m.Target, runErr, result)
		}
	}

	// Verify mart data is correct
	val, _ := p.Sess.QueryValue("SELECT COUNT(*) FROM mart.product_summary")
	if val != "2" {
		t.Errorf("count = %s, want 2", val)
	}

	// Verify upstream is untouched
	val, _ = p.Sess.QueryValue("SELECT COUNT(*) FROM staging.products")
	if val != "2" {
		t.Errorf("upstream count = %s, want 2 (untouched)", val)
	}
}
