// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package validation

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// ---------- Audit Integration Tests ----------

func mustInt64(s string) int64 {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("mustInt64(%q): %v", s, err))
	}
	return v
}

// setupDuckLake attaches a temporary DuckLake catalog to the shared session.
// Returns the catalog-qualified table prefix (e.g. "lake.main") and a cleanup function.
func setupDuckLake(t *testing.T, alias string) (string, func()) {
	t.Helper()
	dir := t.TempDir()
	catPath := filepath.Join(dir, "cat.sqlite")
	if err := shared.Exec(fmt.Sprintf("ATTACH 'ducklake:%s' AS %s", catPath, alias)); err != nil {
		t.Fatalf("attach ducklake: %v", err)
	}
	cleanup := func() {
		_ = shared.Exec(fmt.Sprintf("DETACH %s", alias))
	}
	return alias + ".main", cleanup
}

func TestIntegration_Audit_RowCount(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_arc AS SELECT * FROM (VALUES (1), (2), (3)) t(id)`); err != nil {
		t.Fatal(err)
	}

	// Pass: count >= 1
	rows := execAudit(t, "row_count >= 1", "test_arc", 0)
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	// Fail: count >= 100
	rows = execAudit(t, "row_count >= 100", "test_arc", 0)
	if len(rows) == 0 {
		t.Error("fail: expected error for row_count < 100")
	}
}

func TestIntegration_Audit_RowCountAllOps(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_arco AS SELECT * FROM (VALUES (1), (2), (3)) t(id)`); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		directive string
		wantFail  bool
	}{
		{"row_count > 0", false},
		{"row_count > 100", true},
		{"row_count < 100", false},
		{"row_count < 1", true},
		{"row_count <= 3", false},
		{"row_count <= 2", true},
		{"row_count = 3", false},
		{"row_count = 99", true},
	} {
		t.Run(tc.directive, func(t *testing.T) {
			rows := execAudit(t, tc.directive, "test_arco", 0)
			if tc.wantFail && len(rows) == 0 {
				t.Errorf("expected fail for %q", tc.directive)
			}
			if !tc.wantFail && len(rows) != 0 {
				t.Errorf("expected pass for %q, got %v", tc.directive, rows)
			}
		})
	}
}

func TestIntegration_Audit_RowCountChange(t *testing.T) {
	prefix, cleanup := setupDuckLake(t, "lake_rcc")
	defer cleanup()
	table := prefix + ".rcc"

	if err := shared.Exec(fmt.Sprintf("CREATE TABLE %s(id INTEGER)", table)); err != nil {
		t.Fatal(err)
	}
	if err := shared.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1), (2), (3)", table)); err != nil {
		t.Fatal(err)
	}
	// Snapshot after initial insert
	v1, err := shared.QueryValue("SELECT MAX(snapshot_id) FROM ducklake_snapshots('lake_rcc')")
	if err != nil {
		t.Fatal(err)
	}

	// Add one row (33% change) — within 50% threshold → pass
	if err := shared.Exec(fmt.Sprintf("INSERT INTO %s VALUES (4)", table)); err != nil {
		t.Fatal(err)
	}
	rows := execAudit(t, "row_count_change < 50%%", table, mustInt64(v1))
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	// Delete most rows (large change) → fail
	if err := shared.Exec(fmt.Sprintf("DELETE FROM %s WHERE id > 1", table)); err != nil {
		t.Fatal(err)
	}
	rows = execAudit(t, "row_count_change < 50%%", table, mustInt64(v1))
	if len(rows) == 0 {
		t.Error("fail: expected error for >50% row count change")
	}
}

func TestIntegration_Audit_Freshness(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_fresh AS SELECT NOW() AS updated_at UNION ALL SELECT NOW() - INTERVAL '1 HOUR'`); err != nil {
		t.Fatal(err)
	}

	// Pass: freshness within 24h
	rows := execAudit(t, "freshness(updated_at, 24h)", "test_fresh", 0)
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	// Fail: stale data
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_fresh_f AS SELECT TIMESTAMP '2000-01-01' AS updated_at`); err != nil {
		t.Fatal(err)
	}
	rows = execAudit(t, "freshness(updated_at, 24h)", "test_fresh_f", 0)
	if len(rows) == 0 {
		t.Error("fail: expected error for stale data")
	}
}

func TestIntegration_Audit_FreshnessDays(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_freshd AS SELECT NOW() AS ts`); err != nil {
		t.Fatal(err)
	}

	rows := execAudit(t, "freshness(ts, 7d)", "test_freshd", 0)
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_freshd_f AS SELECT TIMESTAMP '2000-01-01' AS ts`); err != nil {
		t.Fatal(err)
	}
	rows = execAudit(t, "freshness(ts, 7d)", "test_freshd_f", 0)
	if len(rows) == 0 {
		t.Error("fail: expected error for stale data")
	}
}

func TestIntegration_Audit_MeanBetween(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_mean AS SELECT * FROM (VALUES (10), (20), (30)) t(val)`); err != nil {
		t.Fatal(err)
	}
	// mean = 20
	rows := execAudit(t, "mean(val) BETWEEN 15 AND 25", "test_mean", 0)
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	rows = execAudit(t, "mean(val) BETWEEN 0 AND 10", "test_mean", 0)
	if len(rows) == 0 {
		t.Error("fail: expected error for mean outside range")
	}
}

func TestIntegration_Audit_MeanComparison(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_meanc AS SELECT * FROM (VALUES (10), (20), (30)) t(val)`); err != nil {
		t.Fatal(err)
	}
	// mean = 20
	rows := execAudit(t, "mean(val) >= 10", "test_meanc", 0)
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	rows = execAudit(t, "mean(val) >= 50", "test_meanc", 0)
	if len(rows) == 0 {
		t.Error("fail: expected error for mean < 50")
	}
}

func TestIntegration_Audit_Stddev(t *testing.T) {
	t.Parallel()
	// Low variance data
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_std AS SELECT * FROM (VALUES (10), (10), (11), (10)) t(val)`); err != nil {
		t.Fatal(err)
	}
	rows := execAudit(t, "stddev(val) < 5", "test_std", 0)
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	// High variance data
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_std_f AS SELECT * FROM (VALUES (1), (100), (1000), (10000)) t(val)`); err != nil {
		t.Fatal(err)
	}
	rows = execAudit(t, "stddev(val) < 5", "test_std_f", 0)
	if len(rows) == 0 {
		t.Error("fail: expected error for high stddev")
	}
}

func TestIntegration_Audit_Min(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_min AS SELECT * FROM (VALUES (5), (10), (15)) t(val)`); err != nil {
		t.Fatal(err)
	}
	rows := execAudit(t, "min(val) >= 5", "test_min", 0)
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	rows = execAudit(t, "min(val) >= 10", "test_min", 0)
	if len(rows) == 0 {
		t.Error("fail: expected error for min < 10")
	}
}

func TestIntegration_Audit_Max(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_max AS SELECT * FROM (VALUES (5), (10), (15)) t(val)`); err != nil {
		t.Fatal(err)
	}
	rows := execAudit(t, "max(val) <= 15", "test_max", 0)
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	rows = execAudit(t, "max(val) <= 10", "test_max", 0)
	if len(rows) == 0 {
		t.Error("fail: expected error for max > 10")
	}
}

func TestIntegration_Audit_Sum(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_sum AS SELECT * FROM (VALUES (10), (20), (30)) t(val)`); err != nil {
		t.Fatal(err)
	}
	// sum = 60
	rows := execAudit(t, "sum(val) >= 50", "test_sum", 0)
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	rows = execAudit(t, "sum(val) >= 100", "test_sum", 0)
	if len(rows) == 0 {
		t.Error("fail: expected error for sum < 100")
	}
}

func TestIntegration_Audit_Zscore(t *testing.T) {
	t.Parallel()
	// Tight cluster, no outliers
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_zs AS SELECT * FROM (VALUES (10), (11), (10), (11), (10), (11)) t(val)`); err != nil {
		t.Fatal(err)
	}
	rows := execAudit(t, "zscore(val) < 3", "test_zs", 0)
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	// With extreme outlier
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_zs_f AS SELECT * FROM (VALUES (10), (10), (10), (10), (10), (1000)) t(val)`); err != nil {
		t.Fatal(err)
	}
	rows = execAudit(t, "zscore(val) < 2", "test_zs_f", 0)
	if len(rows) == 0 {
		t.Error("fail: expected error for outlier zscore")
	}
}

func TestIntegration_Audit_Percentile(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_pct AS SELECT * FROM generate_series(1, 100) t(val)`); err != nil {
		t.Fatal(err)
	}
	// p95 ≈ 95
	rows := execAudit(t, "percentile(val, 0.95) <= 100", "test_pct", 0)
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	rows = execAudit(t, "percentile(val, 0.95) <= 50", "test_pct", 0)
	if len(rows) == 0 {
		t.Error("fail: expected error for p95 > 50")
	}
}

func TestIntegration_Audit_ReconcileCount(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_rc1 AS SELECT * FROM (VALUES (1), (2), (3)) t(id)`); err != nil {
		t.Fatal(err)
	}
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_rc2 AS SELECT * FROM (VALUES (10), (20), (30)) t(id)`); err != nil {
		t.Fatal(err)
	}

	// Pass: same row count
	rows := execAudit(t, "reconcile_count(test_rc2)", "test_rc1", 0)
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	// Fail: different row count
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_rc3 AS SELECT * FROM (VALUES (1), (2)) t(id)`); err != nil {
		t.Fatal(err)
	}
	rows = execAudit(t, "reconcile_count(test_rc3)", "test_rc1", 0)
	if len(rows) == 0 {
		t.Error("fail: expected error for different row counts")
	}
}

func TestIntegration_Audit_ReconcileSum(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_rs1 AS SELECT * FROM (VALUES (10), (20), (30)) t(amount)`); err != nil {
		t.Fatal(err)
	}
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_rs2 AS SELECT * FROM (VALUES (25), (35)) t(total)`); err != nil {
		t.Fatal(err)
	}

	// Pass: both sum to 60
	rows := execAudit(t, "reconcile_sum(amount, test_rs2.total)", "test_rs1", 0)
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	// Fail: different sums
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_rs3 AS SELECT * FROM (VALUES (1), (2)) t(total)`); err != nil {
		t.Fatal(err)
	}
	rows = execAudit(t, "reconcile_sum(amount, test_rs3.total)", "test_rs1", 0)
	if len(rows) == 0 {
		t.Error("fail: expected error for different sums")
	}
}

func TestIntegration_Audit_ColumnExists(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_ce(id INTEGER, name VARCHAR)`); err != nil {
		t.Fatal(err)
	}

	// Pass: column exists
	rows := execAudit(t, "column_exists(name)", "test_ce", 0)
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	// Fail: column doesn't exist
	rows = execAudit(t, "column_exists(nonexistent)", "test_ce", 0)
	if len(rows) == 0 {
		t.Error("fail: expected error for missing column")
	}
}

func TestIntegration_Audit_ColumnType(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_ct(id INTEGER, name VARCHAR)`); err != nil {
		t.Fatal(err)
	}

	// Pass: correct type
	rows := execAudit(t, "column_type(id, INTEGER)", "test_ct", 0)
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	// Fail: wrong type
	rows = execAudit(t, "column_type(id, VARCHAR)", "test_ct", 0)
	if len(rows) == 0 {
		t.Error("fail: expected error for wrong column type")
	}
}

func TestIntegration_Audit_Golden(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_gold AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(id, name)`); err != nil {
		t.Fatal(err)
	}

	// Write matching CSV → pass
	dir := t.TempDir()
	matchCSV := filepath.Join(dir, "match.csv")
	if err := os.WriteFile(matchCSV, []byte("id,name\n1,a\n2,b\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	rows := execAudit(t, "golden('"+matchCSV+"')", "test_gold", 0)
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	// Write non-matching CSV → fail
	mismatchCSV := filepath.Join(dir, "mismatch.csv")
	if err := os.WriteFile(mismatchCSV, []byte("id,name\n1,a\n3,c\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	rows = execAudit(t, "golden('"+mismatchCSV+"')", "test_gold", 0)
	if len(rows) == 0 {
		t.Error("fail: expected error for mismatched golden data")
	}
}

func TestIntegration_Audit_DistributionStable(t *testing.T) {
	prefix, cleanup := setupDuckLake(t, "lake_dist")
	defer cleanup()
	table := prefix + ".dist"

	if err := shared.Exec(fmt.Sprintf("CREATE TABLE %s(cat VARCHAR)", table)); err != nil {
		t.Fatal(err)
	}
	// 50/50 distribution
	if err := shared.Exec(fmt.Sprintf("INSERT INTO %s VALUES ('a'),('a'),('a'),('a'),('a'),('b'),('b'),('b'),('b'),('b')", table)); err != nil {
		t.Fatal(err)
	}
	v1, err := shared.QueryValue("SELECT MAX(snapshot_id) FROM ducklake_snapshots('lake_dist')")
	if err != nil {
		t.Fatal(err)
	}

	// Same distribution after small change → pass (within 10% default threshold)
	if err := shared.Exec(fmt.Sprintf("INSERT INTO %s VALUES ('a'),('b')", table)); err != nil {
		t.Fatal(err)
	}
	rows := execAudit(t, "distribution(cat) STABLE", table, mustInt64(v1))
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	// Drastically skew distribution → fail
	if err := shared.Exec(fmt.Sprintf("DELETE FROM %s WHERE cat = 'b'", table)); err != nil {
		t.Fatal(err)
	}
	// Insert many 'a' to make it ~100% 'a'
	if err := shared.Exec(fmt.Sprintf("INSERT INTO %s SELECT 'a' FROM generate_series(1, 20)", table)); err != nil {
		t.Fatal(err)
	}
	rows = execAudit(t, "distribution(cat) STABLE", table, mustInt64(v1))
	if len(rows) == 0 {
		t.Error("fail: expected error for unstable distribution")
	}
}
