// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build e2e

// Package e2e tests every shipped macro and variable through the full pipeline:
// parser → dispatcher → macro loading → DuckLake transaction → commit/rollback.
//
// These tests verify that what ondatrasql init generates actually works.

package e2e

import (
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// =============================================================================
// Constraint macros — each must block bad data (FAIL) and pass good data (PASS)
// =============================================================================

func TestE2E_Macro_Constraint_NotNull(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("raw/src.sql", "-- @kind: table\nSELECT 1 AS id, 'Alice' AS name\n")
	runModel(t, p, "raw/src.sql")

	// PASS
	p.AddModel("staging/ok.sql", "-- @kind: table\n-- @constraint: not_null(id)\nSELECT id, name FROM raw.src\n")
	runModel(t, p, "staging/ok.sql")

	// FAIL
	p.AddModel("staging/bad.sql", "-- @kind: table\n-- @constraint: not_null(id)\nSELECT NULL AS id, 'x' AS name\n")
	_, err := runModelErr(t, p, "staging/bad.sql")
	if err == nil {
		t.Fatal("not_null should reject NULL")
	}
}

func TestE2E_Macro_Constraint_Unique(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("raw/src.sql", "-- @kind: table\nSELECT 1 AS id UNION ALL SELECT 1\n")
	runModel(t, p, "raw/src.sql")

	p.AddModel("staging/bad.sql", "-- @kind: table\n-- @constraint: unique(id)\nSELECT * FROM raw.src\n")
	_, err := runModelErr(t, p, "staging/bad.sql")
	if err == nil {
		t.Fatal("unique should reject duplicates")
	}
}

func TestE2E_Macro_Constraint_PrimaryKey(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/ok.sql", "-- @kind: table\n-- @constraint: primary_key(id)\nSELECT 1 AS id UNION ALL SELECT 2\n")
	runModel(t, p, "staging/ok.sql")

	p.AddModel("staging/bad.sql", "-- @kind: table\n-- @constraint: primary_key(id)\nSELECT 1 AS id UNION ALL SELECT 1\n")
	_, err := runModelErr(t, p, "staging/bad.sql")
	if err == nil {
		t.Fatal("primary_key should reject duplicates")
	}
}

func TestE2E_Macro_Constraint_Compare(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/ok.sql", "-- @kind: table\n-- @constraint: compare(amount, >=, 0)\nSELECT 1 AS id, 100 AS amount\n")
	runModel(t, p, "staging/ok.sql")

	p.AddModel("staging/bad.sql", "-- @kind: table\n-- @constraint: compare(amount, >=, 0)\nSELECT 1 AS id, -50 AS amount\n")
	_, err := runModelErr(t, p, "staging/bad.sql")
	if err == nil {
		t.Fatal("compare should reject negative amount")
	}
}

func TestE2E_Macro_Constraint_Between(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/bad.sql", "-- @kind: table\n-- @constraint: between(amount, 0, 100)\nSELECT 1 AS id, 999 AS amount\n")
	_, err := runModelErr(t, p, "staging/bad.sql")
	if err == nil {
		t.Fatal("between should reject out-of-range")
	}
}

func TestE2E_Macro_Constraint_InList(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/ok.sql", "-- @kind: table\n-- @constraint: in_list(status, 'active,pending')\nSELECT 1 AS id, 'active' AS status\n")
	runModel(t, p, "staging/ok.sql")

	p.AddModel("staging/bad.sql", "-- @kind: table\n-- @constraint: in_list(status, 'active,pending')\nSELECT 1 AS id, 'deleted' AS status\n")
	_, err := runModelErr(t, p, "staging/bad.sql")
	if err == nil {
		t.Fatal("in_list should reject 'deleted'")
	}
}

func TestE2E_Macro_Constraint_NotIn(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/bad.sql", "-- @kind: table\n-- @constraint: not_in(status, 'deleted,banned')\nSELECT 1 AS id, 'deleted' AS status\n")
	_, err := runModelErr(t, p, "staging/bad.sql")
	if err == nil {
		t.Fatal("not_in should reject 'deleted'")
	}
}

func TestE2E_Macro_Constraint_Email(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/ok.sql", "-- @kind: table\n-- @constraint: email(email)\nSELECT 'alice@test.com' AS email\n")
	runModel(t, p, "staging/ok.sql")

	p.AddModel("staging/bad.sql", "-- @kind: table\n-- @constraint: email(email)\nSELECT 'not-an-email' AS email\n")
	_, err := runModelErr(t, p, "staging/bad.sql")
	if err == nil {
		t.Fatal("email should reject invalid email")
	}
}

func TestE2E_Macro_Constraint_UUID(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/ok.sql", "-- @kind: table\n-- @constraint: uuid(uid)\nSELECT 'a1b2c3d4-e5f6-7890-abcd-ef1234567890' AS uid\n")
	runModel(t, p, "staging/ok.sql")

	p.AddModel("staging/bad.sql", "-- @kind: table\n-- @constraint: uuid(uid)\nSELECT 'not-a-uuid' AS uid\n")
	_, err := runModelErr(t, p, "staging/bad.sql")
	if err == nil {
		t.Fatal("uuid should reject invalid UUID")
	}
}

func TestE2E_Macro_Constraint_LengthEq(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/bad.sql", "-- @kind: table\n-- @constraint: length_eq(code, 2)\nSELECT 'ABC' AS code\n")
	_, err := runModelErr(t, p, "staging/bad.sql")
	if err == nil {
		t.Fatal("length_eq should reject length != 2")
	}
}

func TestE2E_Macro_Constraint_Check(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/bad.sql", "-- @kind: table\n-- @constraint: check(amount, amount > 0 AND amount < 100)\nSELECT 999 AS amount\n")
	_, err := runModelErr(t, p, "staging/bad.sql")
	if err == nil {
		t.Fatal("check should reject amount=999")
	}
}

func TestE2E_Macro_Constraint_RequiredIf(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/bad.sql", "-- @kind: table\n-- @constraint: required_if(email, status = 'active')\nSELECT 1 AS id, 'active' AS status, NULL AS email\n")
	_, err := runModelErr(t, p, "staging/bad.sql")
	if err == nil {
		t.Fatal("required_if should reject NULL email where status='active'")
	}
}

func TestE2E_Macro_Constraint_Sequential(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/ok.sql", "-- @kind: table\n-- @constraint: sequential(id)\nSELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3\n")
	runModel(t, p, "staging/ok.sql")
}

func TestE2E_Macro_Constraint_DistinctCount(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/bad.sql", "-- @kind: table\n-- @constraint: distinct_count(region, >=, 5)\nSELECT 'SE' AS region UNION ALL SELECT 'US'\n")
	_, err := runModelErr(t, p, "staging/bad.sql")
	if err == nil {
		t.Fatal("distinct_count should reject 2 distinct < 5")
	}
}

// =============================================================================
// Audit macros — each must rollback on failure, commit on success
// =============================================================================

func TestE2E_Macro_Audit_RowCount(t *testing.T) {
	p := testutil.NewProject(t)
	// PASS
	p.AddModel("staging/ok.sql", "-- @kind: table\n-- @audit: row_count(>=, 1)\nSELECT 1 AS id\n")
	runModel(t, p, "staging/ok.sql")

	// FAIL + rollback
	p.AddModel("staging/fail.sql", "-- @kind: table\n-- @audit: row_count(>=, 100)\nSELECT 1 AS id\n")
	_, err := runModelErr(t, p, "staging/fail.sql")
	if err == nil {
		t.Fatal("row_count should fail for count=1 < 100")
	}
}

func TestE2E_Macro_Audit_Mean(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/fail.sql", "-- @kind: table\n-- @audit: mean(amount, >=, 500)\nSELECT 1 AS id, 100 AS amount UNION ALL SELECT 2, 200\n")
	_, err := runModelErr(t, p, "staging/fail.sql")
	if err == nil {
		t.Fatal("mean should fail for avg=150 < 500")
	}
}

func TestE2E_Macro_Audit_Min(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/fail.sql", "-- @kind: table\n-- @audit: min(amount, >=, 200)\nSELECT 1 AS id, 100 AS amount UNION ALL SELECT 2, 300\n")
	_, err := runModelErr(t, p, "staging/fail.sql")
	if err == nil {
		t.Fatal("min should fail for min=100 < 200")
	}
}

func TestE2E_Macro_Audit_Max(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/fail.sql", "-- @kind: table\n-- @audit: max(amount, <=, 100)\nSELECT 1 AS id, 100 AS amount UNION ALL SELECT 2, 500\n")
	_, err := runModelErr(t, p, "staging/fail.sql")
	if err == nil {
		t.Fatal("max should fail for max=500 > 100")
	}
}

func TestE2E_Macro_Audit_Sum(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/fail.sql", "-- @kind: table\n-- @audit: sum(amount, >=, 10000)\nSELECT 1 AS id, 100 AS amount\n")
	_, err := runModelErr(t, p, "staging/fail.sql")
	if err == nil {
		t.Fatal("sum should fail for sum=100 < 10000")
	}
}

func TestE2E_Macro_Audit_ColumnExists(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/fail.sql", "-- @kind: table\n-- @audit: column_exists(nonexistent)\nSELECT 1 AS id\n")
	_, err := runModelErr(t, p, "staging/fail.sql")
	if err == nil {
		t.Fatal("column_exists should fail for missing column")
	}
}

func TestE2E_Macro_Audit_ColumnType(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/ok.sql", "-- @kind: table\n-- @audit: column_type(id, INTEGER)\nSELECT 1 AS id\n")
	runModel(t, p, "staging/ok.sql")

	p.AddModel("staging/fail.sql", "-- @kind: table\n-- @audit: column_type(id, VARCHAR)\nSELECT 1 AS id\n")
	_, err := runModelErr(t, p, "staging/fail.sql")
	if err == nil {
		t.Fatal("column_type should fail for INTEGER != VARCHAR")
	}
}

func TestE2E_Macro_Audit_ReconcileCount(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("raw/a.sql", "-- @kind: table\nSELECT 1 AS id UNION ALL SELECT 2\n")
	p.AddModel("raw/b.sql", "-- @kind: table\nSELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3\n")
	runModel(t, p, "raw/a.sql")
	runModel(t, p, "raw/b.sql")

	p.AddModel("staging/fail.sql", "-- @kind: table\n-- @audit: reconcile_count(raw.b)\nSELECT * FROM raw.a\n")
	_, err := runModelErr(t, p, "staging/fail.sql")
	if err == nil {
		t.Fatal("reconcile_count should fail for 2 != 3")
	}
}

func TestE2E_Macro_Audit_Freshness(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/ok.sql", "-- @kind: table\n-- @audit: freshness(ts, 24h)\nSELECT 1 AS id, NOW() AS ts\n")
	runModel(t, p, "staging/ok.sql")
}

func TestE2E_Macro_Audit_Percentile(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/fail.sql", "-- @kind: table\n-- @audit: percentile(amount, 0.95, <=, 100)\nSELECT 1 AS id, 500 AS amount\n")
	_, err := runModelErr(t, p, "staging/fail.sql")
	if err == nil {
		t.Fatal("percentile should fail for p95=500 > 100")
	}
}

// =============================================================================
// Audit atomicity — failed audit must rollback both schema and data
// =============================================================================

func TestE2E_Macro_Audit_Atomicity(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/t.sql", "-- @kind: table\nSELECT 1 AS id, 100 AS amount\n")
	runModel(t, p, "staging/t.sql")

	// Add column + failing audit → both should rollback
	p.AddModel("staging/t.sql", "-- @kind: table\n-- @audit: row_count(=, 0)\nSELECT 1 AS id, 100 AS amount, 'web' AS channel\n")
	_, err := runModelErr(t, p, "staging/t.sql")
	if err == nil {
		t.Fatal("audit should fail")
	}

	// Column must NOT exist (ALTER rolled back)
	val, qErr := p.Sess.QueryValue("SELECT COUNT(*) FROM (DESCRIBE staging.t) WHERE column_name = 'channel'")
	if qErr != nil {
		t.Fatalf("describe: %v", qErr)
	}
	if val != "0" {
		t.Error("DORA violation: column 'channel' added despite audit failure")
	}
}

// =============================================================================
// Warning macros — must log without blocking
// =============================================================================

func TestE2E_Macro_Warning_RowCount(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/t.sql", "-- @kind: table\n-- @warning: row_count(>=, 1000)\nSELECT 1 AS id\n")
	result := runModel(t, p, "staging/t.sql")

	hasWarning := false
	for _, w := range result.Warnings {
		if strings.Contains(w, "row_count") {
			hasWarning = true
		}
	}
	if !hasWarning {
		t.Errorf("expected row_count warning, got: %v", result.Warnings)
	}
}

func TestE2E_Macro_Warning_Freshness(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/t.sql", "-- @kind: table\n-- @warning: freshness(ts, 1h)\nSELECT 1 AS id, TIMESTAMP '2020-01-01' AS ts\n")
	result := runModel(t, p, "staging/t.sql")

	hasWarning := false
	for _, w := range result.Warnings {
		if strings.Contains(w, "freshness") {
			hasWarning = true
		}
	}
	if !hasWarning {
		t.Errorf("expected freshness warning, got: %v", result.Warnings)
	}
}

// =============================================================================
// Warning delta — must detect changes between runs via table_changes()
// =============================================================================

func TestE2E_Macro_Warning_RowCountDelta(t *testing.T) {
	p := testutil.NewProject(t)

	// Run 1: initial (no warning — first run)
	p.AddModel("raw/src.sql", "-- @kind: table\nSELECT 1 AS id, 100 AS amount UNION ALL SELECT 2, 200 UNION ALL SELECT 3, 300\n")
	p.AddModel("staging/t.sql", "-- @kind: table\n-- @warning: row_count_delta(20)\nSELECT id, amount FROM raw.src\n")
	runModel(t, p, "raw/src.sql")
	r1 := runModel(t, p, "staging/t.sql")

	// First run should NOT warn (no previous snapshot)
	for _, w := range r1.Warnings {
		if strings.Contains(w, "row_count_delta") && !strings.Contains(w, "error") {
			t.Errorf("first run should not produce delta warning, got: %s", w)
		}
	}

	// Run 2: big change (3 → 13 rows)
	p.AddModel("raw/src.sql", "-- @kind: table\nSELECT id, id * 100 AS amount FROM generate_series(1, 13) t(id)\n")
	runModel(t, p, "raw/src.sql")
	r2 := runModel(t, p, "staging/t.sql")

	hasDelta := false
	for _, w := range r2.Warnings {
		if strings.Contains(w, "row_count_delta") && !strings.Contains(w, "error") {
			hasDelta = true
		}
	}
	if !hasDelta {
		t.Errorf("expected row_count_delta warning after big change, got: %v", r2.Warnings)
	}
}

// =============================================================================
// Masking macros — must transform data in materialized output
// =============================================================================

func TestE2E_Macro_Masking_Email(t *testing.T) {
	p := testutil.NewProject(t)

	// Define masking macros (normally in config/macros/masking.sql — already loaded by testutil)
	p.AddModel("staging/masked.sql", "-- @kind: table\n-- @column: email = Contact | mask_email\nSELECT 1 AS id, 'alice@example.com' AS email\n")
	runModel(t, p, "staging/masked.sql")

	val, err := p.Sess.QueryValue("SELECT email FROM staging.masked")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if strings.Contains(val, "alice@") {
		t.Errorf("email should be masked, got: %s", val)
	}
	if !strings.Contains(val, "a***@") {
		t.Errorf("expected a***@ masking pattern, got: %s", val)
	}
}

// =============================================================================
// Helper macros — must be available in model SQL
// =============================================================================

func TestE2E_Macro_Helper_SafeDivide(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/t.sql", "-- @kind: table\nSELECT safe_divide(100, 0) AS result\n")
	runModel(t, p, "staging/t.sql")

	val, err := p.Sess.QueryValue("SELECT result FROM staging.t")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "NULL" && val != "" {
		t.Errorf("safe_divide(100, 0) should be NULL, got: %s", val)
	}
}

// =============================================================================
// Variables — constants, global, and local must be accessible
// =============================================================================

func TestE2E_Variable_Constants(t *testing.T) {
	p := testutil.NewProject(t)
	// alert_amount_threshold is set in config/variables/constants.sql
	p.AddModel("staging/t.sql", "-- @kind: table\nSELECT getvariable('alert_amount_threshold')::BIGINT AS threshold\n")
	runModel(t, p, "staging/t.sql")

	val, err := p.Sess.QueryValue("SELECT threshold FROM staging.t")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "1000000" {
		t.Errorf("alert_amount_threshold should be 1000000, got: %s", val)
	}
}

// =============================================================================
// Combined — constraint + audit + warning on same model
// =============================================================================

func TestE2E_Macro_Combined_AllThree(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/t.sql", `-- @kind: table
-- @constraint: not_null(id)
-- @constraint: compare(amount, >=, 0)
-- @audit: row_count(>=, 1)
-- @warning: row_count(>=, 1000)
SELECT 1 AS id, 100 AS amount UNION ALL SELECT 2, 200
`)
	result := runModel(t, p, "staging/t.sql")

	// Constraints passed (no error)
	// Audit passed (row_count >= 1)
	// Warning should fire (row_count < 1000)
	hasWarning := false
	for _, w := range result.Warnings {
		if strings.Contains(w, "row_count") {
			hasWarning = true
		}
	}
	if !hasWarning {
		t.Errorf("expected row_count warning, got: %v", result.Warnings)
	}

	// Data committed
	val, err := p.Sess.QueryValue("SELECT COUNT(*) FROM staging.t")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "2" {
		t.Errorf("expected 2 rows, got: %s", val)
	}
}

// =============================================================================
// Unknown macro — must produce clear error, not crash
// =============================================================================

func TestE2E_Macro_UnknownAudit(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/t.sql", "-- @kind: table\n-- @audit: this_does_not_exist(amount)\nSELECT 1 AS id, 100 AS amount\n")
	_, err := runModelErr(t, p, "staging/t.sql")
	if err == nil {
		t.Fatal("unknown audit macro should produce error")
	}
}

func TestE2E_Macro_UnknownConstraint(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/t.sql", "-- @kind: table\n-- @constraint: this_does_not_exist(id)\nSELECT 1 AS id\n")
	_, err := runModelErr(t, p, "staging/t.sql")
	if err == nil {
		t.Fatal("unknown constraint macro should produce error")
	}
}
