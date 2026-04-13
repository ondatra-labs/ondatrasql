// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build e2e

// Complete e2e coverage for every shipped macro.
// Each test verifies the macro works through the full pipeline.

package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// =============================================================================
// Missing constraint tests
// =============================================================================

func TestE2E_Macro_Constraint_NotEmpty(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/bad.sql", "-- @kind: table\n-- @constraint: not_empty(name)\nSELECT '' AS name\n")
	_, err := runModelErr(t, p, "staging/bad.sql")
	if err == nil { t.Fatal("not_empty should reject empty string") }
}

func TestE2E_Macro_Constraint_AtLeastOne(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/bad.sql", "-- @kind: table\n-- @constraint: at_least_one(val)\nSELECT NULL AS val\n")
	_, err := runModelErr(t, p, "staging/bad.sql")
	if err == nil { t.Fatal("at_least_one should reject all-NULL column") }
}

func TestE2E_Macro_Constraint_NullPercent(t *testing.T) {
	p := testutil.NewProject(t)
	// 50% NULL with threshold 51 -> pass (50 < 51)
	p.AddModel("staging/ok.sql", "-- @kind: table\n-- @constraint: null_percent(id, 51)\nSELECT 1 AS id UNION ALL SELECT NULL\n")
	runModel(t, p, "staging/ok.sql")

	// 50% NULL with threshold 10 -> fail (50 >= 10)
	p.AddModel("staging/bad.sql", "-- @kind: table\n-- @constraint: null_percent(id, 10)\nSELECT 1 AS id UNION ALL SELECT NULL\n")
	_, err := runModelErr(t, p, "staging/bad.sql")
	if err == nil { t.Fatal("null_percent should reject 50% NULL > 10% threshold") }
}

func TestE2E_Macro_Constraint_CompositeUnique(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/ok.sql", "-- @kind: table\n-- @constraint: composite_unique('a, b')\nSELECT 1 AS a, 1 AS b UNION ALL SELECT 1, 2\n")
	runModel(t, p, "staging/ok.sql")
}

func TestE2E_Macro_Constraint_NotConstant(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/bad.sql", "-- @kind: table\n-- @constraint: not_constant(status)\nSELECT 'active' AS status UNION ALL SELECT 'active'\n")
	_, err := runModelErr(t, p, "staging/bad.sql")
	if err == nil { t.Fatal("not_constant should reject single-value column") }
}

func TestE2E_Macro_Constraint_Like(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/bad.sql", "-- @kind: table\n-- @constraint: like(email, '%@%')\nSELECT 'no-at-sign' AS email\n")
	_, err := runModelErr(t, p, "staging/bad.sql")
	if err == nil { t.Fatal("like should reject missing @") }
}

func TestE2E_Macro_Constraint_NotLike(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/bad.sql", "-- @kind: table\n-- @constraint: not_like(name, '%test%')\nSELECT 'test_user' AS name\n")
	_, err := runModelErr(t, p, "staging/bad.sql")
	if err == nil { t.Fatal("not_like should reject 'test_user'") }
}

func TestE2E_Macro_Constraint_Matches(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/ok.sql", "-- @kind: table\n-- @constraint: matches(code, ^[A-Z]{2}$)\nSELECT 'SE' AS code UNION ALL SELECT 'US'\n")
	runModel(t, p, "staging/ok.sql")
}

func TestE2E_Macro_Constraint_LengthBetween(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/bad.sql", "-- @kind: table\n-- @constraint: length_between(code, 2, 3)\nSELECT 'ABCDE' AS code\n")
	_, err := runModelErr(t, p, "staging/bad.sql")
	if err == nil { t.Fatal("length_between should reject length 5") }
}

func TestE2E_Macro_Constraint_References(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("raw/ref.sql", "-- @kind: table\nSELECT 1 AS id UNION ALL SELECT 2\n")
	runModel(t, p, "raw/ref.sql")
	p.AddModel("staging/bad.sql", "-- @kind: table\n-- @constraint: references(ref_id, raw.ref, id)\nSELECT 99 AS ref_id\n")
	_, err := runModelErr(t, p, "staging/bad.sql")
	if err == nil { t.Fatal("references should reject orphan ref_id=99") }
}

func TestE2E_Macro_Constraint_SequentialStep(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/ok.sql", "-- @kind: table\n-- @constraint: sequential_step(id, 10)\nSELECT 10 AS id UNION ALL SELECT 20 UNION ALL SELECT 30\n")
	runModel(t, p, "staging/ok.sql")
}

func TestE2E_Macro_Constraint_DuplicatePercent(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/bad.sql", "-- @kind: table\n-- @constraint: duplicate_percent(region, 10)\nSELECT 'SE' AS region UNION ALL SELECT 'SE' UNION ALL SELECT 'US'\n")
	_, err := runModelErr(t, p, "staging/bad.sql")
	if err == nil { t.Fatal("duplicate_percent should reject 33% duplicates > 10%") }
}

func TestE2E_Macro_Constraint_NoOverlap(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/ok.sql", "-- @kind: table\n-- @constraint: no_overlap(s, e)\nSELECT 1 AS s, 5 AS e UNION ALL SELECT 6, 10\n")
	runModel(t, p, "staging/ok.sql")
}

func TestE2E_Macro_Constraint_ValidType(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/bad.sql", "-- @kind: table\n-- @constraint: valid_type(uid, UUID)\nSELECT 'not-a-uuid' AS uid\n")
	_, err := runModelErr(t, p, "staging/bad.sql")
	if err == nil { t.Fatal("valid_type should reject invalid UUID") }
}

func TestE2E_Macro_Constraint_NoNan(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/ok.sql", "-- @kind: table\n-- @constraint: no_nan(amount)\nSELECT 100.0 AS amount\n")
	runModel(t, p, "staging/ok.sql")
}

func TestE2E_Macro_Constraint_Finite(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/ok.sql", "-- @kind: table\n-- @constraint: finite(amount)\nSELECT 100.0 AS amount\n")
	runModel(t, p, "staging/ok.sql")
}

// =============================================================================
// Missing audit tests
// =============================================================================

func TestE2E_Macro_Audit_MeanBetween(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/fail.sql", "-- @kind: table\n-- @audit: mean_between(amount, 0, 50)\nSELECT 100 AS id, 100 AS amount UNION ALL SELECT 200, 200\n")
	_, err := runModelErr(t, p, "staging/fail.sql")
	if err == nil { t.Fatal("mean_between should fail for avg=150 outside [0,50]") }
}

func TestE2E_Macro_Audit_Median(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/ok.sql", "-- @kind: table\n-- @audit: median(amount, >=, 100)\nSELECT 1 AS id, 100 AS amount UNION ALL SELECT 2, 200 UNION ALL SELECT 3, 300\n")
	runModel(t, p, "staging/ok.sql")
}

func TestE2E_Macro_Audit_Stddev(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/fail.sql", "-- @kind: table\n-- @audit: stddev(amount, 10)\nSELECT 1 AS id, 100 AS amount UNION ALL SELECT 2, 500\n")
	_, err := runModelErr(t, p, "staging/fail.sql")
	if err == nil { t.Fatal("stddev should fail for high variance") }
}

func TestE2E_Macro_Audit_Zscore(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/ok.sql", "-- @kind: table\n-- @audit: zscore(amount, 3)\nSELECT 1 AS id, 100 AS amount UNION ALL SELECT 2, 101 UNION ALL SELECT 3, 99\n")
	runModel(t, p, "staging/ok.sql")
}

func TestE2E_Macro_Audit_Entropy(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/fail.sql", "-- @kind: table\n-- @audit: entropy(status, >=, 5)\nSELECT 'active' AS status UNION ALL SELECT 'pending'\n")
	_, err := runModelErr(t, p, "staging/fail.sql")
	if err == nil { t.Fatal("entropy should fail for low entropy < 5") }
}

func TestE2E_Macro_Audit_ApproxDistinct(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/fail.sql", "-- @kind: table\n-- @audit: approx_distinct(id, >=, 100)\nSELECT 1 AS id UNION ALL SELECT 2\n")
	_, err := runModelErr(t, p, "staging/fail.sql")
	if err == nil { t.Fatal("approx_distinct should fail for 2 < 100") }
}

func TestE2E_Macro_Audit_ReconcileSum(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("raw/a.sql", "-- @kind: table\nSELECT 100 AS amount UNION ALL SELECT 200\n")
	p.AddModel("raw/b.sql", "-- @kind: table\nSELECT 999 AS total\n")
	runModel(t, p, "raw/a.sql")
	runModel(t, p, "raw/b.sql")
	p.AddModel("staging/fail.sql", "-- @kind: table\n-- @audit: reconcile_sum(amount, raw.b, total)\nSELECT * FROM raw.a\n")
	_, err := runModelErr(t, p, "staging/fail.sql")
	if err == nil { t.Fatal("reconcile_sum should fail for 300 != 999") }
}

// =============================================================================
// Missing warning tests
// =============================================================================

func TestE2E_Macro_Warning_NotNull(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/t.sql", "-- @kind: table\n-- @warning: not_null(val)\nSELECT NULL AS val\n")
	result := runModel(t, p, "staging/t.sql")
	hasWarn := false
	for _, w := range result.Warnings { if strings.Contains(w, "not_null") { hasWarn = true } }
	if !hasWarn { t.Errorf("expected not_null warning, got: %v", result.Warnings) }
}

func TestE2E_Macro_Warning_Unique(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/t.sql", "-- @kind: table\n-- @warning: unique(id)\nSELECT 1 AS id UNION ALL SELECT 1\n")
	result := runModel(t, p, "staging/t.sql")
	hasWarn := false
	for _, w := range result.Warnings { if strings.Contains(w, "unique") { hasWarn = true } }
	if !hasWarn { t.Errorf("expected unique warning, got: %v", result.Warnings) }
}

func TestE2E_Macro_Warning_Min(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/t.sql", "-- @kind: table\n-- @warning: min(amount, >=, 200)\nSELECT 1 AS id, 50 AS amount\n")
	result := runModel(t, p, "staging/t.sql")
	hasWarn := false
	for _, w := range result.Warnings { if strings.Contains(w, "min") { hasWarn = true } }
	if !hasWarn { t.Errorf("expected min warning, got: %v", result.Warnings) }
}

func TestE2E_Macro_Warning_Compare(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/t.sql", "-- @kind: table\n-- @warning: compare(amount, >=, 200)\nSELECT 1 AS id, 50 AS amount\n")
	result := runModel(t, p, "staging/t.sql")
	hasWarn := false
	for _, w := range result.Warnings { if strings.Contains(w, "compare") || strings.Contains(w, "amount") { hasWarn = true } }
	if !hasWarn { t.Errorf("expected compare warning, got: %v", result.Warnings) }
}

func TestE2E_Macro_Warning_Mean(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/t.sql", "-- @kind: table\n-- @warning: mean(amount, >=, 500)\nSELECT 1 AS id, 50 AS amount\n")
	result := runModel(t, p, "staging/t.sql")
	hasWarn := false
	for _, w := range result.Warnings { if strings.Contains(w, "mean") { hasWarn = true } }
	if !hasWarn { t.Errorf("expected mean warning, got: %v", result.Warnings) }
}

func TestE2E_Macro_Warning_NullPercent(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/t.sql", "-- @kind: table\n-- @warning: null_percent(val, 10)\nSELECT NULL AS val UNION ALL SELECT 'a'\n")
	result := runModel(t, p, "staging/t.sql")
	hasWarn := false
	for _, w := range result.Warnings { if strings.Contains(w, "null_percent") || strings.Contains(w, "NULL") { hasWarn = true } }
	if !hasWarn { t.Errorf("expected null_percent warning, got: %v", result.Warnings) }
}

func TestE2E_Macro_Warning_LowEntropy(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/t.sql", "-- @kind: table\n-- @warning: low_entropy(status, 5)\nSELECT 'a' AS status UNION ALL SELECT 'b'\n")
	result := runModel(t, p, "staging/t.sql")
	hasWarn := false
	for _, w := range result.Warnings { if strings.Contains(w, "entropy") { hasWarn = true } }
	if !hasWarn { t.Errorf("expected low_entropy warning, got: %v", result.Warnings) }
}

func TestE2E_Macro_Warning_Cardinality(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/t.sql", "-- @kind: table\n-- @warning: cardinality(id, >=, 100)\nSELECT 1 AS id UNION ALL SELECT 2\n")
	result := runModel(t, p, "staging/t.sql")
	hasWarn := false
	for _, w := range result.Warnings { if strings.Contains(w, "cardinality") { hasWarn = true } }
	if !hasWarn { t.Errorf("expected cardinality warning, got: %v", result.Warnings) }
}

func TestE2E_Macro_Warning_ApproachingLimit(t *testing.T) {
	p := testutil.NewProject(t)
	// alert_amount_threshold = 1000000 (from constants.sql)
	// 950000 > 90% of 1000000 = 900000 -> should warn
	p.AddModel("staging/t.sql", "-- @kind: table\n-- @warning: approaching_limit(amount)\nSELECT 950000 AS amount\n")
	result := runModel(t, p, "staging/t.sql")
	hasWarn := false
	for _, w := range result.Warnings { if strings.Contains(w, "approaching") || strings.Contains(w, "limit") { hasWarn = true } }
	if !hasWarn { t.Errorf("expected approaching_limit warning, got: %v", result.Warnings) }
}

// =============================================================================
// Variable tests — verify every shipped variable has correct values
// =============================================================================

// constants.sql: alert_amount_threshold
func TestE2E_Variable_AlertAmountThreshold(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/t.sql", "-- @kind: table\nSELECT getvariable('alert_amount_threshold')::BIGINT AS val\n")
	runModel(t, p, "staging/t.sql")
	val, err := p.Sess.QueryValue("SELECT val FROM staging.t")
	if err != nil { t.Fatalf("query: %v", err) }
	if val != "1000000" { t.Errorf("alert_amount_threshold = %s, want 1000000", val) }
}

// local.sql: current_model — set per model by runner
func TestE2E_Variable_CurrentModel(t *testing.T) {
	p := testutil.NewProject(t)
	// After running a model, current_model should be set to that model's target.
	// We can't read it from inside the model (it's set before warnings, not before materialize).
	// But we can verify it indirectly: the warning system uses it.
	p.AddModel("raw/src.sql", "-- @kind: table\nSELECT 1 AS id\n")
	runModel(t, p, "raw/src.sql")

	// current_model should have been set to 'raw.src' during the run
	val, err := p.Sess.QueryValue("SELECT getvariable('current_model')")
	if err != nil { t.Fatalf("query: %v", err) }
	if val != "raw.src" { t.Errorf("current_model = %q, want 'raw.src'", val) }
}

// local.sql: prev_model_snapshot — 0 on first run, correct snapshot after
func TestE2E_Variable_PrevModelSnapshot(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("raw/src.sql", "-- @kind: table\nSELECT 1 AS id\n")

	// Run 1: first run — prev should be 0
	runModel(t, p, "raw/src.sql")
	val, err := p.Sess.QueryValue("SELECT getvariable('prev_model_snapshot')")
	if err != nil { t.Fatalf("query: %v", err) }
	// After first run + warning phase, prev_model_snapshot was set.
	// Since we just committed, the LATEST commit is this one.
	// prev_model_snapshot = second-to-last = 0 (only one commit exists)

	// Run 2: second run — prev should be the first commit's snapshot
	p.AddModel("raw/src.sql", "-- @kind: table\nSELECT 1 AS id, 'v2' AS label\n")
	runModel(t, p, "raw/src.sql")
	val, err = p.Sess.QueryValue("SELECT getvariable('prev_model_snapshot')")
	if err != nil { t.Fatalf("query: %v", err) }
	// Now there are two commits for raw.src. prev = first commit's snapshot.
	if val == "0" { t.Error("prev_model_snapshot should be > 0 after second run") }
}

// local.sql: curr_snapshot — should reflect latest DuckLake state
func TestE2E_Variable_CurrSnapshot(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("raw/src.sql", "-- @kind: table\nSELECT 1 AS id\n")
	runModel(t, p, "raw/src.sql")

	// curr_snapshot should match current_snapshot()
	curr, err := p.Sess.QueryValue("SELECT getvariable('curr_snapshot')")
	if err != nil { t.Fatalf("query curr_snapshot: %v", err) }
	actual, err := p.Sess.QueryValue("SELECT id FROM lake.current_snapshot()")
	if err != nil { t.Fatalf("query current_snapshot(): %v", err) }

	if curr != actual { t.Errorf("curr_snapshot = %s, current_snapshot() = %s — should match", curr, actual) }
}

// local.sql: variables change correctly across a 3-model DAG
func TestE2E_Variable_PerModel_DAG(t *testing.T) {
	p := testutil.NewProject(t)

	// 3-model DAG: raw → staging → mart. Each with warnings so variables get set.
	p.AddModel("raw/src.sql", "-- @kind: table\n-- @warning: row_count(>=, 1)\nSELECT 1 AS id, 100 AS amount\n")
	p.AddModel("staging/clean.sql", "-- @kind: table\n-- @warning: row_count(>=, 1)\nSELECT id, amount FROM raw.src\n")
	p.AddModel("mart/total.sql", "-- @kind: table\n-- @warning: row_count(>=, 1)\nSELECT SUM(amount) AS total FROM staging.clean\n")

	// Run 1: all three
	runModel(t, p, "raw/src.sql")
	runModel(t, p, "staging/clean.sql")
	runModel(t, p, "mart/total.sql")

	// current_model = last model that ran warnings
	val, _ := p.Sess.QueryValue("SELECT getvariable('current_model')")
	if val != "mart.total" {
		t.Errorf("current_model = %q after run 1, want 'mart.total'", val)
	}

	// Run 2: change source → all three re-run
	p.AddModel("raw/src.sql", "-- @kind: table\n-- @warning: row_count(>=, 1)\nSELECT 1 AS id, 100 AS amount UNION ALL SELECT 2, 200\n")
	runModel(t, p, "raw/src.sql")
	runModel(t, p, "staging/clean.sql")
	runModel(t, p, "mart/total.sql")

	// current_model = mart.total (last in DAG)
	val, _ = p.Sess.QueryValue("SELECT getvariable('current_model')")
	if val != "mart.total" {
		t.Errorf("current_model = %q after run 2, want 'mart.total'", val)
	}

	// prev_model_snapshot should be > 0 (mart.total has been committed twice)
	prev, _ := p.Sess.QueryValue("SELECT getvariable('prev_model_snapshot')")
	if prev == "0" {
		t.Error("prev_model_snapshot should be > 0 after second run of mart.total")
	}

	// curr_snapshot should match DuckLake
	curr, _ := p.Sess.QueryValue("SELECT getvariable('curr_snapshot')")
	actual, _ := p.Sess.QueryValue("SELECT id FROM lake.current_snapshot()")
	if curr != actual {
		t.Errorf("curr_snapshot = %s, current_snapshot() = %s", curr, actual)
	}

	// Run 3: change source again → verify prev_model_snapshot advances
	prevBefore := prev
	p.AddModel("raw/src.sql", "-- @kind: table\n-- @warning: row_count(>=, 1)\nSELECT 1 AS id, 100 AS amount UNION ALL SELECT 2, 200 UNION ALL SELECT 3, 300\n")
	runModel(t, p, "raw/src.sql")
	runModel(t, p, "staging/clean.sql")
	runModel(t, p, "mart/total.sql")

	prevAfter, _ := p.Sess.QueryValue("SELECT getvariable('prev_model_snapshot')")
	if prevAfter == prevBefore {
		t.Errorf("prev_model_snapshot should advance between runs: before=%s, after=%s", prevBefore, prevAfter)
	}
}

// =============================================================================
// Remaining masking macros
// =============================================================================

func TestE2E_Macro_Masking_SSN(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/t.sql", "-- @kind: table\n-- @column: ssn = SSN | mask_ssn\nSELECT 1 AS id, '123-45-6789' AS ssn\n")
	runModel(t, p, "staging/t.sql")
	val, err := p.Sess.QueryValue("SELECT ssn FROM staging.t")
	if err != nil { t.Fatalf("query: %v", err) }
	if !strings.Contains(val, "***") {
		t.Errorf("ssn should be masked, got: %s", val)
	}
	if !strings.Contains(val, "6789") {
		t.Errorf("ssn should keep last 4 digits, got: %s", val)
	}
}

func TestE2E_Macro_Masking_HashPII(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/t.sql", "-- @kind: table\n-- @column: name = Name | hash_pii\nSELECT 1 AS id, 'Alice' AS name\n")
	runModel(t, p, "staging/t.sql")
	val, err := p.Sess.QueryValue("SELECT name FROM staging.t")
	if err != nil { t.Fatalf("query: %v", err) }
	if val == "Alice" {
		t.Error("name should be hashed, still shows 'Alice'")
	}
	if len(val) < 20 {
		t.Errorf("hash should be long, got: %s", val)
	}
}

func TestE2E_Macro_Masking_Redact(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/t.sql", "-- @kind: table\n-- @column: secret = Secret | redact\nSELECT 1 AS id, 'top-secret-value' AS secret\n")
	runModel(t, p, "staging/t.sql")
	val, err := p.Sess.QueryValue("SELECT secret FROM staging.t")
	if err != nil { t.Fatalf("query: %v", err) }
	if val != "[REDACTED]" {
		t.Errorf("secret should be '[REDACTED]', got: %s", val)
	}
}

// =============================================================================
// Remaining helper macros
// =============================================================================

func TestE2E_Macro_Helper_CentsToDollars(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("staging/t.sql", "-- @kind: table\nSELECT cents_to_dollars(1500) AS dollars\n")
	runModel(t, p, "staging/t.sql")
	val, err := p.Sess.QueryValue("SELECT dollars FROM staging.t")
	if err != nil { t.Fatalf("query: %v", err) }
	if val != "15.0" && val != "15.00" && val != "15" {
		t.Errorf("cents_to_dollars(1500) should be 15, got: %s", val)
	}
}

// =============================================================================
// Init structure — verify ondatrasql init generates correct files
// =============================================================================

func TestE2E_Init_DirectoryStructure(t *testing.T) {
	p := testutil.NewProject(t)

	// testutil uses internal filenames; ondatrasql init uses user-facing names.
	// Both produce a config/macros/ directory with .sql files.
	macroDir := filepath.Join(p.Dir, "config", "macros")
	entries, err := os.ReadDir(macroDir)
	if err != nil {
		t.Fatalf("config/macros/ directory missing: %v", err)
	}
	if len(entries) < 5 {
		t.Errorf("config/macros/ has %d files, want at least 5", len(entries))
	}

	expectedFiles := []string{
		"config/variables/constants.sql",
		"config/variables/global.sql",
		"config/variables/local.sql",
	}

	for _, f := range expectedFiles {
		path := filepath.Join(p.Dir, f)
		info, err := os.Stat(path)
		if err != nil {
			t.Errorf("missing file: %s", f)
			continue
		}
		if info.Size() == 0 {
			t.Errorf("empty file: %s", f)
		}
	}
}

func TestE2E_Init_MacrosLoadable(t *testing.T) {
	// Verify all generated macro files parse and load without errors.
	// testutil.NewProject already loads them — if we get here, they loaded.
	p := testutil.NewProject(t)

	// Verify macros exist in memory by calling one from each file
	tests := []struct {
		name  string
		query string
	}{
		{"helpers", "SELECT safe_divide(10, 2)"},
		{"constraints", "SELECT * FROM memory.ondatra_constraint_not_null('(SELECT 1 AS c)', 'c')"},
		{"audits", "SELECT * FROM memory.ondatra_audit_row_count('(SELECT 1 AS c)', '>=', 0)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := p.Sess.QueryValue(tt.query)
			if err != nil {
				t.Errorf("macro from %s.sql not loadable: %v", tt.name, err)
			}
		})
	}
}

// =============================================================================
// Backwards compatibility — old config/macros.sql still works
// =============================================================================

// v0.15.0: backwards compatibility for single config/macros.sql removed.
// All projects must use config/macros/ directory.
