// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package validation

import (
	"os"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
)

var shared *duckdb.Session

func TestMain(m *testing.M) {
	sess, err := duckdb.NewSession(":memory:")
	if err != nil {
		panic(err)
	}
	shared = sess
	code := m.Run()
	shared.Close()
	os.Exit(code)
}

// helper runs generated SQL and returns the result rows.
func execConstraint(t *testing.T, directive, table string) []string {
	t.Helper()
	sql, err := ConstraintToSQL(directive, table)
	if err != nil {
		t.Fatalf("ConstraintToSQL(%q): %v", directive, err)
	}
	rows, err := shared.QueryRows(sql)
	if err != nil {
		t.Fatalf("exec SQL for %q failed: %v\nSQL: %s", directive, err, sql)
	}
	return rows
}

func execAudit(t *testing.T, directive, table string, prev int64) []string {
	t.Helper()
	sql, err := AuditToSQL(directive, table, "", prev)
	if err != nil {
		t.Fatalf("AuditToSQL(%q): %v", directive, err)
	}
	rows, err := shared.QueryRows(sql)
	if err != nil {
		t.Fatalf("exec SQL for %q failed: %v\nSQL: %s", directive, err, sql)
	}
	return rows
}

// ---------- Constraint Integration Tests ----------

func TestIntegration_PrimaryKey(t *testing.T) {
	t.Parallel()
	setup := `CREATE OR REPLACE TABLE test_pk AS SELECT * FROM (VALUES (1), (2), (3)) t(id)`
	if err := shared.Exec(setup); err != nil {
		t.Fatal(err)
	}

	// Pass: all unique, non-null
	rows := execConstraint(t, "id PRIMARY KEY", "test_pk")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	// Fail: duplicate
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_pk_fail AS SELECT * FROM (VALUES (1), (1), (NULL)) t(id)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "id PRIMARY KEY", "test_pk_fail")
	if len(rows) == 0 {
		t.Error("fail: expected error row for duplicate/null primary key")
	}
}

func TestIntegration_NotNull(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_nn AS SELECT * FROM (VALUES ('a'), ('b')) t(name)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "name NOT NULL", "test_nn")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_nn_fail AS SELECT * FROM (VALUES ('a'), (NULL)) t(name)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "name NOT NULL", "test_nn_fail")
	if len(rows) == 0 {
		t.Error("fail: expected error row for null values")
	}
}

func TestIntegration_Unique(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_uq AS SELECT * FROM (VALUES ('a@b.com'), ('c@d.com')) t(email)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "email UNIQUE", "test_uq")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_uq_fail AS SELECT * FROM (VALUES ('dup'), ('dup')) t(email)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "email UNIQUE", "test_uq_fail")
	if len(rows) == 0 {
		t.Error("fail: expected error row for duplicate values")
	}
}

func TestIntegration_CompositeUnique(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_cuq AS SELECT * FROM (VALUES (1,1), (1,2), (2,1)) t(a, b)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "(a, b) UNIQUE", "test_cuq")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_cuq_fail AS SELECT * FROM (VALUES (1,1), (1,1)) t(a, b)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "(a, b) UNIQUE", "test_cuq_fail")
	if len(rows) == 0 {
		t.Error("fail: expected error row for duplicate composite key")
	}
}

func TestIntegration_NotEmpty(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_ne AS SELECT * FROM (VALUES ('hello'), ('world')) t(name)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "name NOT EMPTY", "test_ne")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_ne_fail AS SELECT * FROM (VALUES ('ok'), (''), (NULL)) t(name)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "name NOT EMPTY", "test_ne_fail")
	if len(rows) == 0 {
		t.Error("fail: expected error row for empty/null values")
	}
}

func TestIntegration_ComparisonGTE(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_gte AS SELECT * FROM (VALUES (0), (5), (100)) t(amount)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "amount >= 0", "test_gte")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_gte_f AS SELECT * FROM (VALUES (-1), (5)) t(amount)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "amount >= 0", "test_gte_f")
	if len(rows) == 0 {
		t.Error("fail: expected error for amount < 0")
	}
}

func TestIntegration_ComparisonLTE(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_lte AS SELECT * FROM (VALUES (1), (50), (100)) t(v)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "v <= 100", "test_lte")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_lte_f AS SELECT * FROM (VALUES (50), (101)) t(v)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "v <= 100", "test_lte_f")
	if len(rows) == 0 {
		t.Error("fail: expected error for v > 100")
	}
}

func TestIntegration_ComparisonGT(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_gt AS SELECT * FROM (VALUES (1), (2)) t(v)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "v > 0", "test_gt")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_gt_f AS SELECT * FROM (VALUES (0), (1)) t(v)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "v > 0", "test_gt_f")
	if len(rows) == 0 {
		t.Error("fail: expected error for v = 0")
	}
}

func TestIntegration_ComparisonLT(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_lt AS SELECT * FROM (VALUES (1), (99)) t(v)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "v < 100", "test_lt")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_lt_f AS SELECT * FROM (VALUES (99), (100)) t(v)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "v < 100", "test_lt_f")
	if len(rows) == 0 {
		t.Error("fail: expected error for v = 100")
	}
}

func TestIntegration_ComparisonEQ(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_eq AS SELECT * FROM (VALUES (42), (42)) t(v)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "v = 42", "test_eq")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_eq_f AS SELECT * FROM (VALUES (42), (43)) t(v)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "v = 42", "test_eq_f")
	if len(rows) == 0 {
		t.Error("fail: expected error for v != 42")
	}
}

func TestIntegration_ComparisonNEQ(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_neq AS SELECT * FROM (VALUES (1), (2)) t(v)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "v != 0", "test_neq")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_neq_f AS SELECT * FROM (VALUES (0), (1)) t(v)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "v != 0", "test_neq_f")
	if len(rows) == 0 {
		t.Error("fail: expected error for v = 0")
	}
}

func TestIntegration_Between(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_btw AS SELECT * FROM (VALUES (0), (50), (100)) t(score)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "score BETWEEN 0 AND 100", "test_btw")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_btw_f AS SELECT * FROM (VALUES (-1), (50), (101)) t(score)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "score BETWEEN 0 AND 100", "test_btw_f")
	if len(rows) == 0 {
		t.Error("fail: expected error for out-of-range values")
	}
}

func TestIntegration_IN(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_in AS SELECT * FROM (VALUES ('active'), ('pending')) t(status)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "status IN ('active', 'pending')", "test_in")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_in_f AS SELECT * FROM (VALUES ('active'), ('deleted')) t(status)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "status IN ('active', 'pending')", "test_in_f")
	if len(rows) == 0 {
		t.Error("fail: expected error for value not in list")
	}
}

func TestIntegration_NotIN(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_nin AS SELECT * FROM (VALUES ('active'), ('pending')) t(status)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "status NOT IN ('deleted')", "test_nin")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_nin_f AS SELECT * FROM (VALUES ('active'), ('deleted')) t(status)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "status NOT IN ('deleted')", "test_nin_f")
	if len(rows) == 0 {
		t.Error("fail: expected error for forbidden value")
	}
}

func TestIntegration_Like(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_like AS SELECT * FROM (VALUES ('AB001'), ('AB002')) t(code)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "code LIKE 'AB%'", "test_like")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_like_f AS SELECT * FROM (VALUES ('AB001'), ('CD001')) t(code)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "code LIKE 'AB%'", "test_like_f")
	if len(rows) == 0 {
		t.Error("fail: expected error for non-matching value")
	}
}

func TestIntegration_NotLike(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_nlike AS SELECT * FROM (VALUES ('real1'), ('real2')) t(name)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "name NOT LIKE 'test%'", "test_nlike")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_nlike_f AS SELECT * FROM (VALUES ('real1'), ('test_bad')) t(name)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "name NOT LIKE 'test%'", "test_nlike_f")
	if len(rows) == 0 {
		t.Error("fail: expected error for test-prefixed value")
	}
}

func TestIntegration_References(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE ref_parent AS SELECT * FROM (VALUES (1), (2), (3)) t(id)`); err != nil {
		t.Fatal(err)
	}
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_ref AS SELECT * FROM (VALUES (1), (2)) t(parent_id)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "parent_id REFERENCES ref_parent(id)", "test_ref")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_ref_f AS SELECT * FROM (VALUES (1), (999)) t(parent_id)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "parent_id REFERENCES ref_parent(id)", "test_ref_f")
	if len(rows) == 0 {
		t.Error("fail: expected error for orphan foreign key")
	}
}

func TestIntegration_Matches(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_match AS SELECT * FROM (VALUES ('SEK'), ('USD'), ('EUR')) t(code)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "code MATCHES '^[A-Z]{3}$'", "test_match")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_match_f AS SELECT * FROM (VALUES ('SEK'), ('us'), ('1234')) t(code)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "code MATCHES '^[A-Z]{3}$'", "test_match_f")
	if len(rows) == 0 {
		t.Error("fail: expected error for non-matching regex")
	}
}

func TestIntegration_Email(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_email AS SELECT * FROM (VALUES ('a@b.com'), ('test@example.org')) t(email)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "email EMAIL", "test_email")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_email_f AS SELECT * FROM (VALUES ('a@b.com'), ('not-an-email')) t(email)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "email EMAIL", "test_email_f")
	if len(rows) == 0 {
		t.Error("fail: expected error for invalid email")
	}
}

func TestIntegration_UUID(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_uuid AS SELECT * FROM (VALUES ('550e8400-e29b-41d4-a716-446655440000'), ('6ba7b810-9dad-11d1-80b4-00c04fd430c8')) t(id)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "id UUID", "test_uuid")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_uuid_f AS SELECT * FROM (VALUES ('550e8400-e29b-41d4-a716-446655440000'), ('not-a-uuid')) t(id)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "id UUID", "test_uuid_f")
	if len(rows) == 0 {
		t.Error("fail: expected error for invalid UUID")
	}
}

func TestIntegration_LengthBetween(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_lbtw AS SELECT * FROM (VALUES ('abc'), ('abcde'), ('abcdefghij')) t(code)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "code LENGTH BETWEEN 3 AND 10", "test_lbtw")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_lbtw_f AS SELECT * FROM (VALUES ('ab'), ('abcdefghijk')) t(code)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "code LENGTH BETWEEN 3 AND 10", "test_lbtw_f")
	if len(rows) == 0 {
		t.Error("fail: expected error for length outside range")
	}
}

func TestIntegration_LengthEquals(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_leq AS SELECT * FROM (VALUES ('12345'), ('67890')) t(zip)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "zip LENGTH = 5", "test_leq")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_leq_f AS SELECT * FROM (VALUES ('12345'), ('1234')) t(zip)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "zip LENGTH = 5", "test_leq_f")
	if len(rows) == 0 {
		t.Error("fail: expected error for wrong length")
	}
}

func TestIntegration_Check(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_chk AS SELECT * FROM (VALUES (10), (500)) t(price)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "price CHECK (price > 0 AND price < 1000000)", "test_chk")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_chk_f AS SELECT * FROM (VALUES (10), (-5)) t(price)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "price CHECK (price > 0 AND price < 1000000)", "test_chk_f")
	if len(rows) == 0 {
		t.Error("fail: expected error for check violation")
	}
}

func TestIntegration_AtLeastOne(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_alo AS SELECT * FROM (VALUES (1), (NULL)) t(value)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "value AT_LEAST_ONE", "test_alo")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_alo_f AS SELECT * FROM (VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT))) t(value)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "value AT_LEAST_ONE", "test_alo_f")
	if len(rows) == 0 {
		t.Error("fail: expected error when all values are NULL")
	}
}

func TestIntegration_NotConstant(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_nc AS SELECT * FROM (VALUES ('a'), ('b'), ('c')) t(status)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "status NOT_CONSTANT", "test_nc")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_nc_f AS SELECT * FROM (VALUES ('same'), ('same'), ('same')) t(status)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "status NOT_CONSTANT", "test_nc_f")
	if len(rows) == 0 {
		t.Error("fail: expected error when column is constant")
	}
}

func TestIntegration_NullPercent(t *testing.T) {
	t.Parallel()
	// 0% nulls
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_np AS SELECT * FROM (VALUES ('a'), ('b'), ('c'), ('d'), ('e'), ('f'), ('g'), ('h'), ('i'), ('j')) t(email)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "email NULL_PERCENT < 10", "test_np")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	// 50% nulls
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_np_f AS SELECT * FROM (VALUES ('a'), (NULL), ('c'), (NULL), ('e'), (NULL), ('g'), (NULL), ('i'), (NULL)) t(email)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "email NULL_PERCENT < 10", "test_np_f")
	if len(rows) == 0 {
		t.Error("fail: expected error for 50% null values")
	}
}

func TestIntegration_Sequential(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_seq AS SELECT * FROM (VALUES (1), (2), (3), (4)) t(seq)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "seq SEQUENTIAL", "test_seq")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_seq_f AS SELECT * FROM (VALUES (1), (2), (5), (6)) t(seq)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "seq SEQUENTIAL", "test_seq_f")
	if len(rows) == 0 {
		t.Error("fail: expected error for gap in sequence")
	}
}

func TestIntegration_SequentialN(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_seqn AS SELECT * FROM (VALUES (0), (5), (10), (15)) t(seq)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "seq SEQUENTIAL(5)", "test_seqn")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_seqn_f AS SELECT * FROM (VALUES (0), (5), (20), (25)) t(seq)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "seq SEQUENTIAL(5)", "test_seqn_f")
	if len(rows) == 0 {
		t.Error("fail: expected error for gap > 5")
	}
}

func TestIntegration_DistinctCount(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_dc AS SELECT * FROM (VALUES ('a'), ('b'), ('c'), ('d'), ('e')) t(v)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "v DISTINCT_COUNT >= 5", "test_dc")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_dc_f AS SELECT * FROM (VALUES ('a'), ('b')) t(v)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "v DISTINCT_COUNT >= 5", "test_dc_f")
	if len(rows) == 0 {
		t.Error("fail: expected error for < 5 distinct values")
	}
}

func TestIntegration_DuplicatePercent(t *testing.T) {
	t.Parallel()
	// All unique → 0% duplicates
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_dp AS SELECT * FROM (VALUES ('a'), ('b'), ('c'), ('d'), ('e')) t(email)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "email DUPLICATE_PERCENT < 10", "test_dp")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	// 50% duplicates
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_dp_f AS SELECT * FROM (VALUES ('a'), ('a'), ('b'), ('b')) t(email)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "email DUPLICATE_PERCENT < 10", "test_dp_f")
	if len(rows) == 0 {
		t.Error("fail: expected error for 50% duplicates")
	}
}

func TestIntegration_NoOverlap(t *testing.T) {
	t.Parallel()
	if err := shared.Exec(`CREATE OR REPLACE TABLE test_no AS SELECT * FROM (VALUES (1, 5), (5, 10), (10, 15)) t(start_date, end_date)`); err != nil {
		t.Fatal(err)
	}
	rows := execConstraint(t, "(start_date, end_date) NO_OVERLAP", "test_no")
	if len(rows) != 0 {
		t.Errorf("pass: expected 0 rows, got %v", rows)
	}

	if err := shared.Exec(`CREATE OR REPLACE TABLE test_no_f AS SELECT * FROM (VALUES (1, 10), (5, 15)) t(start_date, end_date)`); err != nil {
		t.Fatal(err)
	}
	rows = execConstraint(t, "(start_date, end_date) NO_OVERLAP", "test_no_f")
	if len(rows) == 0 {
		t.Error("fail: expected error for overlapping intervals")
	}
}
