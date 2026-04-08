// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package validation

import (
	"strings"
	"testing"
)

func TestAuditToSQL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		directive    string
		table        string
		prevSnapshot int64
		wantParts    []string // all substrings that must be in the SQL
		wantErrMsg   string
	}{
		// Row count
		{
			name:      "row_count >= N",
			directive: "row_count >= 1",
			table:     "staging.orders",
			wantParts: []string{
				"SELECT printf('row_count",
				"COUNT(*) FROM staging.orders",
				"< 1", // inverted >=
			},
		},
		{
			name:         "row_count_change",
			directive:    "row_count_change < 10%",
			table:        "staging.orders",
			prevSnapshot: 42,
			wantParts: []string{
				"SELECT printf('row_count_change failed:",
				"FROM staging.orders",
				"AT (VERSION => 42)",
				">= 10",
			},
		},
		{
			name:         "row_count_change no prev",
			directive:    "row_count_change < 10%",
			table:        "staging.orders",
			prevSnapshot: 0,
			wantParts:    []string{"SELECT 1 WHERE 0"},
		},
		// Freshness
		{
			name:      "freshness hours",
			directive: "freshness(updated_at, 24h)",
			table:     "staging.orders",
			wantParts: []string{
				"SELECT printf('freshness failed:",
				"MAX(updated_at)",
				"FROM staging.orders",
				"24 HOUR",
			},
		},
		{
			name:      "freshness days",
			directive: "freshness(created_at, 7d)",
			table:     "staging.orders",
			wantParts: []string{
				"SELECT printf('freshness failed:",
				"MAX(created_at)",
				"FROM staging.orders",
				"7 DAY",
			},
		},
		// Statistics
		{
			name:      "mean BETWEEN",
			directive: "mean(amount) BETWEEN 10 AND 100",
			table:     "staging.orders",
			wantParts: []string{
				"SELECT printf('mean(",
				"AVG(amount) FROM staging.orders",
				"< 10",
				"> 100",
			},
		},
		{
			name:      "mean comparison",
			directive: "mean(price) >= 0",
			table:     "staging.orders",
			wantParts: []string{
				"SELECT printf('mean(",
				"AVG(price) FROM staging.orders",
				"< 0", // inverted >=
			},
		},
		{
			name:      "stddev",
			directive: "stddev(amount) < 100",
			table:     "staging.orders",
			wantParts: []string{
				"SELECT printf('stddev(",
				"STDDEV(amount) FROM staging.orders",
				">= 100", // inverted <
			},
		},
		{
			name:      "min",
			directive: "min(price) >= 0",
			table:     "staging.orders",
			wantParts: []string{
				"SELECT printf('min(",
				"MIN(price)",
				"FROM staging.orders",
				"< 0",
			},
		},
		{
			name:      "max",
			directive: "max(quantity) <= 1000",
			table:     "staging.orders",
			wantParts: []string{
				"SELECT printf('max(",
				"MAX(quantity)",
				"FROM staging.orders",
				"> 1000",
			},
		},
		{
			name:      "sum",
			directive: "sum(amount) < 1000000",
			table:     "staging.orders",
			wantParts: []string{
				"SELECT printf('sum(",
				"SUM(amount)",
				"FROM staging.orders",
				">= 1000000",
			},
		},
		{
			name:      "zscore",
			directive: "zscore(amount) < 3",
			table:     "staging.orders",
			wantParts: []string{
				"SELECT printf('zscore(",
				"STDDEV(amount) FROM staging.orders",
				"AVG(amount) FROM staging.orders",
				">= 3",
				"LIMIT 1",
			},
		},
		{
			name:      "percentile",
			directive: "percentile(amount, 0.95) < 10000",
			table:     "staging.orders",
			wantParts: []string{
				"SELECT printf('percentile(",
				"PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY amount) FROM staging.orders",
				">= 10000",
			},
		},
		// Reconciliation
		{
			name:      "reconcile_count",
			directive: "reconcile_count(source.orders)",
			table:     "staging.orders",
			wantParts: []string{
				"SELECT printf('reconcile_count failed:",
				"COUNT(*) FROM staging.orders",
				"COUNT(*) FROM source.orders",
			},
		},
		{
			name:      "reconcile_sum",
			directive: "reconcile_sum(amount, source.orders.total)",
			table:     "staging.orders",
			wantParts: []string{
				"SELECT printf('reconcile_sum failed:",
				"SUM(amount)",
				"FROM staging.orders",
				"SUM(total) FROM source.orders",
			},
		},
		// Schema
		{
			name:      "column_exists",
			directive: "column_exists(order_id)",
			table:     "staging.orders",
			wantParts: []string{
				"DESCRIBE staging.orders",
				"column_name = 'order_id'",
				"column_exists failed:",
			},
		},
		{
			name:      "column_type",
			directive: "column_type(amount, DECIMAL)",
			table:     "staging.orders",
			wantParts: []string{
				"DESCRIBE staging.orders",
				"column_name = 'amount'",
				"column_type failed:",
				"expected DECIMAL",
			},
		},
		{
			name:      "column_type with precision",
			directive: "column_type(amount, DECIMAL(18,2))",
			table:     "staging.orders",
			wantParts: []string{
				"DESCRIBE staging.orders",
				"column_name = 'amount'",
				"expected DECIMAL(18,2)",
			},
		},
		// Golden
		{
			name:      "golden",
			directive: "golden('tests/expected.csv')",
			table:     "staging.orders",
			wantParts: []string{
				"SELECT printf('golden failed:",
				"FROM staging.orders EXCEPT SELECT * FROM read_csv_auto('tests/expected.csv')",
				"read_csv_auto('tests/expected.csv') EXCEPT SELECT * FROM staging.orders",
			},
		},
		// Distribution
		{
			name:         "distribution STABLE",
			directive:    "distribution(status) STABLE",
			table:        "staging.orders",
			prevSnapshot: 42,
			wantParts: []string{
				"SELECT printf('distribution(",
				"FROM staging.orders GROUP BY status",
				"AT (VERSION => 42)",
				"> 0.1", // default threshold
				"LIMIT 1",
			},
		},
		{
			name:         "distribution STABLE with threshold",
			directive:    "distribution(category) STABLE(0.05)",
			table:        "staging.orders",
			prevSnapshot: 42,
			wantParts: []string{
				"FROM staging.orders GROUP BY category",
				"AT (VERSION => 42)",
				"> 0.05",
				"LIMIT 1",
			},
		},
		// Error cases
		{
			name:       "empty directive",
			directive:  "",
			table:      "staging.orders",
			wantErrMsg: "empty audit",
		},
		{
			name:       "unknown format",
			directive:  "something weird",
			table:      "staging.orders",
			wantErrMsg: "unknown audit format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sql, err := AuditToSQL(tt.directive, tt.table, "", tt.prevSnapshot)

			if tt.wantErrMsg != "" {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.wantErrMsg)
					return
				}
				if !strings.Contains(err.Error(), tt.wantErrMsg) {
					t.Errorf("error = %q, want error containing %q", err.Error(), tt.wantErrMsg)
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if !strings.HasPrefix(sql, "SELECT ") && !strings.HasPrefix(sql, "WITH ") {
				t.Errorf("SQL should start with SELECT or WITH, got: %s", sql)
			}

			for _, part := range tt.wantParts {
				if !strings.Contains(sql, part) {
					t.Errorf("SQL missing %q\ngot: %s", part, sql)
				}
			}
		})
	}
}

func TestSplitSchemaTable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input      string
		wantSchema string
		wantTable  string
	}{
		{"staging.orders", "staging", "orders"},
		{"orders", "main", "orders"},
		{"mart.dim_customers", "mart", "dim_customers"},
	}
	for _, tt := range tests {
		schema, table := splitSchemaTable(tt.input)
		if schema != tt.wantSchema || table != tt.wantTable {
			t.Errorf("splitSchemaTable(%q) = (%q, %q), want (%q, %q)",
				tt.input, schema, table, tt.wantSchema, tt.wantTable)
		}
	}
}

func TestAuditToSQL_ColumnExistsNoSchema(t *testing.T) {
	t.Parallel()
	sql, err := AuditToSQL("column_exists(id)", "orders", "", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, part := range []string{
		"DESCRIBE orders",
		"column_name = 'id'",
	} {
		if !strings.Contains(sql, part) {
			t.Errorf("SQL missing %q\ngot: %s", part, sql)
		}
	}
}

func TestAuditToSQL_FreshnessDays(t *testing.T) {
	t.Parallel()
	sql, err := AuditToSQL("freshness(created_at, 3d)", "staging.orders", "", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, part := range []string{"MAX(created_at)", "3 DAY", "FROM staging.orders"} {
		if !strings.Contains(sql, part) {
			t.Errorf("SQL missing %q\ngot: %s", part, sql)
		}
	}
}

// --- Regression: historicalTable redirects AT VERSION clauses ---
//
// In sandbox mode the previous-snapshot ID lives in the prod catalog, not
// the sandbox catalog. The audit SQL needs to time-travel against the
// prod-prefixed reference; otherwise DuckLake errors with "No snapshot
// found at version N". Callers (runner.go / script.go) pass the prefixed
// name as `historicalTable`. Two patterns use it:
//   - row_count_change < N%
//   - distribution(col) STABLE

func TestAuditToSQL_RowCountChange_HistoricalTable(t *testing.T) {
	t.Parallel()
	sql, err := AuditToSQL("row_count_change < 50%", "raw.t", "lake.raw.t", 42)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Current data must come from the unprefixed (sandbox-local) name
	if !strings.Contains(sql, "FROM raw.t)") {
		t.Errorf("expected current side to read from raw.t, got: %s", sql)
	}
	// Historical comparison must use the prod-prefixed name
	if !strings.Contains(sql, "FROM lake.raw.t AT (VERSION => 42)") {
		t.Errorf("expected historical side to use lake.raw.t AT VERSION, got: %s", sql)
	}
	// Sanity: the unprefixed name must NOT appear in any AT VERSION clause
	if strings.Contains(sql, "raw.t AT (VERSION =>") && !strings.Contains(sql, "lake.raw.t AT (VERSION =>") {
		t.Errorf("AT VERSION used unprefixed name (would fail in sandbox), got: %s", sql)
	}
}

func TestAuditToSQL_DistributionStable_HistoricalTable(t *testing.T) {
	t.Parallel()
	sql, err := AuditToSQL("distribution(category) STABLE", "raw.t", "lake.raw.t", 42)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// `curr` reads current data from raw.t (no prefix)
	if !strings.Contains(sql, "FROM raw.t GROUP BY category") {
		t.Errorf("expected curr to read from raw.t, got: %s", sql)
	}
	// `prev` time-travels via prod-prefixed reference
	if !strings.Contains(sql, "FROM lake.raw.t AT (VERSION => 42)") {
		t.Errorf("expected prev to use lake.raw.t AT VERSION, got: %s", sql)
	}
}

func TestAuditToSQL_HistoricalTableDefaultsToTable(t *testing.T) {
	t.Parallel()
	// When historicalTable is empty (prod runs), AT VERSION clauses must
	// fall back to the unprefixed `table` argument.
	sql, err := AuditToSQL("row_count_change < 50%", "raw.t", "", 42)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "FROM raw.t AT (VERSION => 42)") {
		t.Errorf("expected unprefixed AT VERSION when historicalTable is empty, got: %s", sql)
	}
}

// --- Regression: historical audits skip on first run (Bug 33 + 35) ---
//
// Earlier `row_count_change` and `distribution STABLE` failed on the first
// run with "Catalog Error: Table with name X does not exist at version N!"
// because the previous-snapshot lookup returned the global current-catalog
// snapshot rather than 0 (= "no commit yet"). The fix returns 0 from
// GetPreviousSnapshot when the model has never been committed, and the
// audit patterns short-circuit `prev == 0` to a no-op `SELECT 1 WHERE 0`.

func TestAuditToSQL_RowCountChange_FirstRunSkips(t *testing.T) {
	t.Parallel()
	sql, err := AuditToSQL("row_count_change < 50%", "raw.t", "", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// First run = prev snapshot 0 = audit must be a no-op.
	if sql != "SELECT 1 WHERE 0" {
		t.Errorf("expected no-op SQL on first run (prev=0), got: %s", sql)
	}
	// Crucially: must NOT contain `AT (VERSION => 0)` which would error out.
	if strings.Contains(sql, "AT (VERSION") {
		t.Errorf("first-run audit must not time-travel, got: %s", sql)
	}
}

func TestAuditToSQL_DistributionStable_FirstRunSkips(t *testing.T) {
	t.Parallel()
	sql, err := AuditToSQL("distribution(category) STABLE", "raw.t", "", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "SELECT 1 WHERE 0" {
		t.Errorf("expected no-op SQL on first run (prev=0), got: %s", sql)
	}
	if strings.Contains(sql, "AT (VERSION") {
		t.Errorf("first-run audit must not time-travel, got: %s", sql)
	}
}

// --- Regression: column_exists / column_type via DESCRIBE (Bug 30) ---
//
// Earlier these audits used `information_schema.columns` which under
// DuckLake routes to `duckdb_columns` and fails with "Unsupported catalog
// type for duckdb_columns". The fix replaces the lookup with a
// `(DESCRIBE schema.table)` subquery that DuckLake supports natively.
// Combining the two audits in one batch surfaced the bug as an
// "Invalid Unicode" error (downstream symptom of the same broken path).

func TestAuditToSQL_ColumnExists_UsesDescribe(t *testing.T) {
	t.Parallel()
	sql, err := AuditToSQL("column_exists(id)", "raw.t", "", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "DESCRIBE raw.t") {
		t.Errorf("expected DESCRIBE-based lookup, got: %s", sql)
	}
	if strings.Contains(sql, "information_schema.columns") {
		t.Errorf("regression: still using information_schema.columns (broken in DuckLake), got: %s", sql)
	}
}

func TestAuditToSQL_ColumnType_UsesDescribe(t *testing.T) {
	t.Parallel()
	sql, err := AuditToSQL("column_type(amount, INTEGER)", "raw.t", "", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "DESCRIBE raw.t") {
		t.Errorf("expected DESCRIBE-based lookup, got: %s", sql)
	}
	if strings.Contains(sql, "information_schema.columns") {
		t.Errorf("regression: still using information_schema.columns (broken in DuckLake), got: %s", sql)
	}
}

func TestAuditsToBatchSQL_ColumnExistsPlusColumnType(t *testing.T) {
	t.Parallel()
	// Bug 30 specifically: combining the two used to produce a batch SQL
	// with `WITH _col_check ...` and `WITH _col_type ...` set up in a way
	// that triggered the Unicode/integer-overflow downstream errors.
	// The fix removes the CTEs entirely.
	sql, errs := AuditsToBatchSQL(
		[]string{"column_exists(id)", "column_type(amount, INTEGER)"},
		"raw.t", "", 0,
	)
	if len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}
	if strings.Contains(sql, "_col_check") || strings.Contains(sql, "_col_type") {
		t.Errorf("regression: CTE-based lookups still present, got: %s", sql)
	}
	if strings.Contains(sql, "information_schema.columns") {
		t.Errorf("regression: still using information_schema.columns, got: %s", sql)
	}
	if !strings.Contains(sql, "DESCRIBE raw.t") {
		t.Errorf("expected DESCRIBE-based lookup, got: %s", sql)
	}
}

// --- Regression: percentile fraction validation (Bug 32) ---
//
// percentile(col, 95) used to fall through to DuckDB's PERCENTILE_CONT and
// produce a cryptic Binder Error at run time. Now we reject 0–100 values
// at parse time with a clear actionable message.

func TestAuditToSQL_PercentileFraction_OutOfRange(t *testing.T) {
	t.Parallel()
	cases := []string{"95", "1.5", "100"}
	for _, frac := range cases {
		_, err := AuditToSQL("percentile(amount, "+frac+") < 100", "raw.t", "", 0)
		if err == nil {
			t.Errorf("percentile fraction %s should be rejected at parse time", frac)
			continue
		}
		if !strings.Contains(err.Error(), "[0, 1]") {
			t.Errorf("error should mention valid range, got: %v", err)
		}
	}
}

func TestAuditToSQL_PercentileFraction_InRange(t *testing.T) {
	t.Parallel()
	cases := []string{"0", "0.5", "0.95", "1"}
	for _, frac := range cases {
		_, err := AuditToSQL("percentile(amount, "+frac+") < 100", "raw.t", "", 0)
		if err != nil {
			t.Errorf("percentile fraction %s should be accepted, got: %v", frac, err)
		}
	}
}

func TestAuditToSQL_DistributionStableNoPrev(t *testing.T) {
	t.Parallel()
	sql, err := AuditToSQL("distribution(status) STABLE", "staging.orders", "", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "SELECT 1 WHERE 0" {
		t.Errorf("should return exact skip SQL, got: %s", sql)
	}
}

func TestAuditToSQL_UsesTableParam(t *testing.T) {
	t.Parallel()
	directives := []string{
		"row_count >= 1",
		"freshness(updated_at, 24h)",
		"mean(price) >= 0",
		"reconcile_count(other.tab)",
		"column_exists(id)",
	}
	for _, d := range directives {
		sql, err := AuditToSQL(d, "custom.my_table", "", 0)
		if err != nil {
			t.Errorf("directive %q: unexpected error: %v", d, err)
			continue
		}
		if !strings.Contains(sql, "custom") || !strings.Contains(sql, "my_table") {
			t.Errorf("directive %q: SQL does not reference table 'custom.my_table'\ngot: %s", d, sql)
		}
	}
}

func TestAuditsToBatchSQL_AllInvalid(t *testing.T) {
	t.Parallel()
	sql, errs := AuditsToBatchSQL([]string{"bad1", "bad2"}, "staging.orders", "", 0)
	if sql != "" {
		t.Errorf("expected empty SQL when all audits invalid, got %q", sql)
	}
	if len(errs) != 2 {
		t.Errorf("expected 2 errors, got %d", len(errs))
	}
}

func TestAuditsToBatchSQL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		directives  []string
		wantQueries int    // number of UNION ALL + 1
		wantContain string // substring that should be in the SQL
		wantErrors  int    // number of parse errors
	}{
		{
			name:        "single audit",
			directives:  []string{"row_count >= 1"},
			wantQueries: 1,
			wantContain: "COUNT(*)",
			wantErrors:  0,
		},
		{
			name:        "multiple audits",
			directives:  []string{"row_count >= 1", "min(amount) >= 0", "max(quantity) <= 1000"},
			wantQueries: 3,
			wantContain: "UNION ALL",
			wantErrors:  0,
		},
		{
			name:        "with invalid audit",
			directives:  []string{"row_count >= 1", "invalid audit here", "min(amount) >= 0"},
			wantQueries: 2,
			wantContain: "UNION ALL",
			wantErrors:  1,
		},
		{
			name:        "empty list",
			directives:  []string{},
			wantQueries: 0,
			wantContain: "",
			wantErrors:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sql, errs := AuditsToBatchSQL(tt.directives, "staging.orders", "", 0)

			if len(errs) != tt.wantErrors {
				t.Errorf("got %d errors, want %d", len(errs), tt.wantErrors)
			}

			if tt.wantQueries == 0 {
				if sql != "" {
					t.Errorf("expected empty SQL, got %q", sql)
				}
				return
			}

			if tt.wantContain != "" && !strings.Contains(sql, tt.wantContain) {
				t.Errorf("SQL should contain %q, got %q", tt.wantContain, sql)
			}

			// Count UNION ALL occurrences (should be wantQueries - 1)
			unionCount := strings.Count(sql, "UNION ALL")
			expectedUnions := tt.wantQueries - 1
			if unionCount != expectedUnions {
				t.Errorf("got %d UNION ALL, want %d (for %d queries)", unionCount, expectedUnions, tt.wantQueries)
			}
		})
	}
}

// AuditsToTransactionalSQL must wrap the batch in a CTE that calls
// error() if any audit message survives the WHERE filter. This is the
// shape required to abort an enclosing BEGIN/COMMIT atomically when an
// audit fails — without it, audits would only return error rows that
// can't roll back the surrounding transaction.
func TestAuditsToTransactionalSQL(t *testing.T) {
	t.Parallel()

	t.Run("empty directives produce empty SQL", func(t *testing.T) {
		t.Parallel()
		sql, errs := AuditsToTransactionalSQL(nil, "staging.orders", "", 0)
		if sql != "" || len(errs) != 0 {
			t.Errorf("expected empty SQL and no errors, got %q / %v", sql, errs)
		}
	})

	t.Run("wraps batch in CTE with error() call", func(t *testing.T) {
		t.Parallel()
		sql, errs := AuditsToTransactionalSQL(
			[]string{"row_count >= 1", "min(amount) >= 0"},
			"staging.orders", "", 0,
		)
		if len(errs) != 0 {
			t.Fatalf("unexpected parse errors: %v", errs)
		}
		// Must include the structural pieces that make abort-on-failure work.
		mustContain := []string{
			"WITH audit_failures AS",
			"string_agg(audit_msg",
			"audit_msg IS NOT NULL",
			"SELECT error('audit failed: ' || msg)",
			"FROM audit_failures",
			"WHERE msg IS NOT NULL",
			"UNION ALL", // batch SQL still embedded
		}
		for _, s := range mustContain {
			if !strings.Contains(sql, s) {
				t.Errorf("transactional SQL missing %q\nFull SQL:\n%s", s, sql)
			}
		}
	})

	t.Run("propagates parse errors from underlying batch builder", func(t *testing.T) {
		t.Parallel()
		_, errs := AuditsToTransactionalSQL(
			[]string{"this is not a valid audit", "row_count >= 1"},
			"staging.orders", "", 0,
		)
		if len(errs) == 0 {
			t.Error("expected parse error to propagate from AuditsToBatchSQL")
		}
	})
}

func TestAuditToSQL_MixedCasePreservesLiterals(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		directive string
		wantPart  string // case-sensitive literal that must appear in SQL
	}{
		{
			name:      "Golden mixed-case preserves path",
			directive: "Golden('Tests/MyFile.csv')",
			wantPart:  "Tests/MyFile.csv",
		},
		{
			name:      "column_exists mixed-case",
			directive: "Column_Exists(myCol)",
			wantPart:  "myCol",
		},
		{
			name:      "column_type mixed-case preserves type",
			directive: "Column_Type(price, DECIMAL)",
			wantPart:  "price",
		},
		{
			name:      "reconcile_count mixed-case preserves table",
			directive: "Reconcile_Count(staging.Orders)",
			wantPart:  "staging.Orders",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sql, err := AuditToSQL(tt.directive, "test_table", "", 0)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !strings.Contains(sql, tt.wantPart) {
				t.Errorf("SQL should contain %q (case-sensitive literal preserved)\ngot: %s", tt.wantPart, sql)
			}
		})
	}
}
