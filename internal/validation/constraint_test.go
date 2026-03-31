// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package validation

import (
	"strings"
	"testing"
)

func TestConstraintToSQL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		directive  string
		table      string
		wantParts  []string // all substrings that must be in the SQL
		wantErrMsg string
	}{
		// Basic constraints
		{
			name:      "NOT NULL",
			directive: "name NOT NULL",
			table:     "tmp_model",
			wantParts: []string{
				"SELECT printf('NOT NULL failed:",
				"FROM tmp_model WHERE name IS NULL",
				"HAVING COUNT(*) > 0",
			},
		},
		{
			name:      "PRIMARY KEY",
			directive: "id PRIMARY KEY",
			table:     "tmp_model",
			wantParts: []string{
				"SELECT printf('PRIMARY KEY failed:",
				"FROM tmp_model WHERE id IS NULL",
				"GROUP BY id HAVING COUNT(*) > 1",
			},
		},
		{
			name:      "UNIQUE",
			directive: "email UNIQUE",
			table:     "tmp_model",
			wantParts: []string{
				"SELECT printf('UNIQUE failed:",
				"FROM tmp_model GROUP BY email HAVING COUNT(*) > 1",
				"LIMIT 1",
			},
		},
		{
			name:      "composite UNIQUE",
			directive: "(user_id, order_id) UNIQUE",
			table:     "tmp_model",
			wantParts: []string{
				"SELECT printf('UNIQUE failed:",
				"FROM tmp_model GROUP BY user_id, order_id HAVING COUNT(*) > 1",
				"LIMIT 1",
			},
		},
		// Comparisons
		{
			name:      "greater than or equal",
			directive: "amount >= 0",
			table:     "tmp_model",
			wantParts: []string{
				"SELECT printf(",
				"'amount', '>=', '0'",
				"FROM tmp_model WHERE amount < 0",
				"HAVING COUNT(*) > 0",
			},
		},
		{
			name:      "less than",
			directive: "age < 100",
			table:     "tmp_model",
			wantParts: []string{
				"FROM tmp_model WHERE age >= 100",
				"HAVING COUNT(*) > 0",
			},
		},
		// BETWEEN
		{
			name:      "BETWEEN",
			directive: "score BETWEEN 0 AND 100",
			table:     "tmp_model",
			wantParts: []string{
				"SELECT printf('BETWEEN failed:",
				"FROM tmp_model WHERE score < 0 OR score > 100",
				"HAVING COUNT(*) > 0",
			},
		},
		// IN / NOT IN
		{
			name:      "IN",
			directive: "status IN ('active', 'pending')",
			table:     "tmp_model",
			wantParts: []string{
				"IN failed: status",
				"FROM tmp_model WHERE status NOT IN ('active', 'pending')",
				"HAVING COUNT(*) > 0",
			},
		},
		{
			name:      "IN unquoted strings",
			directive: "currency IN (SEK, USD, EUR)",
			table:     "tmp_model",
			wantParts: []string{
				"FROM tmp_model WHERE currency NOT IN ('SEK', 'USD', 'EUR')",
				"HAVING COUNT(*) > 0",
			},
		},
		{
			name:      "IN numbers",
			directive: "level IN (1, 2, 3)",
			table:     "tmp_model",
			wantParts: []string{
				"FROM tmp_model WHERE level NOT IN (1, 2, 3)",
				"HAVING COUNT(*) > 0",
			},
		},
		{
			name:      "NOT IN",
			directive: "status NOT IN ('deleted')",
			table:     "tmp_model",
			wantParts: []string{
				"NOT IN failed: status",
				"FROM tmp_model WHERE status IN ('deleted')",
				"HAVING COUNT(*) > 0",
			},
		},
		{
			name:      "NOT IN unquoted",
			directive: "role NOT IN (admin, root)",
			table:     "tmp_model",
			wantParts: []string{
				"FROM tmp_model WHERE role IN ('admin', 'root')",
				"HAVING COUNT(*) > 0",
			},
		},
		// Foreign key
		{
			name:      "REFERENCES",
			directive: "customer_id REFERENCES marts.dim_customers(id)",
			table:     "tmp_model",
			wantParts: []string{
				"SELECT printf('REFERENCES failed:",
				"FROM tmp_model WHERE customer_id IS NOT NULL AND customer_id NOT IN (SELECT id FROM marts.dim_customers)",
				"HAVING COUNT(*) > 0",
			},
		},
		// String validation
		{
			name:      "NOT EMPTY",
			directive: "name NOT EMPTY",
			table:     "tmp_model",
			wantParts: []string{
				"SELECT printf('NOT EMPTY failed:",
				"FROM tmp_model WHERE name IS NULL OR TRIM(name) = ''",
				"HAVING COUNT(*) > 0",
			},
		},
		{
			name:      "MATCHES regex",
			directive: "code MATCHES '^[A-Z]{3}$'",
			table:     "tmp_model",
			wantParts: []string{
				"SELECT printf('MATCHES failed:",
				"FROM tmp_model WHERE NOT regexp_matches(code, '^[A-Z]{3}$')",
				"HAVING COUNT(*) > 0",
			},
		},
		{
			name:      "EMAIL",
			directive: "email EMAIL",
			table:     "tmp_model",
			wantParts: []string{
				"SELECT printf('EMAIL failed:",
				"FROM tmp_model WHERE email IS NOT NULL AND NOT regexp_matches(email,",
				"HAVING COUNT(*) > 0",
			},
		},
		{
			name:      "UUID",
			directive: "id UUID",
			table:     "tmp_model",
			wantParts: []string{
				"SELECT printf('UUID failed:",
				"FROM tmp_model WHERE id IS NOT NULL AND NOT regexp_matches(id,",
				"HAVING COUNT(*) > 0",
			},
		},
		{
			name:      "LENGTH BETWEEN",
			directive: "code LENGTH BETWEEN 3 AND 10",
			table:     "tmp_model",
			wantParts: []string{
				"SELECT printf('LENGTH BETWEEN failed:",
				"FROM tmp_model WHERE LENGTH(code) < 3 OR LENGTH(code) > 10",
				"HAVING COUNT(*) > 0",
			},
		},
		{
			name:      "LENGTH equals",
			directive: "zip LENGTH = 5",
			table:     "tmp_model",
			wantParts: []string{
				"SELECT printf('LENGTH failed:",
				"FROM tmp_model WHERE LENGTH(zip) != 5",
				"HAVING COUNT(*) > 0",
			},
		},
		// CHECK
		{
			name:      "CHECK expression",
			directive: "price CHECK (price > 0 AND price < 1000000)",
			table:     "tmp_model",
			wantParts: []string{
				"SELECT printf('CHECK failed:",
				"FROM tmp_model WHERE NOT (price > 0 AND price < 1000000)",
				"HAVING COUNT(*) > 0",
			},
		},
		// Advanced
		{
			name:      "AT_LEAST_ONE",
			directive: "value AT_LEAST_ONE",
			table:     "tmp_model",
			wantParts: []string{
				"SELECT printf('AT_LEAST_ONE failed:",
				"(SELECT COUNT(value) FROM tmp_model) = 0",
			},
		},
		{
			name:      "NOT_CONSTANT",
			directive: "status NOT_CONSTANT",
			table:     "tmp_model",
			wantParts: []string{
				"SELECT printf('NOT_CONSTANT failed:",
				"COUNT(DISTINCT status) FROM tmp_model",
				"< 2",
			},
		},
		{
			name:      "NULL_PERCENT",
			directive: "email NULL_PERCENT < 10",
			table:     "tmp_model",
			wantParts: []string{
				"SELECT printf('NULL_PERCENT failed:",
				"100.0 * SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) / COUNT(*) FROM tmp_model",
				">= 10",
			},
		},
		{
			name:      "SEQUENTIAL",
			directive: "seq SEQUENTIAL",
			table:     "tmp_model",
			wantParts: []string{
				"SELECT printf('SEQUENTIAL failed:",
				"LAG(seq) OVER (ORDER BY seq) AS gap FROM tmp_model",
				"WHERE gap > 1 LIMIT 1",
			},
		},
		{
			name:      "NO_OVERLAP",
			directive: "(start_date, end_date) NO_OVERLAP",
			table:     "tmp_model",
			wantParts: []string{
				"SELECT printf('NO_OVERLAP failed:",
				"FROM tmp_model a, tmp_model b",
				"a.rowid < b.rowid",
				"a.start_date < b.end_date AND a.end_date > b.start_date",
				"LIMIT 1",
			},
		},
		// LIKE / NOT LIKE
		{
			name:      "LIKE",
			directive: "code LIKE 'AB%'",
			table:     "tmp_model",
			wantParts: []string{
				"SELECT printf('LIKE failed:",
				"FROM tmp_model WHERE code NOT LIKE 'AB%'",
				"HAVING COUNT(*) > 0",
			},
		},
		{
			name:      "NOT LIKE",
			directive: "name NOT LIKE 'test%'",
			table:     "tmp_model",
			wantParts: []string{
				"SELECT printf('NOT LIKE failed:",
				"FROM tmp_model WHERE name LIKE 'test%'",
				"HAVING COUNT(*) > 0",
			},
		},
		// SEQUENTIAL with step
		{
			name:      "SEQUENTIAL with step",
			directive: "seq SEQUENTIAL(5)",
			table:     "tmp_model",
			wantParts: []string{
				"SELECT printf('SEQUENTIAL(",
				"LAG(seq) OVER (ORDER BY seq) AS gap FROM tmp_model",
				"WHERE gap > 5 LIMIT 1",
			},
		},
		// DUPLICATE_PERCENT
		{
			name:      "DUPLICATE_PERCENT",
			directive: "email DUPLICATE_PERCENT < 10",
			table:     "tmp_model",
			wantParts: []string{
				"SELECT printf('DUPLICATE_PERCENT failed:",
				"COUNT(DISTINCT email)",
				"FROM tmp_model",
				">= 10",
			},
		},
		// Error cases
		{
			name:       "empty directive",
			directive:  "",
			table:      "tmp_model",
			wantErrMsg: "empty constraint",
		},
		{
			name:       "unknown format",
			directive:  "something weird",
			table:      "tmp_model",
			wantErrMsg: "unknown constraint format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sql, err := ConstraintToSQL(tt.directive, tt.table)

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

			if !strings.HasPrefix(sql, "SELECT ") {
				t.Errorf("SQL should start with SELECT, got: %s", sql)
			}

			for _, part := range tt.wantParts {
				if !strings.Contains(sql, part) {
					t.Errorf("SQL missing %q\ngot: %s", part, sql)
				}
			}
		})
	}
}

func TestConstraintsToBatchSQL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		directives  []string
		wantQueries int    // number of UNION ALL + 1
		wantContain string // substring that should be in the SQL
		wantErrors  int    // number of parse errors
	}{
		{
			name:        "single constraint",
			directives:  []string{"id NOT NULL"},
			wantQueries: 1,
			wantContain: "WHERE id IS NULL",
			wantErrors:  0,
		},
		{
			name:        "multiple constraints",
			directives:  []string{"id NOT NULL", "email UNIQUE", "amount >= 0"},
			wantQueries: 3,
			wantContain: "UNION ALL",
			wantErrors:  0,
		},
		{
			name:        "with invalid constraint",
			directives:  []string{"id NOT NULL", "invalid constraint here", "email UNIQUE"},
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
		{
			name:        "all invalid",
			directives:  []string{"bad1", "bad2"},
			wantQueries: 0,
			wantContain: "",
			wantErrors:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sql, errs := ConstraintsToBatchSQL(tt.directives, "tmp_model")

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

func TestQuoteINValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{"'a', 'b'", "'a', 'b'"},       // already quoted
		{"1, 2, 3", "1, 2, 3"},         // numeric
		{"foo, bar", "'foo', 'bar'"},    // unquoted strings
		{"1.5, -2, +3", "1.5, -2, +3"}, // float and signed numbers
		{"a, 1, 'b'", "'a', 1, 'b'"},   // mixed
		{"it's", "'it''s'"},             // quote escaping
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			got := quoteINValues(tt.input)
			if got != tt.want {
				t.Errorf("quoteINValues(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestConstraintToSQL_DistinctCount(t *testing.T) {
	t.Parallel()
	sql, err := ConstraintToSQL("email DISTINCT_COUNT >= 5", "tmp_model")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, part := range []string{
		"SELECT printf('DISTINCT_COUNT failed:",
		"COUNT(DISTINCT email) FROM tmp_model",
		"< 5", // inverted >= becomes <
	} {
		if !strings.Contains(sql, part) {
			t.Errorf("SQL missing %q\ngot: %s", part, sql)
		}
	}
}

func TestConstraintToSQL_DistinctCountLessThan(t *testing.T) {
	t.Parallel()
	sql, err := ConstraintToSQL("status DISTINCT_COUNT < 100", "tmp_model")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, part := range []string{
		"SELECT printf('DISTINCT_COUNT failed:",
		"COUNT(DISTINCT status) FROM tmp_model",
		">= 100", // inverted < becomes >=
	} {
		if !strings.Contains(sql, part) {
			t.Errorf("SQL missing %q\ngot: %s", part, sql)
		}
	}
}

func TestQuoteINValues_EmptyParts(t *testing.T) {
	t.Parallel()
	got := quoteINValues("a, , b")
	want := "'a',  , 'b'"
	if got != want {
		t.Errorf("quoteINValues(\"a, , b\") = %q, want %q", got, want)
	}
}

func TestQuoteINValues_SignInMiddle(t *testing.T) {
	t.Parallel()
	got := quoteINValues("1-2, 3+4")
	want := "'1-2', '3+4'"
	if got != want {
		t.Errorf("quoteINValues(\"1-2, 3+4\") = %q, want %q", got, want)
	}
}

func TestConstraintToSQL_UsesTableParam(t *testing.T) {
	t.Parallel()
	// Verify the table parameter is actually embedded in the SQL, not hardcoded.
	directives := []string{
		"id NOT NULL",
		"email UNIQUE",
		"amount >= 0",
		"status IN ('a')",
		"name NOT EMPTY",
	}
	for _, d := range directives {
		sql, err := ConstraintToSQL(d, "custom_schema.my_table")
		if err != nil {
			t.Errorf("directive %q: unexpected error: %v", d, err)
			continue
		}
		if !strings.Contains(sql, "custom_schema.my_table") {
			t.Errorf("directive %q: SQL does not reference table 'custom_schema.my_table'\ngot: %s", d, sql)
		}
	}
}

func TestInvertOp(t *testing.T) {
	t.Parallel()
	tests := []struct {
		op   string
		want string
	}{
		{">=", "<"},
		{"<=", ">"},
		{">", "<="},
		{"<", ">="},
		{"=", "!="},
		{"!=", "="},
		{"unknown", "unknown"},
		{"", ""},
	}
	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			t.Parallel()
			got := invertOp(tt.op)
			if got != tt.want {
				t.Errorf("invertOp(%q) = %q, want %q", tt.op, got, tt.want)
			}
		})
	}
}

func TestConstraintToSQL_MixedCasePreservesLiterals(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		directive string
		wantPart  string // case-sensitive literal that must appear in SQL
	}{
		{
			name:      "LIKE mixed-case preserves pattern",
			directive: "name Like 'Ab%'",
			wantPart:  "'Ab%'",
		},
		{
			name:      "NOT LIKE mixed-case preserves pattern",
			directive: "name Not Like 'Ab%'",
			wantPart:  "'Ab%'",
		},
		{
			name:      "MATCHES mixed-case preserves regex",
			directive: "code Matches '^[A-Z]{2}[0-9]+'",
			wantPart:  "'^[A-Z]{2}[0-9]+'",
		},
		{
			name:      "BETWEEN mixed-case preserves values",
			directive: "price Between '2024-01-01' and '2024-12-31'",
			wantPart:  "'2024-01-01'",
		},
		{
			name:      "IN mixed-case preserves values",
			directive: "status In ('Active', 'Pending')",
			wantPart:  "'Active'",
		},
		{
			name:      "CHECK mixed-case preserves expression",
			directive: "age Check (age >= 0 AND age <= 150)",
			wantPart:  "age >= 0 AND age <= 150",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sql, err := ConstraintToSQL(tt.directive, "tmp_model")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !strings.Contains(sql, tt.wantPart) {
				t.Errorf("SQL should contain %q (case-sensitive literal preserved)\ngot: %s", tt.wantPart, sql)
			}
		})
	}
}
