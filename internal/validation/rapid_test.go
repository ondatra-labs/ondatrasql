// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package validation

import (
	"fmt"
	"strings"
	"testing"
	"unicode"

	"pgregory.net/rapid"
)

// --- Generators ---

func genColumnName() *rapid.Generator[string] {
	return rapid.StringMatching(`^[a-z][a-z0-9_]{0,15}$`)
}

func genTableName() *rapid.Generator[string] {
	return rapid.Just("tmp_model")
}

// randomizeCase randomly changes the case of each character in a string.
// This is the key property: keywords should be case-insensitive, but
// literals (values, paths, column names) must be preserved as-is.
func randomizeCase(t *rapid.T, s string) string {
	runes := []rune(s)
	for i, r := range runes {
		// Don't mutate characters inside single-quoted strings
		if r == '\'' {
			break
		}
		if rapid.Bool().Draw(t, fmt.Sprintf("case_%d", i)) {
			runes[i] = unicode.ToLower(r)
		} else {
			runes[i] = unicode.ToUpper(r)
		}
	}
	return string(runes)
}

func genSimpleConstraint() *rapid.Generator[string] {
	return rapid.Custom[string](func(t *rapid.T) string {
		col := genColumnName().Draw(t, "col")
		kind := rapid.SampledFrom([]string{
			"NOT NULL", "UNIQUE", "PRIMARY KEY", "NOT EMPTY",
			"EMAIL", "UUID", "AT_LEAST_ONE", "NOT_CONSTANT",
		}).Draw(t, "kind")
		return col + " " + randomizeCase(t, kind)
	})
}

func genComparisonConstraint() *rapid.Generator[string] {
	return rapid.Custom[string](func(t *rapid.T) string {
		col := genColumnName().Draw(t, "col")
		op := rapid.SampledFrom([]string{">=", "<=", ">", "<", "="}).Draw(t, "op")
		val := rapid.IntRange(-1000, 1000).Draw(t, "val")
		return fmt.Sprintf("%s %s %d", col, op, val)
	})
}

func genBetweenConstraint() *rapid.Generator[string] {
	return rapid.Custom[string](func(t *rapid.T) string {
		col := genColumnName().Draw(t, "col")
		lo := rapid.IntRange(-1000, 1000).Draw(t, "lo")
		hi := rapid.IntRange(lo, lo+1000).Draw(t, "hi")
		between := randomizeCase(t, "BETWEEN")
		and := randomizeCase(t, "AND")
		return fmt.Sprintf("%s %s %d %s %d", col, between, lo, and, hi)
	})
}

func genINConstraint() *rapid.Generator[string] {
	return rapid.Custom[string](func(t *rapid.T) string {
		col := genColumnName().Draw(t, "col")
		n := rapid.IntRange(1, 5).Draw(t, "n")
		vals := make([]string, n)
		for i := range vals {
			vals[i] = rapid.SampledFrom([]string{
				"'active'", "'pending'", "'closed'", "1", "2", "3",
			}).Draw(t, "val")
		}
		in := randomizeCase(t, "IN")
		return col + " " + in + " (" + strings.Join(vals, ", ") + ")"
	})
}

func genLikeConstraint() *rapid.Generator[string] {
	return rapid.Custom[string](func(t *rapid.T) string {
		col := genColumnName().Draw(t, "col")
		// Pattern with mixed-case literal — must be preserved
		pattern := rapid.SampledFrom([]string{
			"Ab%", "test_%", "%FooBar", "Hello_World%",
		}).Draw(t, "pattern")
		like := randomizeCase(t, "LIKE")
		return fmt.Sprintf("%s %s '%s'", col, like, pattern)
	})
}

func genMatchesConstraint() *rapid.Generator[string] {
	return rapid.Custom[string](func(t *rapid.T) string {
		col := genColumnName().Draw(t, "col")
		regex := rapid.SampledFrom([]string{
			"^[A-Z]{2}[0-9]+$", "^[a-z]+$", "^[A-Za-z]+@[A-Za-z]+$",
		}).Draw(t, "regex")
		matches := randomizeCase(t, "MATCHES")
		return fmt.Sprintf("%s %s '%s'", col, matches, regex)
	})
}

func genAnyValidConstraint() *rapid.Generator[string] {
	return rapid.OneOf(
		genSimpleConstraint(),
		genComparisonConstraint(),
		genBetweenConstraint(),
		genINConstraint(),
		genLikeConstraint(),
		genMatchesConstraint(),
	)
}

// --- Audit Generators ---

func genSimpleAudit() *rapid.Generator[string] {
	return rapid.Custom[string](func(t *rapid.T) string {
		op := rapid.SampledFrom([]string{">=", "<=", ">", "<", "="}).Draw(t, "op")
		n := rapid.IntRange(0, 10000).Draw(t, "n")
		rc := randomizeCase(t, "row_count")
		return fmt.Sprintf("%s %s %d", rc, op, n)
	})
}

func genStatAudit() *rapid.Generator[string] {
	return rapid.Custom[string](func(t *rapid.T) string {
		col := genColumnName().Draw(t, "col")
		fn := rapid.SampledFrom([]string{"mean", "min", "max", "sum"}).Draw(t, "fn")
		fn = randomizeCase(t, fn)
		op := rapid.SampledFrom([]string{">=", "<=", ">", "<", "="}).Draw(t, "op")
		val := rapid.IntRange(-1000, 1000).Draw(t, "val")
		return fmt.Sprintf("%s(%s) %s %d", fn, col, op, val)
	})
}

func genGoldenAudit() *rapid.Generator[string] {
	return rapid.Custom[string](func(t *rapid.T) string {
		// Path with mixed case — must be preserved
		path := rapid.SampledFrom([]string{
			"Tests/MyFile.csv", "data/Expected_Output.csv", "golden/CaseSensitive.csv",
		}).Draw(t, "path")
		golden := randomizeCase(t, "golden")
		return fmt.Sprintf("%s('%s')", golden, path)
	})
}

func genColumnExistsAudit() *rapid.Generator[string] {
	return rapid.Custom[string](func(t *rapid.T) string {
		col := genColumnName().Draw(t, "col")
		ce := randomizeCase(t, "column_exists")
		return fmt.Sprintf("%s(%s)", ce, col)
	})
}

func genAnyValidAudit() *rapid.Generator[string] {
	return rapid.OneOf(
		genSimpleAudit(),
		genStatAudit(),
		genGoldenAudit(),
		genColumnExistsAudit(),
	)
}

// --- Constraint Property Tests ---

// Property: all generated constraints must parse successfully and produce SQL starting with SELECT.
func TestRapid_ConstraintToSQL_ValidInput(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		directive := genAnyValidConstraint().Draw(t, "directive")
		table := genTableName().Draw(t, "table")

		sql, err := ConstraintToSQL(directive, table)
		if err != nil {
			t.Fatalf("ConstraintToSQL(%q) failed: %v", directive, err)
		}
		if sql == "" {
			t.Fatal("no error but empty SQL")
		}
		if !strings.HasPrefix(sql, "SELECT ") {
			t.Fatalf("SQL must start with SELECT, got: %.80s", sql)
		}
	})
}

// Property: output SQL always references the table parameter.
func TestRapid_ConstraintToSQL_ReferencesTable(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		directive := genAnyValidConstraint().Draw(t, "directive")

		sql, err := ConstraintToSQL(directive, "tmp_model")
		if err != nil {
			t.Fatalf("ConstraintToSQL(%q) failed: %v", directive, err)
		}
		if !strings.Contains(sql, "tmp_model") {
			t.Fatalf("SQL does not reference table: %.120s", sql)
		}
	})
}

// Property: output SQL has balanced parentheses and quotes.
func TestRapid_ConstraintToSQL_BalancedSyntax(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		directive := genAnyValidConstraint().Draw(t, "directive")

		sql, err := ConstraintToSQL(directive, "tmp_model")
		if err != nil {
			t.Fatalf("ConstraintToSQL(%q) failed: %v", directive, err)
		}

		// Balanced parentheses
		depth := 0
		for _, c := range sql {
			if c == '(' {
				depth++
			} else if c == ')' {
				depth--
			}
		}
		if depth != 0 {
			t.Fatalf("unbalanced parens (depth=%d) in: %s", depth, sql)
		}

		// Balanced single quotes
		if strings.Count(sql, "'")%2 != 0 {
			t.Fatalf("odd number of quotes in: %s", sql)
		}
	})
}

// Property: case-sensitive literals in constraints are preserved regardless of keyword casing.
// This is the test that would have caught the ToUpper bug.
func TestRapid_ConstraintToSQL_PreservesLiterals(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		col := genColumnName().Draw(t, "col")

		// Generate a LIKE constraint with a mixed-case pattern
		pattern := rapid.SampledFrom([]string{
			"Ab%", "FooBar%", "%TestValue", "Hello_World%",
		}).Draw(t, "pattern")
		like := randomizeCase(t, "LIKE")
		directive := fmt.Sprintf("%s %s '%s'", col, like, pattern)

		sql, err := ConstraintToSQL(directive, "tmp_model")
		if err != nil {
			t.Fatalf("ConstraintToSQL(%q) failed: %v", directive, err)
		}

		// The pattern literal must appear exactly as provided, not uppercased
		if !strings.Contains(sql, pattern) {
			t.Fatalf("literal %q not preserved in SQL for directive %q\ngot: %s", pattern, directive, sql)
		}

		// The column name must appear as provided
		if !strings.Contains(sql, col) {
			t.Fatalf("column %q not preserved in SQL for directive %q\ngot: %s", col, directive, sql)
		}
	})
}

// --- Audit Property Tests ---

// Property: all generated audits must parse successfully.
func TestRapid_AuditToSQL_ValidInput(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		directive := genAnyValidAudit().Draw(t, "directive")
		sql, err := AuditToSQL(directive, "test_table", 0)
		if err != nil {
			t.Fatalf("AuditToSQL(%q) failed: %v", directive, err)
		}
		if sql == "" {
			t.Fatal("no error but empty SQL")
		}
		if !strings.HasPrefix(sql, "SELECT ") && !strings.HasPrefix(sql, "WITH ") {
			t.Fatalf("SQL must start with SELECT or WITH, got: %.80s", sql)
		}
	})
}

// Property: case-sensitive paths in golden() audits are preserved.
func TestRapid_AuditToSQL_PreservesGoldenPath(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		path := rapid.SampledFrom([]string{
			"Tests/MyFile.csv", "data/Expected_Output.csv", "golden/CaseSensitive.csv",
		}).Draw(t, "path")
		golden := randomizeCase(t, "golden")
		directive := fmt.Sprintf("%s('%s')", golden, path)

		sql, err := AuditToSQL(directive, "test_table", 0)
		if err != nil {
			t.Fatalf("AuditToSQL(%q) failed: %v", directive, err)
		}

		// Path must appear exactly as provided, not lowercased
		if !strings.Contains(sql, path) {
			t.Fatalf("path %q not preserved in SQL for directive %q\ngot: %s", path, directive, sql)
		}
	})
}

// Property: column names in audit functions are preserved.
func TestRapid_AuditToSQL_PreservesColumnNames(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		col := genColumnName().Draw(t, "col")
		fn := rapid.SampledFrom([]string{"mean", "min", "max", "sum"}).Draw(t, "fn")
		fn = randomizeCase(t, fn)
		op := rapid.SampledFrom([]string{">=", "<=", ">", "<", "="}).Draw(t, "op")
		val := rapid.IntRange(0, 1000).Draw(t, "val")
		directive := fmt.Sprintf("%s(%s) %s %d", fn, col, op, val)

		sql, err := AuditToSQL(directive, "test_table", 0)
		if err != nil {
			t.Fatalf("AuditToSQL(%q) failed: %v", directive, err)
		}

		if !strings.Contains(sql, col) {
			t.Fatalf("column %q not preserved in SQL for directive %q\ngot: %s", col, directive, sql)
		}
	})
}

// --- Constraint Batch Composition Properties ---

// Property: ConstraintsToBatchSQL produces UNION ALL between all valid constraints.
func TestRapid_ConstraintsBatch_UnionAll(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(2, 6).Draw(t, "n")
		var directives []string
		for i := 0; i < n; i++ {
			directives = append(directives, genAnyValidConstraint().Draw(t, "constraint"))
		}

		sql, errs := ConstraintsToBatchSQL(directives, "tmp_model")
		if len(errs) > 0 {
			t.Fatalf("unexpected parse errors: %v", errs)
		}
		if sql == "" {
			t.Fatal("expected non-empty batch SQL")
		}

		// Should have n-1 separator UNION ALL (with newlines, to avoid counting internal ones)
		unions := strings.Count(sql, "\nUNION ALL\n")
		if unions != n-1 {
			t.Fatalf("expected %d UNION ALL, got %d", n-1, unions)
		}

		// Balanced parentheses
		depth := 0
		for _, c := range sql {
			if c == '(' {
				depth++
			} else if c == ')' {
				depth--
			}
		}
		if depth != 0 {
			t.Fatalf("unbalanced parens (depth=%d)", depth)
		}
	})
}

// Property: ConstraintsToBatchSQL empty input returns empty.
func TestRapid_ConstraintsBatch_EmptyInput(t *testing.T) {
	t.Parallel()
	sql, errs := ConstraintsToBatchSQL(nil, "tmp_model")
	if sql != "" {
		t.Fatalf("expected empty SQL, got: %s", sql)
	}
	if errs != nil {
		t.Fatalf("expected nil errors, got: %v", errs)
	}
}

// Property: invalid constraints in batch are collected as errors, valid ones still produce SQL.
func TestRapid_ConstraintsBatch_MixedValidInvalid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		valid := genAnyValidConstraint().Draw(t, "valid")
		invalid := "TOTALLY INVALID GARBAGE " + rapid.StringMatching(`^[A-Z]{3,8}$`).Draw(t, "garbage")

		sql, errs := ConstraintsToBatchSQL([]string{invalid, valid}, "tmp_model")
		if len(errs) != 1 {
			t.Fatalf("expected 1 error, got %d: %v", len(errs), errs)
		}
		if sql == "" {
			t.Fatal("expected non-empty SQL for the valid constraint")
		}
		// Should not contain UNION ALL (only one valid query)
		if strings.Contains(sql, "UNION ALL") {
			t.Fatalf("single valid constraint should not have UNION ALL: %s", sql)
		}
	})
}

// --- Audit Batch Composition Properties ---

// Property: AuditsToBatchSQL produces separator UNION ALL between all valid audits.
func TestRapid_AuditsBatch_UnionAll(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(2, 5).Draw(t, "n")
		var directives []string
		for i := 0; i < n; i++ {
			directives = append(directives, genAnyValidAudit().Draw(t, "audit"))
		}

		sql, errs := AuditsToBatchSQL(directives, "test_table", 0)
		if len(errs) > 0 {
			t.Fatalf("unexpected parse errors: %v", errs)
		}
		if sql == "" {
			t.Fatal("expected non-empty batch SQL")
		}

		// Count separator UNION ALL (with newlines) — individual queries may contain UNION ALL internally
		separators := strings.Count(sql, "\nUNION ALL\n")
		if separators != n-1 {
			t.Fatalf("expected %d separator UNION ALL, got %d", n-1, separators)
		}
	})
}

// Property: history-aware audits with prevSnapshot=0 produce pass-through SQL.
func TestRapid_AuditsBatch_HistoryAwareNoSnapshot(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		pct := rapid.IntRange(1, 99).Draw(t, "pct")
		directive := fmt.Sprintf("row_count_change < %d%%", pct)

		sql, err := AuditToSQL(directive, "test_table", 0)
		if err != nil {
			t.Fatalf("AuditToSQL: %v", err)
		}
		// With prevSnapshot=0, should be pass-through
		if sql != "SELECT 1 WHERE 0" {
			t.Fatalf("expected pass-through, got: %s", sql)
		}
	})
}

// Property: history-aware audits with prevSnapshot>0 reference the snapshot.
func TestRapid_AuditsBatch_HistoryAwareWithSnapshot(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		pct := rapid.IntRange(1, 99).Draw(t, "pct")
		snap := int64(rapid.IntRange(1, 10000).Draw(t, "snapshot"))
		directive := fmt.Sprintf("row_count_change < %d%%", pct)

		sql, err := AuditToSQL(directive, "test_table", snap)
		if err != nil {
			t.Fatalf("AuditToSQL: %v", err)
		}
		// Should reference the snapshot version
		if !strings.Contains(sql, fmt.Sprintf("VERSION => %d", snap)) {
			t.Fatalf("expected VERSION => %d in SQL: %s", snap, sql)
		}
	})
}

// Property: invalid audits in batch are collected as errors.
func TestRapid_AuditsBatch_MixedValidInvalid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		valid := genAnyValidAudit().Draw(t, "valid")
		invalid := "TOTALLY INVALID GARBAGE " + rapid.StringMatching(`^[A-Z]{3,8}$`).Draw(t, "garbage")

		sql, errs := AuditsToBatchSQL([]string{invalid, valid}, "test_table", 0)
		if len(errs) != 1 {
			t.Fatalf("expected 1 error, got %d: %v", len(errs), errs)
		}
		if sql == "" {
			t.Fatal("expected non-empty SQL for the valid audit")
		}
	})
}

// --- Utility Property Tests ---

// Property: invertOp is an involution (double-invert returns original).
func TestRapid_InvertOp_Involution(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		op := rapid.SampledFrom([]string{
			">=", "<=", ">", "<", "=", "!=",
		}).Draw(t, "op")

		result := invertOp(invertOp(op))
		if result != op {
			t.Fatalf("invertOp not involutory: invertOp(invertOp(%q)) = %q", op, result)
		}
	})
}

// Property: quoteINValues preserves value count and produces balanced quotes.
func TestRapid_QuoteINValues_PreservesValues(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(1, 8).Draw(t, "n")
		vals := make([]string, n)
		for i := range vals {
			vals[i] = rapid.OneOf(
				rapid.StringMatching(`^[a-zA-Z_][a-zA-Z0-9_]{0,10}$`),
				rapid.StringMatching(`^-?[0-9]{1,6}$`),
				rapid.StringMatching(`^'[a-zA-Z0-9]{1,10}'$`),
			).Draw(t, "val")
		}
		input := strings.Join(vals, ", ")

		result := quoteINValues(input)

		// Balanced quotes
		if strings.Count(result, "'")%2 != 0 {
			t.Fatalf("quoteINValues(%q) has odd quotes: %q", input, result)
		}

		// Value count preserved: split result and verify same number of parts
		resultParts := splitRespectingQuotes(result)
		if len(resultParts) != n {
			t.Fatalf("quoteINValues(%q) has %d parts, want %d: %q", input, len(resultParts), n, result)
		}

		// Each part must be either a number or a quoted string
		for _, p := range resultParts {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			isQuoted := len(p) >= 2 && p[0] == '\'' && p[len(p)-1] == '\''
			isNumeric := true
			for j, c := range p {
				if (c == '-' || c == '+') && j == 0 {
					continue
				}
				if c == '.' {
					continue
				}
				if c < '0' || c > '9' {
					isNumeric = false
					break
				}
			}
			if !isQuoted && !isNumeric {
				t.Fatalf("quoteINValues part %q is neither quoted nor numeric (from input %q)", p, input)
			}
		}
	})
}
