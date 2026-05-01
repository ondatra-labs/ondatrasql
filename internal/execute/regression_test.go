// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/collect"
	"github.com/ondatra-labs/ondatrasql/internal/libregistry"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
)

// --- newRateLimiter: zero/negative requests ---

func TestNewRateLimiter_ZeroRequests(t *testing.T) {
	t.Parallel()
	// Before fix: requests=0 caused Wait() to spin forever (infinite CPU).
	_, err := newRateLimiter(0, "10s")
	if err == nil {
		t.Fatal("expected error for requests=0, got nil")
	}
}

func TestNewRateLimiter_NegativeRequests(t *testing.T) {
	t.Parallel()
	_, err := newRateLimiter(-5, "10s")
	if err == nil {
		t.Fatal("expected error for negative requests, got nil")
	}
}

func TestNewRateLimiter_ZeroDuration(t *testing.T) {
	t.Parallel()
	_, err := newRateLimiter(10, "0s")
	if err == nil {
		t.Fatal("expected error for zero duration, got nil")
	}
}

func TestNewRateLimiter_ValidConfig(t *testing.T) {
	t.Parallel()
	rl, err := newRateLimiter(100, "10s")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should be able to get at least one token
	if err := rl.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

// --- DAG runner: downstream skip on upstream failure ---

func TestHasFailedUpstream_DirectDep(t *testing.T) {
	t.Parallel()
	dependents := map[string][]string{
		"raw.source": {"staging.derived"},
	}
	failed := map[string]bool{
		"raw.source": true,
	}
	model := &parser.Model{Target: "staging.derived"}

	if !hasFailedUpstream(model, dependents, failed) {
		t.Error("staging.derived should detect failed upstream raw.source")
	}
}

func TestHasFailedUpstream_NoDep(t *testing.T) {
	t.Parallel()
	dependents := map[string][]string{
		"raw.source": {"staging.derived"},
	}
	failed := map[string]bool{
		"raw.source": true,
	}
	model := &parser.Model{Target: "staging.unrelated"}

	if hasFailedUpstream(model, dependents, failed) {
		t.Error("staging.unrelated should not be affected by raw.source failure")
	}
}

func TestHasFailedUpstream_NoFailures(t *testing.T) {
	t.Parallel()
	dependents := map[string][]string{
		"raw.source": {"staging.derived"},
	}
	failed := map[string]bool{}
	model := &parser.Model{Target: "staging.derived"}

	if hasFailedUpstream(model, dependents, failed) {
		t.Error("no failures means no upstream failure")
	}
}

func TestHasFailedUpstream_TransitiveFail(t *testing.T) {
	t.Parallel()
	// A → B → C, A fails, B gets marked failed, C should detect it
	dependents := map[string][]string{
		"a": {"b"},
		"b": {"c"},
	}
	failed := map[string]bool{
		"a": true,
		"b": true, // marked failed because a failed
	}
	model := &parser.Model{Target: "c"}

	if !hasFailedUpstream(model, dependents, failed) {
		t.Error("c should detect transitive upstream failure")
	}
}

// --- detectLibCallsFromSQL: multiple occurrences ---

func TestDetectLibCallsFromSQL_MultipleOccurrences(t *testing.T) {
	t.Parallel()
	// Before fix: only one LibCall per function name, even if
	// the function appears multiple times in the SQL.
	reg := libregistry.NewRegistryForTest(map[string]*libregistry.LibFunc{
		"my_fetch": {Name: "my_fetch", FuncName: "fetch"},
	})

	sql := "SELECT * FROM my_fetch('a') UNION ALL SELECT * FROM my_fetch('b')"
	calls := detectLibCallsFromSQL(sql, reg)

	if len(calls) != 2 {
		t.Fatalf("expected 2 lib calls, got %d", len(calls))
	}
	if calls[0].FuncName != "my_fetch" || calls[1].FuncName != "my_fetch" {
		t.Errorf("both calls should be my_fetch, got %v", calls)
	}
	if calls[0].CallIndex == calls[1].CallIndex {
		t.Error("calls should have different CallIndex values")
	}
}

func TestDetectLibCallsFromSQL_SingleOccurrence(t *testing.T) {
	t.Parallel()
	reg := libregistry.NewRegistryForTest(map[string]*libregistry.LibFunc{
		"api_fetch": {Name: "api_fetch", FuncName: "fetch"},
	})

	sql := "SELECT * FROM api_fetch('key')"
	calls := detectLibCallsFromSQL(sql, reg)

	if len(calls) != 1 {
		t.Fatalf("expected 1 lib call, got %d", len(calls))
	}
}

func TestDetectLibCallsFromSQL_NoMatch(t *testing.T) {
	t.Parallel()
	reg := libregistry.NewRegistryForTest(map[string]*libregistry.LibFunc{
		"my_fetch": {Name: "my_fetch", FuncName: "fetch"},
	})

	sql := "SELECT * FROM orders"
	calls := detectLibCallsFromSQL(sql, reg)

	if len(calls) != 0 {
		t.Fatalf("expected 0 lib calls, got %d", len(calls))
	}
}

// --- stripStringsAndComments ---

func TestStripStringsAndComments(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		check func(string) bool
		desc  string
	}{
		{
			"string literal removed",
			"SELECT * FROM 'my_fetch(x)' WHERE 1=1",
			func(s string) bool { return !strings.Contains(strings.ToLower(s), "my_fetch") },
			"function name inside string should be blanked",
		},
		{
			"line comment removed",
			"SELECT * FROM t -- my_fetch(x)",
			func(s string) bool { return !strings.Contains(strings.ToLower(s), "my_fetch") },
			"function name inside comment should be blanked",
		},
		{
			"block comment removed",
			"SELECT /* my_fetch(x) */ * FROM t",
			func(s string) bool { return !strings.Contains(strings.ToLower(s), "my_fetch") },
			"function name inside block comment should be blanked",
		},
		{
			"real call preserved",
			"SELECT * FROM my_fetch('key')",
			func(s string) bool { return strings.Contains(strings.ToLower(s), "my_fetch(") },
			"real function call should be preserved",
		},
		{
			"escaped quote in string",
			"SELECT * FROM t WHERE name = 'it''s my_fetch(x)'",
			func(s string) bool { return !strings.Contains(strings.ToLower(s), "my_fetch") },
			"function inside string with escaped quote should be blanked",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := stripStringsAndComments(tt.input)
			if !tt.check(result) {
				t.Errorf("%s\n  input:  %q\n  result: %q", tt.desc, tt.input, result)
			}
		})
	}
}

func TestDetectLibCallsFromSQL_IgnoresStringLiterals(t *testing.T) {
	t.Parallel()
	reg := libregistry.NewRegistryForTest(map[string]*libregistry.LibFunc{
		"my_fetch": {Name: "my_fetch", FuncName: "fetch"},
	})

	// Function name inside string literal — should NOT be detected
	sql := "SELECT * FROM t WHERE label = 'call my_fetch(args)'"
	calls := detectLibCallsFromSQL(sql, reg)
	if len(calls) != 0 {
		t.Errorf("should not detect lib call inside string literal, got %d", len(calls))
	}
}

func TestDetectLibCallsFromSQL_IgnoresComments(t *testing.T) {
	t.Parallel()
	reg := libregistry.NewRegistryForTest(map[string]*libregistry.LibFunc{
		"my_fetch": {Name: "my_fetch", FuncName: "fetch"},
	})

	sql := "SELECT * FROM t -- my_fetch(args)"
	calls := detectLibCallsFromSQL(sql, reg)
	if len(calls) != 0 {
		t.Errorf("should not detect lib call inside comment, got %d", len(calls))
	}
}

// --- extractArgsFromSQL: multiple occurrences get correct args ---

func TestExtractArgsFromSQL_SecondOccurrence(t *testing.T) {
	t.Parallel()
	// Before fix: both calls got args from the first occurrence.
	r := &Runner{}
	lib := &libregistry.LibFunc{Name: "api_fetch", Args: []string{"key"}}

	sql := "SELECT * FROM api_fetch('first') UNION ALL SELECT * FROM api_fetch('second')"

	args0 := r.extractArgsFromSQL(sql, lib, 0)
	args1 := r.extractArgsFromSQL(sql, lib, 1)

	if args0["key"] != "first" {
		t.Errorf("occurrence 0: key = %q, want 'first'", args0["key"])
	}
	if args1["key"] != "second" {
		t.Errorf("occurrence 1: key = %q, want 'second'", args1["key"])
	}
}

func TestExtractArgsFromSQL_SkipsStringLiterals(t *testing.T) {
	t.Parallel()
	r := &Runner{}
	lib := &libregistry.LibFunc{Name: "my_func", Args: []string{"val"}}

	// Function name inside string should not be matched
	sql := "SELECT * FROM t WHERE label = 'my_func(fake)' AND x = my_func('real')"

	args := r.extractArgsFromSQL(sql, lib, 0)
	if args["val"] != "real" {
		t.Errorf("should extract from real call, got %q", args["val"])
	}
}

// Regression: parentheses inside string arguments must not break arg extraction.
func TestExtractArgsFromSQL_ParensInStringArg(t *testing.T) {
	t.Parallel()
	r := &Runner{}
	lib := &libregistry.LibFunc{Name: "my_api", Args: []string{"query"}}

	// The argument contains parentheses inside a string literal
	sql := "SELECT * FROM my_api('SELECT count(*) FROM t WHERE x IN (1,2)')"

	args := r.extractArgsFromSQL(sql, lib, 0)
	want := "SELECT count(*) FROM t WHERE x IN (1,2)"
	if args["query"] != want {
		t.Errorf("got %q, want %q", args["query"], want)
	}
}

// Regression: closing paren inside string literal must not end the match.
func TestExtractArgsFromSQL_ClosingParenInString(t *testing.T) {
	t.Parallel()
	r := &Runner{}
	lib := &libregistry.LibFunc{Name: "my_api", Args: []string{"val"}}

	sql := "SELECT * FROM my_api('x)')"

	args := r.extractArgsFromSQL(sql, lib, 0)
	if args["val"] != "x)" {
		t.Errorf("got %q, want %q", args["val"], "x)")
	}
}

// --- splitArgsRespectingNesting ---

func TestSplitArgsRespectingNesting(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  int
	}{
		{"simple", "'arg1', 'arg2'", 2},
		{"comma in string", "'a,b', 'c'", 2},
		{"nested parens", "json('{\"x\":1,\"y\":2}'), 'key'", 2},
		{"nested function", "concat('a', 'b'), 'c'", 2},
		{"empty", "", 0},
		{"single arg", "'only'", 1},
		{"escaped quote", "'it''s', 'fine'", 2},
		{"no quotes", "42, 100", 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parts := splitArgsRespectingNesting(tt.input)
			if len(parts) != tt.want {
				t.Errorf("splitArgsRespectingNesting(%q) = %d parts %v, want %d",
					tt.input, len(parts), parts, tt.want)
			}
		})
	}
}

// --- ratelimit.go: refill fractional intervals ---

func TestRateLimiter_Refill_PreservesFraction(t *testing.T) {
	t.Parallel()
	// Before fix: refill set lastFill=now, eating fractional intervals.
	// After fix: lastFill advances by whole intervals.
	rl, _ := newRateLimiter(1, "1s")

	// Drain the token
	rl.Wait(context.Background())

	// Simulate time passing: 2.5 intervals elapsed
	rl.mu.Lock()
	rl.lastFill = rl.lastFill.Add(-2500 * time.Millisecond)
	rl.mu.Unlock()

	// Refill should advance by 2 intervals, not jump to now
	rl.mu.Lock()
	rl.refill()
	elapsed := time.Since(rl.lastFill)
	rl.mu.Unlock()

	// lastFill should be ~500ms ago (the fractional part), not ~0ms
	if elapsed < 400*time.Millisecond {
		t.Errorf("lastFill advanced too far (jumped to now), elapsed=%v", elapsed)
	}
}

// --- splitArgsRespectingNesting (already tested above) ---

// --- splitSchemaTable ---

func TestSplitSchemaTable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input      string
		wantSchema string
		wantTable  string
	}{
		{"raw.orders", "raw", "orders"},
		{"orders", "", "orders"},
		{"staging.my_table", "staging", "my_table"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			s, tbl := splitSchemaTable(tt.input)
			if s != tt.wantSchema || tbl != tt.wantTable {
				t.Errorf("splitSchemaTable(%q) = (%q, %q), want (%q, %q)",
					tt.input, s, tbl, tt.wantSchema, tt.wantTable)
			}
		})
	}
}

// --- quoteTarget ---

func TestQuoteTarget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{"raw.orders", `"raw"."orders"`},
		{"orders", `"orders"`},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := quoteTarget(tt.input)
			if got != tt.want {
				t.Errorf("quoteTarget(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// --- rewriteLibCallsByString regressions ---

// Regression: string-based rewrite must search stripped SQL (no strings/comments)
// to avoid matching function names inside string literals.
func TestRewriteLibCallsByString_IgnoresStringLiterals(t *testing.T) {
	t.Parallel()

	sql := "SELECT * FROM t WHERE label = 'my_api(fake)' AND x IN (SELECT * FROM my_api('real'))"
	calls := []LibCall{
		{FuncName: "my_api", TempTable: "tmp_my_api_1"},
	}

	result := rewriteLibCallsByString(sql, calls)

	// The string literal 'my_api(fake)' must be preserved
	if !strings.Contains(result, "'my_api(fake)'") {
		t.Errorf("string literal was incorrectly rewritten: %s", result)
	}
	// The real call must be replaced
	if !strings.Contains(result, "tmp_my_api_1") {
		t.Errorf("real call not rewritten: %s", result)
	}
}

// Regression: string-based rewrite must ignore function names inside comments.
func TestRewriteLibCallsByString_IgnoresComments(t *testing.T) {
	t.Parallel()

	sql := "SELECT * FROM my_api('users') -- my_api('commented')"
	calls := []LibCall{
		{FuncName: "my_api", TempTable: "tmp_my_api_1"},
	}

	result := rewriteLibCallsByString(sql, calls)

	if !strings.Contains(result, "tmp_my_api_1") {
		t.Errorf("real call not rewritten: %s", result)
	}
	// Only one rewrite should happen (the real call, not the comment)
	if strings.Count(result, "tmp_my_api_1") != 1 {
		t.Errorf("comment call was also rewritten: %s", result)
	}
}

// Regression: multiple occurrences of the same function get correct temp tables.
func TestRewriteLibCallsByString_MultipleOccurrences(t *testing.T) {
	t.Parallel()

	sql := "SELECT * FROM my_api('first') UNION ALL SELECT * FROM my_api('second')"
	calls := []LibCall{
		{FuncName: "my_api", TempTable: "tmp_my_api_1"},
		{FuncName: "my_api", TempTable: "tmp_my_api_2"},
	}

	result := rewriteLibCallsByString(sql, calls)

	if !strings.Contains(result, "tmp_my_api_1") {
		t.Errorf("first occurrence not rewritten: %s", result)
	}
	if !strings.Contains(result, "tmp_my_api_2") {
		t.Errorf("second occurrence not rewritten: %s", result)
	}
	// Original function calls should be gone
	if strings.Contains(result, "my_api(") {
		t.Errorf("unrewritten function call remains: %s", result)
	}
}

// Regression: parentheses inside string literals don't break paren matching.
func TestRewriteLibCallsByString_ParensInStringLiteral(t *testing.T) {
	t.Parallel()

	sql := "SELECT * FROM my_api('x)')"
	calls := []LibCall{
		{FuncName: "my_api", TempTable: "tmp_my_api_1"},
	}

	result := rewriteLibCallsByString(sql, calls)

	if !strings.Contains(result, "tmp_my_api_1") {
		t.Errorf("call not rewritten: %s", result)
	}
	if strings.Contains(result, "my_api(") {
		t.Errorf("original call remains: %s", result)
	}
}

// Regression: double-quoted identifiers with parens don't break matching.
func TestRewriteLibCallsByString_DoubleQuotedIdentifier(t *testing.T) {
	t.Parallel()

	sql := `SELECT * FROM my_api('arg', "col(name)")`
	calls := []LibCall{
		{FuncName: "my_api", TempTable: "tmp_my_api_1"},
	}

	result := rewriteLibCallsByString(sql, calls)

	if !strings.Contains(result, "tmp_my_api_1") {
		t.Errorf("call not rewritten: %s", result)
	}
	if strings.Contains(result, "my_api(") {
		t.Errorf("original call remains: %s", result)
	}
}

// Regression: line comments with parens don't break matching.
func TestRewriteLibCallsByString_LineComment(t *testing.T) {
	t.Parallel()

	sql := "SELECT * FROM my_api('arg' -- has paren )\n)"
	calls := []LibCall{
		{FuncName: "my_api", TempTable: "tmp_my_api_1"},
	}

	result := rewriteLibCallsByString(sql, calls)

	if !strings.Contains(result, "tmp_my_api_1") {
		t.Errorf("call not rewritten: %s", result)
	}
}

// Regression: block comments with parens don't break matching.
func TestRewriteLibCallsByString_BlockComment(t *testing.T) {
	t.Parallel()

	sql := "SELECT * FROM my_api('arg' /* ) */ )"
	calls := []LibCall{
		{FuncName: "my_api", TempTable: "tmp_my_api_1"},
	}

	result := rewriteLibCallsByString(sql, calls)

	if !strings.Contains(result, "tmp_my_api_1") {
		t.Errorf("call not rewritten: %s", result)
	}
}

// Regression: nested parentheses in function arguments are handled correctly.
func TestRewriteLibCallsByString_NestedParens(t *testing.T) {
	t.Parallel()

	sql := "SELECT * FROM my_api(json('{\"key\":\"val\"}'))"
	calls := []LibCall{
		{FuncName: "my_api", TempTable: "tmp_my_api_1"},
	}

	result := rewriteLibCallsByString(sql, calls)

	if !strings.Contains(result, "tmp_my_api_1") {
		t.Errorf("call not rewritten: %s", result)
	}
	if strings.Contains(result, "my_api(") {
		t.Errorf("original call remains: %s", result)
	}
}

// --- extractTypedSelectColumns tests ---

func TestExtractTypedSelectColumns_NilAST(t *testing.T) {
	t.Parallel()
	cols := extractTypedSelectColumns(nil)
	if cols != nil {
		t.Errorf("expected nil for nil AST, got %v", cols)
	}
}

func TestNormalizeType(t *testing.T) {
	t.Parallel()
	tests := []struct {
		duckdb   string
		wantType string
	}{
		{"DECIMAL", "decimal"},
		{"DECIMAL(18,2)", "decimal"},
		{"NUMERIC(10,4)", "decimal"},
		{"DOUBLE", "float"},
		{"FLOAT", "float"},
		{"REAL", "float"},
		{"INTEGER", "integer"},
		{"BIGINT", "integer"},
		{"INT8", "integer"},
		{"INT16", "integer"},
		{"INT32", "integer"},
		{"INT64", "integer"},
		{"INT128", "integer"},
		{"BOOLEAN", "boolean"},
		{"LOGICAL", "boolean"},
		{"JSON", "json"},
		{"VARCHAR", "string"},
		{"TEXT", "string"},
		{"DATE", "date"},
		{"TIME", "time"},
		{"TIMESTAMP", "timestamp"},
		{"TIMESTAMPTZ", "timestamp"},
		{"TIMESTAMP_NS", "timestamp"},
		{"UUID", "uuid"},
		{"BLOB", "blob"},
		{"LIST", "list"},
		{"MAP", "map"},
		{"STRUCT", "struct"},
		{"BIT", "bit"},
		{"UNKNOWN_TYPE", "string"},
	}
	for _, tt := range tests {
		result := normalizeType(tt.duckdb)
		got := result["type"].(string)
		if got != tt.wantType {
			t.Errorf("normalizeType(%q) type = %q, want %q", tt.duckdb, got, tt.wantType)
		}
	}
}

func TestNormalizeType_MalformedDecimal(t *testing.T) {
	t.Parallel()
	// Should not panic on malformed input
	result := normalizeType("DECIMAL(18,2")
	if result["type"] != "decimal" {
		t.Errorf("malformed DECIMAL: type = %v, want decimal", result["type"])
	}
	result = normalizeType("DECIMAL(")
	if result["type"] != "decimal" {
		t.Errorf("DECIMAL(: type = %v, want decimal", result["type"])
	}
}

func TestNormalizeType_DecimalPrecision(t *testing.T) {
	t.Parallel()
	result := normalizeType("DECIMAL(10,2)")
	if result["precision"] != "10" || result["scale"] != "2" {
		t.Errorf("DECIMAL(10,2): got precision=%v scale=%v", result["precision"], result["scale"])
	}

	// Bare DECIMAL defaults to 18,3
	result = normalizeType("DECIMAL")
	if result["precision"] != "18" || result["scale"] != "3" {
		t.Errorf("DECIMAL: got precision=%v scale=%v", result["precision"], result["scale"])
	}
}

func TestNormalizeType_Timestamp(t *testing.T) {
	t.Parallel()
	result := normalizeType("TIMESTAMPTZ")
	if result["tz"] != true {
		t.Errorf("TIMESTAMPTZ: tz = %v, want true", result["tz"])
	}
	if result["precision"] != "us" {
		t.Errorf("TIMESTAMPTZ: precision = %v, want us", result["precision"])
	}

	result = normalizeType("TIMESTAMP_NS")
	if result["tz"] != false {
		t.Errorf("TIMESTAMP_NS: tz = %v, want false", result["tz"])
	}
	if result["precision"] != "ns" {
		t.Errorf("TIMESTAMP_NS: precision = %v, want ns", result["precision"])
	}
}

// --- Bugcheck regression tests ---

// TestRewriteLibCallsByString_OccurrenceSkipEmpty verifies that when a lib-call
// has empty TempTable (0 rows), the occurrence counter is still incremented
// so subsequent calls rewrite the correct SQL occurrence.
func TestRewriteLibCallsByString_OccurrenceSkipEmpty(t *testing.T) {
	t.Parallel()

	// Two calls to same function: first has no data, second has data.
	// The second call should rewrite the SECOND occurrence, not the first.
	sql := "SELECT * FROM myapi('a') UNION ALL SELECT * FROM myapi('b')"
	calls := []LibCall{
		{FuncName: "myapi", CallIndex: 0, TempTable: ""},           // empty — skip rewrite
		{FuncName: "myapi", CallIndex: 1, TempTable: "tmp_myapi_1"}, // has data
	}

	result := rewriteLibCallsByString(sql, calls)

	// First occurrence should be untouched, second should be rewritten
	if !strings.Contains(result, "myapi('a')") {
		t.Errorf("first occurrence should be untouched, got: %s", result)
	}
	if !strings.Contains(result, "tmp_myapi_1") {
		t.Errorf("second occurrence should be rewritten to tmp_myapi_1, got: %s", result)
	}
	if strings.Count(result, "myapi(") != 1 {
		t.Errorf("expected exactly one unrewritten myapi( call, got: %s", result)
	}
}

// TestStripStringsAndComments_DollarQuoted verifies that dollar-quoted strings
// ($$...$$) are stripped, preventing false positive lib-call detection.
func TestStripStringsAndComments_DollarQuoted(t *testing.T) {
	t.Parallel()

	sql := "SELECT * FROM $$myapi('hello')$$ AS x"
	stripped := stripStringsAndComments(sql)

	if strings.Contains(stripped, "myapi") {
		t.Errorf("dollar-quoted content should be stripped, got: %s", stripped)
	}
	// The SELECT and AS x should survive
	if !strings.Contains(stripped, "SELECT") {
		t.Errorf("non-quoted SQL should survive, got: %s", stripped)
	}
}

// TestSanitizeTableName_SpecialChars verifies that sanitizeTableName replaces
// all non-alphanumeric characters (not just dots) with underscores.
func TestSanitizeTableName_SpecialChars(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{"raw.orders", "raw_orders"},
		{"schema.table", "schema_table"},
		{"has spaces", "has_spaces"},
		{"semi;colon", "semi_colon"},
		{"quote'inject", "quote_inject"},
		{"normal_name", "normal_name"},
		{"UPPER.Case", "UPPER_Case"},
	}
	for _, tt := range tests {
		got := sanitizeTableName(tt.input)
		if got != tt.want {
			t.Errorf("sanitizeTableName(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// TestLoadExtension_InvalidName verifies that extension names with SQL-unsafe
// characters are rejected before interpolation into INSTALL/LOAD statements.
func TestLoadExtension_InvalidName(t *testing.T) {
	t.Parallel()

	r := &Runner{}

	// These should fail validation BEFORE reaching the session
	invalidTests := []string{
		"bad;name",
		"bad'name",
		"bad name",
		"ext FROM bad;repo",
		"ext FROM --drop",
	}
	for _, ext := range invalidTests {
		err := r.loadExtension(ext)
		if err == nil {
			t.Errorf("loadExtension(%q): expected validation error, got nil", ext)
		} else if !strings.Contains(err.Error(), "invalid extension") {
			t.Errorf("loadExtension(%q): expected 'invalid extension' error, got: %v", ext, err)
		}
	}
}

// TestLoadExtension_BlockCommentInRepo verifies that /* block comment markers
// in extension repo names are rejected.
func TestLoadExtension_BlockCommentInRepo(t *testing.T) {
	t.Parallel()
	r := &Runner{}

	invalidRepos := []string{
		"ext FROM community /* ignored */ core_nightly",
		"ext FROM /* drop */",
		"ext FROM 'https://repo' LOAD unsafe",
		"ext FROM 'https://repo' ; DROP TABLE x",
	}
	for _, ext := range invalidRepos {
		err := r.loadExtension(ext)
		if err == nil {
			t.Errorf("loadExtension(%q): expected validation error for block comment, got nil", ext)
		}
	}
}

// TestLoadExtension_URLRepoAllowed verifies that URL-based extension repos
// (containing :, /, quotes) are NOT rejected by the validation.
func TestLoadExtension_URLRepoAllowed(t *testing.T) {
	t.Parallel()
	r := &Runner{}

	// These should pass validation (will panic on nil session after validation).
	// We recover the panic to verify validation itself passed.
	validRepos := []string{
		"ext FROM 'https://example.com/repo'",
		"ext FROM community",
		"ext FROM core_nightly",
	}
	for _, ext := range validRepos {
		func() {
			defer func() { recover() }() // recover nil session panic
			err := r.loadExtension(ext)
			if err != nil && strings.Contains(err.Error(), "invalid extension") {
				t.Errorf("loadExtension(%q): URL repo should pass validation, got: %v", ext, err)
			}
		}()
	}
}

// TestStripStringsAndComments_UnterminatedDollarQuote verifies that an
// unterminated $$ string blanks all remaining characters including the last byte.
func TestStripStringsAndComments_UnterminatedDollarQuote(t *testing.T) {
	t.Parallel()

	// Unterminated: $$ opens but never closes
	sql := "SELECT $$myapi('x')"
	stripped := stripStringsAndComments(sql)

	if strings.Contains(stripped, "myapi") {
		t.Errorf("unterminated dollar-quoted content should be stripped, got: %s", stripped)
	}
	// Last character should also be blanked
	if stripped[len(stripped)-1] != ' ' {
		t.Errorf("last character should be blanked in unterminated $$, got: %q", stripped)
	}
}

// TestStripStringsAndComments_TaggedDollarQuote verifies that tagged dollar-quoted
// strings ($tag$...$tag$) are stripped correctly.
func TestStripStringsAndComments_TaggedDollarQuote(t *testing.T) {
	t.Parallel()

	sql := "SELECT $foo$myapi('hello')$foo$ AS x"
	stripped := stripStringsAndComments(sql)

	if strings.Contains(stripped, "myapi") {
		t.Errorf("tagged dollar-quoted content should be stripped, got: %s", stripped)
	}
	if !strings.Contains(stripped, "SELECT") || !strings.Contains(stripped, "AS x") {
		t.Errorf("non-quoted SQL should survive, got: %s", stripped)
	}
}

// TestLoadExtension_NewlineInRepo verifies that newlines in extension repo
// are rejected (prevents newline-based SQL injection).
func TestLoadExtension_NewlineInRepo(t *testing.T) {
	t.Parallel()
	r := &Runner{}

	invalidRepos := []string{
		"ext FROM community\nLOAD 'malicious.so'",
		"ext FROM core\r\nDROP TABLE x",
	}
	for _, ext := range invalidRepos {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("loadExtension(%q): panicked instead of returning validation error: %v", ext, r)
				}
			}()
			err := r.loadExtension(ext)
			if err == nil {
				t.Errorf("loadExtension(%q): expected validation error for newline, got nil", ext)
			}
		}()
	}
}

// --- Cat 10 BLOCKER: sink delta early returns ---

// TestCreatePushDelta_NewSnapshotZero verifies that createPushDelta returns
// nil events (and no error) when newSnapshot == 0 — i.e. the post-commit
// snapshot capture failed or no commit happened. Without this guard, the
// table_changes() query would treat 0 as a real range start and replay
// the entire table on every skip run.
func TestCreatePushDelta_NewSnapshotZero(t *testing.T) {
	t.Parallel()

	r := &Runner{} // no session needed; early return must fire first
	model := &parser.Model{
		Target: "mart.orders",
		Push:   "lib/orders_push.star",
	}

	events, err := r.createPushDelta(model, "tmp_orders", 5, 0)
	if err != nil {
		t.Fatalf("expected no error on newSnapshot=0, got: %v", err)
	}
	if events != nil {
		t.Errorf("expected nil events on newSnapshot=0, got: %v", events)
	}
}

// TestCreatePushDelta_EmptyPush verifies the early return when the model
// has no @sink directive. Without this, every model would issue a
// table_changes() query whether it had a sink or not.
func TestCreatePushDelta_EmptyPush(t *testing.T) {
	t.Parallel()

	r := &Runner{} // no session needed; early return must fire first
	model := &parser.Model{
		Target: "mart.orders",
		Push:   "",
	}

	events, err := r.createPushDelta(model, "tmp_orders", 5, 10)
	if err != nil {
		t.Fatalf("expected no error on empty sink, got: %v", err)
	}
	if events != nil {
		t.Errorf("expected nil events on empty sink, got: %v", events)
	}
}

// TestMergeBacklogWithDelta_DedupOnCompositeKey verifies that
// mergeBacklogWithDelta dedups on (RowID, ChangeType) — not just RowID.
//
// The bug class: a row with both update_preimage and update_postimage in the
// backlog must keep both unless the delta explicitly supersedes one of them.
// Deduping on RowID alone would silently drop the postimage when the delta
// only contains the preimage (or vice versa), breaking the at-least-once
// delivery contract that the push() function depends on.
func TestMergeBacklogWithDelta_DedupOnCompositeKey(t *testing.T) {
	t.Parallel()

	store, err := collect.OpenSyncStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSyncStore: %v", err)
	}
	defer store.Close()

	target := "mart.orders"

	// Backlog: rowid=2 has BOTH preimage and postimage
	backlog := []collect.SyncEvent{
		{ChangeType: "insert", RowID: 1, Snapshot: 100},
		{ChangeType: "update_preimage", RowID: 2, Snapshot: 100},
		{ChangeType: "update_postimage", RowID: 2, Snapshot: 100},
		{ChangeType: "delete", RowID: 3, Snapshot: 100},
	}
	if err := store.WriteBatch(target, backlog); err != nil {
		t.Fatalf("WriteBatch backlog: %v", err)
	}

	// Delta: replaces (rowid=2, update_preimage) only — rowid=2's postimage
	// must remain. Also adds a new (rowid=4, insert).
	delta := []collect.SyncEvent{
		{ChangeType: "update_preimage", RowID: 2, Snapshot: 200},
		{ChangeType: "insert", RowID: 4, Snapshot: 200},
	}

	se := &pushExecutor{}
	merged, err := se.mergeBacklogWithDelta(store, target, delta)
	if err != nil {
		t.Fatalf("mergeBacklogWithDelta: %v", err)
	}

	// Build (rowid, change_type) → snapshot map for assertions
	got := map[string]int64{}
	for _, e := range merged {
		key := fmt.Sprintf("%d:%s", e.RowID, e.ChangeType)
		got[key] = e.Snapshot
	}

	// Expected: 5 distinct (rowid, change_type) pairs
	want := map[string]int64{
		"1:insert":           100, // backlog kept (no delta match)
		"2:update_preimage":  200, // delta replaced backlog (snapshot 200 > 100)
		"2:update_postimage": 100, // BACKLOG KEPT — only preimage was in delta
		"3:delete":           100, // backlog kept
		"4:insert":           200, // delta added
	}

	if len(got) != len(want) {
		t.Fatalf("merged size = %d, want %d. got=%v want=%v", len(got), len(want), got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("key %q: got snapshot=%d, want %d (full result: %v)", k, got[k], v, got)
		}
	}

	// Critical assertion: rowid=2's postimage is preserved.
	// If dedup were on RowID alone, it would be removed because (rowid=2,
	// update_preimage) appears in delta.
	if _, ok := got["2:update_postimage"]; !ok {
		t.Errorf("rowid=2 update_postimage was dropped — dedup is on RowID alone, not (RowID, ChangeType) composite")
	}
}

// TestClassifyPerRowStatus_RequiresCompositeKey pins the per-row push contract:
// push() must return status keys in "rowid:change_type" format. The same RowID
// may appear with multiple change_types (update_preimage + update_postimage)
// and each needs its own status. If push() returns RowID-only keys, the
// classifier must reject the response — silently keying on RowID would
// collapse the two statuses and break at-least-once delivery.
func TestClassifyPerRowStatus_RequiresCompositeKey(t *testing.T) {
	t.Parallel()

	se := &pushExecutor{}
	rows := []map[string]any{
		{"__ondatra_rowid": int64(1), "__ondatra_change_type": "insert"},
		{"__ondatra_rowid": int64(2), "__ondatra_change_type": "update_preimage"},
		{"__ondatra_rowid": int64(2), "__ondatra_change_type": "update_postimage"},
	}
	events := []collect.SyncEvent{
		{RowID: 1, ChangeType: "insert"},
		{RowID: 2, ChangeType: "update_preimage"},
		{RowID: 2, ChangeType: "update_postimage"},
	}

	t.Run("rowid-only keys are rejected", func(t *testing.T) {
		// Bug shape: push() returned status keyed only by rowid.
		// rowid=2 has BOTH preimage and postimage but only one entry.
		bad := map[string]string{
			"1": "ok",
			"2": "ok",
		}
		_, err := se.classifyPerRowStatus(bad, rows, events)
		if err == nil {
			t.Fatal("expected error for rowid-only status keys")
		}
		if !strings.Contains(err.Error(), "missing keys") {
			t.Errorf("expected 'missing keys' error, got: %v", err)
		}
	})

	t.Run("composite keys classify both preimage and postimage", func(t *testing.T) {
		good := map[string]string{
			"1:insert":           "ok",
			"2:update_preimage":  "ok",
			"2:update_postimage": "reject:business rule violation",
		}
		res, err := se.classifyPerRowStatus(good, rows, events)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		// rowid=1 + rowid=2 preimage → OK; rowid=2 postimage → Rejected
		if len(res.OK) != 2 {
			t.Errorf("OK = %d events, want 2", len(res.OK))
		}
		if len(res.Rejected) != 1 {
			t.Errorf("Rejected = %d events, want 1", len(res.Rejected))
		}
		if len(res.Rejected) == 1 && res.Rejected[0].ChangeType != "update_postimage" {
			t.Errorf("rejected event = %s, want update_postimage", res.Rejected[0].ChangeType)
		}
	})

	t.Run("missing one of two same-rowid keys is rejected", func(t *testing.T) {
		// Bug shape: push() returned only the postimage status, missing preimage
		partial := map[string]string{
			"1:insert":           "ok",
			"2:update_postimage": "ok",
			// "2:update_preimage" missing
		}
		_, err := se.classifyPerRowStatus(partial, rows, events)
		if err == nil {
			t.Fatal("expected error for missing preimage key")
		}
		if !strings.Contains(err.Error(), "2:update_preimage") {
			t.Errorf("expected error to name the missing key, got: %v", err)
		}
	})
}

// TestQueueDelta_PathSelection pins the four-way branch in queueDelta. The
// bug class: using ClearAllAndWrite where WriteBatch is required (destroys
// an active worker's claims) or using WriteBatch where ClearAllAndWrite is
// required (leaves stale backlog after a backfill). Each path is tested by
// pre-populating the store with non-overlapping events and verifying which
// of those survive the call.
func TestQueueDelta_PathSelection(t *testing.T) {
	t.Parallel()

	// Helper: open a fresh store, populate with two existing events, and
	// return the store. Caller closes.
	openWithBacklog := func(t *testing.T) (*collect.SyncStore, string) {
		t.Helper()
		store, err := collect.OpenSyncStore(t.TempDir())
		if err != nil {
			t.Fatalf("OpenSyncStore: %v", err)
		}
		target := "mart.orders"
		// Existing backlog: non-overlapping with the delta below
		err = store.WriteBatch(target, []collect.SyncEvent{
			{ChangeType: "insert", RowID: 10, Snapshot: 100},
			{ChangeType: "insert", RowID: 11, Snapshot: 100},
		})
		if err != nil {
			t.Fatalf("seed WriteBatch: %v", err)
		}
		return store, target
	}

	// Helper: collect (RowID, ChangeType) → bool for a target.
	keysOf := func(t *testing.T, store *collect.SyncStore, target string) map[string]bool {
		t.Helper()
		all, err := store.ReadAllEvents(target)
		if err != nil {
			t.Fatalf("ReadAllEvents: %v", err)
		}
		out := map[string]bool{}
		for _, e := range all {
			out[fmt.Sprintf("%d:%s", e.RowID, e.ChangeType)] = true
		}
		return out
	}

	delta := []collect.SyncEvent{
		{ChangeType: "insert", RowID: 20, Snapshot: 200},
		{ChangeType: "insert", RowID: 21, Snapshot: 200},
	}

	t.Run("async path uses WriteBatch (preserves backlog + job_ref)", func(t *testing.T) {
		store, target := openWithBacklog(t)
		defer store.Close()

		se := &pushExecutor{
			sinkEvents: delta,
			result:     &Result{RunType: "incremental"},
		}
		if err := se.queueDelta(store, target, "async", false); err != nil {
			t.Fatalf("queueDelta: %v", err)
		}
		got := keysOf(t, store, target)
		// Async = WriteBatch — backlog preserved, delta added → 4 keys
		want := []string{"10:insert", "11:insert", "20:insert", "21:insert"}
		for _, k := range want {
			if !got[k] {
				t.Errorf("async path lost key %q (got %v) — async must preserve backlog via WriteBatch", k, got)
			}
		}
	})

	t.Run("backfill path uses ClearAllAndWrite (replaces backlog)", func(t *testing.T) {
		store, target := openWithBacklog(t)
		defer store.Close()

		se := &pushExecutor{
			sinkEvents: delta,
			result:     &Result{RunType: "backfill"},
		}
		if err := se.queueDelta(store, target, "per_row", false); err != nil {
			t.Fatalf("queueDelta: %v", err)
		}
		got := keysOf(t, store, target)
		// Backfill = ClearAllAndWrite — backlog gone, only delta
		if got["10:insert"] || got["11:insert"] {
			t.Errorf("backfill path kept stale backlog (got %v) — backfill must use ClearAllAndWrite", got)
		}
		if !got["20:insert"] || !got["21:insert"] {
			t.Errorf("backfill path missing delta events (got %v)", got)
		}
	})

	t.Run("incremental + inflight uses WriteBatch (preserves active claims)", func(t *testing.T) {
		store, target := openWithBacklog(t)
		defer store.Close()

		se := &pushExecutor{
			sinkEvents: delta,
			result:     &Result{RunType: "incremental"},
		}
		// hasInflight=true means a worker owns the existing claims.
		// Using ClearAllAndWrite here would destroy them mid-push.
		if err := se.queueDelta(store, target, "per_row", true); err != nil {
			t.Fatalf("queueDelta: %v", err)
		}
		got := keysOf(t, store, target)
		want := []string{"10:insert", "11:insert", "20:insert", "21:insert"}
		for _, k := range want {
			if !got[k] {
				t.Errorf("incremental+inflight lost key %q (got %v) — must preserve via WriteBatch", k, got)
			}
		}
	})

	t.Run("incremental + no inflight merges backlog and replaces", func(t *testing.T) {
		store, target := openWithBacklog(t)
		defer store.Close()

		se := &pushExecutor{
			sinkEvents: delta,
			result:     &Result{RunType: "incremental"},
		}
		if err := se.queueDelta(store, target, "per_row", false); err != nil {
			t.Fatalf("queueDelta: %v", err)
		}
		got := keysOf(t, store, target)
		// Merge: backlog (10, 11) + delta (20, 21), no overlap → 4 keys
		want := []string{"10:insert", "11:insert", "20:insert", "21:insert"}
		for _, k := range want {
			if !got[k] {
				t.Errorf("incremental no-inflight lost key %q (got %v) — merge must keep non-overlapping backlog", k, got)
			}
		}
	})
}

// TestClassifyPerRowStatus_ExpiredSnapshotDeleteIsRejected pins the contract
// that a delete-event whose row cannot be read from DuckLake (because the
// snapshot expired) is classified as **Rejected** (dead-letter), not Failed.
// Putting it in Failed would cause infinite retries on something that can
// never succeed, so this distinction is load-bearing for the at-least-once
// semantics of the sink pipeline.
func TestClassifyPerRowStatus_ExpiredSnapshotDeleteIsRejected(t *testing.T) {
	t.Parallel()

	se := &pushExecutor{}

	// Events include a delete that has no corresponding row read from the lake.
	events := []collect.SyncEvent{
		{RowID: 1, ChangeType: "insert"},
		{RowID: 99, ChangeType: "delete"}, // row data unavailable (expired snapshot)
	}
	// rows only contains the insert — the delete row was not read.
	rows := []map[string]any{
		{"__ondatra_rowid": int64(1), "__ondatra_change_type": "insert"},
	}
	perRow := map[string]string{
		"1:insert": "ok",
		// no entry for rowid=99 — the row wasn't in `rows` so push() never saw it
	}

	res, err := se.classifyPerRowStatus(perRow, rows, events)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(res.OK) != 1 {
		t.Errorf("OK = %d events, want 1 (the insert)", len(res.OK))
	}
	if len(res.Rejected) != 1 {
		t.Fatalf("Rejected = %d events, want 1 (the delete)", len(res.Rejected))
	}
	if res.Rejected[0].RowID != 99 {
		t.Errorf("rejected RowID = %d, want 99", res.Rejected[0].RowID)
	}
	if len(res.Failed) != 0 {
		t.Errorf("Failed should be empty (would cause infinite retry), got %d events: %v", len(res.Failed), res.Failed)
	}
	// Reject message should hint at expiry so operators can investigate.
	if len(res.RejectMsgs) != 1 || !strings.Contains(res.RejectMsgs[0], "snapshot may have expired") {
		t.Errorf("expected reject message about expired snapshot, got: %v", res.RejectMsgs)
	}
}
