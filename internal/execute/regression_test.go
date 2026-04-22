// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"context"
	"strings"
	"testing"
	"time"

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

func TestDuckDBToJSONSchemaType_Mapping(t *testing.T) {
	t.Parallel()
	tests := []struct {
		duckdb, want string
	}{
		{"DECIMAL", "number"},
		{"DOUBLE", "number"},
		{"FLOAT", "number"},
		{"INTEGER", "integer"},
		{"BIGINT", "integer"},
		{"BOOLEAN", "boolean"},
	}
	for _, tt := range tests {
		got, ok := duckDBToJSONSchemaType[tt.duckdb]
		if !ok || got != tt.want {
			t.Errorf("%s: got %q, want %q", tt.duckdb, got, tt.want)
		}
	}

	// JSON maps to "array" (used by dynamic_columns for ::JSON casts)
	if got, ok := duckDBToJSONSchemaType["JSON"]; !ok || got != "array" {
		t.Errorf("JSON: got %q, want array", got)
	}

	// Types not in the map should default to "string"
	for _, missing := range []string{"VARCHAR", "TIMESTAMP", "DATE"} {
		if _, ok := duckDBToJSONSchemaType[missing]; ok {
			t.Errorf("%s should not be in map", missing)
		}
	}
}
