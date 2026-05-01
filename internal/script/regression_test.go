// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.starlark.net/starlark"
)

// --- injectAPIAuth regression tests ---

func TestInjectAPIAuth_BearerToken(t *testing.T) {
	t.Setenv("TEST_KEY_BEARER", "my-secret")
	headers := make(map[string]string)
	urlStr := "https://api.example.com/data"
	auth := map[string]any{"env": "TEST_KEY_BEARER"}

	_ = injectAPIAuth(auth, headers, &urlStr, &apiHTTPConfig{})

	if headers["Authorization"] != "Bearer my-secret" {
		t.Errorf("Authorization = %q, want %q", headers["Authorization"], "Bearer my-secret")
	}
}

func TestInjectAPIAuth_CustomHeader(t *testing.T) {
	t.Setenv("TEST_KEY_HEADER", "key-123")
	headers := make(map[string]string)
	urlStr := "https://api.example.com/data"
	auth := map[string]any{"env": "TEST_KEY_HEADER", "header": "X-Api-Key"}

	_ = injectAPIAuth(auth, headers, &urlStr, &apiHTTPConfig{})

	if headers["X-Api-Key"] != "key-123" {
		t.Errorf("X-Api-Key = %q, want %q", headers["X-Api-Key"], "key-123")
	}
	if _, ok := headers["Authorization"]; ok {
		t.Error("Authorization should not be set for custom header auth")
	}
}

func TestInjectAPIAuth_QueryParam(t *testing.T) {
	t.Setenv("TEST_KEY_PARAM", "qp-secret")
	headers := make(map[string]string)
	urlStr := "https://api.example.com/data?existing=1"
	auth := map[string]any{"env": "TEST_KEY_PARAM", "param": "api_key"}

	_ = injectAPIAuth(auth, headers, &urlStr, &apiHTTPConfig{})

	if !strings.Contains(urlStr, "api_key=qp-secret") {
		t.Errorf("URL should contain api_key param, got %q", urlStr)
	}
	if !strings.Contains(urlStr, "existing=1") {
		t.Errorf("URL should preserve existing params, got %q", urlStr)
	}
}

func TestInjectAPIAuth_BasicAuth(t *testing.T) {
	t.Setenv("TEST_USER_BA", "admin")
	t.Setenv("TEST_PASS_BA", "s3cret")
	headers := make(map[string]string)
	urlStr := "https://api.example.com/data"
	auth := map[string]any{
		"user": map[string]any{"env": "TEST_USER_BA"},
		"pass": map[string]any{"env": "TEST_PASS_BA"},
	}

	_ = injectAPIAuth(auth, headers, &urlStr, &apiHTTPConfig{})

	want := BasicAuth("admin", "s3cret")
	if headers["Authorization"] != want {
		t.Errorf("Authorization = %q, want %q", headers["Authorization"], want)
	}
}

func TestInjectAPIAuth_MissingEnvVar(t *testing.T) {
	os.Unsetenv("NONEXISTENT_KEY_12345")
	headers := make(map[string]string)
	urlStr := "https://api.example.com/data"
	auth := map[string]any{"env": "NONEXISTENT_KEY_12345"}

	_ = injectAPIAuth(auth, headers, &urlStr, &apiHTTPConfig{})

	if _, ok := headers["Authorization"]; ok {
		t.Error("Authorization should not be set when env var is missing")
	}
}

func TestInjectAPIAuth_EmptyAuth(t *testing.T) {
	t.Parallel()
	headers := make(map[string]string)
	urlStr := "https://api.example.com/data"
	auth := map[string]any{}

	_ = injectAPIAuth(auth, headers, &urlStr, &apiHTTPConfig{})

	if len(headers) != 0 {
		t.Errorf("no headers should be set for empty auth, got %v", headers)
	}
}

func TestResolveEnvValue(t *testing.T) {
	t.Setenv("RESOLVE_TEST_VAR", "resolved_value")

	// Plain string passthrough
	if v := resolveEnvValue("literal"); v != "literal" {
		t.Errorf("plain string: got %q, want %q", v, "literal")
	}
	// Nested env lookup
	if v := resolveEnvValue(map[string]any{"env": "RESOLVE_TEST_VAR"}); v != "resolved_value" {
		t.Errorf("env lookup: got %q, want %q", v, "resolved_value")
	}
	// Missing env
	if v := resolveEnvValue(map[string]any{"env": "NONEXISTENT_VAR_XYZ"}); v != "" {
		t.Errorf("missing env: got %q, want empty", v)
	}
	// Wrong type
	if v := resolveEnvValue(42); v != "" {
		t.Errorf("int: got %q, want empty", v)
	}
}

func TestInjectAPIAuth_ServiceAccount_EmptyResolve(t *testing.T) {
	t.Parallel()
	headers := make(map[string]string)
	urlStr := "https://api.example.com"
	auth := map[string]any{
		"service_account": map[string]any{"env": "NONEXISTENT_SA_PATH"},
	}

	err := injectAPIAuth(auth, headers, &urlStr, &apiHTTPConfig{})
	if err == nil {
		t.Fatal("expected error when service_account env resolves to empty")
	}
}

// TestInjectAPIAuth_ServiceAccount_ReadFileFailure pins that os.ReadFile
// failures (file path resolves but doesn't exist on disk) are propagated
// as errors, not silently dropped. The bug class: a future change adds a
// fallback "if read fails, just skip auth" branch and OAuth-protected
// API calls go out unauthenticated.
func TestInjectAPIAuth_ServiceAccount_ReadFileFailure(t *testing.T) {
	t.Setenv("SA_PATH_NOT_EXIST", "/this/path/does/not/exist/sa.json")
	headers := make(map[string]string)
	urlStr := "https://api.example.com"
	auth := map[string]any{
		"service_account": map[string]any{"env": "SA_PATH_NOT_EXIST"},
	}

	err := injectAPIAuth(auth, headers, &urlStr, &apiHTTPConfig{})
	if err == nil {
		t.Fatal("expected error when service_account file does not exist")
	}
	if !strings.Contains(err.Error(), "read service_account") {
		t.Errorf("error %q should mention read failure (saw: %v)", err.Error(), err)
	}
}

// TestInjectAPIAuth_ServiceAccount_MalformedJSON pins that json.Unmarshal
// failures (file exists but is not valid JSON) are propagated as errors.
// The bug class: a future change might catch and ignore the parse error,
// leaving the runtime to call AccessToken() with a zero-value key and
// emit a confusing downstream error instead of a clear "bad SA file" one.
func TestInjectAPIAuth_ServiceAccount_MalformedJSON(t *testing.T) {
	dir := t.TempDir()
	saPath := filepath.Join(dir, "sa.json")
	if err := os.WriteFile(saPath, []byte("not valid json {{{"), 0o600); err != nil {
		t.Fatalf("write sa file: %v", err)
	}
	t.Setenv("SA_PATH_BAD_JSON", saPath)
	headers := make(map[string]string)
	urlStr := "https://api.example.com"
	auth := map[string]any{
		"service_account": map[string]any{"env": "SA_PATH_BAD_JSON"},
	}

	err := injectAPIAuth(auth, headers, &urlStr, &apiHTTPConfig{})
	if err == nil {
		t.Fatal("expected error when service_account file is malformed JSON")
	}
	if !strings.Contains(err.Error(), "parse service_account") {
		t.Errorf("error %q should mention parse failure (saw: %v)", err.Error(), err)
	}
}

// --- RunPush PerRow regression tests ---

func TestRunSink_PerRow_EmptyStringValue(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "emptyval_push", `
API = {
    "base_url": "https://example.com",
    "push": {"batch_size": 100},
}

def push(rows=[], batch_number=1):
    return {"1.0": ""}
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{{"__ondatra_rowid": 1.0}}
	result, err := rt.RunPush(context.Background(), "emptyval_push", rows, 1, "table", "", nil)
	if err != nil {
		t.Fatalf("RunPush: %v", err)
	}
	// Empty string value should be preserved (was dropped before fix)
	if v, ok := result.PerRow["1.0"]; !ok {
		t.Error("PerRow should contain key '1.0' even with empty string value")
	} else if v != "" {
		t.Errorf("PerRow['1.0'] = %q, want empty string", v)
	}
}

func TestRunSink_PerRow_IntegerValue(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "intval_push", `
API = {
    "base_url": "https://example.com",
    "push": {"batch_size": 100},
}

def push(rows=[], batch_number=1):
    return {"1.0": 42}
`)
	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{{"__ondatra_rowid": 1.0}}
	result, err := rt.RunPush(context.Background(), "intval_push", rows, 1, "table", "", nil)
	if err != nil {
		t.Fatalf("RunPush: %v", err)
	}
	// Non-string values should be converted via .String() (was dropped before fix)
	if v, ok := result.PerRow["1.0"]; !ok {
		t.Error("PerRow should contain key '1.0' for integer value")
	} else if v != "42" {
		t.Errorf("PerRow['1.0'] = %q, want '42'", v)
	}
}

// --- Collector inferTypes regression tests ---

func TestInferTypes_Float64WholeNumber_IsBigint(t *testing.T) {
	t.Parallel()
	// After JSON round-trip through Badger, int64 becomes float64.
	// inferTypes should detect whole-number float64 as BIGINT.
	c := &saveCollector{
		columns: []string{"id", "score"},
		data: []map[string]interface{}{
			{"id": float64(42), "score": 3.14},
		},
	}

	types := c.inferTypes()

	if types["id"] != "BIGINT" {
		t.Errorf("inferTypes()[id] = %q, want BIGINT (whole-number float64)", types["id"])
	}
	if types["score"] != "DOUBLE" {
		t.Errorf("inferTypes()[score] = %q, want DOUBLE (fractional float64)", types["score"])
	}

	// The float64 value should also be converted back to int64 in the data
	if _, ok := c.data[0]["id"].(int64); !ok {
		t.Errorf("data[0][id] should be int64 after inferTypes, got %T", c.data[0]["id"])
	}
}

func TestInferTypes_NegativeWholeFloat64_IsBigint(t *testing.T) {
	t.Parallel()
	c := &saveCollector{
		columns: []string{"val"},
		data: []map[string]interface{}{
			{"val": float64(-100)},
		},
	}

	types := c.inferTypes()

	if types["val"] != "BIGINT" {
		t.Errorf("inferTypes()[val] = %q, want BIGINT for -100.0", types["val"])
	}
}

func TestInferTypes_ZeroFloat64_IsBigint(t *testing.T) {
	t.Parallel()
	c := &saveCollector{
		columns: []string{"val"},
		data: []map[string]interface{}{
			{"val": float64(0)},
		},
	}

	types := c.inferTypes()

	if types["val"] != "BIGINT" {
		t.Errorf("inferTypes()[val] = %q, want BIGINT for 0.0", types["val"])
	}
}

// --- csvEncode: empty string regression ---

func TestCsvEncode_EmptyStringValue(t *testing.T) {
	t.Parallel()
	// Before fix: an empty Starlark string "" was encoded as literal '""'
	// (with Go-quoted double quotes) because AsString returned ("", true)
	// but the s=="" check triggered the v.String() fallback.
	// After fix: checks the ok bool from AsString, not the value.
	rt := NewRuntime(nil, nil)
	code := `
result = csv.encode([{"name": "", "age": "30"}])
lines = result.split("\n")
# Header line
if lines[0] != "age,name":
    # columns are sorted
    if lines[0] != "name,age":
        fail("unexpected header: " + lines[0])
# Data line: empty string should NOT have literal quotes
header = lines[0]
data = lines[1]
name_idx = 0
if header == "age,name":
    name_idx = 1
parts = data.split(",")
name_val = parts[name_idx]
if name_val == '""':
    fail("empty string should be empty cell, not literal quotes: " + repr(data))
`
	_, err := rt.Run(context.Background(), "test_csv", code)
	if err != nil {
		t.Fatalf("csv.encode empty string bug: %v", err)
	}
}

func TestCsvEncode_IntegerValue(t *testing.T) {
	t.Parallel()
	// Non-string values should be converted via .String(), not v.String()
	// which adds Go quoting for strings.
	rt := NewRuntime(nil, nil)
	code := `
result = csv.encode([{"count": 42}])
lines = result.split("\n")
if lines[1].strip() != "42":
    fail("integer should be '42', got: " + repr(lines[1]))
`
	_, err := rt.Run(context.Background(), "test_csv_int", code)
	if err != nil {
		t.Fatalf("csv.encode integer value: %v", err)
	}
}

// --- mod_query.go: writable CTE blocked ---

func TestValidateReadOnly_WritableCTE(t *testing.T) {
	t.Parallel()
	// Before fix: WITH ... DELETE ... RETURNING bypassed the read-only check.
	err := validateReadOnly("WITH deleted AS (DELETE FROM users RETURNING *) SELECT * FROM deleted")
	if err == nil {
		t.Error("writable CTE should be rejected")
	}
}

func TestValidateReadOnly_WritableCTE_Insert(t *testing.T) {
	t.Parallel()
	err := validateReadOnly("WITH ins AS (INSERT INTO t VALUES (1) RETURNING *) SELECT * FROM ins")
	if err == nil {
		t.Error("INSERT CTE should be rejected")
	}
}

func TestValidateReadOnly_WritableCTE_Update(t *testing.T) {
	t.Parallel()
	err := validateReadOnly("WITH upd AS (UPDATE t SET x=1 RETURNING *) SELECT * FROM upd")
	if err == nil {
		t.Error("UPDATE CTE should be rejected")
	}
}

func TestValidateReadOnly_NormalCTE(t *testing.T) {
	t.Parallel()
	// Normal read-only CTE should be allowed
	err := validateReadOnly("WITH cte AS (SELECT 1 AS id) SELECT * FROM cte")
	if err != nil {
		t.Errorf("normal CTE should be allowed: %v", err)
	}
}

func TestValidateReadOnly_CTE_StringContainingDML(t *testing.T) {
	t.Parallel()
	// Before second fix: this was rejected because "DELETE" appeared as substring.
	// After fix: "DELETE" inside a string literal is not treated as a DML keyword.
	err := validateReadOnly("WITH t AS (SELECT 'DELETE' AS word) SELECT * FROM t")
	if err != nil {
		t.Errorf("CTE with 'DELETE' in string literal should be allowed: %v", err)
	}
}

func TestValidateReadOnly_CTE_ColumnNameContainingDML(t *testing.T) {
	t.Parallel()
	// Column name "delete_flag" contains "DELETE" as substring but is not DML
	err := validateReadOnly("WITH t AS (SELECT 1 AS delete_flag) SELECT * FROM t")
	if err != nil {
		t.Errorf("CTE with 'delete_flag' column should be allowed: %v", err)
	}
}

func TestValidateReadOnly_Select(t *testing.T) {
	t.Parallel()
	err := validateReadOnly("SELECT * FROM users")
	if err != nil {
		t.Errorf("SELECT should be allowed: %v", err)
	}
}

func TestValidateReadOnly_Delete(t *testing.T) {
	t.Parallel()
	err := validateReadOnly("DELETE FROM users")
	if err == nil {
		t.Error("DELETE should be rejected")
	}
}

// --- mod_env.go: LookupEnv ---

func TestEnvGet_EmptyVarNotDefault(t *testing.T) {
	// Before fix: env.get("VAR", "default") returned "default" when VAR=""
	// After fix: returns "" because the var IS set (just empty)
	t.Setenv("TEST_EMPTY_VAR", "")
	rt := NewRuntime(nil, nil)
	code := `
val = env.get("TEST_EMPTY_VAR", "fallback")
if val != "":
    fail("expected empty string, got: " + repr(val))
`
	_, err := rt.Run(context.Background(), "test_env", code)
	if err != nil {
		t.Fatalf("env.get empty var: %v", err)
	}
}

func TestEnvGet_UnsetVarUsesDefault(t *testing.T) {
	t.Setenv("TEST_UNSET_CHECK", "") // ensure clean
	os.Unsetenv("TEST_UNSET_CHECK")
	rt := NewRuntime(nil, nil)
	code := `
val = env.get("TEST_UNSET_CHECK", "fallback")
if val != "fallback":
    fail("expected 'fallback', got: " + repr(val))
`
	_, err := rt.Run(context.Background(), "test_env_unset", code)
	if err != nil {
		t.Fatalf("env.get unset var: %v", err)
	}
}

// --- collector.go: nested list/dict → JSON string ---

func TestCollector_NestedList_ToJSON(t *testing.T) {
	t.Parallel()
	c := &saveCollector{target: "test"}
	err := c.add(map[string]interface{}{
		"id":   1,
		"tags": []interface{}{"a", "b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Nested list should not crash — it becomes VARCHAR via JSON serialization
	types := c.inferTypes()
	if types["tags"] != "VARCHAR" {
		t.Errorf("nested list type = %q, want VARCHAR", types["tags"])
	}
}

func TestCollector_NestedMap_ToJSON(t *testing.T) {
	t.Parallel()
	c := &saveCollector{target: "test"}
	err := c.add(map[string]interface{}{
		"id":   1,
		"meta": map[string]interface{}{"key": "val"},
	})
	if err != nil {
		t.Fatal(err)
	}
	types := c.inferTypes()
	if types["meta"] != "VARCHAR" {
		t.Errorf("nested map type = %q, want VARCHAR", types["meta"])
	}
}

func TestCollector_EmptyRow_Rejected(t *testing.T) {
	t.Parallel()
	c := &saveCollector{target: "test"}
	err := c.add(map[string]interface{}{})
	if err == nil {
		t.Error("empty row should be rejected")
	}
}

// --- convert.go: extra numeric types ---

func TestGoToStarlark_Int32(t *testing.T) {
	t.Parallel()
	v, err := goToStarlark(int32(42))
	if err != nil {
		t.Fatal(err)
	}
	if v.String() != "42" {
		t.Errorf("int32(42) → %s, want 42", v.String())
	}
}

func TestGoToStarlark_Float32(t *testing.T) {
	t.Parallel()
	v, err := goToStarlark(float32(3.14))
	if err != nil {
		t.Fatal(err)
	}
	// Should be a Starlark Float, not a String
	if _, ok := v.(starlark.Float); !ok {
		t.Errorf("float32 should become starlark.Float, got %T", v)
	}
}

func TestGoToStarlark_Uint64(t *testing.T) {
	t.Parallel()
	v, err := goToStarlark(uint64(18446744073709551615))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := v.(starlark.Int); !ok {
		t.Errorf("uint64 should become starlark.Int, got %T", v)
	}
}

// --- RunPushPoll: per-row non-string values converted (same as RunPush) ---

func TestRunSinkPoll_PerRow_NonStringConverted(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSinkStar(t, dir, "async_poll", `
API = {
    "base_url": "https://example.com",
    "push": {"batch_size": 100, "batch_mode": "async"},
}

def push(rows=[], batch_number=1):
    return {"job_id": "123"}

def poll(job_ref):
    return {"done": True, "per_row": {"1.0": True}}
`)
	rt := NewRuntime(nil, nil, dir)
	done, perRow, err := rt.RunPushPoll(context.Background(), "async_poll", map[string]any{"job_id": "123"})
	if err != nil {
		t.Fatalf("RunPushPoll: %v", err)
	}
	if !done {
		t.Fatal("expected done=true")
	}
	// Before fix: True became "" (empty string). After fix: "True"
	if v, ok := perRow["1.0"]; !ok || v != "True" {
		t.Errorf("perRow[1.0] = %q, want 'True'", v)
	}
}
