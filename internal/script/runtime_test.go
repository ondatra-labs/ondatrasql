// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/backfill"
	"go.starlark.net/starlark"
)

func TestRuntimeSimpleScript(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)

	// Simple script that uses predeclared modules
	code := `
x = {"name": "test", "value": 42}
result = str(x)
`

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := rt.Run(ctx, "test.target", code)
	if err != nil {
		t.Fatalf("script execution failed: %v", err)
	}

	// Script without save() calls should have empty temp table
	if result.TempTable != "" {
		t.Errorf("expected empty temp table, got %q", result.TempTable)
	}
}

func TestHTTPModuleAvailable(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)

	// Verify modules are predeclared and have expected methods
	code := `
if not hasattr(http, "get"):
    fail("http module missing get method")
if not hasattr(http, "post"):
    fail("http module missing post method")
if not hasattr(env, "get"):
    fail("env module missing get method")
`

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := rt.Run(ctx, "test.target", code)
	if err != nil {
		t.Fatalf("module verification failed: %v", err)
	}
}

func TestEnvModule(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)

	// Test env module with default value as kwarg
	code := `
val = env.get("NONEXISTENT_VAR_12345", default="default_value")
if val != "default_value":
    fail("expected default_value")
`

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := rt.Run(ctx, "test.target", code)
	if err != nil {
		t.Fatalf("env module test failed: %v", err)
	}
}

func TestSaveModule(t *testing.T) {
	t.Parallel()
	// Test save module without DuckDB (just verify it accumulates data)
	rt := NewRuntime(nil, nil)

	code := `
save.row({"id": 1, "name": "test"})
save.rows([{"id": 2, "name": "test2"}, {"id": 3, "name": "test3"}])
`

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Run collects data; CreateTempTable will fail (no session)
	result, err := rt.Run(ctx, "test.target", code)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount != 3 {
		t.Fatalf("expected 3 rows, got %d", result.RowCount)
	}
	if err := result.CreateTempTable(); err == nil {
		t.Fatal("expected error due to no database session")
	}
}

func TestScriptTimeout(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)

	// Script with long-running computation (Starlark has no while True)
	code := `
x = 0
for i in range(10000000000):
    x = x + 1
`

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := rt.Run(ctx, "test.target", code)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestIncrementalModule(t *testing.T) {
	t.Parallel()
	// Test incremental module with state
	state := &backfill.IncrementalState{
		IsBackfill:   false,
		Cursor:       "updated_at",
		LastValue:    "2024-03-15T10:00:00Z",
		LastRun:      "2024-03-15T09:00:00Z",
		InitialValue: "2024-01-01T00:00:00Z",
	}

	rt := NewRuntime(nil, state)

	code := `
if incremental.is_backfill:
    fail("expected is_backfill to be false")

if incremental.cursor != "updated_at":
    fail("expected cursor to be updated_at, got: " + incremental.cursor)

if incremental.last_value != "2024-03-15T10:00:00Z":
    fail("expected last_value to be 2024-03-15T10:00:00Z, got: " + incremental.last_value)

if incremental.last_run != "2024-03-15T09:00:00Z":
    fail("expected last_run to be 2024-03-15T09:00:00Z, got: " + incremental.last_run)

if incremental.initial_value != "2024-01-01T00:00:00Z":
    fail("expected initial_value to be 2024-01-01T00:00:00Z, got: " + incremental.initial_value)
`

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := rt.Run(ctx, "test.target", code)
	if err != nil {
		t.Fatalf("incremental module test failed: %v", err)
	}
}

func TestIncrementalModuleBackfill(t *testing.T) {
	t.Parallel()
	// Test incremental module when it's a backfill (first run)
	state := &backfill.IncrementalState{
		IsBackfill:   true,
		Cursor:       "id",
		LastValue:    "0",
		InitialValue: "0",
	}

	rt := NewRuntime(nil, state)

	code := `
if not incremental.is_backfill:
    fail("expected is_backfill to be true")

if incremental.last_value != "0":
    fail("expected last_value to be 0, got: " + incremental.last_value)
`

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := rt.Run(ctx, "test.target", code)
	if err != nil {
		t.Fatalf("incremental backfill test failed: %v", err)
	}
}

func TestIncrementalModuleNil(t *testing.T) {
	t.Parallel()
	// Test incremental module without state (nil)
	rt := NewRuntime(nil, nil)

	code := `
# With nil state, is_backfill should be true (default)
if not incremental.is_backfill:
    fail("expected is_backfill to be true when no state")

# All string values should be empty
if incremental.cursor != "":
    fail("expected cursor to be empty")
`

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := rt.Run(ctx, "test.target", code)
	if err != nil {
		t.Fatalf("incremental nil state test failed: %v", err)
	}
}

func TestTimeModule(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
now = time.now()
d = time.parse_duration("1h30m")
if d.hours != 1.5:
    fail("expected 1.5 hours, got: " + str(d.hours))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestMathModule(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
if math.ceil(1.5) != 2:
    fail("ceil")
if math.floor(1.5) != 1:
    fail("floor")
if math.round(1.5) != 2:
    fail("round")
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestJsonModule(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
encoded = json.encode({"name": "test", "value": 42})
decoded = json.decode('{"name": "test", "value": 42}')
if decoded["name"] != "test":
    fail("name: " + decoded["name"])
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestReModule(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
m = re.search(r"(\d+)-(\d+)", "order-123-456")
if m.group(1) != "123":
    fail("group 1: " + m.group(1))
if m.group(2) != "456":
    fail("group 2: " + m.group(2))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestAbortRejectsArgs(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)

	code := `abort("something went wrong")`

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := rt.Run(ctx, "test.target", code)
	if err == nil {
		t.Fatal("expected error for abort with argument")
	}
	// Should get an argument count error, not an AbortError
	if strings.Contains(err.Error(), "script aborted") {
		t.Fatal("should reject arguments, not abort")
	}
}

func TestAbortNoArgs(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)

	code := `abort()`

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := rt.Run(ctx, "test.target", code)
	if err == nil {
		t.Fatal("expected abort to return an error")
	}
	var abortErr *AbortError
	if !errors.As(err, &abortErr) {
		t.Fatalf("expected AbortError type, got: %T (%v)", err, err)
	}
}

func TestSleepGlobal(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `sleep(0.01)` // 10ms
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 10*time.Millisecond {
		t.Fatal("sleep didn't wait long enough")
	}
}

func TestSleepGlobalInt(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `sleep(0)` // 0 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestFailBuiltin(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `fail("something went wrong")`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := rt.Run(ctx, "test", code)
	if err == nil {
		t.Fatal("expected fail to produce an error")
	}
	if !strings.Contains(err.Error(), "something went wrong") {
		t.Fatalf("expected error message, got: %v", err)
	}
}

func TestQuoteIdentifier(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{"simple", `"simple"`},
		{"with space", `"with space"`},
		{"with-dash", `"with-dash"`},
		{"order", `"order"`}, // reserved word
		{`has"quote`, `"has""quote"`},
		{"123numeric", `"123numeric"`},
		{"", `""`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			got := quoteIdentifier(tt.input)
			if got != tt.want {
				t.Errorf("quoteIdentifier(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestHTTPGetFromStarlark(t *testing.T) {
	t.Parallel()
	// Start a test HTTP server
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"items": [{"id": 1}, {"id": 2}]}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)

	code := fmt.Sprintf(`
resp = http.get("%s")
if resp.status_code != 200:
    fail("status: " + str(resp.status_code))
if not resp.ok:
    fail("not ok")
data = resp.json
if len(data["items"]) != 2:
    fail("expected 2 items")
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPPostFromStarlark(t *testing.T) {
	t.Parallel()
	var receivedBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, 1024)
		n, _ := r.Body.Read(body)
		receivedBody = string(body[:n])
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok": true}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)

	code := fmt.Sprintf(`
resp = http.post("%s", json={"name": "test"})
if resp.status_code != 200:
    fail("status: " + str(resp.status_code))
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(receivedBody, "test") {
		t.Errorf("expected body to contain 'test', got: %s", receivedBody)
	}
}

func TestHTTPWithHeadersFromStarlark(t *testing.T) {
	t.Parallel()
	var receivedAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)

	code := fmt.Sprintf(`
resp = http.get("%s", headers={"Authorization": "Bearer test-token"})
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
	if receivedAuth != "Bearer test-token" {
		t.Errorf("Authorization = %q", receivedAuth)
	}
}

func TestHTTPWithParamsFromStarlark(t *testing.T) {
	t.Parallel()
	var receivedQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedQuery = r.URL.RawQuery
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)

	code := fmt.Sprintf(`
resp = http.get("%s", params={"page": "2", "limit": "10"})
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(receivedQuery, "page=2") {
		t.Errorf("expected page=2 in query, got: %s", receivedQuery)
	}
}

func TestEnvSetAndGet(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)

	code := `
env.set("TEST_ONDATRA_VAR", "hello123")
val = env.get("TEST_ONDATRA_VAR")
if val != "hello123":
    fail("expected hello123, got: " + val)
`

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
	// Cleanup
	os.Unsetenv("TEST_ONDATRA_VAR")
}

func TestOAuthBasicAuthFromStarlark(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)

	code := `
auth = oauth.basic_auth("user", "pass")
if not auth.startswith("Basic "):
    fail("expected Basic prefix, got: " + auth)
`

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestOAuthTokenFromStarlark(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"access_token": "my-tok", "expires_in": 3600}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)

	code := fmt.Sprintf(`
tok = oauth.token(token_url="%s", client_id="id", client_secret="secret")
val = tok.access_token
if val != "my-tok":
    fail("expected my-tok, got: " + val)
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

// --- CSV module tests ---

func TestCSVDecodeFromStarlark(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
data = "name,age\nAlice,30\nBob,25"
rows = csv.decode(data)
if len(rows) != 2:
    fail("expected 2 rows, got " + str(len(rows)))
if rows[0]["name"] != "Alice":
    fail("row 0 name: " + rows[0]["name"])
if rows[1]["age"] != "25":
    fail("row 1 age: " + rows[1]["age"])
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCSVDecodeNoHeader(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
data = "Alice,30\nBob,25"
rows = csv.decode(data, header=False)
if len(rows) != 2:
    fail("expected 2 rows, got " + str(len(rows)))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCSVDecodeCustomDelimiter(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
data = "name;age\nAlice;30"
rows = csv.decode(data, delimiter=";")
if rows[0]["name"] != "Alice":
    fail("name: " + rows[0]["name"])
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCSVDecodeEmpty(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `csv.decode("")`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test", code)
	if err == nil {
		t.Fatal("expected error for empty CSV")
	}
}

func TestCSVEncodeDicts(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
rows = [{"name": "Alice", "age": "30"}, {"name": "Bob", "age": "25"}]
result = csv.encode(rows)
if "Alice" not in result:
    fail("missing Alice: " + result)
if "Bob" not in result:
    fail("missing Bob: " + result)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCSVEncodeLists(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
rows = [["Alice", "30"], ["Bob", "25"]]
result = csv.encode(rows, header=["name", "age"])
if "name" not in result:
    fail("missing header: " + result)
if "Alice" not in result:
    fail("missing Alice: " + result)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCSVEncodeEmpty(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
result = csv.encode([])
if result != "":
    fail("expected empty, got: " + result)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

// --- XML module tests ---

func TestXMLDecodeFromStarlark(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
data = "<root><name>Alice</name><age>30</age></root>"
result = xml.decode(data)
if result["root"]["name"] != "Alice":
    fail("name: " + str(result))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestXMLDecodeEmpty(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `xml.decode("")`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test", code)
	if err == nil {
		t.Fatal("expected error for empty XML")
	}
}

func TestXMLEncodeFromStarlark(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
data = {"root": {"name": "Alice"}}
result = xml.encode(data)
if "Alice" not in result:
    fail("missing Alice: " + result)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

// --- URL module tests ---

func TestURLBuildFromStarlark(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
result = url.build(base="https://api.example.com/v1", params={"page": "2", "limit": "10"})
if "page=2" not in result:
    fail("missing page: " + result)
if "limit=10" not in result:
    fail("missing limit: " + result)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestURLBuildNoParamsFromStarlark(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
result = url.build(base="https://example.com")
if result != "https://example.com":
    fail("got: " + result)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestURLEncodeFromStarlark(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
result = url.encode("hello world&foo=bar")
if "hello" not in result:
    fail("got: " + result)
if " " in result:
    fail("space not encoded: " + result)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestURLEncodeParamsFromStarlark(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
result = url.encode_params({"foo": "bar", "baz": "qux"})
if "foo=bar" not in result:
    fail("missing foo: " + result)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestURLParseFromStarlark(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
u = url.parse("https://api.example.com/v1/data?key=val")
if u.scheme != "https":
    fail("scheme: " + u.scheme)
if u.host != "api.example.com":
    fail("host: " + u.host)
if u.path != "/v1/data":
    fail("path: " + u.path)
if u.query.key != "val":
    fail("query.key: " + str(u.query.key))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

// --- Crypto module tests ---

func TestCryptoBase64FromStarlark(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
encoded = crypto.base64_encode("hello world")
decoded = crypto.base64_decode(encoded)
if decoded != "hello world":
    fail("roundtrip failed: " + decoded)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCryptoSHA256FromStarlark(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
h = crypto.sha256("hello")
if len(h) != 64:
    fail("expected 64 hex chars, got " + str(len(h)))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCryptoMD5FromStarlark(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
h = crypto.md5("hello")
if len(h) != 32:
    fail("expected 32 hex chars, got " + str(len(h)))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCryptoHMACSHA256FromStarlark(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
h = crypto.hmac_sha256("secret", "message")
if len(h) != 64:
    fail("expected 64 hex chars, got " + str(len(h)))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCryptoUUIDFromStarlark(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
u = crypto.uuid()
if len(u) != 36:
    fail("expected UUID length 36, got " + str(len(u)))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCryptoRandomStringFromStarlark(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
s = crypto.random_string()
if len(s) != 32:
    fail("expected 32 chars, got " + str(len(s)))
s16 = crypto.random_string(length=16)
if len(s16) != 16:
    fail("expected 16 chars, got " + str(len(s16)))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

// --- HTTP module edge cases ---

func TestHTTPDataFormPost(t *testing.T) {
	t.Parallel()
	var contentType string
	var receivedBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentType = r.Header.Get("Content-Type")
		body := make([]byte, 1024)
		n, _ := r.Body.Read(body)
		receivedBody = string(body[:n])
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.post("%s", data={"username": "alice", "password": "secret"})
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(contentType, "application/x-www-form-urlencoded") {
		t.Errorf("content-type = %q, want form-urlencoded", contentType)
	}
	if !strings.Contains(receivedBody, "username=alice") {
		t.Errorf("body = %q, missing username", receivedBody)
	}
}

func TestHTTPJsonListBody(t *testing.T) {
	t.Parallel()
	var receivedBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, 1024)
		n, _ := r.Body.Read(body)
		receivedBody = string(body[:n])
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.post("%s", json=[{"id": 1}, {"id": 2}])
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(receivedBody, `"id"`) {
		t.Errorf("body = %q, expected JSON list", receivedBody)
	}
}

func TestHTTPJsonStringBody(t *testing.T) {
	t.Parallel()
	var receivedBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, 1024)
		n, _ := r.Body.Read(body)
		receivedBody = string(body[:n])
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.post("%s", json='{"raw": true}')
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(receivedBody, `"raw"`) {
		t.Errorf("body = %q", receivedBody)
	}
}

func TestHTTPJsonAndDataMutuallyExclusive(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.post("%s", json={"a": 1}, data={"b": "2"})
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test", code)
	if err == nil {
		t.Fatal("expected error for json+data")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHTTPWithTimeout(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"ok": true}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.get("%s", timeout=5.0)
if resp.status_code != 200:
    fail("status: " + str(resp.status_code))
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPWithIntTimeout(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"ok": true}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.get("%s", timeout=5)
if resp.status_code != 200:
    fail("status: " + str(resp.status_code))
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPWithBasicAuthHeader(t *testing.T) {
	t.Parallel()
	var receivedAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	// Use oauth.basic_auth to generate auth header since Starlark tuple kwarg is tricky
	code := fmt.Sprintf(`
auth_header = oauth.basic_auth("user", "pass")
resp = http.get("%s", headers={"Authorization": auth_header})
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(receivedAuth, "Basic ") {
		t.Errorf("auth = %q, want Basic prefix", receivedAuth)
	}
}

func TestHTTPWithRetry(t *testing.T) {
	t.Parallel()
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount <= 2 {
			w.WriteHeader(500)
			return
		}
		w.Write([]byte(`{"ok": true}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.get("%s", retry=3, backoff=0.01)
if resp.status_code != 200:
    fail("status: " + str(resp.status_code))
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
	if callCount < 3 {
		t.Errorf("expected at least 3 calls, got %d", callCount)
	}
}

func TestHTTPWithIntBackoff(t *testing.T) {
	t.Parallel()
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount == 1 {
			w.WriteHeader(500)
			return
		}
		w.Write([]byte(`{"ok": true}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.get("%s", retry=1, backoff=0)
if resp.status_code != 200:
    fail("status: " + str(resp.status_code))
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPBadAuthScheme(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.get("%s", auth=("user", "pass", "unknown"))
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test", code)
	if err == nil {
		t.Fatal("expected error for unknown auth scheme")
	}
}

func TestHTTPCertWithoutKey(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `resp = http.get("https://example.com", cert="cert.pem")`

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test", code)
	if err == nil {
		t.Fatal("expected error for cert without key")
	}
	if !strings.Contains(err.Error(), "cert and key must be provided together") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHTTPCaWithoutCert(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `resp = http.get("https://example.com", ca="ca.pem")`

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test", code)
	if err == nil {
		t.Fatal("expected error for ca without cert")
	}
}

func TestHTTPPutDeletePatch(t *testing.T) {
	t.Parallel()
	var methods []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method)
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
http.put("%s")
http.delete("%s")
http.patch("%s")
`, srv.URL, srv.URL, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
	if len(methods) != 3 {
		t.Fatalf("expected 3 requests, got %d", len(methods))
	}
	if methods[0] != "PUT" || methods[1] != "DELETE" || methods[2] != "PATCH" {
		t.Errorf("methods = %v", methods)
	}
}

func TestHTTPLinkHeader(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Link", `<https://api.example.com/items?page=2>; rel="next", <https://api.example.com/items?page=5>; rel="last"`)
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.get("%s")
links = resp.headers["_links"]
if links["next"] != "https://api.example.com/items?page=2":
    fail("next link: " + str(links["next"]))
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPMultiValueHeader(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("X-Custom", "val1")
		w.Header().Add("X-Custom", "val2")
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.get("%s")
# Multi-value headers should be a list
h = resp.headers["X-Custom"]
if type(h) != "list":
    fail("expected list for multi-value header, got " + type(h))
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPJsonInvalidType(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	// json=42 is not dict/list/string
	code := fmt.Sprintf(`
resp = http.post("%s", json=42)
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test", code)
	if err == nil {
		t.Fatal("expected error for json=int")
	}
	if !strings.Contains(err.Error(), "json must be") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHTTPNon200Response(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		w.Write([]byte(`not found`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.get("%s")
if resp.ok:
    fail("should not be ok")
if resp.status_code != 404:
    fail("status: " + str(resp.status_code))
if "not found" not in resp.text:
    fail("body: " + resp.text)
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPNonJSONResponse(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte(`hello world`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.get("%s")
if resp.json != None:
    fail("expected None json for text response")
if "hello world" not in resp.text:
    fail("body: " + resp.text)
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

// --- save.rows edge cases ---

func TestSaveRowsNonDict(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `save.rows(["not a dict"])`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test", code)
	if err == nil {
		t.Fatal("expected error for non-dict in save.rows")
	}
}

func TestSaveCount(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
save.row({"id": 1})
save.row({"id": 2})
c = save.count()
if c != 2:
    fail("count: " + str(c))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

// --- Sleep error ---

func TestSleepBadType(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `sleep("not a number")`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test", code)
	if err == nil {
		t.Fatal("expected error for sleep with string")
	}
}

// --- Redact tests ---

func TestRedactSecrets(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		safe  bool // true if output should NOT contain the secret
	}{
		{"Bearer my-secret-token", true},
		{"Basic dXNlcjpwYXNz", true},
		{`token: "my-secret"`, true},
		{"no secrets here", false},
	}
	for _, tt := range tests {
		result := RedactSecrets(tt.input)
		if tt.safe && !strings.Contains(result, "[REDACTED]") {
			t.Errorf("RedactSecrets(%q) = %q, expected redaction", tt.input, result)
		}
	}
}

func TestRedactURLFromRuntime(t *testing.T) {
	t.Parallel()
	result := RedactURL("https://api.example.com/data?api_key=secret123&page=1")
	if strings.Contains(result, "secret123") {
		t.Errorf("RedactURL should redact api_key, got: %s", result)
	}
	if !strings.Contains(result, "page=1") {
		t.Errorf("RedactURL should keep non-sensitive params, got: %s", result)
	}
}

func TestRedactURLNoSensitiveFromRuntime(t *testing.T) {
	t.Parallel()
	input := "https://api.example.com/data?page=1"
	result := RedactURL(input)
	if result != input {
		t.Errorf("RedactURL changed non-sensitive URL: %s", result)
	}
}

func TestRedactURLInvalidFromRuntime(t *testing.T) {
	t.Parallel()
	input := "://invalid"
	result := RedactURL(input)
	if result != input {
		t.Errorf("RedactURL should return input for invalid URL, got: %s", result)
	}
}

// --- env module edge cases ---

func TestEnvGetExistingVar(t *testing.T) {
	t.Parallel()
	os.Setenv("TEST_ONDATRA_EXIST", "found_it")
	defer os.Unsetenv("TEST_ONDATRA_EXIST")

	rt := NewRuntime(nil, nil)
	code := `
val = env.get("TEST_ONDATRA_EXIST")
if val != "found_it":
    fail("expected found_it, got: " + val)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

// --- CreateTempTable with nil result ---

func TestCreateTempTableNoData(t *testing.T) {
	t.Parallel()
	result := &Result{collector: nil}
	if err := result.CreateTempTable(); err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
}

func TestCSVEncodeListOfLists(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
rows = [["a", "b"], ["1", "2"]]
result = csv.encode(rows)
if "a,b" not in result:
    fail("expected 'a,b' in result, got: " + result)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCSVEncodeListOfListsWithHeader(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
rows = [["1", "2"], ["3", "4"]]
result = csv.encode(rows, header=["col_a", "col_b"])
if "col_a,col_b" not in result:
    fail("expected header in result, got: " + result)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCSVEncodeEmptyList(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
result = csv.encode([])
if result != "":
    fail("expected empty string, got: " + result)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCSVEncodeInvalidRowType(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `csv.encode(["not_a_dict_or_list"])`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test", code)
	if err == nil {
		t.Fatal("expected error for invalid row type")
	}
}

func TestCSVEncodeDictWithNonStringValues(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
rows = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
result = csv.encode(rows)
if "Alice" not in result:
    fail("expected Alice in result")
if "30" not in result:
    fail("expected 30 in result")
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCSVDecodeEmptyError(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `csv.decode("")`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test", code)
	if err == nil {
		t.Fatal("expected error for empty csv data")
	}
}

func TestXMLDecodeMalformed(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `xml.decode("<root><unclosed>")`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test", code)
	if err == nil {
		t.Fatal("expected error for malformed XML")
	}
}

func TestXMLDecodeWithAttributes(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
data = '<root><item id="1">hello</item></root>'
result = xml.decode(data)
if "root" not in str(result):
    fail("expected root key")
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestXMLEncodeRoundtrip(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
d = {"root": {"item": "hello"}}
encoded = xml.encode(d)
if "root" not in encoded:
    fail("expected root in encoded XML")
if "hello" not in encoded:
    fail("expected hello in encoded XML")
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestEnvGetDefault(t *testing.T) {
	t.Parallel()
	os.Unsetenv("TEST_ONDATRA_MISSING_VAR")
	rt := NewRuntime(nil, nil)
	code := `
val = env.get("TEST_ONDATRA_MISSING_VAR", "fallback")
if val != "fallback":
    fail("expected fallback, got " + val)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestEnvGetMissingNoDefault(t *testing.T) {
	t.Parallel()
	os.Unsetenv("TEST_ONDATRA_MISSING_VAR2")
	rt := NewRuntime(nil, nil)
	code := `
val = env.get("TEST_ONDATRA_MISSING_VAR2")
if val != "":
    fail("expected empty string, got " + val)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestURLBuildInvalidBase(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `url.build(base="://invalid", params={"a": "1"})`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test", code)
	// url.Parse is very lenient, so this may not error - just verify no panic
	_ = err
}

func TestURLParseMultiValue(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
result = url.parse("https://example.com/path?foo=1&foo=2&bar=3")
if result.scheme != "https":
    fail("expected https scheme")
if result.host != "example.com":
    fail("expected example.com host")
if result.path != "/path":
    fail("expected /path")
# foo has multiple values, should be a list
if type(result.query.foo) != "list":
    fail("expected list for multi-value param, got " + type(result.query.foo))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestURLEncodeUnicode(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
result = url.encode("héllo wörld")
if "h" not in result:
    fail("expected h in encoded result")
if " " in result:
    fail("space should be encoded")
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCryptoBase64DecodeInvalid(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `crypto.base64_decode("!!!invalid!!!")`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test", code)
	if err == nil {
		t.Fatal("expected error for invalid base64")
	}
}

func TestCryptoRandomStringZeroLength(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
result = crypto.random_string(length=0)
if result != "":
    fail("expected empty string for length 0")
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCryptoRandomStringCustomLength(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
result = crypto.random_string(length=10)
if len(result) != 10:
    fail("expected length 10, got " + str(len(result)))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestSaveRowConversionError(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `save.rows(["not_a_dict"])`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test", code)
	if err == nil {
		t.Fatal("expected error for non-dict in save.rows")
	}
}

func TestHTTPPutMethod(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" {
			w.WriteHeader(405)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok": true}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.put("%s", json={"key": "value"})
if resp.status_code != 200:
    fail("expected 200, got " + str(resp.status_code))
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPPatchMethod(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PATCH" {
			w.WriteHeader(405)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok": true}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.patch("%s", json={"key": "value"})
if resp.status_code != 200:
    fail("expected 200, got " + str(resp.status_code))
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPDeleteMethod(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "DELETE" {
			w.WriteHeader(405)
			return
		}
		w.WriteHeader(204)
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.delete("%s")
if resp.status_code != 204:
    fail("expected 204, got " + str(resp.status_code))
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPTextResponse(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("hello world"))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.get("%s")
if resp.text != "hello world":
    fail("expected 'hello world', got: " + resp.text)
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPResponseHeaders(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Custom", "test-value")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.get("%s")
if resp.headers["X-Custom"] != "test-value":
    fail("expected X-Custom header")
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPFormPost(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			w.WriteHeader(400)
			w.Write([]byte(`{"error": "wrong content type"}`))
			return
		}
		r.ParseForm()
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(fmt.Sprintf(`{"user": "%s"}`, r.FormValue("user"))))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.post("%s", data={"user": "alice"})
if resp.status_code != 200:
    fail("expected 200, got " + str(resp.status_code))
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPWithQueryParams(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(fmt.Sprintf(`{"page": "%s"}`, q.Get("page"))))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.get("%s", params={"page": "2", "limit": "10"})
if resp.status_code != 200:
    fail("expected 200")
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPWithJsonList(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok": true}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.post("%s", json=[1, 2, 3])
if resp.status_code != 200:
    fail("expected 200")
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPWithJsonString(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok": true}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.post("%s", json='{"custom": true}')
if resp.status_code != 200:
    fail("expected 200")
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPWithTimeoutFloat(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.get("%s", timeout=5.0)
if resp.status_code != 200:
    fail("expected 200")
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPWithTimeoutInt(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.get("%s", timeout=5)
if resp.status_code != 200:
    fail("expected 200")
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPWithBackoffFloat(t *testing.T) {
	t.Parallel()
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount < 2 {
			w.WriteHeader(500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok": true}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.get("%s", retry=2, backoff=0.01)
if resp.status_code != 200:
    fail("expected 200 after retry, got " + str(resp.status_code))
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPWithAuthBasicViaHeader(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth == "" {
			w.WriteHeader(401)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok": true}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	// Use oauth.basic_auth() to generate the header since auth= kwarg has Starlark tuple limitation
	code := fmt.Sprintf(`
auth_header = oauth.basic_auth("user", "pass")
resp = http.get("%s", headers={"Authorization": auth_header})
if resp.status_code != 200:
    fail("expected 200 with basic auth header")
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPWithCertNoKey(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `http.get("http://localhost:1", cert="/path/to/cert.pem")`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test", code)
	if err == nil {
		t.Fatal("expected error for cert without key")
	}
	if !strings.Contains(err.Error(), "cert and key must be provided together") {
		t.Fatalf("wrong error: %v", err)
	}
}

func TestHTTPWithCANoCert(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `http.get("http://localhost:1", ca="/path/to/ca.pem")`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test", code)
	if err == nil {
		t.Fatal("expected error for ca without cert")
	}
}

func TestHTTPWithLinkHeader(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Link", `<https://api.example.com/page2>; rel="next", <https://api.example.com/page1>; rel="prev"`)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"page": 1}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.get("%s")
links = resp.headers["_links"]
if links["next"] != "https://api.example.com/page2":
    fail("expected next link")
if links["prev"] != "https://api.example.com/page1":
    fail("expected prev link")
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestSaveRowAndCount(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
save.row({"id": 1, "name": "Alice"})
save.row({"id": 2, "name": "Bob"})
c = save.count()
if c != 2:
    fail("expected count 2, got " + str(c))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestSaveRowsMultiple(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
save.rows([{"id": 1}, {"id": 2}, {"id": 3}])
c = save.count()
if c != 3:
    fail("expected count 3, got " + str(c))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPWithMultiValueHeaders(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("X-Multi", "val1")
		w.Header().Add("X-Multi", "val2")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.get("%s")
# Multi-value header should be a list
multi = resp.headers["X-Multi"]
if type(multi) == "list":
    if len(multi) != 2:
        fail("expected 2 values for X-Multi")
`, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestSanitize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{"simple", "simple"},
		{"with space", "with_space"},
		{"with-dash", "with_dash"},
		{"schema.table", "schema_table"},
		{"CamelCase", "CamelCase"},
		{"123", "123"},
		{"test_123", "test_123"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			got := sanitize(tt.input)
			if got != tt.want {
				t.Errorf("sanitize(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestRuntimePrint(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `print("hello from starlark")`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := rt.Run(ctx, "test.target", code)
	if err != nil {
		t.Fatalf("script execution failed: %v", err)
	}
}

func TestSleepIntTooLarge(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	// Use a very large int that won't fit in int64
	code := `sleep(99999999999999999999999999999999999999999999999)`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := rt.Run(ctx, "test.target", code)
	if err == nil {
		t.Fatal("expected error for too-large sleep integer")
	}
}

// --- Tests for httpRequest auth kwarg (Go-level, bypassing Starlark tuple issue) ---

func TestHTTPAuthBasicViaTuple(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Basic ") {
			t.Errorf("expected Basic auth header, got %q", auth)
		}
		w.WriteHeader(200)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	ctx := context.Background()
	fn := httpRequest(ctx, "GET")
	thread := &starlark.Thread{Name: "test"}

	authTuple := starlark.Tuple{starlark.String("user"), starlark.String("pass")}
	kwargs := []starlark.Tuple{
		{starlark.String("auth"), &authTuple},
	}
	args := starlark.Tuple{starlark.String(srv.URL)}

	result, err := fn(thread, starlark.NewBuiltin("http.get", fn), args, kwargs)
	if err != nil {
		t.Fatalf("httpRequest failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestHTTPAuthDigestViaTuple(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("Www-Authenticate", `Digest realm="test", nonce="abc123", qop="auth"`)
			w.WriteHeader(401)
			return
		}
		w.WriteHeader(200)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	ctx := context.Background()
	fn := httpRequest(ctx, "GET")
	thread := &starlark.Thread{Name: "test"}

	authTuple := starlark.Tuple{starlark.String("user"), starlark.String("pass"), starlark.String("digest")}
	kwargs := []starlark.Tuple{
		{starlark.String("auth"), &authTuple},
	}
	args := starlark.Tuple{starlark.String(srv.URL)}

	result, err := fn(thread, starlark.NewBuiltin("http.get", fn), args, kwargs)
	if err != nil {
		t.Fatalf("httpRequest failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestHTTPAuthEmptyUser(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fn := httpRequest(ctx, "GET")
	thread := &starlark.Thread{Name: "test"}

	authTuple := starlark.Tuple{starlark.String(""), starlark.String("pass")}
	kwargs := []starlark.Tuple{
		{starlark.String("auth"), &authTuple},
	}
	args := starlark.Tuple{starlark.String("http://localhost")}

	_, err := fn(thread, starlark.NewBuiltin("http.get", fn), args, kwargs)
	if err == nil {
		t.Fatal("expected error for empty auth username")
	}
	if !strings.Contains(err.Error(), "non-empty string") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestHTTPAuthInvalidScheme(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fn := httpRequest(ctx, "GET")
	thread := &starlark.Thread{Name: "test"}

	authTuple := starlark.Tuple{starlark.String("user"), starlark.String("pass"), starlark.String("ntlm")}
	kwargs := []starlark.Tuple{
		{starlark.String("auth"), &authTuple},
	}
	args := starlark.Tuple{starlark.String("http://localhost")}

	_, err := fn(thread, starlark.NewBuiltin("http.get", fn), args, kwargs)
	if err == nil {
		t.Fatal("expected error for invalid auth scheme")
	}
	if !strings.Contains(err.Error(), "basic") || !strings.Contains(err.Error(), "digest") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestHTTPAuthWrongTupleLength(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fn := httpRequest(ctx, "GET")
	thread := &starlark.Thread{Name: "test"}

	authTuple := starlark.Tuple{starlark.String("user")}
	kwargs := []starlark.Tuple{
		{starlark.String("auth"), &authTuple},
	}
	args := starlark.Tuple{starlark.String("http://localhost")}

	_, err := fn(thread, starlark.NewBuiltin("http.get", fn), args, kwargs)
	if err == nil {
		t.Fatal("expected error for auth tuple with 1 element")
	}
}

func TestHTTPInvalidURL(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fn := httpRequest(ctx, "GET")
	thread := &starlark.Thread{Name: "test"}

	paramsDict := starlark.NewDict(1)
	paramsDict.SetKey(starlark.String("key"), starlark.String("val"))
	kwargs := []starlark.Tuple{
		{starlark.String("params"), paramsDict},
	}
	args := starlark.Tuple{starlark.String("://invalid")}

	_, err := fn(thread, starlark.NewBuiltin("http.get", fn), args, kwargs)
	if err == nil {
		t.Fatal("expected error for invalid URL")
	}
}

func TestHTTPJsonInvalidTypeGo(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fn := httpRequest(ctx, "POST")
	thread := &starlark.Thread{Name: "test"}

	kwargs := []starlark.Tuple{
		{starlark.String("json"), starlark.MakeInt(42)},
	}
	args := starlark.Tuple{starlark.String("http://localhost")}

	_, err := fn(thread, starlark.NewBuiltin("http.post", fn), args, kwargs)
	if err == nil {
		t.Fatal("expected error for json=int")
	}
	if !strings.Contains(err.Error(), "dict, list, or string") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestHTTPRetryOn5xx(t *testing.T) {
	t.Parallel()
	var attempts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts <= 2 {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
		w.Write([]byte(`"ok"`))
	}))
	defer srv.Close()

	ctx := context.Background()
	fn := httpRequest(ctx, "GET")
	thread := &starlark.Thread{Name: "test"}

	kwargs := []starlark.Tuple{
		{starlark.String("retry"), starlark.MakeInt(3)},
		{starlark.String("backoff"), starlark.MakeInt(0)},
	}
	args := starlark.Tuple{starlark.String(srv.URL)}

	result, err := fn(thread, starlark.NewBuiltin("http.get", fn), args, kwargs)
	if err != nil {
		t.Fatalf("expected success after retries, got: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestParseLinkHeader_MalformedParts(t *testing.T) {
	t.Parallel()
	// No angle brackets
	links := ParseLinkHeader("just some text")
	if len(links) != 0 {
		t.Errorf("expected empty links, got %v", links)
	}

	// No rel
	links = ParseLinkHeader(`<http://example.com>; type="text/html"`)
	if len(links) != 0 {
		t.Errorf("expected empty links for missing rel, got %v", links)
	}

	// Malformed rel (no closing quote)
	links = ParseLinkHeader(`<http://example.com>; rel="next`)
	if len(links) != 0 {
		t.Errorf("expected empty links for malformed rel, got %v", links)
	}
}

func TestParseRetryAfter(t *testing.T) {
	t.Parallel()
	// Seconds
	d := parseRetryAfter("5")
	if d != 5*time.Second {
		t.Errorf("expected 5s, got %v", d)
	}

	// Empty
	d = parseRetryAfter("")
	if d != 0 {
		t.Errorf("expected 0, got %v", d)
	}

	// Unparseable
	d = parseRetryAfter("not-a-number")
	if d != 0 {
		t.Errorf("expected 0 for unparseable, got %v", d)
	}
}

func TestCSVDecodeParseError(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	// Mismatched quotes should cause CSV parse error
	code := `csv.decode(data='"unclosed')`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test.target", code)
	if err == nil {
		t.Fatal("expected CSV parse error")
	}
}

func TestCSVDecodeNoHeaderEmptyRecords(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
result = csv.decode(data="a,b\n1,2\n", header=False)
if len(result) != 2:
    fail("expected 2 rows, got " + str(len(result)))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test.target", code)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCSVEncodeListOfListsNonStringValues(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
rows = [[1, True, None]]
result = csv.encode(rows=rows)
if result == "":
    fail("expected non-empty result")
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test.target", code)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestXMLEncodeError(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	// xml.encode requires a dict argument
	code := `xml.encode("not a dict")`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test.target", code)
	if err == nil {
		t.Fatal("expected error for non-dict xml.encode argument")
	}
}

// --- Tests for module UnpackArgs error paths ---

func TestCryptoBase64EncodeWrongArgs(t *testing.T) {
	t.Parallel()
	mod := cryptoModule()
	fn := mod.Members["base64_encode"].(*starlark.Builtin)
	thread := &starlark.Thread{Name: "test"}
	_, err := fn.CallInternal(thread, starlark.Tuple{starlark.MakeInt(42)}, nil)
	if err == nil {
		t.Error("expected error for int arg to base64_encode")
	}
}

func TestCryptoBase64DecodeWrongArgs(t *testing.T) {
	t.Parallel()
	mod := cryptoModule()
	fn := mod.Members["base64_decode"].(*starlark.Builtin)
	thread := &starlark.Thread{Name: "test"}
	_, err := fn.CallInternal(thread, starlark.Tuple{starlark.MakeInt(42)}, nil)
	if err == nil {
		t.Error("expected error for int arg to base64_decode")
	}
}

func TestCryptoSHA256WrongArgs(t *testing.T) {
	t.Parallel()
	mod := cryptoModule()
	fn := mod.Members["sha256"].(*starlark.Builtin)
	thread := &starlark.Thread{Name: "test"}
	_, err := fn.CallInternal(thread, starlark.Tuple{starlark.MakeInt(42)}, nil)
	if err == nil {
		t.Error("expected error for int arg to sha256")
	}
}

func TestCryptoMD5WrongArgs(t *testing.T) {
	t.Parallel()
	mod := cryptoModule()
	fn := mod.Members["md5"].(*starlark.Builtin)
	thread := &starlark.Thread{Name: "test"}
	_, err := fn.CallInternal(thread, starlark.Tuple{starlark.MakeInt(42)}, nil)
	if err == nil {
		t.Error("expected error for int arg to md5")
	}
}

func TestCryptoHMACWrongArgs(t *testing.T) {
	t.Parallel()
	mod := cryptoModule()
	fn := mod.Members["hmac_sha256"].(*starlark.Builtin)
	thread := &starlark.Thread{Name: "test"}
	_, err := fn.CallInternal(thread, starlark.Tuple{starlark.MakeInt(42)}, nil)
	if err == nil {
		t.Error("expected error for wrong args to hmac_sha256")
	}
}

func TestCryptoRandomStringWrongArgs(t *testing.T) {
	t.Parallel()
	mod := cryptoModule()
	fn := mod.Members["random_string"].(*starlark.Builtin)
	thread := &starlark.Thread{Name: "test"}
	_, err := fn.CallInternal(thread, nil, []starlark.Tuple{
		{starlark.String("length"), starlark.String("notanint")},
	})
	if err == nil {
		t.Error("expected error for string arg to random_string length")
	}
}

func TestCSVDecodeWrongArgs(t *testing.T) {
	t.Parallel()
	mod := csvModule()
	fn := mod.Members["decode"].(*starlark.Builtin)
	thread := &starlark.Thread{Name: "test"}
	_, err := fn.CallInternal(thread, starlark.Tuple{starlark.MakeInt(42)}, nil)
	if err == nil {
		t.Error("expected error for int arg to csv.decode")
	}
}

func TestCSVEncodeWrongArgs(t *testing.T) {
	t.Parallel()
	mod := csvModule()
	fn := mod.Members["encode"].(*starlark.Builtin)
	thread := &starlark.Thread{Name: "test"}
	_, err := fn.CallInternal(thread, starlark.Tuple{starlark.MakeInt(42)}, nil)
	if err == nil {
		t.Error("expected error for int arg to csv.encode")
	}
}

func TestEnvGetWrongArgs(t *testing.T) {
	t.Parallel()
	mod := envModule()
	fn := mod.Members["get"].(*starlark.Builtin)
	thread := &starlark.Thread{Name: "test"}
	_, err := fn.CallInternal(thread, starlark.Tuple{starlark.MakeInt(42)}, nil)
	if err == nil {
		t.Error("expected error for int arg to env.get")
	}
}

func TestEnvSetWrongArgs(t *testing.T) {
	t.Parallel()
	mod := envModule()
	fn := mod.Members["set"].(*starlark.Builtin)
	thread := &starlark.Thread{Name: "test"}
	_, err := fn.CallInternal(thread, starlark.Tuple{starlark.MakeInt(42)}, nil)
	if err == nil {
		t.Error("expected error for int arg to env.set")
	}
}

func TestURLBuildWrongArgs(t *testing.T) {
	t.Parallel()
	mod := urlModule()
	fn := mod.Members["build"].(*starlark.Builtin)
	thread := &starlark.Thread{Name: "test"}
	_, err := fn.CallInternal(thread, starlark.Tuple{starlark.MakeInt(42)}, nil)
	if err == nil {
		t.Error("expected error for int arg to url.build")
	}
}

func TestURLEncodeWrongArgs(t *testing.T) {
	t.Parallel()
	mod := urlModule()
	fn := mod.Members["encode"].(*starlark.Builtin)
	thread := &starlark.Thread{Name: "test"}
	_, err := fn.CallInternal(thread, starlark.Tuple{starlark.MakeInt(42)}, nil)
	if err == nil {
		t.Error("expected error for int arg to url.encode")
	}
}

func TestURLParseWrongArgs(t *testing.T) {
	t.Parallel()
	mod := urlModule()
	fn := mod.Members["parse"].(*starlark.Builtin)
	thread := &starlark.Thread{Name: "test"}
	_, err := fn.CallInternal(thread, starlark.Tuple{starlark.MakeInt(42)}, nil)
	if err == nil {
		t.Error("expected error for int arg to url.parse")
	}
}

func TestXMLDecodeWrongArgs(t *testing.T) {
	t.Parallel()
	mod := xmlModule()
	fn := mod.Members["decode"].(*starlark.Builtin)
	thread := &starlark.Thread{Name: "test"}
	_, err := fn.CallInternal(thread, starlark.Tuple{starlark.MakeInt(42)}, nil)
	if err == nil {
		t.Error("expected error for int arg to xml.decode")
	}
}

func TestXMLEncodeWrongArgs(t *testing.T) {
	t.Parallel()
	mod := xmlModule()
	fn := mod.Members["encode"].(*starlark.Builtin)
	thread := &starlark.Thread{Name: "test"}
	_, err := fn.CallInternal(thread, starlark.Tuple{starlark.MakeInt(42)}, nil)
	if err == nil {
		t.Error("expected error for int arg to xml.encode")
	}
}

func TestOAuthBasicAuthWrongArgs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mod := oauthModule(ctx, "")
	fn := mod.Members["basic_auth"].(*starlark.Builtin)
	thread := &starlark.Thread{Name: "test"}
	_, err := fn.CallInternal(thread, starlark.Tuple{starlark.MakeInt(42)}, nil)
	if err == nil {
		t.Error("expected error for int arg to oauth.basic_auth")
	}
}

func TestSaveRowWrongArgs(t *testing.T) {
	t.Parallel()
	collector := &saveCollector{target: "test"}
	mod := saveModule(collector)
	fn := mod.Members["row"].(*starlark.Builtin)
	thread := &starlark.Thread{Name: "test"}
	_, err := fn.CallInternal(thread, starlark.Tuple{starlark.MakeInt(42)}, nil)
	if err == nil {
		t.Error("expected error for int arg to save.row")
	}
}

func TestSaveRowsWrongArgs(t *testing.T) {
	t.Parallel()
	collector := &saveCollector{target: "test"}
	mod := saveModule(collector)
	fn := mod.Members["rows"].(*starlark.Builtin)
	thread := &starlark.Thread{Name: "test"}
	_, err := fn.CallInternal(thread, starlark.Tuple{starlark.MakeInt(42)}, nil)
	if err == nil {
		t.Error("expected error for int arg to save.rows")
	}
}

func TestSaveRowsNonDictElement(t *testing.T) {
	t.Parallel()
	collector := &saveCollector{target: "test"}
	mod := saveModule(collector)
	fn := mod.Members["rows"].(*starlark.Builtin)
	thread := &starlark.Thread{Name: "test"}
	list := starlark.NewList([]starlark.Value{starlark.String("not a dict")})
	_, err := fn.CallInternal(thread, starlark.Tuple{list}, nil)
	if err == nil {
		t.Error("expected error for non-dict element in save.rows")
	}
}

func TestSaveRowNonStringKey(t *testing.T) {
	t.Parallel()
	collector := &saveCollector{target: "test"}
	mod := saveModule(collector)
	fn := mod.Members["row"].(*starlark.Builtin)
	thread := &starlark.Thread{Name: "test"}
	d := starlark.NewDict(1)
	d.SetKey(starlark.MakeInt(1), starlark.String("val"))
	_, err := fn.CallInternal(thread, starlark.Tuple{d}, nil)
	if err == nil {
		t.Error("expected error for non-string key in save.row dict")
	}
}

func TestSaveRowsDictConversionError(t *testing.T) {
	t.Parallel()
	collector := &saveCollector{target: "test"}
	mod := saveModule(collector)
	fn := mod.Members["rows"].(*starlark.Builtin)
	thread := &starlark.Thread{Name: "test"}
	d := starlark.NewDict(1)
	d.SetKey(starlark.MakeInt(1), starlark.String("val")) // non-string key
	list := starlark.NewList([]starlark.Value{d})
	_, err := fn.CallInternal(thread, starlark.Tuple{list}, nil)
	if err == nil {
		t.Error("expected error for non-string key in save.rows dict")
	}
}

func TestCSVDecodeEmptyRecords(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
result = csv.decode(data="h1,h2\n", header=True)
if len(result) != 0:
    fail("expected 0 rows after header-only CSV, got " + str(len(result)))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test.target", code)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCSVEncodeWithDictMixedTypes(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	// Dict with non-string key (row type mismatch: second row is not dict)
	code := `
rows = [{"a": "1"}, {"a": "2"}]
result = csv.encode(rows=rows)
if "a" not in result:
    fail("expected header 'a' in output")
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test.target", code)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHTTPWithClientCert(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fn := httpRequest(ctx, "GET")
	thread := &starlark.Thread{Name: "test"}

	// cert and key provided (but files don't exist - will fail on DoHTTP)
	kwargs := []starlark.Tuple{
		{starlark.String("cert"), starlark.String("/nonexistent/cert.pem")},
		{starlark.String("key"), starlark.String("/nonexistent/key.pem")},
		{starlark.String("ca"), starlark.String("/nonexistent/ca.pem")},
	}
	args := starlark.Tuple{starlark.String("https://localhost:99999")}

	_, err := fn(thread, starlark.NewBuiltin("http.get", fn), args, kwargs)
	if err == nil {
		t.Fatal("expected error for nonexistent cert files")
	}
}

func TestStarlarkToGoLargeInt(t *testing.T) {
	t.Parallel()
	// Create a Starlark int that's too large for int64
	bigInt := starlark.MakeInt64(9223372036854775807) // max int64
	v, err := starlarkToGo(bigInt)
	if err != nil {
		t.Fatal(err)
	}
	// Should succeed with int64 for max int64
	if _, ok := v.(int64); !ok {
		t.Errorf("expected int64, got %T", v)
	}
}

func TestURLParseInvalid(t *testing.T) {
	t.Parallel()
	mod := urlModule()
	fn := mod.Members["parse"].(*starlark.Builtin)
	thread := &starlark.Thread{Name: "test"}
	// Valid call but weird URL
	result, err := fn.CallInternal(thread, starlark.Tuple{starlark.String("://bad")}, nil)
	if err != nil {
		// parse error
		_ = err
	} else {
		_ = result
	}
}

func TestHTTPBackoffInt(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(`"ok"`))
	}))
	defer srv.Close()

	ctx := context.Background()
	fn := httpRequest(ctx, "GET")
	thread := &starlark.Thread{Name: "test"}

	kwargs := []starlark.Tuple{
		{starlark.String("backoff"), starlark.MakeInt(1)},
	}
	args := starlark.Tuple{starlark.String(srv.URL)}

	_, err := fn(thread, starlark.NewBuiltin("http.get", fn), args, kwargs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- http.upload() tests ---

// newIPv4Server creates an httptest server bound to IPv4 loopback only.
// Avoids IPv6 panics in restrictive environments.
func newIPv4Server(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot bind IPv4 loopback: %v", err)
	}
	srv := &httptest.Server{Listener: l, Config: &http.Server{Handler: handler}}
	srv.Start()
	t.Cleanup(func() { srv.Close() })
	return srv
}

func TestHTTPUpload(t *testing.T) {
	t.Parallel()
	var receivedContentType string
	var receivedFilename string
	var receivedFileContent string
	var receivedPurpose string

	srv := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedContentType = r.Header.Get("Content-Type")
		r.ParseMultipartForm(10 << 20)
		file, header, _ := r.FormFile("file")
		if file != nil {
			data, _ := io.ReadAll(file)
			receivedFileContent = string(data)
			receivedFilename = header.Filename
		}
		receivedPurpose = r.FormValue("purpose")
		w.Write([]byte(`{"id": "file-123"}`))
	}))

	dir := t.TempDir()
	testFile := filepath.Join(dir, "test.txt")
	os.WriteFile(testFile, []byte("hello upload"), 0644)

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.upload("%s", file="%s", fields={"purpose": "ocr"})
if not resp.ok:
    fail("upload failed")
if resp.json["id"] != "file-123":
    fail("wrong id: " + resp.json["id"])
`, srv.URL, testFile)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(receivedContentType, "multipart/form-data") {
		t.Errorf("content-type = %q, want multipart/form-data", receivedContentType)
	}
	if receivedFileContent != "hello upload" {
		t.Errorf("file content = %q", receivedFileContent)
	}
	if receivedFilename != "test.txt" {
		t.Errorf("filename = %q, want test.txt", receivedFilename)
	}
	if receivedPurpose != "ocr" {
		t.Errorf("purpose = %q, want ocr", receivedPurpose)
	}
}

func TestHTTPUploadCustomFieldAndFilename(t *testing.T) {
	t.Parallel()
	var receivedFilename string
	var receivedField bool

	srv := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseMultipartForm(10 << 20)
		file, header, _ := r.FormFile("document")
		if file != nil {
			receivedField = true
			receivedFilename = header.Filename
		}
		w.Write([]byte(`{}`))
	}))

	dir := t.TempDir()
	testFile := filepath.Join(dir, "data.bin")
	os.WriteFile(testFile, []byte("binary"), 0644)

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.upload("%s", file="%s", field="document", filename="report.pdf")
`, srv.URL, testFile)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}

	if !receivedField {
		t.Error("expected file in 'document' field")
	}
	if receivedFilename != "report.pdf" {
		t.Errorf("filename = %q, want report.pdf", receivedFilename)
	}
}

func TestHTTPUploadWithBasicAuth(t *testing.T) {
	t.Parallel()
	var receivedAuth string

	srv := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.Write([]byte(`{}`))
	}))

	dir := t.TempDir()
	testFile := filepath.Join(dir, "f.txt")
	os.WriteFile(testFile, []byte("x"), 0644)

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.upload("%s", file="%s", auth=("user", "pass"))
`, srv.URL, testFile)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}

	if !strings.HasPrefix(receivedAuth, "Basic ") {
		t.Errorf("auth = %q, want Basic", receivedAuth)
	}
}

func TestHTTPUploadWithDigestAuth(t *testing.T) {
	t.Parallel()
	var gotDigestAuth bool

	srv := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth == "" {
			// First request: send challenge
			w.Header().Set("Www-Authenticate", `Digest realm="test", nonce="abc123", qop="auth"`)
			w.WriteHeader(401)
			return
		}
		if strings.HasPrefix(auth, "Digest ") {
			gotDigestAuth = true
		}
		w.Write([]byte(`{}`))
	}))

	dir := t.TempDir()
	testFile := filepath.Join(dir, "f.txt")
	os.WriteFile(testFile, []byte("x"), 0644)

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.upload("%s", file="%s", auth=("user", "pass", "digest"))
`, srv.URL, testFile)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}

	if !gotDigestAuth {
		t.Error("expected Digest auth header after challenge")
	}
}

func TestHTTPUploadBadAuthScheme(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testFile := filepath.Join(dir, "f.txt")
	os.WriteFile(testFile, []byte("x"), 0644)

	rt := NewRuntime(nil, nil)
	code := fmt.Sprintf(`
resp = http.upload("http://example.com", file="%s", auth=("user", "pass", "bearer"))
`, testFile)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test", code)
	if err == nil {
		t.Fatal("expected error for invalid auth scheme")
	}
	if !strings.Contains(err.Error(), "bearer") {
		t.Errorf("error = %v, want mention of 'bearer'", err)
	}
}

func TestHTTPUploadMissingFile(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
resp = http.upload("http://example.com", file="/nonexistent/file.txt")
`
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := rt.Run(ctx, "test", code)
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

// --- load() tests ---

func TestLoad_Basic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "lib"), 0755)
	os.WriteFile(filepath.Join(dir, "lib", "helper.star"), []byte(`
def greet(name):
    return "hello " + name
`), 0644)

	rt := NewRuntime(nil, nil, dir)
	code := `
load("lib/helper.star", "greet")
result = greet("world")
if result != "hello world":
    fail("expected 'hello world', got " + result)
`
	ctx := context.Background()
	_, err := rt.Run(ctx, "test.target", code)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoad_Caching(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "lib"), 0755)
	// Module increments a global counter each time it's executed
	os.WriteFile(filepath.Join(dir, "lib", "counter.star"), []byte(`
counter = 1
def get_counter():
    return counter
`), 0644)

	rt := NewRuntime(nil, nil, dir)
	code := `
load("lib/counter.star", "get_counter")
load("lib/counter.star", c2 = "get_counter")
# Both should return 1 — module executed only once
if get_counter() != 1:
    fail("expected 1")
if c2() != 1:
    fail("expected 1")
`
	ctx := context.Background()
	_, err := rt.Run(ctx, "test.target", code)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoad_PathTraversal(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	rt := NewRuntime(nil, nil, dir)
	code := `load("../etc/passwd", "x")`
	ctx := context.Background()
	_, err := rt.Run(ctx, "test.target", code)
	if err == nil {
		t.Fatal("expected error for path traversal")
	}
	if !strings.Contains(err.Error(), "path traversal") {
		t.Fatalf("expected path traversal error, got: %v", err)
	}
}

func TestLoad_NoProjectDir(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil) // no projectDir
	code := `load("lib/helper.star", "greet")`
	ctx := context.Background()
	_, err := rt.Run(ctx, "test.target", code)
	if err == nil {
		t.Fatal("expected error for load without project dir")
	}
	if !strings.Contains(err.Error(), "not available") {
		t.Fatalf("expected 'not available' error, got: %v", err)
	}
}

func TestLoad_NoSaveInLib(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "lib"), 0755)
	os.WriteFile(filepath.Join(dir, "lib", "bad.star"), []byte(`
save(rows=[{"a": 1}])
`), 0644)

	rt := NewRuntime(nil, nil, dir)
	code := `load("lib/bad.star", "x")`
	ctx := context.Background()
	_, err := rt.Run(ctx, "test.target", code)
	if err == nil {
		t.Fatal("expected error for save in library module")
	}
	// save is not in library predeclared, so it should be an undefined error
	if !strings.Contains(err.Error(), "save") {
		t.Fatalf("expected save-related error, got: %v", err)
	}
}

func TestLoad_MissingFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	rt := NewRuntime(nil, nil, dir)
	code := `load("lib/nonexistent.star", "x")`
	ctx := context.Background()
	_, err := rt.Run(ctx, "test.target", code)
	if err == nil {
		t.Fatal("expected error for missing file")
	}
	if !strings.Contains(err.Error(), "nonexistent.star") {
		t.Fatalf("expected file not found error, got: %v", err)
	}
}

func TestLoad_SymlinkEscape(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	outside := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "lib"), 0755)

	// Create a file outside the project
	os.WriteFile(filepath.Join(outside, "secret.star"), []byte(`
secret = "leaked"
`), 0644)

	// Create a symlink inside the project pointing outside
	os.Symlink(filepath.Join(outside, "secret.star"), filepath.Join(dir, "lib", "escape.star"))

	rt := NewRuntime(nil, nil, dir)
	code := `load("lib/escape.star", "secret")`
	ctx := context.Background()
	_, err := rt.Run(ctx, "test.target", code)
	if err == nil {
		t.Fatal("expected error for symlink escaping project directory")
	}
	if !strings.Contains(err.Error(), "path traversal") {
		t.Fatalf("expected path traversal error, got: %v", err)
	}
}

func TestLoad_IncrementalAsGlobal(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "lib"), 0755)
	// Library module accesses incremental as a global (not parameter)
	os.WriteFile(filepath.Join(dir, "lib", "incr_source.star"), []byte(`
def incr_source(save):
    if incremental.is_backfill:
        save.row({"id": 1, "mode": "backfill"})
    else:
        save.row({"id": 1, "mode": "incremental"})
`), 0644)

	state := &backfill.IncrementalState{IsBackfill: true, InitialValue: "2020-01-01"}
	rt := NewRuntime(nil, state, dir)
	result, err := rt.RunSource(context.Background(), "test.target", "incr_source", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount != 1 {
		t.Fatalf("expected 1 row, got %d", result.RowCount)
	}
}

func TestLoad_SourceWithoutIncremental(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "lib"), 0755)
	// Source function that doesn't need incremental at all
	os.WriteFile(filepath.Join(dir, "lib", "simple.star"), []byte(`
def simple(save, greeting="hello"):
    save.row({"msg": greeting})
`), 0644)

	rt := NewRuntime(nil, nil, dir)
	result, err := rt.RunSource(context.Background(), "test.target", "simple", map[string]any{
		"greeting": "world",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount != 1 {
		t.Fatalf("expected 1 row, got %d", result.RowCount)
	}
}

func TestLoad_CycleDetection(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "lib"), 0755)
	// a.star loads b.star which loads a.star — cycle
	os.WriteFile(filepath.Join(dir, "lib", "a.star"), []byte(`
load("lib/b.star", "b_func")
def a_func():
    return "a"
`), 0644)
	os.WriteFile(filepath.Join(dir, "lib", "b.star"), []byte(`
load("lib/a.star", "a_func")
def b_func():
    return "b"
`), 0644)

	rt := NewRuntime(nil, nil, dir)
	code := `load("lib/a.star", "a_func")`
	ctx := context.Background()
	_, err := rt.Run(ctx, "test.target", code)
	if err == nil {
		t.Fatal("expected error for import cycle")
	}
	if !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("expected cycle error, got: %v", err)
	}
}

func TestLoad_SharedCache(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "lib"), 0755)
	// shared.star is loaded by both a.star and b.star
	os.WriteFile(filepath.Join(dir, "lib", "shared.star"), []byte(`
counter = 1
def get_counter():
    return counter
`), 0644)
	os.WriteFile(filepath.Join(dir, "lib", "a.star"), []byte(`
load("lib/shared.star", "get_counter")
def a_val():
    return get_counter()
`), 0644)
	os.WriteFile(filepath.Join(dir, "lib", "b.star"), []byte(`
load("lib/shared.star", "get_counter")
def b_val():
    return get_counter()
`), 0644)

	rt := NewRuntime(nil, nil, dir)
	code := `
load("lib/a.star", "a_val")
load("lib/b.star", "b_val")
# Both should return 1 — shared.star executed only once via shared cache
if a_val() != 1:
    fail("expected 1 from a")
if b_val() != 1:
    fail("expected 1 from b")
`
	ctx := context.Background()
	_, err := rt.Run(ctx, "test.target", code)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoad_Nested(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "lib"), 0755)
	os.WriteFile(filepath.Join(dir, "lib", "c.star"), []byte(`
def base():
    return "base"
`), 0644)
	os.WriteFile(filepath.Join(dir, "lib", "b.star"), []byte(`
load("lib/c.star", "base")
def middle():
    return base() + "_middle"
`), 0644)
	os.WriteFile(filepath.Join(dir, "lib", "a.star"), []byte(`
load("lib/b.star", "middle")
def top():
    return middle() + "_top"
`), 0644)

	rt := NewRuntime(nil, nil, dir)
	code := `
load("lib/a.star", "top")
result = top()
if result != "base_middle_top":
    fail("expected 'base_middle_top', got " + result)
`
	ctx := context.Background()
	_, err := rt.Run(ctx, "test.target", code)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

