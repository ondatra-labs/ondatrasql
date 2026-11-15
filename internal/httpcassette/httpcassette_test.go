// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package httpcassette

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// setMode points the package at dir in mode ("" for off) and resets it.
func setMode(t *testing.T, dir, mode string) {
	t.Helper()
	t.Setenv(EnvDir, dir)
	t.Setenv(EnvMode, mode)
	Reset()
	t.Cleanup(Reset)
}

func do(t *testing.T, ctx context.Context, method, rawURL, contentType, body string, header http.Header) (*http.Response, string, error) {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, r)
	if err != nil {
		t.Fatal(err)
	}
	for k, vs := range header {
		req.Header[k] = vs
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	client := &http.Client{Transport: Transport(nil)}
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = resp.Body.Close() }() // test response body
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, string(b), nil
}

func TestOff_TransportIsBase(t *testing.T) {
	setMode(t, "", "")
	base := &http.Transport{}
	if got := Transport(base); got != base {
		t.Fatalf("Transport with no env set = %T, want the base transport itself", got)
	}
	if got := Transport(nil); got != nil {
		t.Fatalf("Transport(nil) with no env set = %T, want nil", got)
	}
	if Current() != Off {
		t.Fatal("mode should be off")
	}
	RegisterSecret("would-be-secret")
	if len(secretList()) != 0 {
		t.Fatal("RegisterSecret stored a value with the package off")
	}
}

func TestConfigErrors(t *testing.T) {
	for _, tc := range []struct{ dir, mode, want string }{
		{"", "replay", "ONDATRA_HTTP_CASSETTE"},
		{"/x", "", "ONDATRA_HTTP_MODE"},
		{"/x", "playback", "record or replay"},
	} {
		setMode(t, tc.dir, tc.mode)
		err := Validate()
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("dir=%q mode=%q: err=%v, want it to mention %q", tc.dir, tc.mode, err, tc.want)
		}
		// A broken configuration fails requests rather than going live.
		_, _, err = do(t, context.Background(), "GET", "http://127.0.0.1:1/", "", "", nil)
		if err == nil || !strings.Contains(err.Error(), "http cassette") {
			t.Errorf("dir=%q mode=%q: request err=%v, want a cassette error", tc.dir, tc.mode, err)
		}
	}
}

func TestRecordReplay_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Page", fmt.Sprint(n))
		fmt.Fprintf(w, `{"n":%d,"path":%q,"echo":%q,"z":1,"a":2}`, n, r.URL.Path, body)
	}))
	ctx := WithScope(context.Background(), "raw.orders", "orders")

	setMode(t, dir, "record")
	var recorded []string
	for _, call := range []struct{ method, url, ct, body string }{
		{"GET", srv.URL + "/items?b=2&a=1", "", ""},
		{"GET", srv.URL + "/items?b=2&a=1", "", ""}, // identical: replays in order
		{"POST", srv.URL + "/items", "application/json", `{"x":1,"y":[1,2]}`},
	} {
		_, body, err := do(t, ctx, call.method, call.url, call.ct, call.body, nil)
		if err != nil {
			t.Fatal(err)
		}
		recorded = append(recorded, body)
	}
	srv.Close()

	path := filepath.Join(dir, "raw.orders", "orders.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Pretty-printed with the API's key order kept.
	if !strings.Contains(string(data), "\n  \"interactions\": [") || !strings.Contains(string(data), `"z": 1,`) {
		t.Fatalf("cassette not pretty-printed as expected:\n%s", data)
	}

	setMode(t, dir, "replay")
	var replayed []string
	for _, call := range []struct{ method, url, ct, body string }{
		{"GET", srv.URL + "/items?a=1&b=2", "", ""}, // query order differs
		{"GET", srv.URL + "/items?b=2&a=1", "", ""},
		{"POST", srv.URL + "/items", "application/json", `{"y":[1,2],  "x":1}`}, // key order and spacing differ
	} {
		resp, body, err := do(t, ctx, call.method, call.url, call.ct, call.body, nil)
		if err != nil {
			t.Fatalf("replay %s %s: %v", call.method, call.url, err)
		}
		if resp.Header.Get("X-Page") == "" {
			t.Error("response headers not replayed")
		}
		replayed = append(replayed, body)
	}
	for i := range recorded {
		if recorded[i] != replayed[i] {
			t.Errorf("interaction %d: recorded %s, replayed %s", i, recorded[i], replayed[i])
		}
	}

	// A third identical GET: the recording had only two.
	_, _, err = do(t, ctx, "GET", srv.URL+"/items?a=1&b=2", "", "", nil)
	if err == nil || !strings.Contains(err.Error(), "already replayed") {
		t.Fatalf("extra request: err=%v, want an already-replayed error", err)
	}
}

func TestReplay_UnmatchedNamesRequestAndNearest(t *testing.T) {
	dir := t.TempDir()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok")) // test server
	}))
	defer srv.Close()
	ctx := WithScope(context.Background(), "m", "l")
	t.Setenv("ONDATRA_NOW", "2026-01-01T00:00:00Z")
	setMode(t, dir, "record")
	if _, _, err := do(t, ctx, "GET", srv.URL+"/v1/orders?since=2026-01-01", "", "", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := do(t, ctx, "GET", srv.URL+"/v1/customers", "", "", nil); err != nil {
		t.Fatal(err)
	}

	t.Setenv("ONDATRA_NOW", "2026-02-01T00:00:00Z")
	setMode(t, dir, "replay")
	_, _, err := do(t, ctx, "POST", srv.URL+"/v1/orders?since=2026-02-01", "application/json", `{"a":1}`, nil)
	if err == nil {
		t.Fatal("unmatched request was answered")
	}
	msg := err.Error()
	for _, want := range []string{
		"no recorded interaction for POST " + srv.URL + "/v1/orders?since=2026-02-01",
		"body sha256:",
		"nearest recorded: #1 GET " + srv.URL + "/v1/orders?since=2026-01-01",
		`recorded with ONDATRA_NOW="2026-01-01T00:00:00Z"`,
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q\nlacks %q", msg, want)
		}
	}

	// No cassette at all for a scope.
	_, _, err = do(t, WithScope(context.Background(), "m", "other"), "GET", srv.URL, "", "", nil)
	if err == nil || !strings.Contains(err.Error(), "record it first") {
		t.Fatalf("missing cassette: err=%v", err)
	}
}

func TestReplay_NoNetwork(t *testing.T) {
	dir := t.TempDir()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte(`{}`)) // test server
	}))
	defer srv.Close()
	ctx := WithScope(context.Background(), "m", "l")
	setMode(t, dir, "record")
	if _, _, err := do(t, ctx, "GET", srv.URL, "", "", nil); err != nil {
		t.Fatal(err)
	}
	setMode(t, dir, "replay")
	before := hits.Load()
	if _, _, err := do(t, ctx, "GET", srv.URL, "", "", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := do(t, ctx, "GET", srv.URL+"/unrecorded", "", "", nil); err == nil {
		t.Fatal("unrecorded request answered")
	}
	if hits.Load() != before {
		t.Fatalf("replay reached the server (%d hits)", hits.Load()-before)
	}
}

func TestRecord_SecretsNeverWritten(t *testing.T) {
	dir := t.TempDir()
	const (
		apiKey       = "sk_live_ABCDEF123456"
		headerKey    = "hdr-secret-998877"
		clientSecret = "cs/with+chars=&?"
		accessToken  = "ya29.access-token-xyz"
		refreshToken = "1//refresh-token-abc"
		cookie       = "session=cookie-value-5544"
		basicPass    = "hunter2-pass"
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Set-Cookie", cookie)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/token" {
			fmt.Fprintf(w, `{"access_token":%q,"refresh_token":%q,"expires_in":3600}`, accessToken, refreshToken)
			return
		}
		// An API that echoes what it got.
		body, _ := io.ReadAll(r.Body)
		fmt.Fprintf(w, `{"echo":%q,"auth":%q}`, body, r.Header.Get("Authorization"))
	}))
	defer srv.Close()

	setMode(t, dir, "record")
	RegisterSecret(apiKey)
	RegisterSecret(headerKey)
	RegisterSecret(clientSecret)
	RegisterSecret(basicPass)
	ctx := WithAuthNames(WithScope(context.Background(), "m", "l"), "X-Custom-Auth", "k")

	form := url.Values{"grant_type": {"refresh_token"}, "client_id": {"id"}, "client_secret": {clientSecret}, "refresh_token": {refreshToken}}
	if _, _, err := do(t, ctx, "POST", srv.URL+"/token", "application/x-www-form-urlencoded", form.Encode(), nil); err != nil {
		t.Fatal(err)
	}
	jwtForm := url.Values{"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"}, "assertion": {"eyJhbGciOi.payload.signature"}}
	if _, _, err := do(t, ctx, "POST", srv.URL+"/token", "application/x-www-form-urlencoded", jwtForm.Encode(), nil); err != nil {
		t.Fatal(err)
	}
	hdr := http.Header{
		"Authorization": {"Bearer " + accessToken},
		"X-Custom-Auth": {headerKey},
		"Cookie":        {cookie},
	}
	if _, _, err := do(t, ctx, "GET", srv.URL+"/data?k="+url.QueryEscape(apiKey)+"&page=2", "", "", hdr); err != nil {
		t.Fatal(err)
	}
	body := fmt.Sprintf(`{"password":%q,"note":"key is %s"}`, basicPass, apiKey)
	if _, _, err := do(t, ctx, "POST", srv.URL+"/data", "application/json", body, http.Header{"Authorization": {BasicAuthForTest("u", basicPass)}}); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(filepath.Join(dir, "m", "l.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{apiKey, headerKey, clientSecret, url.QueryEscape(clientSecret), accessToken, refreshToken, "cookie-value-5544", basicPass, "eyJhbGciOi", strings.TrimPrefix(BasicAuthForTest("u", basicPass), "Basic ")} {
		if strings.Contains(string(data), s) {
			t.Errorf("cassette contains secret %q:\n%s", s, data)
		}
	}
	if !strings.Contains(string(data), Placeholder) {
		t.Error("no placeholder in cassette")
	}
	// Non-secret data survives.
	if !strings.Contains(string(data), "page=2") || !strings.Contains(string(data), `"expires_in": 3600`) {
		t.Errorf("redaction removed non-secret data:\n%s", data)
	}

	// The redacted cassette still replays: credentials are absent now
	// (no env in CI), and the auth parameter is ignored for matching.
	setMode(t, dir, "replay")
	ctx = WithAuthNames(WithScope(context.Background(), "m", "l"), "X-Custom-Auth", "k")
	if _, _, err := do(t, ctx, "POST", srv.URL+"/token", "application/x-www-form-urlencoded", url.Values{"grant_type": {"refresh_token"}, "client_id": {"id"}, "client_secret": {""}, "refresh_token": {""}}.Encode(), nil); err != nil {
		t.Fatalf("replay token request without secrets: %v", err)
	}
	if _, _, err := do(t, ctx, "POST", srv.URL+"/token", "application/x-www-form-urlencoded", url.Values{"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"}, "assertion": {"other.jwt.value"}}.Encode(), nil); err != nil {
		t.Fatalf("replay jwt exchange with a different assertion: %v", err)
	}
	if _, _, err := do(t, ctx, "GET", srv.URL+"/data?page=2", "", "", nil); err != nil {
		t.Fatalf("replay without the auth param: %v", err)
	}
	if _, _, err := do(t, ctx, "POST", srv.URL+"/data", "application/json", `{"password":"","note":"key is [REDACTED]"}`, nil); err != nil {
		t.Fatalf("replay json body with empty password: %v", err)
	}
}

// BasicAuthForTest builds a Basic Authorization header value.
func BasicAuthForTest(user, pass string) string {
	req, _ := http.NewRequest("GET", "http://x", nil)
	req.SetBasicAuth(user, pass)
	return req.Header.Get("Authorization")
}

func TestRecord_TransportErrorReplays(t *testing.T) {
	dir := t.TempDir()
	ctx := WithScope(context.Background(), "m", "l")
	setMode(t, dir, "record")
	// Nothing listens on port 1.
	_, _, recErr := do(t, ctx, "GET", "http://127.0.0.1:1/x", "", "", nil)
	if recErr == nil {
		t.Fatal("expected a connection error")
	}
	setMode(t, dir, "replay")
	_, _, repErr := do(t, ctx, "GET", "http://127.0.0.1:1/x", "", "", nil)
	if repErr == nil || !strings.Contains(repErr.Error(), "connect") {
		t.Fatalf("replayed error = %v, want the recorded connection error", repErr)
	}
}

func TestEncodeBody(t *testing.T) {
	for _, tc := range []struct {
		in, enc string
	}{
		{`{"b":1, "a":[true,null]}`, encJSON},
		{"plain text, åäö", encText},
		{"\xff\xfe\x00binary", encBase64},
		{"", ""},
	} {
		enc, raw := encodeBody([]byte(tc.in))
		if enc != tc.enc {
			t.Errorf("%q: encoding %q, want %q", tc.in, enc, tc.enc)
		}
		back, err := decodeBody(enc, raw)
		if err != nil {
			t.Fatal(err)
		}
		want := tc.in
		if enc == encJSON {
			want = `{"b":1,"a":[true,null]}` // replayed compacted
		}
		if string(back) != want {
			t.Errorf("%q: round trip gave %q", tc.in, back)
		}
	}
}

func TestRedactJSON_KeepsOrderAndNumbers(t *testing.T) {
	in := `{"z":1.50,"access_token":"abc","nested":{"client_secret":{"x":1},"keep":"v"},"list":[{"password":"p"}],"big":12345678901234567890}`
	out, err := redactJSON([]byte(in))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"z":1.50,"access_token":"[REDACTED]","nested":{"client_secret":"[REDACTED]","keep":"v"},"list":[{"password":"[REDACTED]"}],"big":12345678901234567890}`
	if string(out) != want {
		t.Fatalf("got  %s\nwant %s", out, want)
	}
}

func TestCanonical(t *testing.T) {
	if a, b := canonicalURL("HTTPS://API.example.com/x?b=2&a=1&a=0#frag"), canonicalURL("https://api.example.com/x?a=0&a=1&b=2"); a != b {
		t.Errorf("urls differ: %s vs %s", a, b)
	}
	if a, b := canonicalURL("https://h/x?k=%5BREDACTED%5D&p=1"), canonicalURL("https://h/x?p=1"); a != b {
		t.Errorf("placeholder param not dropped: %s vs %s", a, b)
	}
	if a, b := canonicalBody([]byte(`{"a":1,"b":{"d":2,"c":3}}`), ""), canonicalBody([]byte(`{"b":{"c":3,"d":2},"a":1}`), ""); a != b {
		t.Errorf("json bodies differ: %s vs %s", a, b)
	}
	if a, b := canonicalBody([]byte("b=2&a=1"), "application/x-www-form-urlencoded"), canonicalBody([]byte("a=1&b=2"), "application/x-www-form-urlencoded; charset=utf-8"); a != b {
		t.Errorf("form bodies differ: %s vs %s", a, b)
	}
	mp := func(boundary string) string {
		return canonicalBody([]byte("--"+boundary+"\r\ncontent\r\n--"+boundary+"--"), "multipart/form-data; boundary="+boundary)
	}
	if mp("abc123") != mp("zzz999") {
		t.Error("multipart bodies with different boundaries differ")
	}
	if canonicalBody([]byte(`{"a":1}`), "") == canonicalBody([]byte(`{"a":2}`), "") {
		t.Error("different json bodies compare equal")
	}
}

func TestCassettePath(t *testing.T) {
	got := cassettePath("/c", scope{model: "raw.orders", lib: "../../etc/passwd"})
	if got != filepath.Join("/c", "raw.orders", ".._.._etc_passwd.json") {
		t.Fatalf("path = %s", got)
	}
	if got := cassettePath("/c", scopeOf(context.Background())); got != filepath.Join("/c", "_global", "_global.json") {
		t.Fatalf("unscoped path = %s", got)
	}
	if got := safeName(".."); got != "_.." {
		t.Fatalf("safeName(..) = %s", got)
	}
}

func TestFileFormat_StableAndVersioned(t *testing.T) {
	f := &File{Version: formatVersion, Model: "m", Lib: "l", Interactions: []Interaction{{
		Request:  Request{Method: "GET", URL: "https://h/?a=<b>&c", Headers: map[string][]string{"B": {"2"}, "A": {"1"}}},
		Response: &Response{Status: 200, BodyEncoding: encJSON, Body: json.RawMessage(`{"k":"<v>"}`)},
	}}}
	a, err := marshalFile(f)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := marshalFile(f)
	if string(a) != string(b) {
		t.Fatal("marshal not stable")
	}
	s := string(a)
	if strings.Index(s, `"A"`) > strings.Index(s, `"B"`) || strings.Contains(s, `\u003c`) {
		t.Fatalf("header keys unsorted or HTML-escaped:\n%s", s)
	}
	path := filepath.Join(t.TempDir(), "x.json")
	if err := os.WriteFile(path, []byte(strings.Replace(s, `"version": 1`, `"version": 99`, 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readFile(path); err == nil || !strings.Contains(err.Error(), "re-record") {
		t.Fatalf("version mismatch: err=%v", err)
	}
}

func TestSummary_CountsServedInteractions(t *testing.T) {
	dir := t.TempDir()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, r.URL.Path)
	}))
	defer srv.Close()
	orders := WithScope(context.Background(), "raw.orders", "orders")
	users := WithScope(context.Background(), "raw.users", "users")

	setMode(t, dir, "record")
	for _, p := range []string{"/a", "/b", "/c"} {
		if _, _, err := do(t, orders, "GET", srv.URL+p, "", "", nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := do(t, users, "GET", srv.URL+"/u", "", "", nil); err != nil {
		t.Fatal(err)
	}
	if s := Summary(); s != nil {
		t.Fatalf("Summary while recording = %v, want nil", s)
	}

	setMode(t, dir, "replay")
	if s := Summary(); len(s) != 0 {
		t.Fatalf("Summary before any request = %v, want empty", s)
	}
	for _, p := range []string{"/b", "/a"} {
		if _, _, err := do(t, orders, "GET", srv.URL+p, "", "", nil); err != nil {
			t.Fatal(err)
		}
	}
	got := fmt.Sprint(Summary())
	if want := "[{raw.orders/orders.json 2 3}]"; got != want {
		t.Fatalf("Summary = %s, want %s", got, want)
	}

	setMode(t, dir, "")
	if s := Summary(); s != nil {
		t.Fatalf("Summary when off = %v, want nil", s)
	}
}
