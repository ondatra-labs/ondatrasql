// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package httpcassette

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMaskString(t *testing.T) {
	// printf %s ada@example.com | sha256sum
	if got := MaskString("ada@example.com"); got != "masked-b5fc85e5" {
		t.Fatalf("MaskString = %q", got)
	}
	if MaskString("a") == MaskString("b") {
		t.Fatal("placeholders must keep distinct values distinct")
	}
}

func TestMaskJSON(t *testing.T) {
	mask := parseMask(" Email, rate ,active,,user")
	in := `{"id":7,"email":"ada@x.io","EMAIL":"ada@x.io","rate":12.50,"active":true,"gone":null,` +
		`"list":[{"email":"bo@x.io","rate":-3,"active":false,"name":"Bo"}],` +
		`"user":{"name":"Cy","age":40,"admin":true,"tags":["a",1,null]},"keep":{"active_x":true}}`
	out, err := maskJSON([]byte(in), mask)
	if err != nil {
		t.Fatal(err)
	}
	ada, bo, cy, a := MaskString("ada@x.io"), MaskString("bo@x.io"), MaskString("Cy"), MaskString("a")
	want := `{"id":7,"email":"` + ada + `","EMAIL":"` + ada + `","rate":0,"active":false,"gone":null,` +
		`"list":[{"email":"` + bo + `","rate":0,"active":false,"name":"Bo"}],` +
		`"user":{"name":"` + cy + `","age":0,"admin":false,"tags":["` + a + `",0,null]},"keep":{"active_x":true}}`
	if string(out) != want {
		t.Fatalf("got  %s\nwant %s", out, want)
	}
	if _, err := maskJSON([]byte(`{"a":1} 2`), mask); err == nil {
		t.Fatal("trailing data accepted")
	}
	if parseMask(" , ") != nil {
		t.Fatal("an empty list must mask nothing")
	}
}

// Recording with ONDATRA_HTTP_MASK writes masked response bodies, hands
// the caller the real one, leaves requests and non-JSON bodies alone,
// and replays the masked body.
func TestRecord_MaskedResponse(t *testing.T) {
	dir := t.TempDir()
	const resp = `{"users":[{"id":1,"email":"ada@x.io","hourly_rate_in_cents":12345,"admin":true,"access_token":"tok-abcdefgh"},` +
		`{"id":2,"email":"bo@x.io","hourly_rate_in_cents":6789,"admin":true}],"owner":{"email":"ada@x.io"}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/text" {
			w.Header().Set("Content-Type", "text/csv")
			fmt.Fprint(w, "email\nada@x.io\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, resp)
	}))
	defer srv.Close()
	ctx := WithScope(context.Background(), "raw.users", "users")

	setMode(t, dir, "record")
	t.Setenv(EnvMask, "email,hourly_rate_in_cents,admin")
	Reset()
	_, got, err := do(t, ctx, "POST", srv.URL+"/users", "application/json", `{"email":"req@x.io"}`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != resp {
		t.Fatalf("record must hand the caller the real body, got %s", got)
	}
	if _, _, err := do(t, ctx, "GET", srv.URL+"/text", "", "", nil); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(filepath.Join(dir, "raw.users", "users.json"))
	if err != nil {
		t.Fatal(err)
	}
	cas := string(data)
	for _, gone := range []string{`"ada@x.io"`, "bo@x.io", "12345", "6789", "true", "tok-abcdefgh"} {
		if strings.Contains(cas, gone) {
			t.Errorf("cassette keeps %s:\n%s", gone, cas)
		}
	}
	for _, kept := range []string{
		`"email": "req@x.io"`,          // request bodies are not masked
		`"body": "email\nada@x.io\n"`,  // nor non-JSON responses
		`"access_token": "[REDACTED]"`, // credentials still win
		`"hourly_rate_in_cents": 0`,
		`"admin": false`,
	} {
		if !strings.Contains(cas, kept) {
			t.Errorf("cassette lacks %s:\n%s", kept, cas)
		}
	}
	if n := strings.Count(cas, `"email": "`+MaskString("ada@x.io")+`"`); n != 2 {
		t.Errorf("ada's two emails gave %d equal placeholders, want 2:\n%s", n, cas)
	}

	// Replay serves the masked body; ONDATRA_HTTP_MASK plays no part.
	srv.Close()
	setMode(t, dir, "replay")
	_, got, err = do(t, ctx, "POST", srv.URL+"/users", "application/json", `{"email":"req@x.io"}`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, "ada@x.io") || !strings.Contains(got, MaskString("bo@x.io")) {
		t.Fatalf("replayed %s", got)
	}
}
