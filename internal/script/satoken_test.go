// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/httpcassette"
	"go.starlark.net/starlark"
)

// fakeGoogle is a token endpoint and an API behind one server. The token
// endpoint hands out tok-1, tok-2, ...; the API answers 401 to any token
// in reject and otherwise echoes the token it saw.
type fakeGoogle struct {
	srv       *httptest.Server
	exchanges atomic.Int32
	requests  atomic.Int32
	expiresIn int
	mu        sync.Mutex
	reject    map[string]bool
	scopes    []string
}

func newFakeGoogle(t *testing.T) *fakeGoogle {
	t.Helper()
	f := &fakeGoogle{expiresIn: 3600, reject: map[string]bool{}}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/token" {
			if err := r.ParseForm(); err == nil {
				f.mu.Lock()
				f.scopes = append(f.scopes, jwtScope(r.PostForm.Get("assertion")))
				f.mu.Unlock()
			}
			n := f.exchanges.Add(1)
			// Slow enough that parallel callers overlap with it.
			time.Sleep(20 * time.Millisecond)
			fmt.Fprintf(w, `{"access_token":"tok-%d","expires_in":%d,"token_type":"Bearer"}`, n, f.expiresIn)
			return
		}
		f.requests.Add(1)
		tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		f.mu.Lock()
		rejected := f.reject[tok]
		f.mu.Unlock()
		if rejected {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"error":"invalid token"}`)
			return
		}
		fmt.Fprintf(w, `{"page":%q,"token":%q}`, r.URL.Query().Get("page"), tok)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeGoogle) rejectToken(tok string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reject[tok] = true
}

// jwtScope reads the scope claim of an unverified JWT.
func jwtScope(jwt string) string {
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		return ""
	}
	var claims struct {
		Scope string `json:"scope"`
	}
	b, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || json.Unmarshal(b, &claims) != nil {
		return ""
	}
	return claims.Scope
}

// saConfig writes a service account key pointing at f's token endpoint
// and returns an API config using it with scope.
func saConfig(t *testing.T, f *fakeGoogle, ctx context.Context, scope string) *apiHTTPConfig {
	t.Helper()
	keyPath, key := writeTestServiceAccount(t)
	key.TokenURI = f.srv.URL + "/token"
	rewriteServiceAccount(t, keyPath, key)
	return &apiHTTPConfig{BaseURL: f.srv.URL, Ctx: ctx, Auth: map[string]any{"service_account": keyPath, "scope": scope}}
}

// isolateSATokens gives a test an empty token cache and no cassette.
func isolateSATokens(t *testing.T) {
	t.Helper()
	resetServiceAccountTokens()
	t.Cleanup(resetServiceAccountTokens)
	setCassette(t, "", "")
}

const pagesSrc = `out = [http.get("/values", params={"page": str(i)}).json["token"] for i in range(5)]`

// TestServiceAccountToken_OneExchangePerRun: pages of one fetch call,
// several fetch calls (each with its own http module, as each lib call
// gets) and parallel requests share one token exchange.
func TestServiceAccountToken_OneExchangePerRun(t *testing.T) {
	isolateSATokens(t)
	f := newFakeGoogle(t)
	cfg := saConfig(t, f, context.Background(), "https://www.googleapis.com/auth/admanager")

	for call := 0; call < 3; call++ {
		v, err := runLib(t, context.Background(), cfg, pagesSrc)
		if err != nil {
			t.Fatal(err)
		}
		if got := v.String(); got != `["tok-1", "tok-1", "tok-1", "tok-1", "tok-1"]` {
			t.Fatalf("call %d: tokens %s", call, got)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := runLib(t, context.Background(), cfg, `out = http.get("/values").json["token"]`)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	if n := f.exchanges.Load(); n != 1 {
		t.Fatalf("%d token exchanges for %d requests, want 1", n, f.requests.Load())
	}
	if n := f.requests.Load(); n != 31 {
		t.Fatalf("%d API requests, want 31", n)
	}
}

// TestServiceAccountToken_ParallelColdCache: many goroutines asking for a
// token before any is cached start one exchange between them.
func TestServiceAccountToken_ParallelColdCache(t *testing.T) {
	isolateSATokens(t)
	f := newFakeGoogle(t)
	cfg := saConfig(t, f, context.Background(), "s")
	key, scope, err := loadServiceAccount(cfg.Auth)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	toks := make([]string, 32)
	for i := range toks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			tok, err := serviceAccountToken(context.Background(), key, scope)
			if err != nil {
				t.Error(err)
			}
			toks[i] = tok
		}()
	}
	close(start)
	wg.Wait()
	for i, tok := range toks {
		if tok != "tok-1" {
			t.Fatalf("goroutine %d got %q", i, tok)
		}
	}
	if n := f.exchanges.Load(); n != 1 {
		t.Fatalf("%d exchanges, want 1", n)
	}
}

// TestServiceAccountToken_Expiry: the token is reused until refreshMargin
// before expires_in, then exchanged again.
func TestServiceAccountToken_Expiry(t *testing.T) {
	isolateSATokens(t)
	f := newFakeGoogle(t)
	f.expiresIn = 600
	cfg := saConfig(t, f, context.Background(), "s")

	now := time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
	var clockMu sync.Mutex
	saTokens.mu.Lock()
	saTokens.now = func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return now }
	saTokens.mu.Unlock()
	advance := func(d time.Duration) { clockMu.Lock(); now = now.Add(d); clockMu.Unlock() }

	get := func() string {
		t.Helper()
		v, err := runLib(t, context.Background(), cfg, `out = http.get("/values").json["token"]`)
		if err != nil {
			t.Fatal(err)
		}
		s, _ := starlark.AsString(v)
		return s
	}

	if tok := get(); tok != "tok-1" {
		t.Fatalf("first: %s", tok)
	}
	advance(600*time.Second - refreshMargin - time.Second)
	if tok := get(); tok != "tok-1" {
		t.Fatalf("1s before the refresh margin: %s, want the cached tok-1", tok)
	}
	advance(time.Second)
	if tok := get(); tok != "tok-2" {
		t.Fatalf("at the refresh margin: %s, want tok-2", tok)
	}
	if tok := get(); tok != "tok-2" {
		t.Fatalf("after refresh: %s, want the cached tok-2", tok)
	}
	if n := f.exchanges.Load(); n != 2 {
		t.Fatalf("%d exchanges, want 2", n)
	}
}

// TestServiceAccountToken_KeyedByScopeAndCredential: another scope or
// another key file gets its own token.
func TestServiceAccountToken_KeyedByScopeAndCredential(t *testing.T) {
	isolateSATokens(t)
	f := newFakeGoogle(t)
	a := saConfig(t, f, context.Background(), "scope-a")
	b := &apiHTTPConfig{BaseURL: a.BaseURL, Ctx: a.Ctx, Auth: map[string]any{"service_account": a.Auth["service_account"], "scope": "scope-b"}}
	other := saConfig(t, f, context.Background(), "scope-a")
	for _, cfg := range []*apiHTTPConfig{a, b, other, a, b, other} {
		if _, err := runLib(t, context.Background(), cfg, `out = http.get("/values").json`); err != nil {
			t.Fatal(err)
		}
	}
	if n := f.exchanges.Load(); n != 3 {
		t.Fatalf("%d exchanges, want 3 (two scopes of one key, one other key)", n)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if got := strings.Join(f.scopes, ","); got != "scope-a,scope-b,scope-a" {
		t.Fatalf("scopes exchanged: %s", got)
	}
}

// TestServiceAccountToken_401RefreshesOnce: a 401 drops the cached token,
// exchanges once and repeats the request once; the next requests use the
// new token without another exchange.
func TestServiceAccountToken_401RefreshesOnce(t *testing.T) {
	isolateSATokens(t)
	f := newFakeGoogle(t)
	cfg := saConfig(t, f, context.Background(), "s")

	if _, err := runLib(t, context.Background(), cfg, `out = http.get("/values").json`); err != nil {
		t.Fatal(err)
	}
	f.rejectToken("tok-1") // revoked mid-run
	v, err := runLib(t, context.Background(), cfg, pagesSrc)
	if err != nil {
		t.Fatal(err)
	}
	if got := v.String(); got != `["tok-2", "tok-2", "tok-2", "tok-2", "tok-2"]` {
		t.Fatalf("tokens %s", got)
	}
	if n := f.exchanges.Load(); n != 2 {
		t.Fatalf("%d exchanges, want 2", n)
	}
	if n := f.requests.Load(); n != 7 {
		t.Fatalf("%d API requests, want 7 (1 + the rejected one + 5 pages)", n)
	}
}

// TestServiceAccountToken_401NotRetriedTwice: when the fresh token is
// rejected too, the script gets the 401 after one retry.
func TestServiceAccountToken_401NotRetriedTwice(t *testing.T) {
	isolateSATokens(t)
	f := newFakeGoogle(t)
	f.rejectToken("tok-1")
	f.rejectToken("tok-2")
	f.rejectToken("tok-3")
	cfg := saConfig(t, f, context.Background(), "s")
	v, err := runLib(t, context.Background(), cfg, `out = http.get("/values").status_code`)
	if err != nil {
		t.Fatal(err)
	}
	if v.String() != "401" {
		t.Fatalf("status %s, want 401", v)
	}
	if n, r := f.exchanges.Load(), f.requests.Load(); n != 2 || r != 2 {
		t.Fatalf("%d exchanges, %d requests, want 2 and 2", n, r)
	}
}

// TestServiceAccountToken_401Upload: http.upload refreshes on 401 too.
func TestServiceAccountToken_401Upload(t *testing.T) {
	isolateSATokens(t)
	f := newFakeGoogle(t)
	f.rejectToken("tok-1")
	cfg := saConfig(t, f, context.Background(), "s")
	file := filepath.Join(t.TempDir(), "a.txt")
	if err := os.WriteFile(file, []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	v, err := runLib(t, context.Background(), cfg, fmt.Sprintf(`out = http.upload("/upload", file=%q).json["token"]`, file))
	if err != nil {
		t.Fatal(err)
	}
	if v.String() != `"tok-2"` {
		t.Fatalf("token %s, want tok-2", v)
	}
}

// TestServiceAccountToken_CassetteOneExchange: a recording holds one token
// exchange for all requests of the run, and replays with the key file
// gone, including the 401 and its retry.
func TestServiceAccountToken_CassetteOneExchange(t *testing.T) {
	resetServiceAccountTokens()
	t.Cleanup(resetServiceAccountTokens)
	dir := t.TempDir()
	f := newFakeGoogle(t)
	ctx := httpcassette.WithScope(context.Background(), "raw.gam", "gam")
	cfg := saConfig(t, f, ctx, "s")
	src := pagesSrc + `
out.append(http.get("/values", params={"page": "after-revoke"}).json["token"])
`

	setCassette(t, dir, "record")
	recorded, err := runLib(t, ctx, cfg, pagesSrc)
	if err != nil {
		t.Fatal(err)
	}
	f.rejectToken("tok-1")
	recorded2, err := runLib(t, ctx, cfg, `out = http.get("/values", params={"page": "after-revoke"}).json["token"]`)
	if err != nil {
		t.Fatal(err)
	}
	if recorded2.String() != `"tok-2"` {
		t.Fatalf("after revoke: %s", recorded2)
	}
	data, err := os.ReadFile(filepath.Join(dir, "raw.gam", "gam.json"))
	if err != nil {
		t.Fatal(err)
	}
	// 1 exchange + 5 pages, then the 401, 1 exchange and the retry.
	if n := strings.Count(string(data), "jwt-bearer"); n != 2 {
		t.Fatalf("cassette holds %d token exchanges, want 2 (one per token):\n%s", n, data)
	}
	for _, s := range []string{"tok-1", "tok-2"} {
		if strings.Contains(string(data), `"`+s+`"`) {
			t.Fatalf("cassette contains the access token %s", s)
		}
	}

	cfg.Auth = map[string]any{"service_account": "/no/such/key.json", "scope": "s"}
	setCassette(t, dir, "replay")
	replayed, err := runLib(t, ctx, cfg, src)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	// Tokens are masked in the recording, so replay sees the placeholder;
	// what must agree is that every request was answered, the revoked one
	// with its retry's 200.
	if l, ok := replayed.(*starlark.List); !ok || l.Len() != 6 {
		t.Fatalf("replayed %s, recorded %s + %s", replayed, recorded, recorded2)
	}
	for _, u := range httpcassette.Summary() {
		if u.Replayed != 7 || u.Recorded != 9 {
			t.Fatalf("%s: replayed %d of %d, want 7 of 9 (token exchanges are not replayed)", u.Path, u.Replayed, u.Recorded)
		}
	}
}

// TestServiceAccountToken_OldCassetteStillReplays: a cassette recorded
// before the cache, with one token exchange per request, replays: replay
// makes no exchange, so the extra exchanges are simply not used.
func TestServiceAccountToken_OldCassetteStillReplays(t *testing.T) {
	resetServiceAccountTokens()
	t.Cleanup(resetServiceAccountTokens)
	dir := t.TempDir()
	f := newFakeGoogle(t)
	ctx := httpcassette.WithScope(context.Background(), "raw.sheet", "gsheets")
	cfg := saConfig(t, f, ctx, "s")

	setCassette(t, dir, "record")
	for i := 0; i < 3; i++ {
		resetServiceAccountTokens() // the old behaviour: a new exchange per request
		if _, err := runLib(t, ctx, cfg, fmt.Sprintf(`out = http.get("/values", params={"page": "%d"}).json`, i)); err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(filepath.Join(dir, "raw.sheet", "gsheets.json"))
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(data), "jwt-bearer"); n != 3 {
		t.Fatalf("old-style cassette holds %d exchanges, want 3", n)
	}

	cfg.Auth = map[string]any{"service_account": "/no/such/key.json", "scope": "s"}
	setCassette(t, dir, "replay")
	v, err := runLib(t, ctx, cfg, `out = [http.get("/values", params={"page": str(i)}).json["page"] for i in range(3)]`)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if v.String() != `["0", "1", "2"]` {
		t.Fatalf("replayed %s", v)
	}
}
