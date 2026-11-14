// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/clock"
	"github.com/ondatra-labs/ondatrasql/internal/httpcassette"
	starlarktime "go.starlark.net/lib/time"
	"go.starlark.net/starlark"
)

func setCassette(t *testing.T, dir, mode string) {
	t.Helper()
	t.Setenv(httpcassette.EnvDir, dir)
	t.Setenv(httpcassette.EnvMode, mode)
	httpcassette.Reset()
	t.Cleanup(httpcassette.Reset)
}

// runLib executes src with the http/env/time modules a lib function sees
// and returns the value of its global "out". Compare results with
// starlark.Equal: resp.json dicts come from a Go map, so their key order
// differs between calls (live or replayed alike).
func runLib(t *testing.T, ctx context.Context, cfg *apiHTTPConfig, src string) (starlark.Value, error) {
	t.Helper()
	thread := &starlark.Thread{Name: "test"}
	applyFrozenClock(thread)
	pre := starlark.StringDict{
		"http": httpModule(ctx, cfg),
		"env":  envModule(),
		"time": starlarktime.Module,
	}
	globals, err := starlark.ExecFileOptions(blueprintFileOptions(), thread, "lib.star", src, pre)
	if err != nil {
		return nil, err
	}
	return globals["out"], nil
}

// TestCassette_StarlarkRecordReplay records http.* calls made the way a
// lib function makes them (API dict with base_url and an env API key in a
// query parameter, a JSON POST with retry), then replays them with the
// server shut down and the key unset, and gets the same values back.
func TestCassette_StarlarkRecordReplay(t *testing.T) {
	const apiKey = "sk_test_51HxYzSecretValue"
	dir := t.TempDir()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if r.URL.Query().Get("api_key") != apiKey {
			w.WriteHeader(401)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "POST" && n == 2 {
			w.WriteHeader(503) // retried
			return
		}
		fmt.Fprintf(w, `{"path":%q,"page":%q,"n":%d}`, r.URL.Path, r.URL.Query().Get("page"), n)
	}))
	cfg := &apiHTTPConfig{
		BaseURL: srv.URL,
		Auth:    map[string]any{"env": "TEST_CASSETTE_KEY", "param": "api_key"},
		Backoff: 30, // seconds: replay must not wait for it
	}
	src := `
r1 = http.get("/orders", params={"page": "1"})
r2 = http.post("/orders", json={"b": 1, "a": [1, 2]}, retry=2)
out = [r1.status_code, r1.json, r2.status_code, r2.json]
`
	ctx := httpcassette.WithScope(context.Background(), "raw.orders", "orders")

	t.Setenv("TEST_CASSETTE_KEY", apiKey)
	setCassette(t, dir, "record")
	cfg.Ctx = ctx
	recorded, err := runLib(t, ctx, &apiHTTPConfig{BaseURL: cfg.BaseURL, Auth: cfg.Auth, Ctx: ctx, Backoff: 0}, src)
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	srv.Close()

	data, err := os.ReadFile(filepath.Join(dir, "raw.orders", "orders.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), apiKey) {
		t.Fatalf("cassette contains the API key:\n%s", data)
	}

	t.Setenv("TEST_CASSETTE_KEY", "")
	setCassette(t, dir, "replay")
	start := time.Now()
	replayed, err := runLib(t, ctx, cfg, src)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("replay took %v: retry backoff was not skipped", d)
	}
	if eq, err := starlark.Equal(recorded, replayed); err != nil || !eq {
		t.Fatalf("recorded %s\nreplayed %s", recorded, replayed)
	}
}

// TestCassette_ReplayAuthNeedsNoCredentials: service_account and provider
// auth work in replay with no key file, no token and no state database.
func TestCassette_ReplayAuthNeedsNoCredentials(t *testing.T) {
	setCassette(t, t.TempDir(), "replay")
	for _, auth := range []map[string]any{
		{"service_account": map[string]any{"env": "TEST_CASSETTE_NO_SUCH_SA"}, "scope": "s"},
		{"service_account": "/no/such/key.json", "scope": "s"},
		{"provider": "fortnox"},
	} {
		headers := map[string]string{}
		u := "https://api.example.com/x"
		if err := injectAPIAuth(auth, headers, &u, &apiHTTPConfig{Ctx: context.Background()}); err != nil {
			t.Fatalf("%v: %v", auth, err)
		}
		if headers["Authorization"] != "Bearer "+httpcassette.Placeholder {
			t.Fatalf("%v: Authorization = %q", auth, headers["Authorization"])
		}
	}
}

// TestCassette_OffLeavesAuthAlone: with no cassette the service account
// path still reads its key file, as before.
func TestCassette_OffLeavesAuthAlone(t *testing.T) {
	setCassette(t, "", "")
	headers := map[string]string{}
	u := "https://api.example.com/x"
	err := injectAPIAuth(map[string]any{"service_account": "/no/such/key.json"}, headers, &u, &apiHTTPConfig{Ctx: context.Background()})
	if err == nil || !strings.Contains(err.Error(), "read service_account") {
		t.Fatalf("err = %v, want the key file read error", err)
	}
}

func TestCassette_ServiceAccountExchangeRecordedWithoutSecrets(t *testing.T) {
	dir := t.TempDir()
	keyPath, key := writeTestServiceAccount(t)
	const accessToken = "ya29.c.service-account-token"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/token" {
			fmt.Fprintf(w, `{"access_token":%q,"expires_in":3600,"token_type":"Bearer"}`, accessToken)
			return
		}
		fmt.Fprintf(w, `{"values":[["a","b"]],"seen_auth":%q}`, r.Header.Get("Authorization"))
	}))
	defer srv.Close()
	key.TokenURI = srv.URL + "/token"
	rewriteServiceAccount(t, keyPath, key)

	t.Setenv("TEST_CASSETTE_SA", keyPath)
	ctx := httpcassette.WithScope(context.Background(), "raw.sheet", "gsheets")
	cfg := &apiHTTPConfig{BaseURL: srv.URL, Ctx: ctx, Auth: map[string]any{"service_account": map[string]any{"env": "TEST_CASSETTE_SA"}, "scope": "https://www.googleapis.com/auth/spreadsheets.readonly"}}
	src := `out = http.get("/values").json["values"]`

	setCassette(t, dir, "record")
	recorded, err := runLib(t, ctx, cfg, src)
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "raw.sheet", "gsheets.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{accessToken, "PRIVATE KEY", key.PrivateKeyID, "eyJ"} {
		if strings.Contains(string(data), s) {
			t.Errorf("cassette contains %q:\n%s", s, data)
		}
	}
	if !strings.Contains(string(data), "jwt-bearer") {
		t.Errorf("token exchange not recorded:\n%s", data)
	}

	// Replay with the key file gone.
	srv.Close()
	t.Setenv("TEST_CASSETTE_SA", "")
	setCassette(t, dir, "replay")
	replayed, err := runLib(t, ctx, cfg, src)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if eq, err := starlark.Equal(recorded, replayed); err != nil || !eq {
		t.Fatalf("recorded %s, replayed %s", recorded, replayed)
	}
}

func TestCassette_EnvSecretMasked(t *testing.T) {
	dir := t.TempDir()
	const token = "shpat_0123456789abcdef"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"ok":true}`)
	}))
	defer srv.Close()
	t.Setenv("SHOP_ACCESS_TOKEN", token)
	setCassette(t, dir, "record")
	ctx := httpcassette.WithScope(context.Background(), "m", "shop")
	// The token goes into a header and a body by hand, not through auth.
	src := fmt.Sprintf(`
tok = env.get("SHOP_ACCESS_TOKEN")
out = http.post(%q, headers={"X-Shop-Access": tok}, json={"note": "t=" + tok}).json
`, srv.URL)
	if _, err := runLib(t, ctx, nil, src); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "m", "shop.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), token) {
		t.Fatalf("cassette contains the env secret:\n%s", data)
	}
}

func TestFrozenClock(t *testing.T) {
	t.Setenv(clock.EnvNow, "2026-03-01T08:30:00Z")
	v, err := runLib(t, context.Background(), nil, `
a = time.now()
b = time.now()
out = [str(a), a == b, str(a - time.parse_duration("168h"))]
`)
	if err != nil {
		t.Fatal(err)
	}
	want := `["2026-03-01 08:30:00 +0000 UTC", True, "2026-02-22 08:30:00 +0000 UTC"]`
	if v.String() != want {
		t.Fatalf("got %s, want %s", v, want)
	}

	t.Setenv(clock.EnvNow, "yesterday")
	if _, err := runLib(t, context.Background(), nil, `out = time.now()`); err == nil || !strings.Contains(err.Error(), "ONDATRA_NOW") {
		t.Fatalf("malformed ONDATRA_NOW: err = %v", err)
	}

	// Unset: the wall clock.
	t.Setenv(clock.EnvNow, "")
	before := time.Now()
	v, err = runLib(t, context.Background(), nil, `out = time.now()`)
	if err != nil {
		t.Fatal(err)
	}
	got := time.Time(v.(starlarktime.Time))
	if got.Before(before) || got.After(time.Now()) {
		t.Fatalf("time.now() = %v outside [%v, now]", got, before)
	}
}

// TestCassette_SensitiveParamsCovered keeps the cassette's parameter
// redaction a superset of the log redaction in redact.go.
func TestCassette_SensitiveParamsCovered(t *testing.T) {
	dir := t.TempDir()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	setCassette(t, dir, "record")
	ctx := httpcassette.WithScope(context.Background(), "m", "params")
	for name := range sensitiveParams {
		u := srv.URL + "/?" + name + "=value-of-" + name
		if _, err := DoHTTP(ctx, "GET", u, nil, nil, httpOptions{Timeout: 5 * time.Second}); err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(filepath.Join(dir, "m", "params.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "value-of-") {
		t.Fatalf("a parameter RedactURL masks was written to the cassette:\n%s", data)
	}
}

// writeTestServiceAccount writes a service account key file with a fresh
// RSA key and returns its path and contents.
func writeTestServiceAccount(t *testing.T) (string, ServiceAccountKey) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	key := ServiceAccountKey{
		Type:         "service_account",
		PrivateKeyID: "0123456789abcdef0123456789abcdef",
		PrivateKey:   string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		ClientEmail:  "sa@project.iam.gserviceaccount.com",
	}
	path := filepath.Join(t.TempDir(), "sa.json")
	rewriteServiceAccount(t, path, key)
	return path, key
}

func rewriteServiceAccount(t *testing.T, path string, key ServiceAccountKey) {
	t.Helper()
	data, err := json.Marshal(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}
