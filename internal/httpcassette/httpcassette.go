// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package httpcassette records the HTTP traffic of lib functions to files
// and answers it from them, so a blueprint can be tested offline and
// deterministically.
//
// It is driven by two environment variables:
//
//	ONDATRA_HTTP_CASSETTE=<dir>   where the cassette files live
//	ONDATRA_HTTP_MODE=record      perform each request and append it to a file
//	ONDATRA_HTTP_MODE=replay      answer each request from a file, no network
//
// With neither set, Transport returns its base unchanged and every other
// function here is a no-op, so behaviour is exactly what it was without the
// package.
//
// Every HTTP client a lib function can reach (http.* in Starlark, OAuth
// token exchanges, Google service account JWT exchange, the local OAuth
// refresh) builds its client with Transport, which is the one seam. The
// cassette a request goes to is chosen from its context: WithScope names
// the model and lib function, giving <dir>/<model>/<lib>.json.
package httpcassette

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/ondatra-labs/ondatrasql/internal/clock"
)

// Environment variables read by this package.
const (
	EnvDir  = "ONDATRA_HTTP_CASSETTE"
	EnvMode = "ONDATRA_HTTP_MODE"
)

// Mode is what the transport does with a request.
type Mode int

// Modes.
const (
	Off Mode = iota
	Record
	Replay
)

type config struct {
	mode Mode
	dir  string
	err  error
	now  string // ONDATRA_NOW when the config was read, stored in recorded files
}

var (
	mu      sync.Mutex
	cfg     *config
	open    = map[string]*cassette{}
	secrets = map[string]bool{}
)

// loadConfig reads the environment once per process (or per Reset).
// It is lazy so that variables from the project's .env, loaded after
// start-up, are seen.
func loadConfig() *config {
	mu.Lock()
	defer mu.Unlock()
	return loadConfigLocked()
}

func loadConfigLocked() *config {
	if cfg != nil {
		return cfg
	}
	c := &config{now: os.Getenv(clock.EnvNow)}
	dir := os.Getenv(EnvDir)
	mode := os.Getenv(EnvMode)
	switch {
	case dir == "" && mode == "":
		c.mode = Off
	case dir == "":
		c.err = fmt.Errorf("%s=%s is set but %s is not: set it to the cassette directory", EnvMode, mode, EnvDir)
	case mode == "record":
		c.mode, c.dir = Record, dir
	case mode == "replay":
		c.mode, c.dir = Replay, dir
	case mode == "":
		c.err = fmt.Errorf("%s=%s is set but %s is not: set it to record or replay", EnvDir, dir, EnvMode)
	default:
		c.err = fmt.Errorf("%s=%q: must be record or replay", EnvMode, mode)
	}
	if c.err != nil {
		// A broken configuration must not fall back to live traffic:
		// every request fails with the error instead.
		c.mode = Replay
	}
	cfg = c
	return c
}

// Validate reports a malformed ONDATRA_HTTP_CASSETTE / ONDATRA_HTTP_MODE
// pair. The CLI calls it before a run so the mistake is reported up front
// rather than at the first request.
func Validate() error {
	return loadConfig().err
}

// Reset forgets the configuration, the open cassettes and the registered
// secrets, so the next call reads the environment again. For tests that
// change the variables within one process.
func Reset() {
	mu.Lock()
	defer mu.Unlock()
	cfg = nil
	open = map[string]*cassette{}
	secrets = map[string]bool{}
}

// Current returns the active mode.
func Current() Mode { return loadConfig().mode }

// Replaying reports whether requests are answered from cassettes. Callers
// use it to skip waits (retry backoff, rate limits, polling, sleep()) and
// credential lookups that only matter against a live API.
func Replaying() bool { return loadConfig().mode == Replay }

// Transport wraps base (nil means http.DefaultTransport) so that requests
// are recorded or replayed. With the package off it returns base itself.
func Transport(base http.RoundTripper) http.RoundTripper {
	if loadConfig().mode == Off {
		return base
	}
	return &transport{base: base}
}

// Use is how much of one cassette a replay served.
type Use struct {
	Path     string // relative to ONDATRA_HTTP_CASSETTE, slash-separated
	Replayed int    // interactions answered so far
	Recorded int    // interactions in the file
}

// Summary lists, sorted by path, every cassette a replay has opened in
// this process and how many of its interactions were served. A run that
// made no request opens none. Nil unless replaying.
func Summary() []Use {
	c := loadConfig()
	if c.mode != Replay || c.err != nil {
		return nil
	}
	mu.Lock()
	cs := make([]*cassette, 0, len(open))
	for _, cas := range open {
		cs = append(cs, cas)
	}
	mu.Unlock()
	out := make([]Use, 0, len(cs))
	for _, cas := range cs {
		cas.mu.Lock()
		u := Use{Path: cas.path}
		if rel, err := filepath.Rel(c.dir, cas.path); err == nil {
			u.Path = filepath.ToSlash(rel)
		}
		if cas.file != nil {
			u.Recorded = len(cas.file.Interactions)
		}
		for _, used := range cas.used {
			if used {
				u.Replayed++
			}
		}
		cas.mu.Unlock()
		out = append(out, u)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// RegisterSecret marks s as a credential: every occurrence of it (raw,
// URL-escaped or JSON-escaped) is replaced by the placeholder before an
// interaction is written. Values shorter than minSecretLen are ignored,
// since masking them would corrupt unrelated data. No-op when off.
func RegisterSecret(s string) {
	if len(s) < minSecretLen || loadConfig().mode == Off {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	secrets[s] = true
}

// minSecretLen is the shortest value RegisterSecret masks.
const minSecretLen = 4

func secretList() []string {
	mu.Lock()
	defer mu.Unlock()
	out := make([]string, 0, len(secrets))
	for s := range secrets {
		out = append(out, s)
	}
	return out
}

type scopeKey struct{}

type scope struct {
	model string
	lib   string
}

// WithScope names the model and lib function whose requests ctx carries;
// they go to <dir>/<model>/<lib>.json. Requests without a scope go to
// <dir>/_global/_global.json.
func WithScope(ctx context.Context, model, lib string) context.Context {
	return context.WithValue(ctx, scopeKey{}, scope{model: model, lib: lib})
}

type authKey struct{}

// authNames are the header and query parameter names the API dict's auth
// uses for its credential. They are redacted on record and ignored when
// matching.
type authNames struct {
	headers []string
	params  []string
}

// WithAuthNames adds a header and/or query parameter name (either may be
// empty) that carries a credential for requests made with ctx.
func WithAuthNames(ctx context.Context, header, param string) context.Context {
	prev, _ := ctx.Value(authKey{}).(authNames)
	next := authNames{
		headers: append([]string(nil), prev.headers...),
		params:  append([]string(nil), prev.params...),
	}
	if header != "" {
		next.headers = append(next.headers, header)
	}
	if param != "" {
		next.params = append(next.params, param)
	}
	return context.WithValue(ctx, authKey{}, next)
}

func scopeOf(ctx context.Context) scope {
	s, ok := ctx.Value(scopeKey{}).(scope)
	if !ok || (s.model == "" && s.lib == "") {
		return scope{model: "_global", lib: "_global"}
	}
	if s.model == "" {
		s.model = "_global"
	}
	if s.lib == "" {
		s.lib = "_global"
	}
	return s
}

// cassettePath is <dir>/<model>/<lib>.json with both names reduced to
// characters that are safe in a file name on every platform.
func cassettePath(dir string, s scope) string {
	return filepath.Join(dir, safeName(s.model), safeName(s.lib)+".json")
}

func safeName(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	out := b.String()
	if out == "" || strings.Trim(out, ".") == "" {
		out = "_" + out
	}
	return out
}
