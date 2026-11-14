// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package httpcassette

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
)

type transport struct {
	base http.RoundTripper
}

// cassette is one open file. In record mode it starts empty (a recording
// replaces the previous one) and is rewritten after every interaction. In
// replay mode used marks the interactions already served, so identical
// requests get their recorded responses in order.
type cassette struct {
	mu   sync.Mutex
	path string
	file *File
	used []bool
	err  error  // load error, reported on every request in replay
	now  string // ONDATRA_NOW of this run, compared with the recorded one
}

func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	c := loadConfig()
	if c.err != nil {
		return nil, fmt.Errorf("http cassette: %w", c.err)
	}
	body, err := readRequestBody(req)
	if err != nil {
		return nil, fmt.Errorf("http cassette: read request body: %w", err)
	}
	sc := scopeOf(req.Context())
	names, _ := req.Context().Value(authKey{}).(authNames)
	red := newRedactor(names)
	red.learnHeaderSecrets(req.Header)
	rec := recordRequest(red, req, body)

	cas := getCassette(c, sc)
	if c.mode == Replay {
		return cas.replay(req, rec)
	}
	return t.record(req, body, red, rec, cas)
}

func (t *transport) record(req *http.Request, body []byte, red *redactor, rec Request, cas *cassette) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	out := req.Clone(req.Context())
	if body != nil {
		out.Body = io.NopCloser(bytes.NewReader(body))
		out.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	}
	resp, rtErr := base.RoundTrip(out)
	in := Interaction{Request: rec}
	if rtErr != nil {
		msg := strings.ReplaceAll(rtErr.Error(), req.URL.String(), rec.URL)
		in.Error = red.scrub(msg)
	} else {
		respBody, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close() // fully read; the replacement body below is what the caller closes
		if err != nil {
			return nil, err
		}
		resp.Body = io.NopCloser(bytes.NewReader(respBody))
		enc, raw := encodeBody(red.redactBody(respBody, resp.Header.Get("Content-Type")))
		in.Response = &Response{
			Status:       resp.StatusCode,
			Headers:      red.redactResponseHeaders(resp.Header),
			BodyEncoding: enc,
			Body:         raw,
		}
	}
	if err := cas.append(in); err != nil {
		if resp != nil {
			_ = resp.Body.Close() // in-memory body; the write error is what matters
		}
		return nil, fmt.Errorf("http cassette: write %s: %w", cas.path, err)
	}
	return resp, rtErr
}

func readRequestBody(req *http.Request) ([]byte, error) {
	if req.Body == nil || req.Body == http.NoBody {
		return nil, nil
	}
	// A RoundTripper owns and closes the request body; record forwards
	// an in-memory copy on a clone of the request.
	b, err := io.ReadAll(req.Body)
	_ = req.Body.Close() // fully read; the error that matters is the read's
	if err != nil {
		return nil, err
	}
	return b, nil
}

func recordRequest(red *redactor, req *http.Request, body []byte) Request {
	enc, raw := encodeBody(red.redactBody(body, req.Header.Get("Content-Type")))
	return Request{
		Method:       req.Method,
		URL:          red.redactURL(req.URL.String()),
		Headers:      red.redactRequestHeaders(req.Header),
		BodyEncoding: enc,
		Body:         raw,
	}
}

func getCassette(c *config, sc scope) *cassette {
	path := cassettePath(c.dir, sc)
	mu.Lock()
	defer mu.Unlock()
	if cas, ok := open[path]; ok {
		return cas
	}
	cas := &cassette{path: path, now: c.now}
	if c.mode == Record {
		cas.file = &File{Version: formatVersion, Model: sc.model, Lib: sc.lib, Now: c.now, Interactions: []Interaction{}}
	} else {
		f, err := readFile(path)
		switch {
		case errors.Is(err, os.ErrNotExist):
			cas.err = fmt.Errorf("no cassette %s for model %q, lib %q: record it first with %s=record", path, sc.model, sc.lib, EnvMode)
		case err != nil:
			cas.err = err
		default:
			cas.file = f
			cas.used = make([]bool, len(f.Interactions))
		}
	}
	open[path] = cas
	return cas
}

func (cas *cassette) append(in Interaction) error {
	cas.mu.Lock()
	defer cas.mu.Unlock()
	cas.file.Interactions = append(cas.file.Interactions, in)
	data, err := marshalFile(cas.file)
	if err != nil {
		return err
	}
	// Last line of defence: a secret registered after an earlier
	// interaction was stored (an access token from a token response) is
	// masked in it too, since the whole file is rewritten each time.
	data = []byte(newRedactor(authNames{}).scrub(string(data)))
	return writeFile(cas.path, data)
}

func (cas *cassette) replay(req *http.Request, rec Request) (*http.Response, error) {
	cas.mu.Lock()
	defer cas.mu.Unlock()
	if cas.err != nil {
		return nil, fmt.Errorf("http replay: %w", cas.err)
	}
	want := keyOf(&rec)
	for i := range cas.file.Interactions {
		if cas.used[i] {
			continue
		}
		in := &cas.file.Interactions[i]
		if keyOf(&in.Request) != want {
			continue
		}
		cas.used[i] = true
		return in.toResponse(req, cas.path, i)
	}
	return nil, cas.unmatched(rec, want)
}

func (cas *cassette) unmatched(rec Request, want matchKey) error {
	var b strings.Builder
	fmt.Fprintf(&b, "http replay: no recorded interaction for %s %s (body %s) in %s", rec.Method, rec.URL, want.bodyHash(), cas.path)
	best, bestScore := -1, -1
	for i := range cas.file.Interactions {
		if s := similarity(want, keyOf(&cas.file.Interactions[i].Request)); s > bestScore {
			best, bestScore = i, s
		}
	}
	if best < 0 {
		b.WriteString("; the cassette has no interactions")
	} else {
		in := cas.file.Interactions[best].Request
		k := keyOf(&in)
		fmt.Fprintf(&b, "; nearest recorded: #%d %s %s (body %s)", best+1, in.Method, in.URL, k.bodyHash())
		if k == want {
			fmt.Fprintf(&b, ", identical but already replayed: the run made this request more often than the recording did")
		}
	}
	if cas.file.Now != cas.now {
		fmt.Fprintf(&b, "; recorded with ONDATRA_NOW=%q, replaying with ONDATRA_NOW=%q, so URLs built from time.now() differ", cas.file.Now, cas.now)
	}
	return errors.New(b.String())
}

func (in *Interaction) toResponse(req *http.Request, path string, i int) (*http.Response, error) {
	if in.Response == nil {
		if in.Error == "" {
			return nil, fmt.Errorf("http replay: %s interaction #%d has neither response nor error", path, i+1)
		}
		return nil, errors.New(in.Error)
	}
	body, err := decodeBody(in.Response.BodyEncoding, in.Response.Body)
	if err != nil {
		return nil, fmt.Errorf("http replay: %s interaction #%d body: %w", path, i+1, err)
	}
	h := http.Header{}
	for k, vs := range in.Response.Headers {
		h[k] = append([]string(nil), vs...)
	}
	if h.Get("Content-Length") != "" {
		h.Set("Content-Length", strconv.Itoa(len(body)))
	}
	status := in.Response.Status
	return &http.Response{
		Status:        fmt.Sprintf("%d %s", status, http.StatusText(status)),
		StatusCode:    status,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        h,
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}, nil
}
