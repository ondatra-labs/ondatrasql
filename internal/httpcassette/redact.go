// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package httpcassette

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/redact"
)

// Placeholder replaces every credential written to a cassette.
const Placeholder = redact.Marker

// Request header names whose value is always a credential.
var sensitiveRequestHeaders = map[string]bool{
	"authorization":       true,
	"proxy-authorization": true,
	"cookie":              true,
}

// Fragments that mark a request header as carrying a credential. Request
// headers take no part in matching, so masking too much costs only
// documentation; masking too little leaks.
var sensitiveHeaderFragments = []string{"token", "secret", "password", "key", "auth", "session", "signature", "credential", "cookie"}

// Response headers are replayed to the blueprint (pagination tokens, rate
// limit counters), so only the ones that are credentials by definition
// are masked there.
var sensitiveResponseHeaders = map[string]bool{
	"set-cookie":          true,
	"authorization":       true,
	"proxy-authorization": true,
}

// Query parameter names that carry a credential. A superset of
// script.sensitiveParams (the log redaction), which a test keeps in step.
var sensitiveParams = map[string]bool{
	"api_key":              true,
	"apikey":               true,
	"key":                  true,
	"token":                true,
	"access_token":         true,
	"refresh_token":        true,
	"id_token":             true,
	"secret":               true,
	"password":             true,
	"client_secret":        true,
	"auth":                 true,
	"auth_token":           true,
	"sig":                  true,
	"signature":            true,
	"x-amz-signature":      true,
	"x-amz-credential":     true,
	"x-amz-security-token": true,
}

// Body field names (JSON keys and form fields) that carry a credential:
// the OAuth client secret, refresh token and JWT assertion in token
// requests, the tokens in token responses, and the usual password/key
// fields. "code" (the OAuth authorization code) is left out on purpose:
// it is single-use, and "code" is a common data field.
var sensitiveFields = map[string]bool{
	"client_secret":    true,
	"client_assertion": true,
	"assertion":        true,
	"refresh_token":    true,
	"access_token":     true,
	"id_token":         true,
	"password":         true,
	"api_key":          true,
	"apikey":           true,
	"private_key":      true,
	"private_key_id":   true,
	"secret":           true,
}

func isSensitiveField(k string) bool { return sensitiveFields[strings.ToLower(k)] }

// redactor holds what one request needs to be masked: the auth header
// and parameter names from its context and the registered secret values.
type redactor struct {
	authHeaders map[string]bool
	authParams  map[string]bool
	secrets     []string
}

func newRedactor(names authNames) *redactor {
	r := &redactor{
		authHeaders: map[string]bool{},
		authParams:  map[string]bool{},
		secrets:     secretList(),
	}
	for _, h := range names.headers {
		r.authHeaders[strings.ToLower(h)] = true
	}
	for _, p := range names.params {
		r.authParams[strings.ToLower(p)] = true
	}
	return r
}

// scrub replaces every registered secret in s, in each form it can take
// on the wire: raw, URL query-escaped, URL path-escaped and JSON-escaped.
func (r *redactor) scrub(s string) string {
	for _, sec := range r.secrets {
		for _, form := range secretForms(sec) {
			s = strings.ReplaceAll(s, form, Placeholder)
		}
	}
	return s
}

func secretForms(sec string) []string {
	forms := []string{sec}
	add := func(f string) {
		for _, have := range forms {
			if have == f {
				return
			}
		}
		forms = append(forms, f)
	}
	add(url.QueryEscape(sec))
	add(url.PathEscape(sec))
	if b, err := json.Marshal(sec); err == nil {
		add(string(b[1 : len(b)-1]))
	}
	add(strings.Trim(string(marshalString(sec)), `"`))
	return forms
}

func (r *redactor) sensitiveParam(name string) bool {
	n := strings.ToLower(name)
	return sensitiveParams[n] || r.authParams[n]
}

// redactURL masks credential query parameters and scrubs secrets. The
// raw query is rewritten only when a parameter is masked, so an
// unaffected URL is stored exactly as sent.
func (r *redactor) redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return r.scrub(raw)
	}
	if u.User != nil {
		u.User = url.User(Placeholder)
	}
	if u.RawQuery != "" {
		parts := strings.Split(u.RawQuery, "&")
		changed := false
		for i, p := range parts {
			k, _, _ := strings.Cut(p, "=")
			name, err := url.QueryUnescape(k)
			if err != nil {
				name = k
			}
			if r.sensitiveParam(name) {
				parts[i] = k + "=" + url.QueryEscape(Placeholder)
				changed = true
			}
		}
		if changed {
			u.RawQuery = strings.Join(parts, "&")
		}
	}
	return r.scrub(u.String())
}

func (r *redactor) sensitiveRequestHeader(name string) bool {
	lk := strings.ToLower(name)
	// Content-Type and similar never carry credentials, whatever
	// fragment they contain.
	if lk == "content-type" || lk == "accept" || lk == "content-length" {
		return false
	}
	if sensitiveRequestHeaders[lk] || r.authHeaders[lk] {
		return true
	}
	for _, frag := range sensitiveHeaderFragments {
		if strings.Contains(lk, frag) {
			return true
		}
	}
	return false
}

// learnHeaderSecrets registers the credentials a request carries in its
// headers (the whole value, and the part after the scheme of
// "Bearer <token>" / "Basic <base64>"), so a server that echoes them in
// its response, or a later request that reuses them, is masked too.
// Only headers that are credentials by name are learned, and only values
// of minLearnedLen or more: a learned value is masked everywhere, so an
// "X-Idempotency-Key: 1234" must not turn every 1234 in the data into the
// placeholder.
func (r *redactor) learnHeaderSecrets(h http.Header) {
	for k, vs := range h {
		lk := strings.ToLower(k)
		learn := sensitiveRequestHeaders[lk] || r.authHeaders[lk]
		for _, frag := range []string{"token", "secret", "password", "apikey", "api-key", "api_key"} {
			if strings.Contains(lk, frag) {
				learn = true
			}
		}
		if !learn {
			continue
		}
		for _, v := range vs {
			cands := []string{v}
			if _, cred, ok := strings.Cut(v, " "); ok {
				cands = append(cands, strings.TrimSpace(cred))
			}
			for _, c := range cands {
				if len(c) >= minLearnedLen && !strings.Contains(c, Placeholder) {
					RegisterSecret(c)
					r.secrets = append(r.secrets, c)
				}
			}
		}
	}
}

// minLearnedLen is the shortest header value learnHeaderSecrets masks.
const minLearnedLen = 8

func (r *redactor) redactRequestHeaders(h http.Header) map[string][]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string][]string, len(h))
	for k, vs := range h {
		out[k] = r.headerValues(vs, r.sensitiveRequestHeader(k))
	}
	return out
}

func (r *redactor) redactResponseHeaders(h http.Header) map[string][]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string][]string, len(h))
	for k, vs := range h {
		lk := strings.ToLower(k)
		out[k] = r.headerValues(vs, sensitiveResponseHeaders[lk] || r.authHeaders[lk])
	}
	return out
}

func (r *redactor) headerValues(vs []string, mask bool) []string {
	out := make([]string, len(vs))
	for i, v := range vs {
		if mask {
			out[i] = Placeholder
		} else {
			out[i] = r.scrub(v)
		}
	}
	return out
}

// redactBody masks credential fields in a JSON or form body and scrubs
// secrets. contentType decides whether a non-JSON body is a form.
func (r *redactor) redactBody(b []byte, contentType string) []byte {
	if len(b) == 0 {
		return b
	}
	b = []byte(r.scrub(string(b)))
	if json.Valid(b) {
		if out, err := redactJSON(b); err == nil {
			return out
		}
		return b
	}
	if isForm(contentType) {
		return redactForm(b)
	}
	return b
}

func isForm(contentType string) bool {
	mt, _, err := mime.ParseMediaType(contentType)
	return err == nil && mt == "application/x-www-form-urlencoded"
}

// redactForm masks credential fields in an application/x-www-form-urlencoded
// body, keeping field order.
func redactForm(b []byte) []byte {
	parts := strings.Split(string(b), "&")
	for i, p := range parts {
		k, _, _ := strings.Cut(p, "=")
		name, err := url.QueryUnescape(k)
		if err != nil {
			name = k
		}
		if isSensitiveField(name) || sensitiveParams[strings.ToLower(name)] {
			parts[i] = k + "=" + url.QueryEscape(Placeholder)
		}
	}
	return []byte(strings.Join(parts, "&"))
}

// redactJSON rewrites a JSON document with the value of every sensitive
// key (at any depth) replaced by the placeholder string. Key order and
// number literals are kept as they were, so the stored body reads like
// the API's own.
func redactJSON(b []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var out bytes.Buffer
	if err := copyJSON(dec, &out); err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("trailing data after JSON value")
	}
	return out.Bytes(), nil
}

func copyJSON(dec *json.Decoder, out *bytes.Buffer) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			out.WriteByte('{')
			first := true
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return err
				}
				k, _ := kt.(string)
				if !first {
					out.WriteByte(',')
				}
				first = false
				out.Write(marshalString(k))
				out.WriteByte(':')
				if isSensitiveField(k) {
					var discard json.RawMessage
					if err := dec.Decode(&discard); err != nil {
						return err
					}
					out.Write(marshalString(Placeholder))
					continue
				}
				if err := copyJSON(dec, out); err != nil {
					return err
				}
			}
			if _, err := dec.Token(); err != nil {
				return err
			}
			out.WriteByte('}')
		case '[':
			out.WriteByte('[')
			first := true
			for dec.More() {
				if !first {
					out.WriteByte(',')
				}
				first = false
				if err := copyJSON(dec, out); err != nil {
					return err
				}
			}
			if _, err := dec.Token(); err != nil {
				return err
			}
			out.WriteByte(']')
		}
	case string:
		out.Write(marshalString(t))
	case json.Number:
		out.WriteString(t.String())
	case bool:
		if t {
			out.WriteString("true")
		} else {
			out.WriteString("false")
		}
	case nil:
		out.WriteString("null")
	}
	return nil
}
