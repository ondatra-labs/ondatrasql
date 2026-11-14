// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package httpcassette

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"mime"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

// matchKey is what a replayed request must agree on with a recorded one:
// method, URL with query parameters in any order, and body (JSON compared
// canonically, forms field-order-insensitive, multipart with the random
// boundary neutralised). Headers are not compared, so credentials that
// are absent in replay do not matter. Query parameters whose value is the
// placeholder are dropped: a credential parameter is masked on record and
// usually absent in replay, where its environment variable is unset.
type matchKey struct {
	method string
	url    string
	body   string
}

func (k matchKey) bodyHash() string {
	if k.body == "" {
		return "empty"
	}
	sum := sha256.Sum256([]byte(k.body))
	return "sha256:" + hex.EncodeToString(sum[:6])
}

func keyOf(req *Request) matchKey {
	body, err := decodeBody(req.BodyEncoding, req.Body)
	if err != nil {
		body = req.Body
	}
	return matchKey{
		method: strings.ToUpper(req.Method),
		url:    canonicalURL(req.URL),
		body:   canonicalBody(body, http.Header(req.Headers).Get("Content-Type")),
	}
}

func canonicalURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	u.User = nil
	u.Fragment = ""
	u.RawFragment = ""
	if u.Path == "" {
		u.Path = "/"
		u.RawPath = ""
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return u.String()
	}
	for k, vs := range q {
		kept := vs[:0]
		for _, v := range vs {
			if v != Placeholder {
				kept = append(kept, v)
			}
		}
		if len(kept) == 0 {
			delete(q, k)
			continue
		}
		sort.Strings(kept)
		q[k] = kept
	}
	u.RawQuery = q.Encode() // Encode sorts by key
	return u.String()
}

func canonicalBody(b []byte, contentType string) string {
	if len(b) == 0 {
		return ""
	}
	if json.Valid(b) {
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.UseNumber()
		var v any
		if dec.Decode(&v) == nil {
			if out, err := json.Marshal(v); err == nil { // maps marshal with sorted keys
				return string(out)
			}
		}
	}
	mt, params, err := mime.ParseMediaType(contentType)
	if err == nil && mt == "application/x-www-form-urlencoded" {
		if q, err := url.ParseQuery(string(b)); err == nil {
			for k := range q {
				sort.Strings(q[k])
			}
			return q.Encode()
		}
	}
	if err == nil && strings.HasPrefix(mt, "multipart/") && params["boundary"] != "" {
		return strings.ReplaceAll(string(b), params["boundary"], "BOUNDARY")
	}
	return string(b)
}

// similarity scores how close a recorded request is to an unmatched one,
// for the "nearest recorded interaction" in the replay error.
func similarity(a, b matchKey) int {
	score := 0
	if a.method == b.method {
		score += 1
	}
	ua, errA := url.Parse(a.url)
	ub, errB := url.Parse(b.url)
	if errA == nil && errB == nil {
		if ua.Host == ub.Host {
			score += 2
		}
		if ua.Path == ub.Path {
			score += 4
		}
		qa, qb := ua.Query(), ub.Query()
		for k, va := range qa {
			if vb, ok := qb[k]; ok {
				score++
				if strings.Join(va, "\x00") == strings.Join(vb, "\x00") {
					score++
				}
			}
		}
	}
	if a.body == b.body {
		score += 3
	}
	return score
}
