// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
)

func TestParseWWWAuthenticate(t *testing.T) {
	t.Parallel()
	header := `Digest realm="example.com", nonce="abc123", qop="auth", algorithm=MD5, opaque="xyz789"`
	params := parseWWWAuthenticate(header)

	tests := map[string]string{
		"realm":     "example.com",
		"nonce":     "abc123",
		"qop":       "auth",
		"algorithm": "MD5",
		"opaque":    "xyz789",
	}
	for key, want := range tests {
		if got := params[key]; got != want {
			t.Errorf("params[%q] = %q, want %q", key, got, want)
		}
	}
}

func TestParseWWWAuthenticateNoQuotes(t *testing.T) {
	t.Parallel()
	header := `Digest realm="test", nonce="n1", algorithm=SHA-256`
	params := parseWWWAuthenticate(header)

	if params["algorithm"] != "SHA-256" {
		t.Errorf("algorithm = %q, want SHA-256", params["algorithm"])
	}
	if params["realm"] != "test" {
		t.Errorf("realm = %q, want test", params["realm"])
	}
}

func TestComputeDigestResponseMD5(t *testing.T) {
	t.Parallel()
	creds := &digestCredentials{Username: "Mufasa", Password: "Circle Of Life"}
	challenge := `Digest realm="testrealm@host.com", nonce="dcd98b7102dd2f0e8b11d0f600bfb0c093", qop="auth", algorithm=MD5, opaque="5ccc069c403ebaf9f0171e9517f40e41"`

	result := computeDigestResponse(creds, challenge, "GET", "/dir/index.html")

	// Verify it starts with Digest and contains expected fields
	if !strings.HasPrefix(result, "Digest ") {
		t.Fatalf("result should start with 'Digest ', got %q", result)
	}

	// Verify the computed hash values manually
	// HA1 = MD5(Mufasa:testrealm@host.com:Circle Of Life)
	ha1Raw := md5.Sum([]byte("Mufasa:testrealm@host.com:Circle Of Life"))
	ha1 := hex.EncodeToString(ha1Raw[:])

	// HA2 = MD5(GET:/dir/index.html)
	ha2Raw := md5.Sum([]byte("GET:/dir/index.html"))
	ha2 := hex.EncodeToString(ha2Raw[:])

	// Extract cnonce and nc from result to compute expected response
	params := parseDigestAuthHeader(result)
	cnonce := params["cnonce"]
	nc := params["nc"]

	// response = MD5(HA1:nonce:nc:cnonce:auth:HA2)
	respRaw := md5.Sum([]byte(fmt.Sprintf("%s:%s:%s:%s:%s:%s",
		ha1, "dcd98b7102dd2f0e8b11d0f600bfb0c093", nc, cnonce, "auth", ha2)))
	wantResp := hex.EncodeToString(respRaw[:])

	if params["response"] != wantResp {
		t.Errorf("response = %q, want %q", params["response"], wantResp)
	}
	if params["username"] != "Mufasa" {
		t.Errorf("username = %q, want Mufasa", params["username"])
	}
	if params["realm"] != "testrealm@host.com" {
		t.Errorf("realm = %q, want testrealm@host.com", params["realm"])
	}
	if params["qop"] != "auth" {
		t.Errorf("qop = %q, want auth", params["qop"])
	}
	if params["opaque"] != "5ccc069c403ebaf9f0171e9517f40e41" {
		t.Errorf("opaque = %q", params["opaque"])
	}
}

func TestComputeDigestResponseSHA256(t *testing.T) {
	t.Parallel()
	creds := &digestCredentials{Username: "user", Password: "secret"}
	challenge := `Digest realm="example.com", nonce="testnonce", qop="auth", algorithm=SHA-256`

	result := computeDigestResponse(creds, challenge, "POST", "/api/data")

	params := parseDigestAuthHeader(result)

	// Verify SHA-256 was used
	if params["algorithm"] != "SHA-256" {
		t.Errorf("algorithm = %q, want SHA-256", params["algorithm"])
	}

	// Verify the hash using SHA-256
	ha1Raw := sha256.Sum256([]byte("user:example.com:secret"))
	ha1 := hex.EncodeToString(ha1Raw[:])

	ha2Raw := sha256.Sum256([]byte("POST:/api/data"))
	ha2 := hex.EncodeToString(ha2Raw[:])

	cnonce := params["cnonce"]
	nc := params["nc"]
	respRaw := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%s:%s:%s:%s",
		ha1, "testnonce", nc, cnonce, "auth", ha2)))
	wantResp := hex.EncodeToString(respRaw[:])

	if params["response"] != wantResp {
		t.Errorf("response = %q, want %q", params["response"], wantResp)
	}
}

func TestComputeDigestResponseNoQop(t *testing.T) {
	t.Parallel()
	creds := &digestCredentials{Username: "user", Password: "pass"}
	challenge := `Digest realm="test", nonce="abc"`

	result := computeDigestResponse(creds, challenge, "GET", "/path")

	params := parseDigestAuthHeader(result)

	// Without qop, response = MD5(HA1:nonce:HA2)
	ha1Raw := md5.Sum([]byte("user:test:pass"))
	ha1 := hex.EncodeToString(ha1Raw[:])
	ha2Raw := md5.Sum([]byte("GET:/path"))
	ha2 := hex.EncodeToString(ha2Raw[:])
	respRaw := md5.Sum([]byte(fmt.Sprintf("%s:%s:%s", ha1, "abc", ha2)))
	wantResp := hex.EncodeToString(respRaw[:])

	if params["response"] != wantResp {
		t.Errorf("response = %q, want %q", params["response"], wantResp)
	}

	// qop, nc, cnonce should not be present
	if _, ok := params["qop"]; ok {
		t.Error("qop should not be present when server doesn't send it")
	}
}

func TestParseWWWAuthenticate_NoEquals(t *testing.T) {
	t.Parallel()
	// Header with no equals sign should not crash
	params := parseWWWAuthenticate("Digest invalidheader")
	if len(params) != 0 {
		t.Errorf("expected empty params, got %v", params)
	}
}

func TestParseWWWAuthenticate_UnclosedQuote(t *testing.T) {
	t.Parallel()
	// Quoted value without closing quote
	params := parseWWWAuthenticate(`Digest realm="unclosed`)
	if params["realm"] != "unclosed" {
		t.Errorf("realm = %q, want 'unclosed'", params["realm"])
	}
}

// parseDigestAuthHeader is a test helper that parses "Digest key=value, ..." into a map.
func parseDigestAuthHeader(header string) map[string]string {
	header = strings.TrimPrefix(header, "Digest ")
	params := make(map[string]string)
	for header != "" {
		header = strings.TrimLeft(header, " ")
		eq := strings.IndexByte(header, '=')
		if eq == -1 {
			break
		}
		key := header[:eq]
		header = header[eq+1:]

		var value string
		if len(header) > 0 && header[0] == '"' {
			end := strings.IndexByte(header[1:], '"')
			if end == -1 {
				value = header[1:]
				header = ""
			} else {
				value = header[1 : end+1]
				header = header[end+2:]
			}
		} else {
			end := strings.IndexByte(header, ',')
			if end == -1 {
				value = strings.TrimSpace(header)
				header = ""
			} else {
				value = strings.TrimSpace(header[:end])
				header = header[end:]
			}
		}
		header = strings.TrimLeft(header, ", ")
		params[key] = value
	}
	return params
}
