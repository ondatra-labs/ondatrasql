// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// digestCredentials holds username/password for HTTP Digest Authentication.
type digestCredentials struct {
	Username string
	Password string
}

// parseWWWAuthenticate extracts Digest challenge parameters from a
// WWW-Authenticate header value (RFC 2617/7616).
func parseWWWAuthenticate(header string) map[string]string {
	params := make(map[string]string)

	// Strip "Digest " prefix
	header = strings.TrimPrefix(header, "Digest ")

	// Parse key=value pairs, handling quoted values
	for header != "" {
		header = strings.TrimLeft(header, " ")

		eq := strings.IndexByte(header, '=')
		if eq == -1 {
			break
		}
		key := strings.TrimSpace(header[:eq])
		header = header[eq+1:]

		var value string
		if len(header) > 0 && header[0] == '"' {
			// Quoted value
			end := strings.IndexByte(header[1:], '"')
			if end == -1 {
				value = header[1:]
				header = ""
			} else {
				value = header[1 : end+1]
				header = header[end+2:]
			}
		} else {
			// Unquoted value
			end := strings.IndexByte(header, ',')
			if end == -1 {
				value = strings.TrimSpace(header)
				header = ""
			} else {
				value = strings.TrimSpace(header[:end])
				header = header[end:]
			}
		}

		// Skip comma separator
		header = strings.TrimLeft(header, ", ")

		params[key] = value
	}

	return params
}

// computeDigestResponse builds the Authorization header value for Digest auth.
// Supports MD5 (default) and SHA-256 algorithms with qop=auth.
func computeDigestResponse(creds *digestCredentials, challenge, method, uri string) string {
	params := parseWWWAuthenticate(challenge)

	realm := params["realm"]
	nonce := params["nonce"]
	qop := params["qop"]
	opaque := params["opaque"]
	algorithm := params["algorithm"]
	if algorithm == "" {
		algorithm = "MD5"
	}

	// Pick hash function
	hashFn := hashMD5
	if strings.EqualFold(algorithm, "SHA-256") {
		hashFn = hashSHA256
	}

	// Generate cnonce
	cnonce := generateCnonce()
	nc := "00000001"

	// HA1 = H(username:realm:password)
	ha1 := hashFn(fmt.Sprintf("%s:%s:%s", creds.Username, realm, creds.Password))

	// HA2 = H(method:uri) for auth, H(method:uri:H(body)) for auth-int
	var ha2 string
	if qop == "auth-int" {
		// RFC 2617: HA2 = H(method:uri:H(entityBody))
		// We don't have the request body, so use H("") as body hash
		ha2 = hashFn(fmt.Sprintf("%s:%s:%s", method, uri, hashFn("")))
	} else {
		ha2 = hashFn(fmt.Sprintf("%s:%s", method, uri))
	}

	// Response
	var response string
	if qop == "auth" || qop == "auth-int" {
		// qop specified: H(HA1:nonce:nc:cnonce:qop:HA2)
		response = hashFn(fmt.Sprintf("%s:%s:%s:%s:%s:%s", ha1, nonce, nc, cnonce, qop, ha2))
	} else {
		// No qop: H(HA1:nonce:HA2)
		response = hashFn(fmt.Sprintf("%s:%s:%s", ha1, nonce, ha2))
	}

	// Build header value
	parts := []string{
		fmt.Sprintf(`username="%s"`, creds.Username),
		fmt.Sprintf(`realm="%s"`, realm),
		fmt.Sprintf(`nonce="%s"`, nonce),
		fmt.Sprintf(`uri="%s"`, uri),
		fmt.Sprintf(`algorithm=%s`, algorithm),
		fmt.Sprintf(`response="%s"`, response),
	}
	if qop != "" {
		parts = append(parts, fmt.Sprintf("qop=%s", qop))
		parts = append(parts, fmt.Sprintf("nc=%s", nc))
		parts = append(parts, fmt.Sprintf(`cnonce="%s"`, cnonce))
	}
	if opaque != "" {
		parts = append(parts, fmt.Sprintf(`opaque="%s"`, opaque))
	}

	return "Digest " + strings.Join(parts, ", ")
}

func hashMD5(s string) string {
	h := md5.Sum([]byte(s))
	return hex.EncodeToString(h[:])
}

func hashSHA256(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func generateCnonce() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}
