// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"net/url"
	"regexp"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/redact"
)

const redacted = redact.Marker

// bearerRe matches "Bearer <base64-or-jwt-token>".
var bearerRe = regexp.MustCompile(`(?i)(Bearer\s+)\S+`)

// basicRe matches "Basic <base64>".
var basicRe = regexp.MustCompile(`(?i)(Basic\s+)\S+`)

// kvRe matches key=value or key: "value" patterns for sensitive keys.
var kvRe = regexp.MustCompile(
	`(?i)((?:token|secret|password|api_key|apikey|access_token|client_secret|authorization|refresh_token)` +
		`\s*[:=]\s*)"?([^\s",}{[\]]+)"?`)

// sensitiveParams are URL query parameter names that should be redacted.
var sensitiveParams = map[string]bool{
	"api_key":       true,
	"apikey":        true,
	"key":           true,
	"token":         true,
	"access_token":  true,
	"secret":        true,
	"password":      true,
	"client_secret": true,
}

// RedactSecrets replaces known secret patterns in s with [REDACTED]: the
// connection strings and SQL options the CLI masks in DuckDB errors
// (internal/redact), then the HTTP-shaped ones here (headers, key=value in
// bodies), since a script's output can carry either.
func RedactSecrets(s string) string {
	// Connection strings first: redact.String masks a quoted value whole,
	// while kvRe would take only its first word and leave the rest
	// unrecognisable. kvRe does not match [REDACTED] again.
	s = redact.String(s)
	s = bearerRe.ReplaceAllString(s, "${1}"+redacted)
	s = basicRe.ReplaceAllString(s, "${1}"+redacted)
	return kvRe.ReplaceAllString(s, "${1}"+redacted)
}

// RedactURL redacts sensitive query parameters from a raw URL string.
func RedactURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	q := u.Query()
	changed := false
	for param := range q {
		if sensitiveParams[strings.ToLower(param)] {
			q.Set(param, redacted)
			changed = true
		}
	}
	if !changed {
		return rawURL
	}
	u.RawQuery = q.Encode()
	return u.String()
}
