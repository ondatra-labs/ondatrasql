// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package redact masks credentials in text before it reaches a terminal,
// a log or JSON output.
//
// Much of what it guards against is not ondatrasql's own text. DuckDB's
// extensions echo whole connection strings in their errors: a failed
// DuckLake ATTACH over Postgres prints the keyword/value string twice,
// password included, and a parser error in a CREATE SECRET prints the line
// with the key in it. Fixing that at the source is not possible from here,
// so every error is redacted on its way out instead.
package redact

import "regexp"

// Marker replaces every masked value.
const Marker = "[REDACTED]"

// The patterns cover what DuckDB and libpq echo back in their errors, and
// nothing else: HTTP bodies and headers are the Starlark runtime's business
// (script.RedactSecrets, which adds its own patterns on top of these). A
// rule that also tried to recognise JSON, camelCase API fields or header
// syntax would mangle the diagnostics it is meant to keep readable — token
// counts, pagination cursors, query strings.
//
// No rule crosses a line break: DuckDB errors span several lines (the echoed
// SQL, then a caret line), and the next line is diagnosis, not value.

var (
	// libpq keyword/value strings: password=, and the env-style names that
	// hold one (PGPASSWORD=, PG_PASSWORD=); Azure connection strings'
	// AccountKey= and SharedAccessSignature= have the same shape. The value is either quoted —
	// libpq's password='a b', or the same inside a SQL literal with the
	// quotes doubled, password=''a b'' — or runs to the next space or quote.
	// An unterminated quote runs to the end of its line, since DuckDB cuts
	// long echoed lines short. An empty password='' masks nothing.
	libpqPasswordRe = regexp.MustCompile(
		`(?im)((?:^|[^A-Za-z0-9])[A-Za-z0-9_]*(?:password|passwd|accountkey|sharedaccesssignature)[ \t]*=[ \t]*)` +
			`(?:''[^'\s](?:[^'\n]|'[^'\n])*(?:''|$)|'[^'\s](?:[^'\\\n]|\\.)*(?:'|$)|[^\s'"]+)`)

	// The password in a URL's userinfo: postgresql://user:PASSWORD@host. It
	// runs to the last @ before the path, so an unencoded @ inside it is
	// masked too. It stops at / ? #, which keeps https://host:8080/p?e=a@b.com
	// intact; a / inside a URL password is not recognised.
	urlUserinfoRe = regexp.MustCompile(`([A-Za-z][A-Za-z0-9+.\-]*://[^\s:/@"']*:)([^\s"'/?#]+)@`)

	// DuckDB option syntax, as echoed by a parser error: SECRET 'value' and
	// KEY_ID 'value' in CREATE SECRET, ENCRYPTION_KEY 'value' in the default
	// state.sql ATTACH. A doubled quote inside the value is part of it, and a
	// missing closing quote redacts to the end of the line.
	sqlOptionRe = regexp.MustCompile(
		`(?im)((?:^|[^A-Za-z0-9_])(?:secret|password|token|session_token|bearer_token|access_token|refresh_token|sas_token|client_secret|key_id|account_key|connection_string|encryption_key)[ \t]+)'(?:''|[^'\n])*(?:'|$)`)

	// A MAP or struct entry keyed by a header or credential name, as in an
	// http secret's EXTRA_HTTP_HEADERS MAP {'Authorization': 'Token x'}.
	sqlMapEntryRe = regexp.MustCompile(
		`(?im)('(?:authorization|x-api-key|api[-_]?key|token|password|secret|cookie)'[ \t]*:[ \t]*)'(?:''|[^'\n])*(?:'|$)`)

	// "Bearer <token>" wherever it appears, as echoed from an http secret.
	bearerRe = regexp.MustCompile(`(?i)(Bearer[ \t]+)[A-Za-z0-9._~+/=\-]+`)
)

// String returns s with every recognised credential replaced by Marker.
func String(s string) string {
	s = urlUserinfoRe.ReplaceAllString(s, "${1}"+Marker+"@")
	s = libpqPasswordRe.ReplaceAllString(s, "${1}"+Marker)
	s = sqlOptionRe.ReplaceAllString(s, "${1}"+Marker)
	s = sqlMapEntryRe.ReplaceAllString(s, "${1}"+Marker)
	s = bearerRe.ReplaceAllString(s, "${1}"+Marker)
	return s
}

// Error returns err with its message redacted. An error with nothing to hide
// comes back unchanged. A redacted one does not unwrap: anything reachable
// through Unwrap still carries the original text, and errors.As would hand it
// to the next caller that prints a field of it. Only errors that held a
// credential lose their chain, and those are connection failures that no
// caller branches on.
func Error(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	clean := String(msg)
	if clean == msg {
		return err
	}
	return redactedError(clean)
}

type redactedError string

func (e redactedError) Error() string { return string(e) }
