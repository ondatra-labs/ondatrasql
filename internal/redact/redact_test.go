// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package redact

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

const pw = "S3ntinel-pw_42"

// The inputs are DuckDB 1.5.4's own error text, captured from the CLI.
func TestString_DuckDBErrors(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, in string }{
		{"ducklake attach over postgres, keyword/value echoed twice",
			`IO Error: Failed to attach DuckLake MetaData "__ducklake_metadata_lake" at path + "postgres:dbname=x host= port=5432 user=postgres password=` + pw + ` sslmode=disable"Unable to connect to Postgres at "dbname=x host= port=5432 user=postgres password=` + pw + ` sslmode=disable": could not translate host name "port=5432" to address: Name or service not known`},
		{"postgres attach, URL form",
			`IO Error: Unable to connect to Postgres at "postgresql://postgres:` + pw + `@nohost.invalid:5432/x": could not translate host name "nohost.invalid" to address`},
		{"URL password with an unencoded @", `postgresql://u:pa@` + pw + `@host:5432/db`},
		{"parser error echoing a CREATE SECRET line",
			"Parser Error: syntax error at or near \"REGION\"\n\nLINE 1: ... SECRET s (TYPE s3, KEY_ID 'AKIAXX', SECRET '" + pw + "' REGION 'x');"},
		{"line cut short by DuckDB, no closing quote",
			"LINE 1: CREATE SECRET s (TYPE s3, SECRET '" + pw + "..."},
		{"state.sql encryption key in a parser echo",
			"Parser Error: syntax error at or near \"BOGUS\"\n\nLINE 1: ATTACH 'x.duckdb' AS s (ENCRYPTION_KEY '" + pw + "' BOGUS);"},
		{"http secret bearer token option", "CREATE SECRET h (TYPE http, BEARER_TOKEN '" + pw + "')"},
		{"http secret header map", "EXTRA_HTTP_HEADERS MAP {'Authorization': 'Token " + pw + "', 'X-Api-Key': 'k'}"},
		{"bearer token echoed", "Authorization: Bearer " + pw},
		{"doubled quote inside a SQL option value", "SECRET 'ab''" + pw + "'"},
		{"libpq quoted value", `password='` + pw + ` with space' host=x`},
		{"libpq quoted value inside a SQL literal", `ATTACH 'dbname=x password=''` + pw + ` w'' host=h'`},
		{"unterminated quoted value", `password='` + pw},
		{"apostrophe and comma inside an unquoted libpq value", `password=ab,` + pw + ` host=h`},
		{"libpq sslpassword", `sslpassword=` + pw + ` host=x`},
		{"libpq env var", `PGPASSWORD=` + pw},
		{"env-style name", `PG_PASSWORD=` + pw},
		{"semicolon inside the value", `password=ab;` + pw},
		{"Azure connection string", `DefaultEndpointsProtocol=https;AccountName=a;AccountKey=` + pw + `;EndpointSuffix=x`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := String(tt.in)
			if strings.Contains(got, pw) {
				t.Fatalf("password survived redaction:\n%s", got)
			}
			if !strings.Contains(got, Marker) {
				t.Fatalf("expected %s in:\n%s", Marker, got)
			}
		})
	}
}

// A quoted password may contain spaces, and DuckDB cuts the echoed line
// short and puts a caret line under it. Every word of the password must go,
// which only the quoted rule can see: the unquoted one stops at the space.
func TestString_UnterminatedQuoteBeforeCaretLine(t *testing.T) {
	t.Parallel()
	for _, in := range []string{
		"LINE 1: ATTACH 'x' (password='first-half second-half...\n        ^",
		"LINE 1: ATTACH 'x:password=''first-half second-half...\n        ^",
	} {
		got := String(in)
		if strings.Contains(got, "first-half") || strings.Contains(got, "second-half") {
			t.Errorf("part of the password survived:\n%s", got)
		}
		if !strings.HasSuffix(got, "\n        ^") {
			t.Errorf("the caret line must be kept:\n%s", got)
		}
	}
}

func TestString_KeepsTheDiagnosis(t *testing.T) {
	t.Parallel()
	in := `Unable to connect to Postgres at "dbname=x host=db port=5432 user=postgres password=` + pw + ` sslmode=disable": could not translate host name`
	want := `Unable to connect to Postgres at "dbname=x host=db port=5432 user=postgres password=[REDACTED] sslmode=disable": could not translate host name`
	if got := String(in); got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	url := `postgresql://postgres:` + pw + `@db:5432/x`
	if got, want := String(url), `postgresql://postgres:[REDACTED]@db:5432/x`; got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

// Text with nothing secret in it comes through unchanged — including the
// word "secret" in DuckDB's own prose and a CREATE SECRET name.
func TestString_LeavesOrdinaryTextAlone(t *testing.T) {
	t.Parallel()
	for _, in := range []string{
		`Catalog Error: Table with name orders does not exist!`,
		`CREATE SECRET garage_s3 (TYPE s3, REGION 'hel1')`,
		`audit failed: row_count > 60 failed: actual count is 0`,
		`http://example.com:8080/path`,
		`https://host:8080/path?email=a@b.com`,
		`https://api.example.com:443/users/me@example.com`,
		// Found in review: each was mangled by a broader rule.
		`{"promptTokenCount": 12, "totalTokenCount": 40, "nextPageToken": "abc"}`,
		`GET https://api.x/v1?api_key_hint=none&page=2&limit=50 returned 404`,
		`Basic authentication is not supported`,
		`dbname=x password='' host=db port=5432 user=x`,
		"password:\nconnection refused",
		`input_token_count=5 token_type=bearer`,
		// PWD is the shell's working directory far more often than an ODBC
		// password, and Starlark print() output goes through this too.
		`PWD=/home/x OLDPWD=/tmp`,
	} {
		if got := String(in); got != in {
			t.Errorf("changed ordinary text:\n in  %s\n out %s", in, got)
		}
	}
}

func TestError(t *testing.T) {
	t.Parallel()
	if Error(nil) != nil {
		t.Fatal("nil must stay nil")
	}
	plain := errors.New("nothing to hide")
	if Error(plain) != plain {
		t.Fatal("an error with nothing to redact must come back unchanged")
	}
	cause := fmt.Errorf("attach: password=%s: %w", pw, context.Canceled)
	got := Error(cause)
	if strings.Contains(got.Error(), pw) {
		t.Fatalf("password survived: %s", got)
	}
	// Nothing reachable through the chain may still hold the original.
	if errors.Unwrap(got) != nil {
		t.Fatal("a redacted error must not unwrap to the original")
	}
	if wrapped := fmt.Errorf("init session: %w", got); strings.Contains(wrapped.Error(), pw) {
		t.Fatalf("wrapping must not bring the password back: %s", wrapped)
	}
}
