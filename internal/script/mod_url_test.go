// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"context"
	"testing"
	"time"
)

func TestURLBuild(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
result = url.build("https://api.com/data", params={"page": "1", "limit": "100"})
if result != "https://api.com/data?limit=100&page=1":
    fail("unexpected: " + result)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestURLBuildNoParams(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
result = url.build("https://api.com/data")
if result != "https://api.com/data":
    fail("unexpected: " + result)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestURLEncode(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
result = url.encode("hello world&foo=bar")
if result != "hello+world%26foo%3Dbar":
    fail("unexpected: " + result)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestURLEncodeParams(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
result = url.encode_params({"foo": "bar", "baz": "hello world"})
if result != "baz=hello+world&foo=bar":
    fail("unexpected: " + result)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestURLParse(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
parsed = url.parse("https://api.example.com/v1/data?foo=bar&baz=qux")
if parsed.scheme != "https":
    fail("scheme: " + parsed.scheme)
if parsed.host != "api.example.com":
    fail("host: " + parsed.host)
if parsed.path != "/v1/data":
    fail("path: " + parsed.path)
if parsed.query.foo != "bar":
    fail("query.foo: " + parsed.query.foo)
if parsed.query.baz != "qux":
    fail("query.baz: " + parsed.query.baz)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}
