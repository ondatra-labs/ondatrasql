// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"context"
	"testing"
	"time"
)

func TestXmlDecodeSimple(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
result = xml.decode("<user><name>Alice</name></user>")
if result["user"]["name"] != "Alice":
    fail("expected Alice, got: " + str(result))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestXmlDecodeAttributes(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
result = xml.decode('<user id="1"><name>Alice</name></user>')
user = result["user"]
if user["@id"] != "1":
    fail("expected @id=1, got: " + str(user))
if user["name"] != "Alice":
    fail("expected name=Alice, got: " + str(user))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestXmlDecodeList(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
result = xml.decode("<users><name>Alice</name><name>Bob</name></users>")
names = result["users"]["name"]
if type(names) != "list":
    fail("expected list, got: " + type(names))
if len(names) != 2:
    fail("expected 2 names, got: " + str(len(names)))
if names[0] != "Alice":
    fail("expected Alice, got: " + str(names[0]))
if names[1] != "Bob":
    fail("expected Bob, got: " + str(names[1]))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestXmlDecodeEmpty(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `xml.decode("")`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err == nil {
		t.Fatal("expected error on empty string")
	}
}

func TestXmlEncode(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
result = xml.encode({"user": {"name": "Alice"}})
# Decode it back to verify round-trip
parsed = xml.decode(result)
if parsed["user"]["name"] != "Alice":
    fail("round-trip failed: " + str(parsed))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestXmlModuleAvailable(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
if not xml:
    fail("xml module not available")
if not xml.decode:
    fail("xml.decode not available")
if not xml.encode:
    fail("xml.encode not available")
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}
