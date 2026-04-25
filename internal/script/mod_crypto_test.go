// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"context"
	"testing"
	"time"
)

func TestBase64Roundtrip(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
encoded = base64_encode("hello world")
if encoded != "aGVsbG8gd29ybGQ=":
    fail("encode: " + encoded)
decoded = base64_decode(encoded)
if decoded != "hello world":
    fail("decode: " + decoded)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestHMACSHA256(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
sig = hmac_sha256("secret", "message")
if sig == "":
    fail("empty hmac")
if len(sig) != 64:
    fail("hmac length: " + str(len(sig)))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}
