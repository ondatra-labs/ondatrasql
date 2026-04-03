// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"context"
	"testing"
	"time"
)

func TestCryptoBase64Roundtrip(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
encoded = crypto.base64_encode("hello world")
if encoded != "aGVsbG8gd29ybGQ=":
    fail("encode: " + encoded)
decoded = crypto.base64_decode(encoded)
if decoded != "hello world":
    fail("decode: " + decoded)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCryptoSHA256(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	// sha256("hello") = 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
	code := `
hash = crypto.sha256("hello")
if hash != "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824":
    fail("sha256: " + hash)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCryptoMD5(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	// md5("hello") = 5d41402abc4b2a76b9719d911017c592
	code := `
hash = crypto.md5("hello")
if hash != "5d41402abc4b2a76b9719d911017c592":
    fail("md5: " + hash)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCryptoHMACSHA256(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
sig = crypto.hmac_sha256("secret", "message")
if sig == "":
    fail("empty hmac")
# Just verify it produces a 64-char hex string (32 bytes)
if len(sig) != 64:
    fail("hmac length: " + str(len(sig)))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCryptoUUID(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
id = crypto.uuid()
# UUID format: 8-4-4-4-12 = 36 chars
if len(id) != 36:
    fail("uuid length: " + str(len(id)))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCryptoRandomString(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
s = crypto.random_string(length=16)
if len(s) != 16:
    fail("length: " + str(len(s)))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCryptoRandomStringDefault(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
s = crypto.random_string()
if len(s) != 32:
    fail("default length should be 32, got: " + str(len(s)))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}
