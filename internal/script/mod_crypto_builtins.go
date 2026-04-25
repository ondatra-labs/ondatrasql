// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"

	"go.starlark.net/starlark"
)

// hmacSha256Builtin computes HMAC-SHA256.
// Usage: sig = hmac_sha256("key", "message")
func hmacSha256Builtin() *starlark.Builtin {
	return starlark.NewBuiltin("hmac_sha256", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var key, message string
		if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 2, &key, &message); err != nil {
			return nil, err
		}
		h := hmac.New(sha256.New, []byte(key))
		h.Write([]byte(message))
		return starlark.String(hex.EncodeToString(h.Sum(nil))), nil
	})
}

// base64EncodeBuiltin encodes to base64.
// Usage: b = base64_encode("data")
func base64EncodeBuiltin() *starlark.Builtin {
	return starlark.NewBuiltin("base64_encode", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var data string
		if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &data); err != nil {
			return nil, err
		}
		return starlark.String(base64.StdEncoding.EncodeToString([]byte(data))), nil
	})
}

// base64DecodeBuiltin decodes from base64.
// Usage: s = base64_decode(encoded)
func base64DecodeBuiltin() *starlark.Builtin {
	return starlark.NewBuiltin("base64_decode", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var data string
		if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &data); err != nil {
			return nil, err
		}
		decoded, err := base64.StdEncoding.DecodeString(data)
		if err != nil {
			return nil, err
		}
		return starlark.String(decoded), nil
	})
}
