// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"

	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

// cryptoModule provides cryptographic functions.
func cryptoModule() *starlarkstruct.Module {
	return &starlarkstruct.Module{
		Name: "crypto",
		Members: starlark.StringDict{
			// base64_encode(string) -> string
			"base64_encode": starlark.NewBuiltin("crypto.base64_encode", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var data string
				if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &data); err != nil {
					return nil, err
				}
				return starlark.String(base64.StdEncoding.EncodeToString([]byte(data))), nil
			}),

			// base64_decode(string) -> string
			"base64_decode": starlark.NewBuiltin("crypto.base64_decode", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var data string
				if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &data); err != nil {
					return nil, err
				}
				decoded, err := base64.StdEncoding.DecodeString(data)
				if err != nil {
					return nil, err
				}
				return starlark.String(decoded), nil
			}),

			// sha256(string) -> string (hex)
			"sha256": starlark.NewBuiltin("crypto.sha256", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var data string
				if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &data); err != nil {
					return nil, err
				}
				h := sha256.Sum256([]byte(data))
				return starlark.String(hex.EncodeToString(h[:])), nil
			}),

			// md5(string) -> string (hex)
			"md5": starlark.NewBuiltin("crypto.md5", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var data string
				if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &data); err != nil {
					return nil, err
				}
				h := md5.Sum([]byte(data))
				return starlark.String(hex.EncodeToString(h[:])), nil
			}),

			// hmac_sha256(key, message) -> string (hex)
			"hmac_sha256": starlark.NewBuiltin("crypto.hmac_sha256", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var key, message string
				if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 2, &key, &message); err != nil {
					return nil, err
				}
				h := hmac.New(sha256.New, []byte(key))
				h.Write([]byte(message))
				return starlark.String(hex.EncodeToString(h.Sum(nil))), nil
			}),
		},
	}
}
