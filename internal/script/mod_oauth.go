// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"context"

	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

// oauthModule provides OAuth 2.0 helpers with kwargs.
func oauthModule(ctx context.Context) *starlarkstruct.Module {
	return &starlarkstruct.Module{
		Name: "oauth",
		Members: starlark.StringDict{
			// token(token_url?, client_id?, client_secret?, scope?, google_service_account?, google_key_file?) -> managed token
			"token": starlark.NewBuiltin("oauth.token", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				return newTokenProvider(ctx, kwargs)
			}),

			// basic_auth(username, password) -> "Basic base64..."
			"basic_auth": starlark.NewBuiltin("oauth.basic_auth", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var username, password string
				if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 2, &username, &password); err != nil {
					return nil, err
				}
				return starlark.String(BasicAuth(username, password)), nil
			}),
		},
	}
}
