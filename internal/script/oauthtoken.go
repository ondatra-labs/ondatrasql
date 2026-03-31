// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sync"
	"time"

	"go.starlark.net/starlark"
)

// tokenProvider is a managed OAuth token that auto-refreshes before expiry.
// It implements starlark.HasAttrs so scripts can access token.access_token.
type tokenProvider struct {
	ctx context.Context
	mu  sync.Mutex

	// Config for client_credentials flow
	tokenURL     string
	clientID     string
	clientSecret string
	scope        string

	// Config for Google service account flow
	googleSAKey *ServiceAccountKey

	// Cached token state
	accessToken string
	expiresAt   time.Time

	// For testing: allow injecting a clock
	now func() time.Time
}

var _ starlark.HasAttrs = (*tokenProvider)(nil)

func (tp *tokenProvider) String() string        { return "oauth.Token" }
func (tp *tokenProvider) Type() string           { return "oauth.Token" }
func (tp *tokenProvider) Freeze()                {}
func (tp *tokenProvider) Truth() starlark.Bool   { return true }
func (tp *tokenProvider) Hash() (uint32, error)  { return 0, fmt.Errorf("unhashable: oauth.Token") }
func (tp *tokenProvider) AttrNames() []string    { return []string{"access_token"} }

func (tp *tokenProvider) Attr(name string) (starlark.Value, error) {
	if name != "access_token" {
		return nil, nil
	}
	tok, err := tp.AccessToken()
	if err != nil {
		return nil, err
	}
	return starlark.String(tok), nil
}

// refreshMargin is how long before expiry we trigger a refresh.
const refreshMargin = 60 * time.Second

// AccessToken returns a valid access token, refreshing if needed.
func (tp *tokenProvider) AccessToken() (string, error) {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	now := time.Now()
	if tp.now != nil {
		now = tp.now()
	}

	if tp.accessToken != "" && now.Before(tp.expiresAt.Add(-refreshMargin)) {
		return tp.accessToken, nil
	}

	tokenResp, err := tp.fetchToken()
	if err != nil {
		return "", fmt.Errorf("token refresh: %w", err)
	}

	tok, ok := tokenResp["access_token"].(string)
	if !ok || tok == "" {
		return "", fmt.Errorf("token response missing access_token")
	}
	tp.accessToken = tok

	// Parse expires_in (seconds)
	tp.expiresAt = now.Add(3600 * time.Second) // default 1h
	if ei, ok := tokenResp["expires_in"].(float64); ok && ei > 0 {
		tp.expiresAt = now.Add(time.Duration(ei) * time.Second)
	}

	return tp.accessToken, nil
}

func (tp *tokenProvider) fetchToken() (map[string]interface{}, error) {
	if tp.googleSAKey != nil {
		return tp.fetchGoogleToken()
	}
	return tp.fetchClientCredentials()
}

func (tp *tokenProvider) fetchClientCredentials() (map[string]interface{}, error) {
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", tp.clientID)
	form.Set("client_secret", tp.clientSecret)
	if tp.scope != "" {
		form.Set("scope", tp.scope)
	}
	return DoOAuthRequest(tp.ctx, tp.tokenURL, form)
}

func (tp *tokenProvider) fetchGoogleToken() (map[string]interface{}, error) {
	jwt, err := CreateGoogleJWT(*tp.googleSAKey, tp.scope)
	if err != nil {
		return nil, fmt.Errorf("create JWT: %w", err)
	}
	return ExchangeJWTForToken(tp.ctx, tp.googleSAKey.TokenURI, jwt)
}

// newTokenProvider creates a managed token from Starlark kwargs.
func newTokenProvider(ctx context.Context, kwargs []starlark.Tuple) (*tokenProvider, error) {
	var tokenURL, clientID, clientSecret, scope string
	var googleServiceAccount, googleKeyFile string

	if err := starlark.UnpackArgs("oauth.token", nil, kwargs,
		"token_url?", &tokenURL,
		"client_id?", &clientID,
		"client_secret?", &clientSecret,
		"scope?", &scope,
		"google_service_account?", &googleServiceAccount,
		"google_key_file?", &googleKeyFile,
	); err != nil {
		return nil, err
	}

	tp := &tokenProvider{
		ctx:          ctx,
		tokenURL:     tokenURL,
		clientID:     clientID,
		clientSecret: clientSecret,
		scope:        scope,
	}

	// Google service account flow
	if googleServiceAccount != "" || googleKeyFile != "" {
		var keyData []byte
		var err error
		if googleKeyFile != "" {
			keyData, err = os.ReadFile(googleKeyFile)
			if err != nil {
				return nil, fmt.Errorf("read key file: %w", err)
			}
		} else {
			keyData = []byte(googleServiceAccount)
		}

		var saKey ServiceAccountKey
		if err := json.Unmarshal(keyData, &saKey); err != nil {
			return nil, fmt.Errorf("parse service account key: %w", err)
		}
		tp.googleSAKey = &saKey
		return tp, nil
	}

	// Client credentials flow requires token_url, client_id, client_secret
	if tokenURL == "" || clientID == "" || clientSecret == "" {
		return nil, fmt.Errorf("oauth.token: client_credentials requires token_url, client_id, client_secret")
	}

	return tp, nil
}
