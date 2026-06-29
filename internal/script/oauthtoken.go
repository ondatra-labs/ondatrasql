// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/oauth2host"
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

	// Config for the local provider flow. stateDB is the state-session
	// handle where the encrypted-at-rest tokens table lives; nil when this
	// provider isn't OAuth-backed.
	provider string
	stateDB  *sql.DB

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
	if tp.provider != "" {
		return tp.fetchProviderToken()
	}
	if tp.googleSAKey != nil {
		return tp.fetchGoogleToken()
	}
	return tp.fetchClientCredentials()
}

func (tp *tokenProvider) fetchProviderToken() (map[string]interface{}, error) {
	if err := oauth2host.ValidateProvider(tp.provider); err != nil {
		return nil, err
	}

	// Externally-injected access token: when ONDATRA_OAUTH_TOKEN_<PREFIX> is set
	// (PREFIX = provider upper-cased with '-'→'_', via ProviderEnvPrefix), the
	// caller (an orchestrator / OpenBao) owns the OAuth lifecycle and hands us a
	// fresh access token. Use it directly as the Bearer credential — no consent,
	// no refresh, no state. Preferred where a secrets manager owns the refresh
	// token; it takes precedence over the self-contained local flow below.
	//
	// TrimSpace so a trailing newline from a secrets injector doesn't yield a
	// malformed "Bearer <whitespace>" header; a whitespace-only value is treated
	// as unset (falls through to the local flow). The token carries no
	// expires_in, so AccessToken() caches it for the run's default window — the
	// caller must inject a token whose lifetime covers the run (for runs longer
	// than the provider's token TTL, use the local flow instead).
	if envTok := strings.TrimSpace(os.Getenv("ONDATRA_OAUTH_TOKEN_" + oauth2host.ProviderEnvPrefix(tp.provider))); envTok != "" {
		return map[string]interface{}{"access_token": envTok}, nil
	}

	if tp.stateDB == nil {
		return nil, fmt.Errorf("oauth provider %q requested but no token available: set ONDATRA_OAUTH_TOKEN_%s (injected access token) or run `ondatrasql auth %s` for the local flow (needs state.duckdb)", tp.provider, oauth2host.ProviderEnvPrefix(tp.provider), tp.provider)
	}
	tokenFile, err := oauth2host.ReadToken(tp.stateDB, tp.provider)
	if err != nil {
		return nil, err
	}

	// Managed (hosted-broker) tokens were removed in v0.36.0. A row that
	// isn't local came from the old managed flow — point the user at re-auth.
	if !tokenFile.Local {
		prefix := oauth2host.ProviderEnvPrefix(tp.provider)
		return nil, fmt.Errorf("provider %q was authenticated via the removed managed OAuth flow — re-authenticate locally with `ondatrasql auth %s` (set %s_CLIENT_ID/%s_CLIENT_SECRET/%s_AUTH_URL/%s_TOKEN_URL/%s_SCOPE in .env)",
			tp.provider, tp.provider, prefix, prefix, prefix, prefix, prefix)
	}

	// Local: refresh directly against the provider with the user's creds.
	if tokenFile.TokenURL == "" {
		return nil, fmt.Errorf("invalid token file for %s: missing token_url (re-run: ondatrasql auth %s)", tp.provider, tp.provider)
	}
	prefix := oauth2host.ProviderEnvPrefix(tp.provider)
	clientID := os.Getenv(prefix + "_CLIENT_ID")
	clientSecret := os.Getenv(prefix + "_CLIENT_SECRET")
	if clientID == "" || clientSecret == "" {
		return nil, fmt.Errorf("%s_CLIENT_ID and %s_CLIENT_SECRET must be set in .env", prefix, prefix)
	}
	result, err := oauth2host.RefreshLocal(tp.ctx, tokenFile.TokenURL, clientID, clientSecret, tokenFile.RefreshToken)
	if err != nil {
		return nil, fmt.Errorf("refresh %s token: %w", tp.provider, err)
	}
	// Save the rotated refresh token.
	if result.RefreshToken != "" {
		if err := oauth2host.WriteLocalToken(tp.stateDB, tp.provider, result.RefreshToken, tokenFile.TokenURL); err != nil {
			return nil, fmt.Errorf("save refreshed token for %s: %w", tp.provider, err)
		}
	}

	expiresIn := float64(3600)
	if result.ExpiresIn > 0 {
		expiresIn = float64(result.ExpiresIn)
	}

	return map[string]interface{}{
		"access_token": result.AccessToken,
		"expires_in":   expiresIn,
	}, nil
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

