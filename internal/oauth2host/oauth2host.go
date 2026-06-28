// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package oauth2host implements the self-contained ("local") OAuth flow:
// browser consent and token refresh run directly against the provider using
// the user's own client_id/client_secret (from env), with the refresh token
// stored in the encrypted state catalog. The hosted broker ("managed") flow
// via oauth2.ondatra.sh was removed in v0.36.0 — ondatrasql no longer depends
// on any external credential service.
package oauth2host

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

var client = &http.Client{Timeout: 30 * time.Second}

// RefreshResult holds the result of a token exchange or refresh.
type RefreshResult struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
}

// ExchangeCode exchanges an authorization code for tokens directly with a provider.
func ExchangeCode(ctx context.Context, tokenURL, clientID, clientSecret, code, redirectURI string) (*RefreshResult, error) {
	form := url.Values{
		"grant_type":    {"authorization_code"},
		"client_id":     {clientID},
		"client_secret": {clientSecret},
		"code":          {code},
		"redirect_uri":  {redirectURI},
	}
	req, err := http.NewRequestWithContext(ctx, "POST", tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("exchange code: %w", err)
	}
	defer func() { _ = resp.Body.Close() }() // read-only HTTP body close
	if resp.StatusCode != 200 {
		msg, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("exchange code: HTTP %d: %s", resp.StatusCode, msg)
	}
	var exchangeResult RefreshResult
	if err := json.NewDecoder(resp.Body).Decode(&exchangeResult); err != nil {
		return nil, fmt.Errorf("parse token response: %w", err)
	}
	return &exchangeResult, nil
}

// RefreshLocal refreshes an access token directly with a provider using the
// user's own client credentials (no external broker).
func RefreshLocal(ctx context.Context, tokenURL, clientID, clientSecret, refreshToken string) (*RefreshResult, error) {
	form := url.Values{
		"grant_type":    {"refresh_token"},
		"client_id":     {clientID},
		"client_secret": {clientSecret},
		"refresh_token": {refreshToken},
	}
	req, err := http.NewRequestWithContext(ctx, "POST", tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("refresh token: %w", err)
	}
	defer func() { _ = resp.Body.Close() }() // read-only HTTP body close
	if resp.StatusCode != 200 {
		msg, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("refresh token: HTTP %d: %s", resp.StatusCode, msg)
	}
	var localResult RefreshResult
	if err := json.NewDecoder(resp.Body).Decode(&localResult); err != nil {
		return nil, fmt.Errorf("parse refresh result: %w", err)
	}
	return &localResult, nil
}

// ListLocalProviders scans environment for *_CLIENT_ID patterns and returns provider names.
func ListLocalProviders() []string {
	var providers []string
	seen := map[string]bool{}
	for _, env := range os.Environ() {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := parts[0]
		if !strings.HasSuffix(key, "_CLIENT_ID") {
			continue
		}
		prefix := strings.TrimSuffix(key, "_CLIENT_ID")
		if os.Getenv(prefix+"_CLIENT_SECRET") == "" ||
			os.Getenv(prefix+"_AUTH_URL") == "" ||
			os.Getenv(prefix+"_TOKEN_URL") == "" ||
			os.Getenv(prefix+"_SCOPE") == "" {
			continue
		}
		name := strings.ToLower(strings.ReplaceAll(prefix, "_", "-"))
		if !seen[name] {
			seen[name] = true
			providers = append(providers, name)
		}
	}
	return providers
}
