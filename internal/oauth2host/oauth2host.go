// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package oauth2host

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// DefaultHost is the default OAuth2 edge service host.
const DefaultHost = "https://oauth2.ondatra.sh"

var client = &http.Client{Timeout: 30 * time.Second}

// Host returns the configured OAuth2 host.
func Host() string {
	if h := os.Getenv("ONDATRA_OAUTH_HOST"); h != "" {
		return h
	}
	return DefaultHost
}

// ProviderConfig holds provider configuration from the edge service.
type ProviderConfig struct {
	Name        string `json:"name"`
	AuthURL     string `json:"auth_url"`
	TokenURL    string `json:"token_url"`
	ClientID    string `json:"client_id"`
	Scope       string `json:"scope"`
	RedirectURI string `json:"redirect_uri"`
	AuthParams  string `json:"auth_params,omitempty"` // extra query params (e.g. "access_type=offline&prompt=consent")
}

// ListProviders fetches the list of available providers.
func ListProviders(ctx context.Context, host string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", host+"/providers", nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list providers: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("list providers: HTTP %d", resp.StatusCode)
	}
	var providers []string
	if err := json.NewDecoder(resp.Body).Decode(&providers); err != nil {
		return nil, fmt.Errorf("parse providers list: %w", err)
	}
	return providers, nil
}

// FetchProviderConfig fetches the provider configuration.
func FetchProviderConfig(ctx context.Context, host, provider string) (*ProviderConfig, error) {
	if err := ValidateProvider(provider); err != nil {
		return nil, err
	}
	url := host + "/providers/" + provider + ".json"
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch provider config: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return nil, fmt.Errorf("unknown provider: %s", provider)
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("fetch provider config: HTTP %d", resp.StatusCode)
	}
	var cfg ProviderConfig
	if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse provider config: %w", err)
	}
	return &cfg, nil
}

// Register registers an auth request with the edge service.
func Register(ctx context.Context, host, provider, state, licenseKey string) error {
	body, _ := json.Marshal(map[string]string{
		"provider":    provider,
		"state":       state,
		"license_key": licenseKey,
	})
	req, err := http.NewRequestWithContext(ctx, "POST", host+"/oauth/register", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("register auth: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == 403 {
		return fmt.Errorf("invalid license key (check ONDATRA_KEY in .env)")
	}
	if resp.StatusCode != 200 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("register auth: HTTP %d: %s", resp.StatusCode, msg)
	}
	return nil
}

// ErrPending indicates the token is not yet available.
var ErrPending = fmt.Errorf("pending")

// PollResult holds the result from polling.
type PollResult struct {
	Provider     string `json:"provider"`
	RefreshToken string `json:"refresh_token"`
}

// Poll checks if the refresh token is available.
func Poll(ctx context.Context, host, state, licenseKey string) (*PollResult, error) {
	body, _ := json.Marshal(map[string]string{
		"state":       state,
		"license_key": licenseKey,
	})
	req, err := http.NewRequestWithContext(ctx, "POST", host+"/oauth/poll", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("poll: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return nil, ErrPending
	}
	if resp.StatusCode == 403 {
		return nil, fmt.Errorf("license key mismatch")
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("poll: HTTP %d", resp.StatusCode)
	}
	var result PollResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("parse poll result: %w", err)
	}
	return &result, nil
}

// RefreshResult holds the result of a token refresh.
type RefreshResult struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
}

// Refresh refreshes an access token via the edge service.
func Refresh(ctx context.Context, host, provider, refreshToken, licenseKey string) (*RefreshResult, error) {
	body, _ := json.Marshal(map[string]string{
		"provider":      provider,
		"refresh_token": refreshToken,
		"license_key":   licenseKey,
	})
	req, err := http.NewRequestWithContext(ctx, "POST", host+"/oauth/refresh", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("refresh token: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == 403 {
		return nil, fmt.Errorf("invalid license key (check ONDATRA_KEY in .env)")
	}
	if resp.StatusCode != 200 {
		msg, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("refresh token: HTTP %d: %s", resp.StatusCode, msg)
	}
	var result RefreshResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("parse refresh result: %w", err)
	}
	return &result, nil
}
