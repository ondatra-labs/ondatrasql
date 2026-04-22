// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/browser"
	"github.com/ondatra-labs/ondatrasql/internal/config"
	"github.com/ondatra-labs/ondatrasql/internal/oauth2host"
	"github.com/ondatra-labs/ondatrasql/internal/output"
)

func runAuthList(ctx context.Context) error {
	if os.Getenv("ONDATRA_KEY") != "" {
		// Managed: fetch from edge script
		host := oauth2host.Host()
		providers, err := oauth2host.ListProviders(ctx, host)
		if err != nil {
			return err
		}
		if len(providers) == 0 {
			output.Fprintf("No providers available yet.\n")
			return nil
		}
		output.Fprintf("Available providers:\n\n")
		for _, p := range providers {
			output.Fprintf("  %s\n", p)
		}
		output.Fprintf("\nRun: ondatrasql auth <provider>\n")
		return nil
	}

	// Local: scan .env for *_CLIENT_ID patterns
	providers := oauth2host.ListLocalProviders()
	if len(providers) == 0 {
		output.Fprintf("No providers configured.\n\n")
		output.Fprintf("Add provider credentials to .env:\n\n")
		output.Fprintf("  FORTNOX_CLIENT_ID=your-client-id\n")
		output.Fprintf("  FORTNOX_CLIENT_SECRET=your-client-secret\n")
		output.Fprintf("  FORTNOX_AUTH_URL=https://apps.fortnox.se/oauth-v1/auth\n")
		output.Fprintf("  FORTNOX_TOKEN_URL=https://apps.fortnox.se/oauth-v1/token\n")
		output.Fprintf("  FORTNOX_SCOPE=companyinformation\n")
		output.Fprintf("\nOr add ONDATRA_KEY for managed providers.\n")
		return nil
	}
	output.Fprintf("Available providers (from .env):\n\n")
	for _, p := range providers {
		output.Fprintf("  %s\n", p)
	}
	output.Fprintf("\nRun: ondatrasql auth <provider>\n")
	return nil
}

func runAuth(ctx context.Context, cfg *config.Config, provider string) error {
	if err := oauth2host.ValidateProvider(provider); err != nil {
		return fmt.Errorf("invalid provider: %w", err)
	}

	if os.Getenv("ONDATRA_KEY") != "" {
		return runAuthManaged(ctx, cfg, provider)
	}
	return runAuthLocal(ctx, cfg, provider)
}

func runAuthManaged(ctx context.Context, cfg *config.Config, provider string) error {
	licenseKey := os.Getenv("ONDATRA_KEY")
	host := oauth2host.Host()

	providerCfg, err := oauth2host.FetchProviderConfig(ctx, host, provider)
	if err != nil {
		return err
	}

	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Errorf("generate state: %w", err)
	}
	hash := sha256.Sum256(buf)
	state := fmt.Sprintf("%x", hash)

	// Generate ephemeral key for client-side encryption
	ekBuf := make([]byte, 32)
	if _, err := rand.Read(ekBuf); err != nil {
		return fmt.Errorf("generate ephemeral key: %w", err)
	}
	ephemeralKey := fmt.Sprintf("%x", ekBuf)

	if err := oauth2host.Register(ctx, host, provider, state, licenseKey, ephemeralKey); err != nil {
		return err
	}

	authParams := url.Values{
		"client_id":     {providerCfg.ClientID},
		"redirect_uri":  {providerCfg.RedirectURI},
		"scope":         {providerCfg.Scope},
		"state":         {state},
		"response_type": {"code"},
	}
	if providerCfg.AuthParams != "" {
		extra, err := url.ParseQuery(providerCfg.AuthParams)
		if err != nil {
			return fmt.Errorf("invalid auth_params from provider config: %w", err)
		}
		for k, v := range extra {
			authParams[k] = v
		}
	}
	authURL := providerCfg.AuthURL + "?" + authParams.Encode()

	_ = browser.Open(authURL)

	output.Fprintf("Open this URL to authenticate:\n\n  %s\n\n", authURL)
	output.Fprintf("Waiting for authentication...\n")

	deadline := time.After(5 * time.Minute)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("authentication timed out after 5 minutes")
		case <-ticker.C:
			result, err := oauth2host.Poll(ctx, host, state, licenseKey)
			if err != nil {
				if errors.Is(err, oauth2host.ErrPending) {
					continue
				}
				return err
			}

			if result.Provider != provider {
				return fmt.Errorf("provider mismatch: expected %q, got %q", provider, result.Provider)
			}

			refreshToken := result.RefreshToken
			if !result.Encrypted {
				return fmt.Errorf("server did not encrypt token with ephemeral key — aborting for safety")
			}
			decrypted, err := oauth2host.DecryptToken(refreshToken, ephemeralKey)
			if err != nil {
				return fmt.Errorf("decrypt token: %w", err)
			}
			refreshToken = decrypted
			output.Fprintf("Token decrypted locally (end-to-end encrypted)\n")

			if err := oauth2host.WriteToken(cfg.ProjectDir, provider, refreshToken); err != nil {
				return fmt.Errorf("save token: %w", err)
			}

			output.Fprintf("Authenticated with %s\n", provider)
			return nil
		}
	}
}

func runAuthLocal(ctx context.Context, cfg *config.Config, provider string) error {
	prefix := oauth2host.ProviderEnvPrefix(provider)

	clientID := os.Getenv(prefix + "_CLIENT_ID")
	clientSecret := os.Getenv(prefix + "_CLIENT_SECRET")
	authURL := os.Getenv(prefix + "_AUTH_URL")
	tokenURL := os.Getenv(prefix + "_TOKEN_URL")
	scope := os.Getenv(prefix + "_SCOPE")

	if clientID == "" || clientSecret == "" || authURL == "" || tokenURL == "" || scope == "" {
		return fmt.Errorf("missing .env variables for %s:\n  %s_CLIENT_ID\n  %s_CLIENT_SECRET\n  %s_AUTH_URL\n  %s_TOKEN_URL\n  %s_SCOPE",
			provider, prefix, prefix, prefix, prefix, prefix)
	}

	// Start local callback server — bind to 127.0.0.1 and use it in redirect
	// to avoid IPv4/IPv6 mismatch when "localhost" resolves to ::1
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("start callback server: %w", err)
	}
	addr := listener.Addr().(*net.TCPAddr)
	redirectURI := fmt.Sprintf("http://127.0.0.1:%d/callback", addr.Port)

	codeCh := make(chan string, 1)
	errCh := make(chan error, 1)

	mux := http.NewServeMux()
	// Generate random state for CSRF protection
	stateBytes := make([]byte, 32)
	if _, err := rand.Read(stateBytes); err != nil {
		return fmt.Errorf("generate state: %w", err)
	}
	expectedState := fmt.Sprintf("%x", stateBytes)

	mux.HandleFunc("/callback", func(w http.ResponseWriter, r *http.Request) {
		// Validate state parameter to prevent CSRF
		if r.URL.Query().Get("state") != expectedState {
			errCh <- fmt.Errorf("state mismatch in callback (possible CSRF)")
			fmt.Fprint(w, "<h1>Error</h1><p>State mismatch — possible CSRF attack.</p>")
			return
		}
		code := r.URL.Query().Get("code")
		if code == "" {
			errCh <- fmt.Errorf("no code in callback")
			fmt.Fprint(w, "<h1>Error</h1><p>No authorization code received.</p>")
			return
		}
		codeCh <- code
		fmt.Fprint(w, "<h1>Authorized</h1><p>You can close this window and return to your terminal.</p>")
	})

	srv := &http.Server{Handler: mux}
	go func() { srv.Serve(listener) }()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	// Build auth URL
	parsedAuthURL, err := url.Parse(authURL)
	if err != nil {
		return fmt.Errorf("invalid auth URL: %w", err)
	}
	params := parsedAuthURL.Query()
	params.Set("client_id", clientID)
	params.Set("redirect_uri", redirectURI)
	params.Set("response_type", "code")
	params.Set("scope", scope)
	params.Set("state", expectedState)
	parsedAuthURL.RawQuery = params.Encode()
	fullAuthURL := parsedAuthURL.String()

	_ = browser.Open(fullAuthURL)

	output.Fprintf("Local callback server on http://127.0.0.1:%d\n", addr.Port)
	output.Fprintf("Open this URL to authenticate:\n\n  %s\n\n", fullAuthURL)
	output.Fprintf("Waiting for authentication...\n")

	// Wait for callback
	var code string
	select {
	case code = <-codeCh:
	case err := <-errCh:
		return err
	case <-time.After(5 * time.Minute):
		return fmt.Errorf("authentication timed out after 5 minutes")
	case <-ctx.Done():
		return ctx.Err()
	}

	// Exchange code for tokens
	result, err := oauth2host.ExchangeCode(ctx, tokenURL, clientID, clientSecret, code, redirectURI)
	if err != nil {
		return err
	}

	if result.RefreshToken == "" {
		return fmt.Errorf("provider did not return a refresh token")
	}

	// Save with local flag
	if err := oauth2host.WriteLocalToken(cfg.ProjectDir, provider, result.RefreshToken, tokenURL); err != nil {
		return fmt.Errorf("save token: %w", err)
	}

	output.Fprintf("Authenticated with %s (local)\n", provider)
	return nil
}
