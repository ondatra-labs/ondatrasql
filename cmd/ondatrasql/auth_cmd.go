// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/browser"
	"github.com/ondatra-labs/ondatrasql/internal/config"
	"github.com/ondatra-labs/ondatrasql/internal/oauth2host"
	"github.com/ondatra-labs/ondatrasql/internal/output"
)

func runAuthList(ctx context.Context) error {
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

func runAuth(ctx context.Context, cfg *config.Config, provider string) error {
	// Validate provider name
	if err := oauth2host.ValidateProvider(provider); err != nil {
		return fmt.Errorf("invalid provider: %w", err)
	}

	// Read license key
	licenseKey := os.Getenv("ONDATRA_KEY")
	if licenseKey == "" {
		return fmt.Errorf("ONDATRA_KEY not set in .env (get one at https://account.ondatra.sh)")
	}

	host := oauth2host.Host()

	// Fetch provider config
	providerCfg, err := oauth2host.FetchProviderConfig(ctx, host, provider)
	if err != nil {
		return err
	}

	// Generate cryptographic state
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Errorf("generate state: %w", err)
	}
	hash := sha256.Sum256(buf)
	state := fmt.Sprintf("%x", hash)

	// Register auth request
	if err := oauth2host.Register(ctx, host, provider, state, licenseKey); err != nil {
		return err
	}

	// Build auth URL
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

	// Open browser (non-fatal on error)
	_ = browser.Open(authURL)

	output.Fprintf("Open this URL to authenticate:\n\n  %s\n\n", authURL)
	output.Fprintf("Waiting for authentication...\n")

	// Poll for token
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

			// Verify provider matches
			if result.Provider != provider {
				return fmt.Errorf("provider mismatch: expected %q, got %q", provider, result.Provider)
			}

			// Save refresh token
			if err := oauth2host.WriteToken(cfg.ProjectDir, provider, result.RefreshToken); err != nil {
				return fmt.Errorf("save token: %w", err)
			}

			output.Fprintf("Authenticated with %s\n", provider)
			return nil
		}
	}
}
