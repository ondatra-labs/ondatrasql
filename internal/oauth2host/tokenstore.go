// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package oauth2host

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// TokenFile represents a stored refresh token.
//
// The "File" name is legacy from the original filesystem-backed
// implementation. Today the token lives as a row in the state catalog's
// `tokens` table, encrypted at rest via DuckDB's file-level encryption
// (configured in config/state.sql).
type TokenFile struct {
	Provider     string
	RefreshToken string
	Local        bool
	TokenURL     string
}

var validProvider = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*[a-z0-9]$`)

// ValidateProvider checks that a provider name is safe.
func ValidateProvider(name string) error {
	if len(name) < 2 || len(name) > 64 {
		return fmt.Errorf("provider name must be 2-64 characters")
	}
	if !validProvider.MatchString(name) {
		return fmt.Errorf("provider name must be lowercase alphanumeric with hyphens")
	}
	return nil
}

// ReadToken loads a stored refresh token for a provider from the state
// catalog's tokens table. The caller passes the state-session DB handle
// (USE state already in effect, so unqualified table names resolve into
// the state catalog).
func ReadToken(db *sql.DB, provider string) (*TokenFile, error) {
	if err := ValidateProvider(provider); err != nil {
		return nil, err
	}
	var tf TokenFile
	tf.Provider = provider
	var tokenURL sql.NullString
	err := db.QueryRow(
		`SELECT refresh_token, local, token_url FROM tokens WHERE provider = ?`,
		provider,
	).Scan(&tf.RefreshToken, &tf.Local, &tokenURL)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("no auth token for %q (run: ondatrasql auth %s)", provider, provider)
	}
	if err != nil {
		return nil, fmt.Errorf("read token for %q: %w", provider, err)
	}
	if tf.RefreshToken == "" {
		return nil, fmt.Errorf("token row for %q has empty refresh_token", provider)
	}
	if tokenURL.Valid {
		tf.TokenURL = tokenURL.String
	}
	return &tf, nil
}

// WriteLocalToken stores a refresh token for a locally-managed provider
// (refreshes directly against the provider's token endpoint, using
// CLIENT_ID/CLIENT_SECRET from env).
func WriteLocalToken(db *sql.DB, provider, refreshToken, tokenURL string) error {
	if err := ValidateProvider(provider); err != nil {
		return err
	}
	_, err := db.Exec(
		`INSERT OR REPLACE INTO tokens (provider, refresh_token, local, token_url, updated_at)
		 VALUES (?, ?, true, ?, now())`,
		provider, refreshToken, tokenURL,
	)
	if err != nil {
		return fmt.Errorf("write local token for %q: %w", provider, err)
	}
	return nil
}

// ProviderEnvPrefix returns the env variable prefix for a provider name.
// e.g. "google-sheets" → "GOOGLE_SHEETS"
func ProviderEnvPrefix(provider string) string {
	s := strings.ToUpper(provider)
	return strings.ReplaceAll(s, "-", "_")
}
