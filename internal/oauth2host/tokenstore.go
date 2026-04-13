// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package oauth2host

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// TokenFile represents a stored refresh token on disk.
type TokenFile struct {
	Provider     string `json:"provider"`
	RefreshToken string `json:"refresh_token"`
	Local        bool   `json:"local,omitempty"`
	TokenURL     string `json:"token_url,omitempty"`
	UpdatedAt    int64  `json:"updated_at"`
}

var validProvider = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*[a-z0-9]$`)

// ValidateProvider checks that a provider name is safe for use in file paths.
func ValidateProvider(name string) error {
	if len(name) < 2 || len(name) > 64 {
		return fmt.Errorf("provider name must be 2-64 characters")
	}
	if !validProvider.MatchString(name) {
		return fmt.Errorf("provider name must be lowercase alphanumeric with hyphens")
	}
	return nil
}

// TokensDir returns the tokens directory path.
func TokensDir(projectDir string) string {
	return filepath.Join(projectDir, ".ondatra", "tokens")
}

// ReadToken reads a stored refresh token for a provider.
func ReadToken(projectDir, provider string) (*TokenFile, error) {
	if err := ValidateProvider(provider); err != nil {
		return nil, err
	}
	path := filepath.Join(TokensDir(projectDir), provider+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("no auth token for %q (run: ondatrasql auth %s)", provider, provider)
	}
	var tf TokenFile
	if err := json.Unmarshal(data, &tf); err != nil {
		return nil, fmt.Errorf("invalid token file for %q: %w", provider, err)
	}
	if tf.RefreshToken == "" {
		return nil, fmt.Errorf("token file for %q has empty refresh_token", provider)
	}
	return &tf, nil
}

// WriteToken writes a refresh token to disk for a provider.
func WriteToken(projectDir, provider, refreshToken string) error {
	if err := ValidateProvider(provider); err != nil {
		return err
	}
	dir := TokensDir(projectDir)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("create tokens dir: %w", err)
	}
	tf := TokenFile{
		Provider:     provider,
		RefreshToken: refreshToken,
		UpdatedAt:    time.Now().Unix(),
	}
	data, err := json.Marshal(tf)
	if err != nil {
		return fmt.Errorf("marshal token: %w", err)
	}
	path := filepath.Join(dir, provider+".json")
	return os.WriteFile(path, data, 0600)
}

// WriteLocalToken writes a refresh token for a locally-managed provider.
func WriteLocalToken(projectDir, provider, refreshToken, tokenURL string) error {
	if err := ValidateProvider(provider); err != nil {
		return err
	}
	dir := TokensDir(projectDir)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("create tokens dir: %w", err)
	}
	tf := TokenFile{
		Provider:     provider,
		RefreshToken: refreshToken,
		Local:        true,
		TokenURL:     tokenURL,
		UpdatedAt:    time.Now().Unix(),
	}
	data, err := json.Marshal(tf)
	if err != nil {
		return fmt.Errorf("marshal token: %w", err)
	}
	path := filepath.Join(dir, provider+".json")
	return os.WriteFile(path, data, 0600)
}

// ProviderEnvPrefix returns the env variable prefix for a provider name.
// e.g. "google-sheets" → "GOOGLE_SHEETS"
func ProviderEnvPrefix(provider string) string {
	s := strings.ToUpper(provider)
	return strings.ReplaceAll(s, "-", "_")
}
