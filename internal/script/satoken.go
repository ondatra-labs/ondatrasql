// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/httpcassette"
)

// saTokenKey identifies one service account access token: the credential
// (a hash of the key, not the key itself), the scope and the audience
// (the key's token_uri).
type saTokenKey struct {
	credential string
	scope      string
	audience   string
}

// saTokenEntry holds the cached token of one key. Its mutex is held for
// the whole exchange, so concurrent requests for the same key wait for
// one exchange instead of starting their own.
type saTokenEntry struct {
	mu        sync.Mutex
	token     string
	expiresAt time.Time
}

// saTokens caches service account access tokens for the process, so a run
// exchanges its JWT once per key and reuses the token until refreshMargin
// before expires_in, across pages, fetch calls, models and goroutines.
var saTokens = struct {
	mu      sync.Mutex
	entries map[saTokenKey]*saTokenEntry
	now     func() time.Time // for tests
}{entries: map[saTokenKey]*saTokenEntry{}}

func serviceAccountKeyOf(key *ServiceAccountKey, scope string) saTokenKey {
	sum := sha256.Sum256([]byte(key.ClientEmail + "\x00" + key.PrivateKeyID + "\x00" + key.PrivateKey))
	return saTokenKey{credential: hex.EncodeToString(sum[:]), scope: scope, audience: key.TokenURI}
}

func saTokenEntryFor(k saTokenKey) *saTokenEntry {
	saTokens.mu.Lock()
	defer saTokens.mu.Unlock()
	e, ok := saTokens.entries[k]
	if !ok {
		e = &saTokenEntry{}
		saTokens.entries[k] = e
	}
	return e
}

func saNow() time.Time {
	saTokens.mu.Lock()
	now := saTokens.now
	saTokens.mu.Unlock()
	if now != nil {
		return now()
	}
	return time.Now()
}

// serviceAccountToken returns a cached access token for key and scope, or
// exchanges a JWT for one. ctx carries the cassette scope of the request
// that triggers the exchange.
func serviceAccountToken(ctx context.Context, key *ServiceAccountKey, scope string) (string, error) {
	e := saTokenEntryFor(serviceAccountKeyOf(key, scope))
	e.mu.Lock()
	defer e.mu.Unlock()

	now := saNow()
	if e.token != "" && now.Before(e.expiresAt.Add(-refreshMargin)) {
		return e.token, nil
	}

	tp := &tokenProvider{ctx: ctx, googleSAKey: key, scope: scope}
	tokenResp, err := tp.fetchGoogleToken()
	if err != nil {
		return "", fmt.Errorf("token refresh: %w", err)
	}
	tok, ok := tokenResp["access_token"].(string)
	if !ok || tok == "" {
		return "", fmt.Errorf("token response missing access_token")
	}
	httpcassette.RegisterSecret(tok)
	e.token = tok
	e.expiresAt = now.Add(3600 * time.Second) // default 1h, as tokenProvider
	if ei, ok := tokenResp["expires_in"].(float64); ok && ei > 0 {
		e.expiresAt = now.Add(time.Duration(ei) * time.Second)
	}
	return tok, nil
}

// invalidateServiceAccountToken drops the cached token for key and scope
// if it is still tok, after the API rejected it. Comparing with tok means
// that concurrent requests rejected with the same token cause one new
// exchange, not one each.
func invalidateServiceAccountToken(key *ServiceAccountKey, scope, tok string) {
	e := saTokenEntryFor(serviceAccountKeyOf(key, scope))
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.token == tok {
		e.token = ""
		e.expiresAt = time.Time{}
	}
}

// resetServiceAccountTokens empties the cache. For tests.
func resetServiceAccountTokens() {
	saTokens.mu.Lock()
	defer saTokens.mu.Unlock()
	saTokens.entries = map[saTokenKey]*saTokenEntry{}
	saTokens.now = nil
}
