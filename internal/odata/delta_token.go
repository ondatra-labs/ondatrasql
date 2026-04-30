// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package odata

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"
)

// DeltaToken is the payload carried in @odata.deltaLink. It pins the
// snapshot the client has already seen, the entity the link applies to,
// and a hash of the query options that produced the link — so that
// subsequent calls validate that the deltaLink is still being used
// against the same query.
//
// Stateless on the server side: HMAC-signed, no per-client storage.
type DeltaToken struct {
	Snapshot   int64  `json:"s"` // DuckLake snapshot id at the time the link was issued
	Entity     string `json:"e"` // OData entity name
	FilterHash string `json:"f"` // normalized hash of $select+$filter+$orderby+$apply+$compute+$search
}

// encodeDeltaToken signs the token and returns a URL-safe opaque string.
// The format is `base64url(json) + "." + base64url(hmac-sha256(json))`.
//
// Two-segment form (not base64url(json+sig)) makes the token cheap to
// inspect for debugging — you can decode just the payload without the
// signature — while still requiring the signature for verification.
func encodeDeltaToken(t DeltaToken, key []byte) (string, error) {
	if len(key) == 0 {
		return "", fmt.Errorf("delta token signing key is empty")
	}
	payload, err := json.Marshal(t)
	if err != nil {
		return "", fmt.Errorf("marshal token: %w", err)
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(payload)
	sig := mac.Sum(nil)
	enc := base64.RawURLEncoding
	return enc.EncodeToString(payload) + "." + enc.EncodeToString(sig), nil
}

// decodeDeltaToken verifies the HMAC signature and parses the payload.
// Errors are returned as plain errors; the OData layer turns them into
// 410 Gone responses (since an invalid signature means the deltaLink
// has been tampered with or was issued by a server with a different key,
// either way it's no longer usable).
func decodeDeltaToken(s string, key []byte) (DeltaToken, error) {
	if len(key) == 0 {
		return DeltaToken{}, fmt.Errorf("delta token verification key is empty")
	}
	parts := strings.SplitN(s, ".", 2)
	if len(parts) != 2 {
		return DeltaToken{}, fmt.Errorf("malformed delta token: expected payload.signature")
	}
	enc := base64.RawURLEncoding
	payload, err := enc.DecodeString(parts[0])
	if err != nil {
		return DeltaToken{}, fmt.Errorf("decode payload: %w", err)
	}
	sig, err := enc.DecodeString(parts[1])
	if err != nil {
		return DeltaToken{}, fmt.Errorf("decode signature: %w", err)
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(payload)
	if !hmac.Equal(mac.Sum(nil), sig) {
		return DeltaToken{}, fmt.Errorf("delta token signature mismatch")
	}
	var t DeltaToken
	if err := json.Unmarshal(payload, &t); err != nil {
		return DeltaToken{}, fmt.Errorf("unmarshal token: %w", err)
	}
	return t, nil
}

// filterHash builds a stable hash of the OData query options that
// affect what the client believes its result set looks like. If any of
// these options change between the original query and the deltaLink
// follow-up, the deltaLink is no longer applicable and the server
// rejects it (the client must re-issue the original query).
//
// Hashed options: $select, $filter, $orderby, $apply, $compute, $search.
// NOT hashed: $top, $skip (paging is independent), $count (cosmetic),
// $expand (navigation properties expand independently), $deltatoken (this
// is the token itself), $skiptoken (paging).
func filterHash(params url.Values) string {
	relevant := []string{"$select", "$filter", "$orderby", "$apply", "$compute", "$search"}
	parts := make([]string, 0, len(relevant))
	for _, k := range relevant {
		if v := params.Get(k); v != "" {
			parts = append(parts, k+"="+v)
		}
	}
	sort.Strings(parts) // hash is order-independent
	h := sha256.Sum256([]byte(strings.Join(parts, "&")))
	return hex.EncodeToString(h[:8]) // 64-bit truncation: collision-resistant enough for a tamper signal
}

// generateDeltaKey returns a fresh 32-byte random key, hex-encoded for
// transport in env vars. Used at server startup if no key is configured.
func generateDeltaKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("read random: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// parseDeltaKey decodes a hex-encoded delta key into raw bytes for use
// by HMAC.
func parseDeltaKey(hexKey string) ([]byte, error) {
	if hexKey == "" {
		return nil, fmt.Errorf("delta key is empty")
	}
	b, err := hex.DecodeString(hexKey)
	if err != nil {
		return nil, fmt.Errorf("delta key must be hex-encoded: %w", err)
	}
	if len(b) < 16 {
		return nil, fmt.Errorf("delta key must be at least 16 bytes, got %d", len(b))
	}
	return b, nil
}
