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
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
)

// errDeltaTokenExpired is the sentinel returned by decodeDeltaToken when
// max-age verification fails. The HTTP handler checks for it via
// errors.Is to map to OData's DeltaLinkExpired (vs DeltaLinkInvalid for
// signature/parse errors). Sentinel rather than substring match so error
// wording can change without breaking the HTTP response classification.
var errDeltaTokenExpired = errors.New("delta token expired")

// DeltaToken is the payload carried in @odata.deltaLink. It pins the
// snapshot the client has already seen, the entity the link applies to,
// and a hash of the query options that produced the link — so that
// subsequent calls validate that the deltaLink is still being used
// against the same query.
//
// Stateless on the server side: HMAC-signed, no per-client storage.
//
// IssuedAt is an optional Unix-seconds timestamp of when the token was
// minted. Used by max-age verification (ONDATRA_ODATA_DELTA_MAX_AGE env)
// to reject tokens older than the configured limit. Pre-v0.27.0 tokens
// have IssuedAt=0 and are treated as having unknown age — accepted only
// when max-age is unset.
//
// KeyID is the identifier of the HMAC key used to sign the token. Empty
// for single-key configurations. Used during multi-key verification to
// pick the right key from the keyset (zero-downtime rotation: sign with
// new key, accept tokens signed with old key during the rotation window).
type DeltaToken struct {
	Snapshot   int64  `json:"s"` // DuckLake snapshot id at the time the link was issued
	Entity     string `json:"e"` // OData entity name
	FilterHash string `json:"f"` // normalized hash of $select+$filter+$orderby+$apply+$compute+$search
	IssuedAt   int64  `json:"i,omitempty"` // Unix seconds; 0 = legacy (pre-v0.27.0) token
	KeyID      string `json:"k,omitempty"` // kid for multi-key rotation; "" = legacy single-key
}

// KeyEntry is one HMAC key with its identifier. Empty ID for single-key
// configurations preserves the v0.26.0 token shape.
type KeyEntry struct {
	ID   string
	Key  []byte
}

// Keyset is the multi-key store used for signing and verifying delta
// tokens. The signing key is always the first entry; verification tries
// every entry by KeyID until one matches. Empty Sign means delta links
// are disabled (server fails soft, omits @odata.deltaLink).
type Keyset struct {
	Sign   *KeyEntry
	Accept []*KeyEntry
}

// findByID returns the KeyEntry matching the given ID, or nil if absent.
// "" looks up the legacy entry (single-key, no kid).
func (ks *Keyset) findByID(id string) *KeyEntry {
	if ks == nil {
		return nil
	}
	for _, k := range ks.Accept {
		if k.ID == id {
			return k
		}
	}
	return nil
}

// encodeDeltaToken signs the token with the keyset's signing key and
// returns a URL-safe opaque string. The format is
// `base64url(json) + "." + base64url(hmac-sha256(json))`.
//
// Two-segment form (not base64url(json+sig)) makes the token cheap to
// inspect for debugging — you can decode just the payload without the
// signature — while still requiring the signature for verification.
//
// IssuedAt is set to the current Unix timestamp before signing, so every
// token minted by this function carries an age usable by max-age checks.
// KeyID is copied from the keyset's signing entry.
func encodeDeltaToken(t DeltaToken, ks *Keyset) (string, error) {
	if ks == nil || ks.Sign == nil || len(ks.Sign.Key) == 0 {
		return "", fmt.Errorf("delta token signing key is empty")
	}
	t.IssuedAt = time.Now().Unix()
	t.KeyID = ks.Sign.ID
	payload, err := json.Marshal(t)
	if err != nil {
		return "", fmt.Errorf("marshal token: %w", err)
	}
	mac := hmac.New(sha256.New, ks.Sign.Key)
	mac.Write(payload)
	sig := mac.Sum(nil)
	enc := base64.RawURLEncoding
	return enc.EncodeToString(payload) + "." + enc.EncodeToString(sig), nil
}

// decodeDeltaToken verifies the HMAC signature and parses the payload.
// Looks up the key by the token's KeyID — rejects unknown kids as
// "DeltaLinkInvalid" (caller turns into 410 Gone).
//
// If maxAge > 0, additionally rejects tokens whose IssuedAt is older
// than maxAge from now. Tokens with IssuedAt=0 (pre-v0.27.0) are
// rejected when max-age is configured, accepted when it isn't.
func decodeDeltaToken(s string, ks *Keyset, maxAge time.Duration) (DeltaToken, error) {
	if ks == nil || len(ks.Accept) == 0 {
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

	// Parse payload first so we know which kid to verify against. The
	// payload bytes themselves are still untrusted at this point — we
	// only use them to look up the right key, and we never act on the
	// parsed values until the HMAC verifies.
	var t DeltaToken
	if err := json.Unmarshal(payload, &t); err != nil {
		return DeltaToken{}, fmt.Errorf("unmarshal token: %w", err)
	}

	key := ks.findByID(t.KeyID)
	if key == nil {
		return DeltaToken{}, fmt.Errorf("unknown delta-key id %q", t.KeyID)
	}
	mac := hmac.New(sha256.New, key.Key)
	mac.Write(payload)
	if !hmac.Equal(mac.Sum(nil), sig) {
		return DeltaToken{}, fmt.Errorf("delta token signature mismatch")
	}

	if maxAge > 0 {
		if t.IssuedAt <= 0 {
			return DeltaToken{}, fmt.Errorf("%w: token has no issued-at, cannot verify against max-age", errDeltaTokenExpired)
		}
		age := time.Since(time.Unix(t.IssuedAt, 0))
		if age > maxAge {
			return DeltaToken{}, fmt.Errorf("%w: age %s exceeds max-age %s", errDeltaTokenExpired, age.Round(time.Second), maxAge)
		}
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

// parseKeyset parses ONDATRA_ODATA_DELTA_KEY into a multi-key keyset.
// Accepts either:
//   - a single hex string (legacy v0.26.0 form): one entry, kid="" — used
//     to keep existing single-key deployments working without env changes
//   - comma-separated entries, each either `hex` (kid="") or `kid:hex`:
//     first entry signs new tokens, all entries are tried during verify
//
// Returns nil and an error if the input is empty or any entry fails to
// parse. The caller (loadDeltaKey) downgrades to an ephemeral key if
// the env is unset.
//
// Rotation pattern: deploy a new entry as the first item, keep the old
// one as the second. After max-age has elapsed, drop the old one.
func parseKeyset(env string) (*Keyset, error) {
	env = strings.TrimSpace(env)
	if env == "" {
		return nil, fmt.Errorf("delta key is empty")
	}
	entries := strings.Split(env, ",")
	ks := &Keyset{}
	seen := make(map[string]bool, len(entries))
	for i, raw := range entries {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return nil, fmt.Errorf("delta key entry %d is empty", i)
		}
		var id, hexKey string
		if idx := strings.Index(raw, ":"); idx >= 0 {
			id = raw[:idx]
			hexKey = raw[idx+1:]
		} else {
			// Legacy single-key form: no kid.
			id = ""
			hexKey = raw
		}
		key, err := parseDeltaKey(hexKey)
		if err != nil {
			return nil, fmt.Errorf("delta key entry %d (%q): %w", i, id, err)
		}
		if seen[id] {
			return nil, fmt.Errorf("duplicate delta-key id %q", id)
		}
		seen[id] = true
		entry := &KeyEntry{ID: id, Key: key}
		if ks.Sign == nil {
			ks.Sign = entry
		}
		ks.Accept = append(ks.Accept, entry)
	}
	return ks, nil
}
