// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package odata

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// errSkipTokenExpired is the sentinel returned by decodeSkipToken when
// max-age verification fails. Mirrors errDeltaTokenExpired.
var errSkipTokenExpired = errors.New("skip token expired")

// SkipToken is the payload carried in @odata.nextLink. It pins the
// entity, the cumulative offset reached, and a hash of the query options
// — so a client following the link cannot tamper with the offset and
// cannot swap query options between pages (which would silently produce
// inconsistent paging).
//
// Stateless on the server side: HMAC-signed, no per-client storage.
// Reuses the Keyset / kid rotation / iat machinery shared with delta
// tokens — the threat model is identical (opaque server-issued URL,
// must round-trip integrity).
type SkipToken struct {
	Entity     string `json:"e"` // OData entity name
	Offset     int64  `json:"o"` // cumulative offset = previous offset + page size
	FilterHash string `json:"f"` // hash of $select+$filter+$orderby+$compute+$search
	IssuedAt   int64  `json:"i,omitempty"`
	KeyID      string `json:"k,omitempty"`
}

// skipTokenFilterHash hashes the query options that, if changed between
// pages, would produce inconsistent paging. Differs from filterHash for
// delta tokens by NOT hashing $apply (paging through aggregations is
// allowed but the aggregation must remain consistent — same as filter)
// and NOT hashing $top (server enforces pageSize cap, client's top is
// honored as a smaller limit but doesn't break paging consistency).
//
// Hashed: $select, $filter, $orderby, $compute, $search, $apply.
// Not hashed: $top, $skip, $count, $expand, $skiptoken (the token itself).
func skipTokenFilterHash(params url.Values) string {
	relevant := []string{"$select", "$filter", "$orderby", "$compute", "$search", "$apply"}
	parts := make([]string, 0, len(relevant))
	for _, k := range relevant {
		if v := params.Get(k); v != "" {
			parts = append(parts, k+"="+v)
		}
	}
	// hash is order-independent — sort first
	for i := 0; i < len(parts); i++ {
		for j := i + 1; j < len(parts); j++ {
			if parts[j] < parts[i] {
				parts[i], parts[j] = parts[j], parts[i]
			}
		}
	}
	h := sha256.Sum256([]byte(strings.Join(parts, "&")))
	return fmt.Sprintf("%x", h[:8])
}

// encodeSkipToken signs the token with the keyset's signing key and
// returns a URL-safe opaque string. Format: same as delta tokens —
// `base64url(json) + "." + base64url(hmac-sha256(json))`.
func encodeSkipToken(t SkipToken, ks *Keyset) (string, error) {
	if ks == nil || ks.Sign == nil || len(ks.Sign.Key) == 0 {
		return "", fmt.Errorf("skip token signing key is empty")
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

// decodeSkipToken verifies the HMAC signature and parses the payload.
// Looks up the key by KeyID — rejects unknown kids. Optional max-age
// check via the maxAge argument.
func decodeSkipToken(s string, ks *Keyset, maxAge time.Duration) (SkipToken, error) {
	if ks == nil || len(ks.Accept) == 0 {
		return SkipToken{}, fmt.Errorf("skip token verification key is empty")
	}
	parts := strings.SplitN(s, ".", 2)
	if len(parts) != 2 {
		return SkipToken{}, fmt.Errorf("malformed skip token: expected payload.signature")
	}
	enc := base64.RawURLEncoding
	payload, err := enc.DecodeString(parts[0])
	if err != nil {
		return SkipToken{}, fmt.Errorf("decode payload: %w", err)
	}
	sig, err := enc.DecodeString(parts[1])
	if err != nil {
		return SkipToken{}, fmt.Errorf("decode signature: %w", err)
	}

	var t SkipToken
	if err := json.Unmarshal(payload, &t); err != nil {
		return SkipToken{}, fmt.Errorf("unmarshal token: %w", err)
	}

	key := ks.findByID(t.KeyID)
	if key == nil {
		return SkipToken{}, fmt.Errorf("unknown skip-key id %q", t.KeyID)
	}
	mac := hmac.New(sha256.New, key.Key)
	mac.Write(payload)
	if !hmac.Equal(mac.Sum(nil), sig) {
		return SkipToken{}, fmt.Errorf("skip token signature mismatch")
	}

	if maxAge > 0 {
		if t.IssuedAt <= 0 {
			return SkipToken{}, fmt.Errorf("%w: token has no issued-at, cannot verify against max-age", errSkipTokenExpired)
		}
		age := time.Since(time.Unix(t.IssuedAt, 0))
		if age > maxAge {
			return SkipToken{}, fmt.Errorf("%w: age %s exceeds max-age %s", errSkipTokenExpired, age.Round(time.Second), maxAge)
		}
	}

	// Defensive: encodeSkipToken only ever emits non-negative offsets
	// (callers compute offset+pageSize, both non-negative). A signed
	// token with a negative offset means either a key leak or a server
	// bug; either way reject up front rather than letting a "$skip=-1"
	// flow into godata.ParseSkipString and surface as a confusing
	// 400 BadRequest with no token context in the error.
	if t.Offset < 0 {
		return SkipToken{}, fmt.Errorf("skip token has negative offset %d (corrupt or maliciously signed)", t.Offset)
	}

	return t, nil
}

// emitNextLink builds the URL the server appends to a paged collection
// response so the client can fetch the next page. Carries forward any
// query options that change the result shape ($select, $orderby,
// $filter, $compute, $search, $apply) but not paging options ($top,
// $skip) — the server controls paging via the opaque token.
//
// Returns "" if the keyset is nil or signing fails (server then omits
// @odata.nextLink and the client treats the response as the final page).
func emitNextLink(baseURL string, entity EntitySchema, params url.Values, nextOffset int64, ks *Keyset) string {
	if ks == nil || ks.Sign == nil || len(ks.Sign.Key) == 0 {
		return ""
	}
	tok := SkipToken{
		Entity:     entity.ODataName,
		Offset:     nextOffset,
		FilterHash: skipTokenFilterHash(params),
	}
	encoded, err := encodeSkipToken(tok, ks)
	if err != nil {
		return ""
	}
	// Carry forward query options that affect the result shape so the
	// follow-up request hashes the same and produces consistent rows.
	// `$count` is intentionally NOT carried forward: it would force the
	// server to recompute COUNT(*) on every page (potentially expensive
	// for large filtered cross products), and the spec only requires
	// @odata.count on the first response — clients that want it have it.
	// The cost is one COUNT(*) per first request, not per page.
	q := url.Values{}
	q.Set("$skiptoken", encoded)
	for _, k := range []string{"$select", "$orderby", "$filter", "$compute", "$search", "$apply", "$expand"} {
		if v := params.Get(k); v != "" {
			q.Set(k, v)
		}
	}
	return fmt.Sprintf("%s/odata/%s?%s",
		strings.TrimSuffix(baseURL, "/"), entity.ODataName, q.Encode())
}
