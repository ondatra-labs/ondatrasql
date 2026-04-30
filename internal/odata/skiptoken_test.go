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
	"net/url"
	"strings"
	"testing"
	"time"
)

// TestSkipToken_RoundTrip pins encode → decode preserves the user-set
// fields. iat and kid are populated by the encoder.
func TestSkipToken_RoundTrip(t *testing.T) {
	t.Parallel()
	ks := ksSingle(t, "0123456789abcdef0123456789abcdef")
	original := SkipToken{
		Entity:     "Orders",
		Offset:     500,
		FilterHash: "abc123",
	}
	encoded, err := encodeSkipToken(original, ks)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := decodeSkipToken(encoded, ks, 0)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Entity != original.Entity ||
		decoded.Offset != original.Offset ||
		decoded.FilterHash != original.FilterHash {
		t.Errorf("round trip mismatch: got %+v, want %+v", decoded, original)
	}
	if decoded.IssuedAt == 0 {
		t.Error("IssuedAt should be populated by encode")
	}
}

// TestSkipToken_RejectsTamperedOffset pins that flipping bits in the
// payload — e.g. changing the offset to skip ahead in the result set —
// invalidates the signature. Without HMAC, a malicious client could walk
// past their authorised view.
func TestSkipToken_RejectsTamperedOffset(t *testing.T) {
	t.Parallel()
	ks := ksSingle(t, "0123456789abcdef0123456789abcdef")
	encoded, _ := encodeSkipToken(SkipToken{Entity: "Orders", Offset: 100}, ks)
	parts := strings.SplitN(encoded, ".", 2)
	// Replace payload with one claiming offset=999999. Same kid (empty),
	// different content → signature must mismatch.
	tampered := "eyJlIjoiT3JkZXJzIiwibyI6OTk5OTk5LCJmIjoiIn0" + "." + parts[1]
	_, err := decodeSkipToken(tampered, ks, 0)
	if err == nil {
		t.Fatal("expected signature mismatch on tampered payload")
	}
}

// TestSkipToken_MaxAgeRejectsExpired pins that the same iat/max-age
// machinery used for delta tokens applies to skip tokens.
func TestSkipToken_MaxAgeRejectsExpired(t *testing.T) {
	t.Parallel()
	ks := ksSingle(t, "0123456789abcdef0123456789abcdef")
	encoded, _ := encodeSkipToken(SkipToken{Entity: "Orders", Offset: 100}, ks)
	time.Sleep(2 * time.Millisecond)
	_, err := decodeSkipToken(encoded, ks, 1*time.Nanosecond)
	if err == nil {
		t.Fatal("expected expiry error")
	}
	if !errors.Is(err, errSkipTokenExpired) {
		t.Errorf("expected errSkipTokenExpired, got: %v", err)
	}
}

// TestSkipTokenFilterHash_StableUnderReorder pins that the hash ignores
// param order. A client that reorders $select before $filter must get
// the same hash as the original request.
func TestSkipTokenFilterHash_StableUnderReorder(t *testing.T) {
	t.Parallel()
	a := url.Values{}
	a.Set("$filter", "id eq 1")
	a.Set("$select", "id,name")
	a.Set("$orderby", "name asc")

	b := url.Values{}
	b.Set("$select", "id,name")
	b.Set("$orderby", "name asc")
	b.Set("$filter", "id eq 1")

	if skipTokenFilterHash(a) != skipTokenFilterHash(b) {
		t.Errorf("hash should be order-independent: %s vs %s",
			skipTokenFilterHash(a), skipTokenFilterHash(b))
	}
}

// TestSkipTokenFilterHash_IgnoresPagingParams pins that $top, $skip,
// $skiptoken, $expand, $count don't affect the hash. Without this, the
// nextLink — which strips $top/$skip — would always fail filter-hash
// verification on the follow-up request.
func TestSkipTokenFilterHash_IgnoresPagingParams(t *testing.T) {
	t.Parallel()
	a := url.Values{}
	a.Set("$filter", "id eq 1")
	a.Set("$top", "10")
	a.Set("$skip", "0")
	a.Set("$count", "true")
	a.Set("$expand", "Customer")

	b := url.Values{}
	b.Set("$filter", "id eq 1")

	if skipTokenFilterHash(a) != skipTokenFilterHash(b) {
		t.Errorf("paging params should not affect hash: %s vs %s",
			skipTokenFilterHash(a), skipTokenFilterHash(b))
	}
}

// TestEmitNextLink_PreservesShapeOptions pins that the emitted nextLink
// carries forward $select / $orderby / $filter etc. (so the follow-up
// computes the same filter hash) but NOT $top / $skip (server controls
// paging via the token's offset).
func TestEmitNextLink_PreservesShapeOptions(t *testing.T) {
	t.Parallel()
	ks := ksSingle(t, "0123456789abcdef0123456789abcdef")
	entity := EntitySchema{ODataName: "Orders", KeyColumn: "id"}

	params := url.Values{}
	params.Set("$select", "id,name")
	params.Set("$orderby", "name asc")
	params.Set("$filter", "name ne 'foo'")
	params.Set("$top", "999")    // should NOT appear in nextLink
	params.Set("$skip", "100")   // should NOT appear in nextLink
	params.Set("$count", "true") // should NOT appear in nextLink (avoid recomputing COUNT(*) per page)

	link := emitNextLink("https://api", entity, params, 50, ks)
	if link == "" {
		t.Fatal("expected non-empty nextLink")
	}
	u, _ := url.Parse(link)
	q := u.Query()

	if q.Get("$select") != "id,name" {
		t.Errorf("$select dropped: %q", q.Get("$select"))
	}
	if q.Get("$orderby") != "name asc" {
		t.Errorf("$orderby dropped: %q", q.Get("$orderby"))
	}
	if q.Get("$filter") != "name ne 'foo'" {
		t.Errorf("$filter dropped: %q", q.Get("$filter"))
	}
	if q.Get("$top") != "" {
		t.Errorf("$top should not be in nextLink: %q", q.Get("$top"))
	}
	if q.Get("$skip") != "" {
		t.Errorf("$skip should not be in nextLink: %q", q.Get("$skip"))
	}
	if q.Get("$count") != "" {
		t.Errorf("$count should not be in nextLink (avoids recomputing COUNT(*) per page): %q", q.Get("$count"))
	}
	if q.Get("$skiptoken") == "" {
		t.Error("$skiptoken missing from nextLink")
	}

	// Round-trip: filterHash on the link's query must match the token's
	// filterHash. Without this property paging breaks on every nextLink.
	tok, err := decodeSkipToken(q.Get("$skiptoken"), ks, 0)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if skipTokenFilterHash(q) != tok.FilterHash {
		t.Errorf("filter-hash mismatch on round-trip: link=%s token=%s",
			skipTokenFilterHash(q), tok.FilterHash)
	}
}

// TestSkipToken_RejectsNegativeOffset pins the defensive offset check.
// encodeSkipToken never emits negative offsets, so a token with one is
// either a key leak or a server bug — reject up front with a clear
// error rather than letting "$skip=-1" propagate.
func TestSkipToken_RejectsNegativeOffset(t *testing.T) {
	t.Parallel()
	ks := ksSingle(t, "0123456789abcdef0123456789abcdef")

	// Hand-craft a signed token with offset=-1 by directly composing
	// the wire format. encodeSkipToken would refuse to emit a negative
	// offset (and stamps IssuedAt = now), so we compose by hand to
	// simulate a leaked-key adversary.
	payload, _ := json.Marshal(SkipToken{Entity: "Orders", Offset: -1})
	mac := hmac.New(sha256.New, ks.Sign.Key)
	mac.Write(payload)
	enc := base64.RawURLEncoding
	encoded := enc.EncodeToString(payload) + "." + enc.EncodeToString(mac.Sum(nil))

	_, err := decodeSkipToken(encoded, ks, 0)
	if err == nil {
		t.Fatal("expected error on negative offset, got nil")
	}
	if !strings.Contains(err.Error(), "negative offset") {
		t.Errorf("expected 'negative offset' in error, got: %v", err)
	}
}

// TestLoadPageSize pins the env contract — same shape as loadPoolSize.
func TestLoadPageSize(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		env     string
		want    int
		wantErr bool
	}{
		{"empty falls back to default", "", defaultPageSize, false},
		{"valid 1000", "1000", 1000, false},
		{"whitespace trimmed", "  500  ", 500, false},
		{"zero rejected", "0", 0, true},
		{"negative rejected", "-1", 0, true},
		{"non-numeric rejected", "lots", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := loadPageSize(tc.env)
			if (err != nil) != tc.wantErr {
				t.Fatalf("loadPageSize(%q) err = %v, wantErr = %v", tc.env, err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("loadPageSize(%q) = %d, want %d", tc.env, got, tc.want)
			}
		})
	}
}
