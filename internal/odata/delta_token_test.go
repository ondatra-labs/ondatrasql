// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package odata

import (
	"net/url"
	"strings"
	"testing"
)

// TestDeltaToken_RoundTrip pins that encode → decode preserves all fields
// when signed with the same key. Without this, every deltaLink we ever
// emit would be unverifiable on the next request.
func TestDeltaToken_RoundTrip(t *testing.T) {
	t.Parallel()
	key := []byte("0123456789abcdef0123456789abcdef")
	original := DeltaToken{
		Snapshot:   12345,
		Entity:     "Orders",
		FilterHash: "abc123",
	}
	encoded, err := encodeDeltaToken(original, key)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if encoded == "" {
		t.Fatal("encoded token is empty")
	}
	if !strings.Contains(encoded, ".") {
		t.Errorf("expected payload.signature format, got %q", encoded)
	}

	decoded, err := decodeDeltaToken(encoded, key)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded != original {
		t.Errorf("round trip mismatch: got %+v, want %+v", decoded, original)
	}
}

// TestDeltaToken_RejectsWrongKey pins that a token signed by one key is
// rejected when verified with another. Otherwise tokens issued by a
// different server (or after a key rotation) would be silently accepted.
func TestDeltaToken_RejectsWrongKey(t *testing.T) {
	t.Parallel()
	key1 := []byte("0123456789abcdef0123456789abcdef")
	key2 := []byte("fedcba9876543210fedcba9876543210")
	encoded, err := encodeDeltaToken(DeltaToken{Snapshot: 1, Entity: "Orders"}, key1)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	_, err = decodeDeltaToken(encoded, key2)
	if err == nil {
		t.Fatal("expected signature mismatch error")
	}
	if !strings.Contains(err.Error(), "signature mismatch") {
		t.Errorf("expected signature error, got %v", err)
	}
}

// TestDeltaToken_RejectsTamperedPayload pins that flipping bits in the
// payload section invalidates the signature.
func TestDeltaToken_RejectsTamperedPayload(t *testing.T) {
	t.Parallel()
	key := []byte("0123456789abcdef0123456789abcdef")
	encoded, _ := encodeDeltaToken(DeltaToken{Snapshot: 1, Entity: "Orders"}, key)
	parts := strings.SplitN(encoded, ".", 2)
	// Replace payload with a different valid base64url payload.
	tampered := "eyJzIjozLCJlIjoiT3JkZXJzIiwiZiI6IiJ9" + "." + parts[1]
	_, err := decodeDeltaToken(tampered, key)
	if err == nil {
		t.Fatal("expected signature mismatch on tampered payload")
	}
}

// TestDeltaToken_RejectsMalformed pins various malformed inputs return
// errors rather than panicking or accepting them.
func TestDeltaToken_RejectsMalformed(t *testing.T) {
	t.Parallel()
	key := []byte("0123456789abcdef0123456789abcdef")
	cases := []string{
		"",
		"no-dot",
		"only.one.dot.too.many",
		"!!!.!!!", // not base64
	}
	for _, tc := range cases {
		t.Run(tc, func(t *testing.T) {
			if _, err := decodeDeltaToken(tc, key); err == nil {
				t.Errorf("expected error for %q, got nil", tc)
			}
		})
	}
}

// TestFilterHash_StableUnderReorder pins that the hash depends only on
// the set of relevant params, not the order they appear in the URL.
// Otherwise a client appending params in a different order would get
// a "deltaLink no longer applies" error on follow-up.
func TestFilterHash_StableUnderReorder(t *testing.T) {
	t.Parallel()
	a := url.Values{}
	a.Set("$filter", "id eq 1")
	a.Set("$select", "id,name")
	a.Set("$orderby", "name asc")

	b := url.Values{}
	b.Set("$select", "id,name")
	b.Set("$orderby", "name asc")
	b.Set("$filter", "id eq 1")

	if filterHash(a) != filterHash(b) {
		t.Errorf("filterHash should be order-independent: %s vs %s", filterHash(a), filterHash(b))
	}
}

// TestFilterHash_IgnoresPagingParams pins that $top, $skip, $count, and
// $expand do NOT participate in the hash. A client that pages through
// the original result and later follows the deltaLink must not be
// rejected because $top changed between the original and the follow-up.
func TestFilterHash_IgnoresPagingParams(t *testing.T) {
	t.Parallel()
	a := url.Values{}
	a.Set("$filter", "id eq 1")
	a.Set("$top", "10")
	a.Set("$skip", "0")
	a.Set("$count", "true")
	a.Set("$expand", "Customer")

	b := url.Values{}
	b.Set("$filter", "id eq 1")
	b.Set("$top", "100")
	b.Set("$skip", "200")

	if filterHash(a) != filterHash(b) {
		t.Errorf("paging params should not affect hash: %s vs %s", filterHash(a), filterHash(b))
	}
}

// TestFilterHash_DistinguishesContentChanges pins that an actual change
// to the filter shape produces a different hash, so deltaLinks correctly
// reject re-use against a different query.
func TestFilterHash_DistinguishesContentChanges(t *testing.T) {
	t.Parallel()
	a := url.Values{}
	a.Set("$filter", "id eq 1")

	b := url.Values{}
	b.Set("$filter", "id eq 2")

	if filterHash(a) == filterHash(b) {
		t.Error("different $filter values must produce different hashes")
	}
}

// TestEmitDeltaLink_PreservesSelectAndOrderby pins that emitDeltaLink
// re-emits $select and $orderby in the URL. filterHash hashes those, so
// if they were dropped the follow-up request would hash empty and be
// rejected as DeltaLinkFilterChanged. The link must be self-contained —
// the client is expected to follow it as opaque, not patch query options
// back in.
func TestEmitDeltaLink_PreservesSelectAndOrderby(t *testing.T) {
	t.Parallel()
	key := []byte("0123456789abcdef0123456789abcdef")
	entity := EntitySchema{ODataName: "Orders", KeyColumn: "id"}

	params := url.Values{}
	params.Set("$select", "id,name")
	params.Set("$orderby", "name asc")

	link := emitDeltaLink("https://api", entity, params, 42, key)
	if link == "" {
		t.Fatal("expected non-empty deltaLink")
	}

	// The link must round-trip: parsing its query string should yield a
	// filterHash equal to the one stamped into the token.
	u, err := url.Parse(link)
	if err != nil {
		t.Fatalf("parse link: %v", err)
	}
	q := u.Query()
	if q.Get("$select") != "id,name" {
		t.Errorf("$select dropped from link: %q", q.Get("$select"))
	}
	if q.Get("$orderby") != "name asc" {
		t.Errorf("$orderby dropped from link: %q", q.Get("$orderby"))
	}

	tokenStr := q.Get("$deltatoken")
	if tokenStr == "" {
		t.Fatal("$deltatoken missing from link")
	}
	tok, err := decodeDeltaToken(tokenStr, key)
	if err != nil {
		t.Fatalf("decode token: %v", err)
	}
	// The follow-up handler computes filterHash from the URL's query and
	// compares against the token's FilterHash. They must match.
	if filterHash(q) != tok.FilterHash {
		t.Errorf("filterHash mismatch on round-trip: link=%s token=%s",
			filterHash(q), tok.FilterHash)
	}
}

// TestParseDeltaKey covers the key-parsing edge cases callers care about:
// hex round-trip, length minimum, hex validation.
func TestParseDeltaKey(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		hexKey    string
		wantError bool
	}{
		{"valid 32-byte key", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", false},
		{"valid 16-byte key", "0123456789abcdef0123456789abcdef", false},
		{"empty rejected", "", true},
		{"short rejected", "deadbeef", true},
		{"non-hex rejected", "not hex at all!!", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseDeltaKey(tc.hexKey)
			if (err != nil) != tc.wantError {
				t.Errorf("parseDeltaKey(%q) err = %v, wantError = %v", tc.hexKey, err, tc.wantError)
			}
		})
	}
}
