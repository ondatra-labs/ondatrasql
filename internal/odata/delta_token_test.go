// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package odata

import (
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"
)

// helper: a minimal single-entry keyset for tests that don't care about kid
// or rotation.
func ksSingle(t *testing.T, key string) *Keyset {
	t.Helper()
	ks, err := parseKeyset(key)
	if err != nil {
		t.Fatalf("parseKeyset(%q): %v", key, err)
	}
	return ks
}

// TestDeltaToken_RoundTrip pins that encode → decode preserves all fields
// when signed with the same key. Without this, every deltaLink we ever
// emit would be unverifiable on the next request.
func TestDeltaToken_RoundTrip(t *testing.T) {
	t.Parallel()
	ks := ksSingle(t, "0123456789abcdef0123456789abcdef")
	original := DeltaToken{
		Snapshot:   12345,
		Entity:     "Orders",
		FilterHash: "abc123",
	}
	encoded, err := encodeDeltaToken(original, ks)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if encoded == "" {
		t.Fatal("encoded token is empty")
	}
	if !strings.Contains(encoded, ".") {
		t.Errorf("expected payload.signature format, got %q", encoded)
	}

	decoded, err := decodeDeltaToken(encoded, ks, 0)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	// IssuedAt and KeyID are populated by encodeDeltaToken, so compare
	// the user-controlled fields explicitly.
	if decoded.Snapshot != original.Snapshot ||
		decoded.Entity != original.Entity ||
		decoded.FilterHash != original.FilterHash {
		t.Errorf("round trip mismatch: got %+v, want %+v", decoded, original)
	}
	if decoded.IssuedAt == 0 {
		t.Error("IssuedAt should be populated by encodeDeltaToken")
	}
	if decoded.KeyID != "" {
		t.Errorf("expected empty KeyID for legacy single-key path, got %q", decoded.KeyID)
	}
}

// TestDeltaToken_RejectsWrongKey pins that a token signed by one key is
// rejected when verified with another. Otherwise tokens issued by a
// different server (or after a key rotation) would be silently accepted.
func TestDeltaToken_RejectsWrongKey(t *testing.T) {
	t.Parallel()
	ks1 := ksSingle(t, "0123456789abcdef0123456789abcdef")
	ks2 := ksSingle(t, "fedcba9876543210fedcba9876543210")
	encoded, err := encodeDeltaToken(DeltaToken{Snapshot: 1, Entity: "Orders"}, ks1)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	_, err = decodeDeltaToken(encoded, ks2, 0)
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
	ks := ksSingle(t, "0123456789abcdef0123456789abcdef")
	encoded, _ := encodeDeltaToken(DeltaToken{Snapshot: 1, Entity: "Orders"}, ks)
	parts := strings.SplitN(encoded, ".", 2)
	// Replace payload with a different valid base64url payload (no kid, no iat).
	tampered := "eyJzIjozLCJlIjoiT3JkZXJzIiwiZiI6IiJ9" + "." + parts[1]
	_, err := decodeDeltaToken(tampered, ks, 0)
	if err == nil {
		t.Fatal("expected signature mismatch on tampered payload")
	}
}

// TestDeltaToken_RejectsMalformed pins various malformed inputs return
// errors rather than panicking or accepting them.
func TestDeltaToken_RejectsMalformed(t *testing.T) {
	t.Parallel()
	ks := ksSingle(t, "0123456789abcdef0123456789abcdef")
	cases := []string{
		"",
		"no-dot",
		"only.one.dot.too.many",
		"!!!.!!!", // not base64
	}
	for _, tc := range cases {
		t.Run(tc, func(t *testing.T) {
			if _, err := decodeDeltaToken(tc, ks, 0); err == nil {
				t.Errorf("expected error for %q, got nil", tc)
			}
		})
	}
}

// TestDeltaToken_MaxAgeRejectsExpired pins that tokens older than
// max-age are rejected with the errDeltaTokenExpired sentinel. The
// HTTP handler classifies via errors.Is so wording can change without
// breaking the 410 DeltaLinkExpired response code.
func TestDeltaToken_MaxAgeRejectsExpired(t *testing.T) {
	t.Parallel()
	ks := ksSingle(t, "0123456789abcdef0123456789abcdef")
	encoded, err := encodeDeltaToken(DeltaToken{Snapshot: 1, Entity: "Orders"}, ks)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// Sleep long enough that any positive max-age below 100ms triggers expiry.
	time.Sleep(2 * time.Millisecond)
	_, err = decodeDeltaToken(encoded, ks, 1*time.Nanosecond)
	if err == nil {
		t.Fatal("expected expiry error")
	}
	if !errors.Is(err, errDeltaTokenExpired) {
		t.Errorf("expected errDeltaTokenExpired, got: %v", err)
	}
}

// TestDeltaToken_MaxAgeAcceptsFresh pins that tokens within max-age pass
// verification — the env knob doesn't accidentally reject every token.
func TestDeltaToken_MaxAgeAcceptsFresh(t *testing.T) {
	t.Parallel()
	ks := ksSingle(t, "0123456789abcdef0123456789abcdef")
	encoded, err := encodeDeltaToken(DeltaToken{Snapshot: 1, Entity: "Orders"}, ks)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// 1h is plenty of slack for a token issued microseconds ago.
	if _, err := decodeDeltaToken(encoded, ks, time.Hour); err != nil {
		t.Errorf("fresh token rejected: %v", err)
	}
}

// TestDeltaToken_MaxAgeUnsetSkipsCheck pins that maxAge=0 disables the
// expiry check entirely — the default behaviour for operators that
// haven't set ONDATRA_ODATA_DELTA_MAX_AGE.
func TestDeltaToken_MaxAgeUnsetSkipsCheck(t *testing.T) {
	t.Parallel()
	ks := ksSingle(t, "0123456789abcdef0123456789abcdef")
	encoded, _ := encodeDeltaToken(DeltaToken{Snapshot: 1, Entity: "Orders"}, ks)
	if _, err := decodeDeltaToken(encoded, ks, 0); err != nil {
		t.Errorf("token rejected with max-age unset: %v", err)
	}
}

// TestDeltaToken_KidRotation pins the multi-key rotation contract:
// 1. Sign with k2, verify the token under {k2,k1}: passes.
// 2. Sign with k1 (simulating an old token from before the rotation),
//    verify under {k2,k1}: passes — both keys are accepted.
// 3. Sign with k1, verify under {k2}: fails — old key is no longer in
//    the keyset, the rotation window has closed.
func TestDeltaToken_KidRotation(t *testing.T) {
	t.Parallel()
	hex32a := "0123456789abcdef0123456789abcdef"
	hex32b := "fedcba9876543210fedcba9876543210"

	ksRotation, _ := parseKeyset("k2:" + hex32a + ",k1:" + hex32b)
	ksOldOnly, _ := parseKeyset("k1:" + hex32b)
	ksNewOnly, _ := parseKeyset("k2:" + hex32a)

	// 1. New token under rotation keyset → accepted.
	enc1, err := encodeDeltaToken(DeltaToken{Snapshot: 1, Entity: "X"}, ksRotation)
	if err != nil {
		t.Fatalf("encode rotation: %v", err)
	}
	if _, err := decodeDeltaToken(enc1, ksRotation, 0); err != nil {
		t.Errorf("rotation-keyset token failed verify: %v", err)
	}

	// 2. Token signed under k1 verifies under {k2,k1} too — that's the
	// whole point of multi-key.
	enc2, err := encodeDeltaToken(DeltaToken{Snapshot: 1, Entity: "X"}, ksOldOnly)
	if err != nil {
		t.Fatalf("encode old: %v", err)
	}
	if _, err := decodeDeltaToken(enc2, ksRotation, 0); err != nil {
		t.Errorf("k1-signed token rejected by rotation keyset: %v", err)
	}

	// 3. Same k1-signed token fails under {k2}-only — the rotation has
	// completed, k1 is no longer accepted.
	if _, err := decodeDeltaToken(enc2, ksNewOnly, 0); err == nil {
		t.Error("expected reject when k1 keyset is removed; got nil")
	}
}

// TestDeltaToken_KidUnknown pins that a token with a kid that's not in
// the keyset is rejected with a clear error. The HMAC check would fail
// anyway, but this gives the operator a more useful diagnostic.
func TestDeltaToken_KidUnknown(t *testing.T) {
	t.Parallel()
	ks, _ := parseKeyset("v1:0123456789abcdef0123456789abcdef")
	encoded, _ := encodeDeltaToken(DeltaToken{Snapshot: 1, Entity: "X"}, ks)
	// Now strip v1 from the keyset.
	ks2, _ := parseKeyset("v2:fedcba9876543210fedcba9876543210")
	_, err := decodeDeltaToken(encoded, ks2, 0)
	if err == nil {
		t.Fatal("expected error for unknown kid")
	}
	if !strings.Contains(err.Error(), "unknown delta-key id") {
		t.Errorf("expected 'unknown delta-key id' in error, got: %v", err)
	}
}

// TestParseKeyset_LegacyAndPair pins both env-var formats:
//   - bare hex (v0.26.0 single-key form): kid="" with that key
//   - kid:hex pairs (v0.27.0 rotation form): kid taken verbatim
func TestParseKeyset_LegacyAndPair(t *testing.T) {
	t.Parallel()

	t.Run("legacy single hex", func(t *testing.T) {
		ks, err := parseKeyset("0123456789abcdef0123456789abcdef")
		if err != nil {
			t.Fatalf("parseKeyset: %v", err)
		}
		if len(ks.Accept) != 1 {
			t.Fatalf("Accept len = %d, want 1", len(ks.Accept))
		}
		if ks.Sign.ID != "" {
			t.Errorf("Sign.ID = %q, want empty (legacy form)", ks.Sign.ID)
		}
	})

	t.Run("kid:hex pair", func(t *testing.T) {
		ks, err := parseKeyset("v1:0123456789abcdef0123456789abcdef")
		if err != nil {
			t.Fatalf("parseKeyset: %v", err)
		}
		if ks.Sign.ID != "v1" {
			t.Errorf("Sign.ID = %q, want v1", ks.Sign.ID)
		}
	})

	t.Run("multiple pairs", func(t *testing.T) {
		ks, err := parseKeyset("v2:0123456789abcdef0123456789abcdef,v1:fedcba9876543210fedcba9876543210")
		if err != nil {
			t.Fatalf("parseKeyset: %v", err)
		}
		if len(ks.Accept) != 2 {
			t.Fatalf("Accept len = %d, want 2", len(ks.Accept))
		}
		if ks.Sign.ID != "v2" {
			t.Errorf("Sign.ID = %q, want v2 (first entry signs)", ks.Sign.ID)
		}
	})

	t.Run("duplicate kid rejected", func(t *testing.T) {
		_, err := parseKeyset("v1:0123456789abcdef0123456789abcdef,v1:fedcba9876543210fedcba9876543210")
		if err == nil {
			t.Fatal("expected error for duplicate kid")
		}
	})

	t.Run("empty rejected", func(t *testing.T) {
		if _, err := parseKeyset(""); err == nil {
			t.Error("empty input should be rejected")
		}
	})

	t.Run("invalid hex rejected", func(t *testing.T) {
		if _, err := parseKeyset("v1:nothexvalid"); err == nil {
			t.Error("invalid hex should be rejected")
		}
	})
}

// TestLoadDeltaMaxAge pins the env-var parsing for max-age.
func TestLoadDeltaMaxAge(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		env     string
		want    time.Duration
		wantErr bool
	}{
		{"empty disables", "", 0, false},
		{"7-day window", "168h", 7 * 24 * time.Hour, false},
		{"24h", "24h", 24 * time.Hour, false},
		{"30 minutes", "30m", 30 * time.Minute, false},
		{"unparseable rejected", "7d", 0, true}, // Go duration doesn't accept "d"
		{"negative rejected", "-1h", 0, true},
		{"zero rejected", "0s", 0, true},
		{"garbage rejected", "lol", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := loadDeltaMaxAge(tc.env)
			if (err != nil) != tc.wantErr {
				t.Fatalf("loadDeltaMaxAge(%q) err = %v, wantErr = %v", tc.env, err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("loadDeltaMaxAge(%q) = %s, want %s", tc.env, got, tc.want)
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
	ks := ksSingle(t, "0123456789abcdef0123456789abcdef")
	entity := EntitySchema{ODataName: "Orders", KeyColumn: "id"}

	params := url.Values{}
	params.Set("$select", "id,name")
	params.Set("$orderby", "name asc")

	link := emitDeltaLink("https://api", entity, params, 42, ks)
	if link == "" {
		t.Fatal("expected non-empty deltaLink")
	}

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
	tok, err := decodeDeltaToken(tokenStr, ks, 0)
	if err != nil {
		t.Fatalf("decode token: %v", err)
	}
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
