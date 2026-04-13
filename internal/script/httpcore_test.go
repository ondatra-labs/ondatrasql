// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"context"
	"testing"
	"time"
)

func TestParseLinkHeader(t *testing.T) {
	t.Parallel()
	header := `<https://api.example.com/items?page=2>; rel="next", <https://api.example.com/items?page=5>; rel="last"`

	got := ParseLinkHeader(header)

	if got["next"] != "https://api.example.com/items?page=2" {
		t.Errorf("next = %q, want page=2 URL", got["next"])
	}
	if got["last"] != "https://api.example.com/items?page=5" {
		t.Errorf("last = %q, want page=5 URL", got["last"])
	}
}

func TestParseLinkHeaderSingle(t *testing.T) {
	t.Parallel()
	header := `<https://api.example.com/next>; rel="next"`
	got := ParseLinkHeader(header)

	if len(got) != 1 {
		t.Errorf("expected 1 link, got %d", len(got))
	}
	if got["next"] != "https://api.example.com/next" {
		t.Errorf("next = %q", got["next"])
	}
}

func TestParseLinkHeaderEmpty(t *testing.T) {
	t.Parallel()
	got := ParseLinkHeader("")
	if len(got) != 0 {
		t.Errorf("expected empty map, got %v", got)
	}
}

func TestParseLinkHeaderMalformed(t *testing.T) {
	t.Parallel()
	got := ParseLinkHeader("not a link header")
	if len(got) != 0 {
		t.Errorf("expected empty map for malformed input, got %v", got)
	}
}

func TestParseRetryAfterSeconds(t *testing.T) {
	t.Parallel()
	got := parseRetryAfter("120")
	if got != 120*time.Second {
		t.Errorf("got %v, want 120s", got)
	}
}

func TestParseRetryAfterHTTPDate(t *testing.T) {
	t.Parallel()
	// Use a large offset so that even slow CI machines stay within bounds.
	future := time.Now().Add(300 * time.Second).UTC().Format(time.RFC1123)
	got := parseRetryAfter(future)
	if got < 295*time.Second || got > 305*time.Second {
		t.Errorf("got %v, want ~300s", got)
	}
}

func TestParseRetryAfterHTTPDate_Past(t *testing.T) {
	t.Parallel()
	past := time.Now().Add(-10 * time.Second).UTC().Format(time.RFC1123)
	got := parseRetryAfter(past)
	if got != 0 {
		t.Errorf("past date should return 0, got %v", got)
	}
}

func TestParseRetryAfterEmpty(t *testing.T) {
	t.Parallel()
	got := parseRetryAfter("")
	if got != 0 {
		t.Errorf("got %v, want 0", got)
	}
}

func TestParseRetryAfterInvalid(t *testing.T) {
	t.Parallel()
	got := parseRetryAfter("not-a-number")
	if got != 0 {
		t.Errorf("got %v, want 0", got)
	}
}

func TestDoHTTPWithRetry_CancelDuringBackoff(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel immediately so the retry backoff returns quickly
	cancel()

	opts := httpOptions{
		Retry:   3,
		Backoff: 60000, // 60s backoff — would block without cancellation
	}

	start := time.Now()
	_, err := DoHTTPWithRetry(ctx, "GET", "http://127.0.0.1:1/nope", nil, nil, opts)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
	// Should return almost immediately, not wait 60s
	if elapsed > 2*time.Second {
		t.Errorf("cancellation took %v, expected <2s", elapsed)
	}
}

