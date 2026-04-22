// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"context"
	"testing"
	"time"
)

func TestParseDuration(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  time.Duration
		err   bool
	}{
		{"10s", 10 * time.Second, false},
		{"1m", 1 * time.Minute, false},
		{"2h", 2 * time.Hour, false},
		{"", 0, true},
		{"x", 0, true},
		{"10x", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseDuration(tt.input)
			if tt.err && err == nil {
				t.Error("expected error")
			}
			if !tt.err && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRateLimiter_Wait(t *testing.T) {
	t.Parallel()
	rl, err := newRateLimiter(5, "1s")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Should be able to get 5 tokens immediately
	for i := 0; i < 5; i++ {
		start := time.Now()
		if err := rl.Wait(ctx); err != nil {
			t.Fatalf("token %d: %v", i, err)
		}
		if time.Since(start) > 50*time.Millisecond {
			t.Errorf("token %d took too long", i)
		}
	}
}


