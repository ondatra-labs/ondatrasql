// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// rateLimiter implements a token bucket rate limiter.
type rateLimiter struct {
	mu       sync.Mutex
	tokens   int
	max      int
	interval time.Duration
	lastFill time.Time
}

// newRateLimiter creates a rate limiter from a config like {"requests": 100, "per": "10s"}.
func newRateLimiter(requests int, per string) (*rateLimiter, error) {
	if requests <= 0 {
		return nil, fmt.Errorf("rate_limit requests must be > 0, got %d", requests)
	}
	dur, err := parseDuration(per)
	if err != nil {
		return nil, fmt.Errorf("invalid rate_limit per %q: %w", per, err)
	}
	if dur <= 0 {
		return nil, fmt.Errorf("rate_limit per must be positive, got %v", dur)
	}
	return &rateLimiter{
		tokens:   requests,
		max:      requests,
		interval: dur,
		lastFill: time.Now(),
	}, nil
}

// Wait blocks until a token is available or ctx is cancelled.
func (rl *rateLimiter) Wait(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		rl.mu.Lock()
		rl.refill()
		if rl.tokens > 0 {
			rl.tokens--
			rl.mu.Unlock()
			return nil
		}
		rl.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (rl *rateLimiter) refill() {
	now := time.Now()
	elapsed := now.Sub(rl.lastFill)
	if elapsed >= rl.interval {
		intervals := int(elapsed / rl.interval)
		rl.tokens = rl.max
		rl.lastFill = rl.lastFill.Add(time.Duration(intervals) * rl.interval)
	}
}

// parseDuration parses "Ns", "Nm", "Nh" format.
func parseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if len(s) < 2 {
		return 0, fmt.Errorf("too short: %q", s)
	}

	unit := s[len(s)-1]
	numStr := s[:len(s)-1]
	num, err := strconv.Atoi(numStr)
	if err != nil {
		return 0, fmt.Errorf("invalid number in %q: %w", s, err)
	}

	switch unit {
	case 's':
		return time.Duration(num) * time.Second, nil
	case 'm':
		return time.Duration(num) * time.Minute, nil
	case 'h':
		return time.Duration(num) * time.Hour, nil
	default:
		return 0, fmt.Errorf("unknown unit %q in %q (use s, m, or h)", string(unit), s)
	}
}
