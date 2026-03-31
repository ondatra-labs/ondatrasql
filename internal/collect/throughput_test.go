// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package collect

import (
	"bytes"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestThroughput_Sustained runs a 5-second sustained load test and reports events/sec.
// Requires network sockets — run with: go test -tags=integration ./internal/collect/ -run TestThroughput
func TestThroughput_Sustained(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping sustained throughput test in short mode")
	}

	_, port, _, cleanup := startBenchServer(t)
	defer cleanup()

	url := "http://127.0.0.1:" + port + "/collect/raw/events"
	body := []byte(`{"event_name":"pageview","page_url":"/home","user_id":"u123","session_id":"s456"}`)

	duration := 5 * time.Second
	workers := 8
	var total atomic.Int64
	var errors atomic.Int64

	var wg sync.WaitGroup
	start := time.Now()
	deadline := start.Add(duration)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Timeout: 5 * time.Second}
			for time.Now().Before(deadline) {
				resp, err := client.Post(url, "application/json", bytes.NewReader(body))
				if err != nil {
					errors.Add(1)
					continue
				}
				resp.Body.Close()
				if resp.StatusCode == 202 {
					total.Add(1)
				} else {
					errors.Add(1)
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	eventsPerSec := float64(total.Load()) / elapsed.Seconds()
	t.Logf("Sustained throughput (%d workers, %v):", workers, elapsed.Round(time.Millisecond))
	t.Logf("  Total events:  %d", total.Load())
	t.Logf("  Errors:        %d", errors.Load())
	t.Logf("  Events/sec:    %.0f", eventsPerSec)
	t.Logf("  Visitors/day:  %.0f (at 5 events/visitor)", eventsPerSec*86400/5)
}
