// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build e2e && bench

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/collect"
	"github.com/ondatra-labs/ondatrasql/internal/dag"
	"github.com/ondatra-labs/ondatrasql/internal/execute"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// preloadEvents writes n events to the store using WriteBatch (1000 events per batch).
func preloadEvents(store *collect.Store, target string, n int) {
	batchSize := 1000
	for start := 0; start < n; start += batchSize {
		end := start + batchSize
		if end > n {
			end = n
		}
		batch := make([]map[string]any, 0, end-start)
		for i := start; i < end; i++ {
			batch = append(batch, map[string]any{
				"event_name":  fmt.Sprintf("ev%d", i),
				"page_url":    fmt.Sprintf("/p%d", i),
				"user_id":     fmt.Sprintf("u%d", i%100),
				"received_at": time.Now().UTC().Format(time.RFC3339Nano),
			})
		}
		if err := store.WriteBatch(target, batch); err != nil {
			panic("preload: " + err.Error())
		}
	}
}

// startDaemon starts an in-process event daemon and returns ports and cleanup.
func startDaemon(t testing.TB, model *parser.Model) (*collect.Store, string, string, context.CancelFunc) {
	t.Helper()

	badgerDir := filepath.Join(t.TempDir(), "events")
	os.MkdirAll(badgerDir, 0755)
	store, err := collect.Open(badgerDir)
	if err != nil {
		t.Fatal(err)
	}

	publicPort := freePortE2E(t)
	adminPort := freePortE2E(t)

	srv := collect.NewServer(store, []*parser.Model{model}, publicPort, adminPort)
	ctx, cancel := context.WithCancel(context.Background())
	go srv.Start(ctx)

	// Wait for ready
	for i := 0; i < 50; i++ {
		resp, err := http.Get("http://127.0.0.1:" + adminPort + "/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	return store, publicPort, adminPort, func() {
		cancel()
		store.Close()
	}
}

func newTestModel(t *testing.T, p *testutil.Project) *parser.Model {
	t.Helper()
	p.AddModel("raw/events.sql", `-- @kind: events
event_name VARCHAR NOT NULL,
page_url VARCHAR,
user_id VARCHAR,
received_at TIMESTAMPTZ
`)
	modelPath := filepath.Join(p.Dir, "models", "raw/events.sql")
	model, err := parser.ParseModel(modelPath, p.Dir)
	if err != nil {
		t.Fatal(err)
	}
	return model
}

// TestE2E_EventsBench_FlushOnly measures the flush step in isolation
// (Badger → Claim → INSERT INTO DuckLake → Ack) for different event counts.
func TestE2E_EventsBench_FlushOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping flush benchmark")
	}

	for _, count := range []int{100, 1000, 10000, 50000} {
		t.Run(fmt.Sprintf("events_%d", count), func(t *testing.T) {
			p := testutil.NewProject(t)
			model := newTestModel(t, p)

			store, _, adminPort, cleanup := startDaemon(t, model)
			defer cleanup()

			// Pre-load events directly into Badger (skip HTTP overhead)
			preloadEvents(store, "raw.events", count)

			// Measure flush only
			start := time.Now()

			runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
			runner.SetAdminPort(adminPort)
			result, err := runner.Run(context.Background(), model)
			if err != nil {
				t.Fatal(err)
			}

			elapsed := time.Since(start)

			if result.RowsAffected != int64(count) {
				t.Fatalf("rows = %d, want %d", result.RowsAffected, count)
			}

			rate := float64(count) / elapsed.Seconds()
			t.Logf("Flush %d events: %v (%.0f events/sec)", count, elapsed.Round(time.Millisecond), rate)
		})
	}
}

// TestE2E_EventsBench_FullPipeline measures the complete flow:
// HTTP POST → Badger → Claim → INSERT INTO DuckLake → Ack
func TestE2E_EventsBench_FullPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping full pipeline benchmark")
	}

	for _, count := range []int{100, 1000, 10000} {
		t.Run(fmt.Sprintf("events_%d", count), func(t *testing.T) {
			p := testutil.NewProject(t)
			model := newTestModel(t, p)

			_, publicPort, adminPort, cleanup := startDaemon(t, model)
			defer cleanup()

			url := "http://127.0.0.1:" + publicPort + "/collect/raw/events"

			start := time.Now()

			// Phase 1: Ingest events via HTTP
			for i := 0; i < count; i++ {
				body := fmt.Sprintf(`{"event_name":"ev%d","page_url":"/p%d","user_id":"u%d"}`, i, i, i%100)
				resp, err := http.Post(url, "application/json", bytes.NewReader([]byte(body)))
				if err != nil {
					t.Fatal(err)
				}
				resp.Body.Close()
			}
			ingestDone := time.Since(start)

			// Phase 2: Flush to DuckLake
			flushStart := time.Now()
			runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
			runner.SetAdminPort(adminPort)
			result, err := runner.Run(context.Background(), model)
			if err != nil {
				t.Fatal(err)
			}
			flushDone := time.Since(flushStart)

			total := time.Since(start)
			rate := float64(count) / total.Seconds()
			t.Logf("Full pipeline %d events: ingest=%v flush=%v total=%v (%.0f events/sec)",
				count, ingestDone.Round(time.Millisecond), flushDone.Round(time.Millisecond),
				total.Round(time.Millisecond), rate)

			if result.RowsAffected != int64(count) {
				t.Fatalf("rows = %d, want %d", result.RowsAffected, count)
			}
		})
	}
}

// TestE2E_EventsBench_ClaimLimit finds the optimal claim limit by flushing
// 20000 events with different batch sizes through the full Badger → DuckLake path.
func TestE2E_EventsBench_ClaimLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping claim limit benchmark")
	}

	totalEvents := 20000

	for _, limit := range []int{100, 250, 500, 1000, 2000, 5000, 10000} {
		t.Run(fmt.Sprintf("limit_%d", limit), func(t *testing.T) {
			p := testutil.NewProject(t)
			model := newTestModel(t, p)

			store, _, adminPort, cleanup := startDaemon(t, model)
			defer cleanup()

			// Pre-load events
			preloadEvents(store, "raw.events", totalEvents)

			start := time.Now()

			runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
			runner.SetAdminPort(adminPort)
			runner.SetClaimLimit(limit)
			result, err := runner.Run(context.Background(), model)
			if err != nil {
				t.Fatal(err)
			}

			elapsed := time.Since(start)

			if result.RowsAffected != int64(totalEvents) {
				t.Fatalf("rows = %d, want %d", result.RowsAffected, totalEvents)
			}

			rate := float64(totalEvents) / elapsed.Seconds()
			batches := (totalEvents + limit - 1) / limit
			t.Logf("limit=%5d  batches=%3d  time=%v  rate=%.0f events/sec",
				limit, batches, elapsed.Round(time.Millisecond), rate)
		})
	}
}

// TestE2E_EventsThroughput_FullPipeline measures sustained end-to-end throughput
// including DuckLake flush. This gives the real-world number.
func TestE2E_EventsThroughput_FullPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping full pipeline throughput test")
	}

	p := testutil.NewProject(t)
	p.AddModel("raw/events.sql", `-- @kind: events
event_name VARCHAR NOT NULL,
page_url VARCHAR,
user_id VARCHAR,
session_id VARCHAR,
received_at TIMESTAMPTZ
`)

	modelPath := filepath.Join(p.Dir, "models", "raw/events.sql")
	model, err := parser.ParseModel(modelPath, p.Dir)
	if err != nil {
		t.Fatal(err)
	}

	store, publicPort, adminPort, cleanup := startDaemon(t, model)
	defer cleanup()
	_ = store

	url := "http://127.0.0.1:" + publicPort + "/collect/raw/events"
	body := []byte(`{"event_name":"pageview","page_url":"/home","user_id":"u123","session_id":"s456"}`)

	// Phase 1: Ingest for 3 seconds with concurrent writers
	workers := 8
	duration := 3 * time.Second
	var ingested atomic.Int64

	var wg sync.WaitGroup
	deadline := time.Now().Add(duration)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Timeout: 5 * time.Second}
			for time.Now().Before(deadline) {
				resp, err := client.Post(url, "application/json", bytes.NewReader(body))
				if err != nil {
					continue
				}
				resp.Body.Close()
				if resp.StatusCode == 202 {
					ingested.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	ingestCount := ingested.Load()
	t.Logf("Phase 1 - Ingest (%d workers, %v):", workers, duration)
	t.Logf("  Events ingested: %d", ingestCount)
	t.Logf("  Ingest rate:     %.0f events/sec", float64(ingestCount)/duration.Seconds())

	// Phase 2: Flush to DuckLake
	flushStart := time.Now()
	runner := execute.NewRunner(p.Sess, execute.ModeRun, dag.GenerateRunID())
	runner.SetAdminPort(adminPort)
	result, err := runner.Run(context.Background(), model)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	flushDuration := time.Since(flushStart)

	t.Logf("Phase 2 - Flush to DuckLake:")
	t.Logf("  Events flushed:  %d", result.RowsAffected)
	t.Logf("  Flush duration:  %v", flushDuration.Round(time.Millisecond))
	t.Logf("  Flush rate:      %.0f events/sec", float64(result.RowsAffected)/flushDuration.Seconds())

	// Phase 3: Verify data in DuckLake
	count, err := p.Sess.QueryValue("SELECT COUNT(*) FROM raw.events")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	t.Logf("Phase 3 - Verify:")
	t.Logf("  Rows in DuckLake: %s", count)

	// Total pipeline throughput
	totalDuration := duration + flushDuration
	t.Logf("Total pipeline (ingest + flush):")
	t.Logf("  Duration:        %v", totalDuration.Round(time.Millisecond))
	t.Logf("  Throughput:      %.0f events/sec", float64(result.RowsAffected)/totalDuration.Seconds())
	t.Logf("  Visitors/day:    %.0f (at 5 events/visitor)", float64(result.RowsAffected)/totalDuration.Seconds()*86400/5)
}
