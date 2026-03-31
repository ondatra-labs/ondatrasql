// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package collect

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/parser"
)

// freePort allocates a free TCP port and returns it as a string.
func freePort(t testing.TB) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := fmt.Sprintf("%d", ln.Addr().(*net.TCPAddr).Port)
	ln.Close()
	return port
}

func benchModel() *parser.Model {
	return &parser.Model{
		Target: "raw.events",
		Kind:   "events",
		Columns: []parser.ColumnDef{
			{Name: "event_name", Type: "VARCHAR", NotNull: true},
			{Name: "page_url", Type: "VARCHAR"},
			{Name: "user_id", Type: "VARCHAR"},
			{Name: "session_id", Type: "VARCHAR"},
			{Name: "received_at", Type: "TIMESTAMPTZ"},
		},
	}
}

// startBenchServer creates a store and starts an HTTP server, returning ports and cleanup.
func startBenchServer(t testing.TB) (*Store, string, string, context.CancelFunc) {
	t.Helper()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	model := benchModel()
	port := freePort(t)
	adminPort := freePort(t)

	srv := NewServer(store, []*parser.Model{model}, port, adminPort)
	ctx, cancel := context.WithCancel(context.Background())
	go srv.Start(ctx)

	// Wait for ready
	for i := 0; i < 50; i++ {
		resp, err := http.Get("http://127.0.0.1:" + port + "/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	return store, port, adminPort, func() {
		cancel()
		store.Close()
	}
}

// BenchmarkStore_Write measures raw Badger write throughput.
func BenchmarkStore_Write(b *testing.B) {
	dir := b.TempDir()
	store, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	event := map[string]any{
		"event_name": "pageview",
		"page_url":   "/home",
		"user_id":    "u123",
		"session_id": "s456",
	}

	b.ResetTimer()
	for b.Loop() {
		if err := store.Write("raw.events", event); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStore_WriteBatch measures batch write throughput.
func BenchmarkStore_WriteBatch(b *testing.B) {
	for _, batchSize := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			dir := b.TempDir()
			store, err := Open(dir)
			if err != nil {
				b.Fatal(err)
			}
			defer store.Close()

			events := make([]map[string]any, batchSize)
			for i := range events {
				events[i] = map[string]any{
					"event_name": "pageview",
					"page_url":   fmt.Sprintf("/page/%d", i),
					"user_id":    "u123",
				}
			}

			b.ResetTimer()
			for b.Loop() {
				if err := store.WriteBatch("raw.events", events); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkStore_ClaimAck measures claim+ack cycle throughput.
func BenchmarkStore_ClaimAck(b *testing.B) {
	dir := b.TempDir()
	store, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	for i := 0; i < b.N; i++ {
		store.Write("raw.events", map[string]any{
			"event_name": "test",
			"page_url":   fmt.Sprintf("/%d", i),
		})
	}

	b.ResetTimer()
	for {
		claimID, events, err := store.Claim("raw.events", 1000)
		if err != nil {
			b.Fatal(err)
		}
		if len(events) == 0 {
			break
		}
		if err := store.Ack(claimID); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHTTP_Collect measures end-to-end HTTP event collection throughput.
func BenchmarkHTTP_Collect(b *testing.B) {
	_, port, _, cleanup := startBenchServer(b)
	defer cleanup()

	url := "http://127.0.0.1:" + port + "/collect/raw/events"
	body := []byte(`{"event_name":"pageview","page_url":"/home","user_id":"u123","session_id":"s456"}`)

	b.ResetTimer()
	for b.Loop() {
		resp, err := http.Post(url, "application/json", bytes.NewReader(body))
		if err != nil {
			b.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 202 {
			b.Fatalf("status %d", resp.StatusCode)
		}
	}
}

// BenchmarkHTTP_Collect_Parallel measures concurrent HTTP collection.
func BenchmarkHTTP_Collect_Parallel(b *testing.B) {
	_, port, _, cleanup := startBenchServer(b)
	defer cleanup()

	url := "http://127.0.0.1:" + port + "/collect/raw/events"
	body := []byte(`{"event_name":"pageview","page_url":"/home","user_id":"u123","session_id":"s456"}`)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := http.Post(url, "application/json", bytes.NewReader(body))
			if err != nil {
				b.Fatal(err)
			}
			resp.Body.Close()
		}
	})
}

// BenchmarkHTTP_CollectBatch measures batch endpoint throughput.
func BenchmarkHTTP_CollectBatch(b *testing.B) {
	for _, batchSize := range []int{10, 100, 500} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			_, port, _, cleanup := startBenchServer(b)
			defer cleanup()

			url := "http://127.0.0.1:" + port + "/collect/raw/events/batch"

			var buf bytes.Buffer
			buf.WriteByte('[')
			for i := 0; i < batchSize; i++ {
				if i > 0 {
					buf.WriteByte(',')
				}
				fmt.Fprintf(&buf, `{"event_name":"ev%d","page_url":"/p%d","user_id":"u%d"}`, i, i, i)
			}
			buf.WriteByte(']')
			payload := buf.Bytes()

			b.ResetTimer()
			for b.Loop() {
				resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
				if err != nil {
					b.Fatal(err)
				}
				resp.Body.Close()
				if resp.StatusCode != 202 {
					b.Fatalf("status %d", resp.StatusCode)
				}
			}
		})
	}
}

