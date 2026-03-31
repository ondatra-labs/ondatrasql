// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/collect"
	"github.com/ondatra-labs/ondatrasql/internal/config"
	"github.com/ondatra-labs/ondatrasql/internal/output"
)

// runDaemon starts the event collection daemon.
// It opens a Badger store, recovers any inflight events from previous crashes,
// starts public and admin HTTP servers, and runs Badger GC periodically.
func runDaemon(ctx context.Context, cfg *config.Config) error {
	// Load models, filter to @kind: events
	models, err := loadModelsFromDir(cfg)
	if err != nil {
		return fmt.Errorf("load models: %w", err)
	}

	var eventModels []*eventModelInfo
	for _, m := range models {
		if m.Kind == "events" {
			eventModels = append(eventModels, &eventModelInfo{target: m.Target})
		}
	}

	if len(eventModels) == 0 {
		return fmt.Errorf("no @kind: events models found in %s", cfg.ModelsPath)
	}

	// Open Badger store
	badgerDir := filepath.Join(cfg.ProjectDir, ".ondatra", "events")
	if err := os.MkdirAll(badgerDir, 0755); err != nil {
		return fmt.Errorf("create events dir: %w", err)
	}

	store, err := collect.Open(badgerDir)
	if err != nil {
		return fmt.Errorf("open event store: %w", err)
	}
	defer store.Close()

	// Recover all inflight events from previous crashes.
	// At daemon startup, no runner is active, so all inflight events
	// should be returned to the queue.
	if err := store.RecoverAllInflight(); err != nil {
		return fmt.Errorf("recover inflight: %w", err)
	}

	// Resolve ports
	publicPort := os.Getenv("COLLECT_PORT")
	if publicPort == "" {
		publicPort = "8080"
	}
	adminPort := os.Getenv("COLLECT_ADMIN_PORT")
	if adminPort == "" {
		if p, err := strconv.Atoi(publicPort); err == nil {
			adminPort = strconv.Itoa(p + 1)
		} else {
			adminPort = "8081"
		}
	}

	// Start HTTP servers
	srv := collect.NewServer(store, models, publicPort, adminPort)

	output.Fprintf("Event daemon starting...\n")
	output.Fprintf("  Public:  :%s (POST /collect/{schema}/{table})\n", publicPort)
	output.Fprintf("  Admin:   127.0.0.1:%s (flush endpoints)\n", adminPort)
	output.Fprintf("  Store:   %s\n", badgerDir)
	output.Fprintf("  Models:  ")
	for i, em := range eventModels {
		if i > 0 {
			output.Fprintf(", ")
		}
		output.Fprintf("%s", em.target)
	}
	output.Fprintf("\n")

	// Start Badger GC ticker
	gcTicker := time.NewTicker(5 * time.Minute)
	defer gcTicker.Stop()
	go func() {
		for {
			select {
			case <-gcTicker.C:
				store.RunGC()
			case <-ctx.Done():
				return
			}
		}
	}()

	// Start servers (blocks until context cancelled or error)
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start(ctx)
	}()

	// Wait for shutdown signal or server error
	select {
	case <-ctx.Done():
		output.Fprintf("\nShutting down daemon...\n")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

type eventModelInfo struct {
	target string
}
