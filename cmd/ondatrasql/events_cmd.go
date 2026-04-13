// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/collect"
	"github.com/ondatra-labs/ondatrasql/internal/config"
	"github.com/ondatra-labs/ondatrasql/internal/output"
)

// runEvents starts the event collection daemon on the given public port.
// The admin port is always public+1. Opens a Badger store, recovers inflight
// events from previous crashes, starts public and admin HTTP servers, and runs
// Badger GC periodically.
func runEvents(ctx context.Context, cfg *config.Config, publicPort string) error {
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

	// Validate ports BEFORE opening Badger. Otherwise a busy port leaves
	// a stale Badger lock behind on retry, masking the real port-bind
	// failure with a confusing "another process is using this Badger
	// database" error on the next attempt. (Bug 26)
	//
	// The admin port is auto-derived as public+1 — surface that in errors
	// so users aren't surprised by a port number they didn't pick.
	pNum, parseErr := strconv.Atoi(publicPort)
	if parseErr != nil {
		return fmt.Errorf("invalid port %q: %w", publicPort, parseErr)
	}
	if pNum < 1 || pNum > 65534 {
		return fmt.Errorf("invalid public port %d: must be 1-65534 (admin port = public+1, so 65535 is reserved)", pNum)
	}
	adminPort := strconv.Itoa(pNum + 1)
	if err := checkPortAvailable(publicPort); err != nil {
		return fmt.Errorf("public port %s unavailable: %w", publicPort, err)
	}
	// Admin server only binds 127.0.0.1, so check the same address —
	// otherwise we'd reject ports that are busy on a different interface
	// but would actually have worked for the loopback admin server.
	// (Review finding 3)
	if err := checkLoopbackPortAvailable(adminPort); err != nil {
		return fmt.Errorf("admin port %s (public+1) unavailable: %w — pick a different public port so the admin port is also free", adminPort, err)
	}

	// Open Badger store
	badgerDir := filepath.Join(cfg.ProjectDir, ".ondatra", "events")
	if err := os.MkdirAll(badgerDir, 0755); err != nil {
		return fmt.Errorf("create events dir: %w", err)
	}

	store, err := collect.Open(badgerDir)
	if err != nil {
		// Translate Badger's directory-lock error to something honest:
		// we know the store is locked, we don't know by what. (Bug 26)
		if strings.Contains(err.Error(), "Cannot acquire directory lock") {
			return fmt.Errorf(
				"events store at %s is locked by another process — "+
					"if no other 'ondatrasql events' is running, the lock may be "+
					"stale (remove %s/LOCK to recover)",
				badgerDir, badgerDir)
		}
		return fmt.Errorf("open event store: %w", err)
	}
	defer store.Close()

	// Recover all inflight events from previous crashes.
	// At daemon startup, no runner is active, so all inflight events
	// should be returned to the queue.
	if err := store.RecoverAllInflight(); err != nil {
		return fmt.Errorf("recover inflight: %w", err)
	}

	// Write admin port to a runtime file so `ondatrasql run` can find it.
	// Removed on shutdown.
	portFile := filepath.Join(cfg.ProjectDir, ".ondatra", "events.admin.port")
	if err := os.WriteFile(portFile, []byte(adminPort), 0o644); err != nil {
		return fmt.Errorf("write port file: %w", err)
	}
	defer os.Remove(portFile)

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

// checkPortAvailable verifies that the given TCP port can be bound on
// 0.0.0.0. Returns an error describing the conflict otherwise. Used to
// fail fast before opening Badger so a busy port doesn't leave a stale
// directory lock that masks the real error on the next attempt.
func checkPortAvailable(port string) error {
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}
	return ln.Close()
}

// checkLoopbackPortAvailable verifies the port is bindable on 127.0.0.1
// specifically — used for servers that only listen on loopback (e.g. the
// events admin server) so we don't reject a port that's busy on a public
// interface but free on loopback. (Review finding 3)
func checkLoopbackPortAvailable(port string) error {
	ln, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		return err
	}
	return ln.Close()
}

type eventModelInfo struct {
	target string
}
