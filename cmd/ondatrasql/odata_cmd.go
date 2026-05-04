// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/config"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/odata"
	"github.com/ondatra-labs/ondatrasql/internal/output"
)

func runOData(ctx context.Context, cfg *config.Config, port string) error {
	// Load models, filter @expose
	models, err := loadModelsFromDir(cfg)
	if err != nil {
		return fmt.Errorf("load models: %w", err)
	}

	var targets []odata.ExposeTarget
	for _, m := range models {
		if m.Expose {
			targets = append(targets, odata.ExposeTarget{
				Target:    m.Target,
				KeyColumn: m.ExposeKey,
			})
		}
	}

	if len(targets) == 0 {
		return fmt.Errorf("no models with @expose directive found\n\nAdd -- @expose to models you want to serve via OData")
	}

	// Validate port up front so a busy port surfaces a clean error
	// instead of being delayed inside the server goroutine. (Bug 26)
	if err := checkPortAvailable(port); err != nil {
		return fmt.Errorf("port %s unavailable: %w", port, err)
	}

	// Init DuckDB session
	sess, err := duckdb.NewSession("")
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	defer closeSessionOrLog(sess)

	if err := sess.InitWithCatalog(cfg.ConfigPath); err != nil {
		return fmt.Errorf("init session: %w", err)
	}

	// Discover schemas
	schemas, err := odata.DiscoverSchemas(sess, targets)
	if err != nil {
		return fmt.Errorf("discover schemas: %w", err)
	}

	// Discover navigation properties between exposed entities
	odata.DiscoverNavigationProperties(schemas)

	addr := "127.0.0.1:" + port
	baseURL := "http://" + addr

	// Create server. Returns an error only when ONDATRA_ODATA_DELTA_MAX_AGE
	// is set to an unparseable value — fail-closed so a typo can't silently
	// disable token expiry.
	handler, err := odata.NewServer(sess, schemas, baseURL)
	if err != nil {
		return fmt.Errorf("create odata server: %w", err)
	}
	// Timeouts bound the worst-case time a single request can hold a
	// goroutine + read-pool connection. ReadHeaderTimeout protects
	// against slow-loris-style attacks that drip request bytes
	// forever; ReadTimeout/WriteTimeout cap the full request/response
	// budget; IdleTimeout closes idle keep-alive sockets. Without
	// these, a slow `$count` subquery could hold a read-pool slot
	// indefinitely because acquireDB only honours r.Context() and the
	// bare http.Server has no deadline of its own. (R10 #7.)
	//
	// 60s read/write is generous enough for legitimate analytical
	// queries on cold caches but short enough that a wedged backend
	// can't drain the pool by accumulating stuck requests.
	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// Output
	output.Fprintf("OData server starting...\n")
	output.Fprintf("  Endpoint: %s/odata\n", baseURL)
	for _, s := range schemas {
		output.Fprintf("  %s (%d columns)\n", s.Target, len(s.Columns))
	}

	// Start server. http.ErrServerClosed is the documented "graceful
	// shutdown" sentinel from Shutdown below; surface anything else.
	// Without this filter SIGINT/SIGTERM looked like a real error to
	// any supervisor watching the daemon's exit code (Codex round 4
	// finding).
	errCh := make(chan error, 1)
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		} else {
			errCh <- nil
		}
	}()

	// Graceful shutdown (same pattern as daemon)
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return httpSrv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}
