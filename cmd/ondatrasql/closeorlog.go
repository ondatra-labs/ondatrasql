// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"fmt"
	"io"
	"os"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
)

// closeSessionOrLog closes a DuckDB session and logs any error to
// stderr. Used as a deferred call-site so the caller doesn't need a
// closure per use, while still surfacing close-time failures (postgres
// sandbox database drop errors, DuckDB close errors) instead of
// silently swallowing them.
//
// Close errors here do NOT indicate data loss: DuckLake commits are
// persisted at commit time, not on session close. Session close drains
// the read pool, detaches the sandbox catalog, and drops the sandbox
// postgres database (when applicable). A failure leaks resources, not
// data — but the operator should still see it.
func closeSessionOrLog(sess *duckdb.Session) {
	// Nil guard: callers typically use this in `defer
	// closeSessionOrLog(sess)` immediately after `NewSession`, but
	// some setup paths return an error before assigning sess (e.g.
	// `if err := sess.InitWithCatalog(...)` succeeds, but a later
	// helper fails and the deferred close fires anyway). Silently
	// no-oping on nil prevents the close-time panic from masking the
	// caller's primary error.
	if sess == nil {
		return
	}
	if err := sess.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: close session: %v\n", err)
	}
}

// writeBody writes body to the HTTP response writer and logs any
// write error to stderr. Used by the OAuth callback handler in
// auth_cmd.go where write failures are not actionable from the
// server side (the user's browser closed the tab, network dropped,
// etc.) but the operator running `ondatrasql auth` should still see
// an unexpected pattern (e.g. every callback failing).
//
// Extracted as a package-level function rather than a closure inside
// the handler so the write-error logging path is unit-testable
// without standing up an HTTP server.
func writeBody(w io.Writer, body string) {
	if _, err := fmt.Fprint(w, body); err != nil {
		fmt.Fprintf(os.Stderr, "warning: write auth callback response: %v\n", err)
	}
}
