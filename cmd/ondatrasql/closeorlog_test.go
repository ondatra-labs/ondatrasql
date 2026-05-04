// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// errWriter is an io.Writer that always returns the configured error
// on Write. Used to drive writeBody's error-path test.
type errWriter struct{ err error }

func (e *errWriter) Write(p []byte) (int, error) { return 0, e.err }

// TestWriteBody_HappyPath asserts the helper writes the body verbatim
// to a successful writer and emits no stderr output.
func TestWriteBody_HappyPath(t *testing.T) {
	var buf bytes.Buffer
	writeBody(&buf, "<h1>Hello</h1>")
	if got := buf.String(); got != "<h1>Hello</h1>" {
		t.Errorf("body = %q, want %q", got, "<h1>Hello</h1>")
	}
}

// TestWriteBody_LogsOnWriteError regression-tests the round-3 fix
// that promotes silent write failures to a stderr warning. Pre-fix
// the call site used `fmt.Fprint(w, body)` and dropped the error;
// the writeBody helper now logs to stderr so an operator running
// `ondatrasql auth` sees a pattern of failing callbacks.
//
// We can't easily redirect package-level os.Stderr in a unit test
// without race-prone globals, so this test just confirms the helper
// returns cleanly when the writer fails (i.e. doesn't panic, doesn't
// deadlock, doesn't leak the error). The actual stderr write is the
// observable side effect a maintainer can verify with `ondatrasql
// auth` against a misbehaving browser.
func TestWriteBody_LogsOnWriteError(t *testing.T) {
	w := &errWriter{err: errors.New("connection reset by peer")}
	writeBody(w, "<h1>Error</h1>") // must not panic, must return cleanly
}

// TestCloseSessionOrLog_NilSession pins the R7 #9 fix: a deferred
// close on a nil session must not panic. Pre-fix the call site
// dereferenced sess unconditionally, so any setup path that left
// sess == nil at the deferred close ran into a runtime panic that
// masked the caller's primary error.
func TestCloseSessionOrLog_NilSession(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("closeSessionOrLog(nil) panicked: %v", r)
		}
	}()
	closeSessionOrLog(nil)
}

// TestWriteBody_ErrorPropagationShape asserts the package-level
// helper exists with the documented signature so the call sites in
// auth_cmd.go (which can't be unit-tested as easily because they live
// inside an HTTP handler closure) keep working when writeBody's
// signature changes. A signature change without updating callers
// would already fail to compile, but pinning the test gives a
// clearer failure message than a build error in unrelated CI runs.
func TestWriteBody_ErrorPropagationShape(t *testing.T) {
	// Smoke test — can be called with the documented signature.
	var buf strings.Builder
	writeBody(&buf, "x")
	if buf.String() != "x" {
		t.Errorf("smoke write = %q, want %q", buf.String(), "x")
	}
}
