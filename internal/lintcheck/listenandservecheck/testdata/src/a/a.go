package a

import (
	"context"
	"errors"
	"net"
	"net/http"
)

// good: explicit ErrServerClosed check inline.
func goodInline() {
	srv := &http.Server{}
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		_ = err
	}
}

// good: assignment + later if-stmt that checks ErrServerClosed.
func goodAssignThenCheck() {
	srv := &http.Server{}
	l, _ := net.Listen("tcp", ":0")
	err := srv.Serve(l)
	if !errors.Is(err, http.ErrServerClosed) && err != nil {
		_ = err
	}
}

// bad: bare-statement Serve in a goroutine — no check.
func badGoroutine() {
	srv := &http.Server{}
	l, _ := net.Listen("tcp", ":0")
	go func() {
		srv.Serve(l) // want `must check via errors.Is\(err, http.ErrServerClosed\)`
	}()
}

// bad: returned verbatim.
func badReturned(srv *http.Server) error {
	return srv.ListenAndServe() // want `must check via errors.Is\(err, http.ErrServerClosed\)`
}

// bad: assigned but never checked.
func badAssignedUnchecked() {
	srv := &http.Server{}
	err := srv.ListenAndServe() // want `must check via errors.Is\(err, http.ErrServerClosed\)`
	_ = err
}

// good: ListenAndServeTLS via inline check.
func goodTLS() {
	srv := &http.Server{}
	if err := srv.ListenAndServeTLS("", ""); !errors.Is(err, http.ErrServerClosed) {
		_ = err
	}
}

// not flagged: graceful shutdown context plumbing isn't a Server method.
func unrelated(ctx context.Context) {
	_ = ctx.Err()
}

// bad: ErrServerClosed mention exists in a sibling, but inside a
// dead/nested block — the err variable is never actually checked
// against the sentinel in a real condition. Pre-R6 this slipped
// through because mentionsErrServerClosed walked all subsequent
// statements recursively. (R6 finding.)
func badDeadBranchCitation() {
	srv := &http.Server{}
	err := srv.ListenAndServe() // want `must check via errors.Is\(err, http.ErrServerClosed\)`
	_ = err
	if false {
		// citation buried in unreachable body — must NOT count
		_ = errors.Is(err, http.ErrServerClosed)
	}
}

// bad: comment-only mention of ErrServerClosed shouldn't suppress.
func badCommentOnlyCitation() {
	srv := &http.Server{}
	err := srv.ListenAndServe() // want `must check via errors.Is\(err, http.ErrServerClosed\)`
	// see http.ErrServerClosed for shutdown behavior
	_ = err
}
