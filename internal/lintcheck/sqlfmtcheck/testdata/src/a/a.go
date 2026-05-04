package a

import "fmt"

// EscapeSQL is the project-internal escape helper. Stub for testdata
// so the analyzer's safe-list logic has something to recognise.
func EscapeSQL(s string) string { return s }

// QuoteIdentifier likewise.
func QuoteIdentifier(s string) string { return s }

func bad() {
	target := getTarget()
	id := getID()
	// SQL with %v — calls .String() on user-controlled value.
	_ = fmt.Sprintf("SELECT * FROM %s WHERE id = '%v'", target, id) // want `potential SQL injection`
}

func goodS() {
	target := getTarget()
	// %s without escape is currently allowed (codebase convention —
	// most %s sites are identifier interpolation with safe values).
	_ = fmt.Sprintf("SELECT * FROM %s", target)
}

func goodEscaped() {
	target := getTarget()
	_ = fmt.Sprintf("SELECT * FROM %s", QuoteIdentifier(target))
}

func goodEscapeSQL() {
	id := getID()
	_ = fmt.Sprintf("SELECT * FROM t WHERE id = '%s'", EscapeSQL(id))
}

func bypassed() {
	literal := "main.users"
	//sqlfmtcheck:trusted-input compile-time literal value
	_ = fmt.Sprintf("SELECT * FROM dynamic WHERE x = '%v'", literal)
}

func bypassedWithoutReason() {
	literal := "main.users"
	// Bare bypass marker (no reason) does NOT suppress the diagnostic;
	// the analyzer still flags it so reviewers can see the violation
	// and require a real opt-out justification.
	//sqlfmtcheck:trusted-input
	_ = fmt.Sprintf("SELECT * FROM dynamic WHERE x = '%v'", literal) // want `potential SQL injection`
}

func bypassMidComment() {
	literal := "main.users"
	// see also //sqlfmtcheck:trusted-input prose citation should NOT count
	_ = fmt.Sprintf("SELECT * FROM dynamic WHERE x = '%v'", literal) // want `potential SQL injection`
}

func nonSQLFormatString() {
	// "set high water mark" — English prose, not SQL — must not flag.
	_ = fmt.Sprintf("set high water mark warning: %v", "x")
	_ = fmt.Sprintf("ran in %v ms", 42)
}

func mixedCaseEnglish() {
	// "From" lowercase in prose — must not flag.
	_ = fmt.Sprintf("got error from upstream: %v", "x")
}

func nonConstFormat() {
	// Non-const format strings are out of scope (we don't speculate).
	q := getQuery()
	_ = fmt.Sprintf(q, "value")
}

func getTarget() string { return "x" }
func getID() string     { return "1" }
func getQuery() string  { return "" }
