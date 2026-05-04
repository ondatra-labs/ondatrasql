package a

import "fmt"

// EscapeSQL is the project-internal escape helper.
func EscapeSQL(s string) string { return s }

func badAttach() {
	connStr := getConnStr()
	_ = fmt.Sprintf("ATTACH '%s' AS lake", connStr) // want `must be wrapped with EscapeSQL`
}

func badAttachV() {
	connStr := getConnStr()
	_ = fmt.Sprintf("ATTACH '%v' AS lake", connStr) // want `must be wrapped with EscapeSQL`
}

func badSearchPath() {
	schema := getSchema()
	_ = fmt.Sprintf("SET search_path = '%s'", schema) // want `must be wrapped with EscapeSQL`
}

func goodAttach() {
	connStr := getConnStr()
	_ = fmt.Sprintf("ATTACH '%s' AS lake", EscapeSQL(connStr))
}

func goodSearchPath() {
	schema := getSchema()
	_ = fmt.Sprintf("SET search_path = '%s'", EscapeSQL(schema))
}

func bypassed() {
	literal := "ducklake:sqlite:test.sqlite"
	//escapesqlcheck:trusted-input compile-time literal value
	_ = fmt.Sprintf("ATTACH '%s' AS lake", literal)
}

func bypassedWithoutReason() {
	literal := "ducklake:sqlite:test.sqlite"
	// Bare bypass marker (no reason) does NOT suppress the diagnostic.
	//escapesqlcheck:trusted-input
	_ = fmt.Sprintf("ATTACH '%s' AS lake", literal) // want `must be wrapped with EscapeSQL`
}

func bypassMidComment() {
	literal := "ducklake:sqlite:test.sqlite"
	// see also //escapesqlcheck:trusted-input prose citation should NOT count
	_ = fmt.Sprintf("ATTACH '%s' AS lake", literal) // want `must be wrapped with EscapeSQL`
}

func unrelatedFmt() {
	// Identifier ATTACH (no quotes) — not in scope.
	_ = fmt.Sprintf("ATTACH %s AS lake", "lake")
}

func multiArg() {
	a := "x"
	b := getConnStr()
	// Second %s is in the dangerous position; b must be EscapeSQL'd.
	_ = fmt.Sprintf("PRAGMA %s; ATTACH '%s'", a, b) // want `must be wrapped with EscapeSQL`
}

func getConnStr() string { return "" }
func getSchema() string  { return "" }
