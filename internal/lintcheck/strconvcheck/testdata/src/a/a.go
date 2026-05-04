package a

import "strconv"

// bad: error is discarded — silent zero on parse failure.
func badAtoi() {
	v, _ := strconv.Atoi("123") // want `strconv parse error discarded`
	_ = v
}

func badParseInt() {
	v, _ := strconv.ParseInt("123", 10, 64) // want `strconv parse error discarded`
	_ = v
}

func badParseUint() {
	v, _ := strconv.ParseUint("123", 10, 64) // want `strconv parse error discarded`
	_ = v
}

func badParseFloat() {
	v, _ := strconv.ParseFloat("1.5", 64) // want `strconv parse error discarded`
	_ = v
}

func badParseBool() {
	v, _ := strconv.ParseBool("true") // want `strconv parse error discarded`
	_ = v
}

// good: error is bound and inspected.
func goodAtoi() error {
	v, err := strconv.Atoi("123")
	if err != nil {
		return err
	}
	_ = v
	return nil
}

// good: bypass marker WITH reason on the line directly above.
func bypassedWithReason() {
	//strconvcheck:silent best-effort hint parse — zero fallback is fine for telemetry
	v, _ := strconv.Atoi("hint")
	_ = v
}

// bad: bare marker (no reason) is treated as if absent so the
// diagnostic still fires — guards against drive-by suppressions.
func bypassedWithoutReason() {
	//strconvcheck:silent
	v, _ := strconv.Atoi("hint") // want `strconv parse error discarded`
	_ = v
}

// out-of-scope: unrelated package with the same function name.
type other struct{}

func (other) Atoi(string) (int, error) { return 0, nil }

func notStrconv() {
	var o other
	v, _ := o.Atoi("123")
	_ = v
}

// bad: marker embedded mid-comment must NOT suppress the diagnostic
// (R6 finding — strings.Contains based check let prose-citations
// silently bypass the analyzer).
func bypassMidComment() {
	// see https://example.com //strconvcheck:silent fake reason
	v, _ := strconv.Atoi("hint") // want `strconv parse error discarded`
	_ = v
}

// bad: package-level var declaration with discarded error
// (R7 #7 — pre-fix the analyzer only visited AssignStmt).
var pkgLevel, _ = strconv.Atoi("123") // want `strconv parse error discarded`

// bad: function-scoped var declaration with discarded error.
func varDeclDiscarded() {
	var v, _ = strconv.ParseInt("123", 10, 64) // want `strconv parse error discarded`
	_ = v
}

// good: var declaration that actually binds the error.
var pkgGood, pkgErr = strconv.Atoi("ok")

func _() { _ = pkgGood; _ = pkgErr }

// good: var declaration with bypass marker and reason.
//
//strconvcheck:silent literal default for telemetry hint
var bypassedDecl, _ = strconv.Atoi("default")

func _() { _ = bypassedDecl }
