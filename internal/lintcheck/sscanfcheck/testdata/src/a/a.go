package a

import "fmt"

func numericVerbsFlagged() {
	var n int
	var f float64
	fmt.Sscanf("42", "%d", &n)        // want `silently falls back to zero`
	fmt.Sscanf("3.14", "%f", &f)      // want `silently falls back to zero`
	fmt.Sscanf("abc", "%x", &n)       // want `silently falls back to zero`
	fmt.Sscanf("1 2", "%d %d", &n, &f) // want `silently falls back to zero`
}

func stringVerbAllowed() {
	var s string
	fmt.Sscanf("hello", "%s", &s) // OK — %s isn't a numeric verb
}

func nonFmtSscanfAllowed() {
	// Not the fmt package — must not flag.
	var n int
	custom.Sscanf("42", "%d", &n)
}

type fakeFmt struct{}

func (fakeFmt) Sscanf(s, format string, a ...any) (int, error) { return 0, nil }

var custom fakeFmt
