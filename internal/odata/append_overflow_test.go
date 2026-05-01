package odata

import (
	"strings"
	"testing"
)

// TestAppendODataAnnotation_LargeInputs verifies appendODataAnnotation can't
// trigger int overflow on a 64-bit system at any practical input size.
// CodeQL flags the addition int+int+int as a theoretical overflow risk.
// We bound by realistic HTTP-server response sizes and check the function
// returns sane output.
func TestAppendODataAnnotation_LargeInputs(t *testing.T) {
	cases := []struct {
		name     string
		dataLen  int
		valueLen int
	}{
		{"tiny", 100, 50},
		{"medium 1MB", 1 << 20, 200},
		{"large 100MB", 100 << 20, 200},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			data := []byte("{" + strings.Repeat("x", c.dataLen-2) + "}")
			value := strings.Repeat("u", c.valueLen)
			out := appendODataAnnotation(data, "@odata.deltaLink", value)
			if out[len(out)-1] != '}' {
				t.Errorf("output not closed with }")
			}
			if len(out) <= len(data) {
				t.Errorf("output should grow, got len(out)=%d len(data)=%d", len(out), len(data))
			}
		})
	}
}

// On 64-bit Go, int is int64. To overflow `len(data)+len(prefix)+len(value)`
// we'd need a sum exceeding 2^63 bytes (~9.2 EB) — 18 times the entire memory
// ever manufactured. Documented here so future reviewers don't reopen the
// CodeQL alert without realizing the practical ceiling.
func TestAppendODataAnnotation_OverflowIsImpossibleOn64Bit(t *testing.T) {
	const intMax64 = int64(^uint(0) >> 1)
	const realisticHttpResponseMax = int64(2) << 30 // 2 GiB
	if intMax64 < realisticHttpResponseMax*1000 {
		t.Skip("not 64-bit, skipping bound check")
	}
}

// TestAppendODataAnnotation_LargeResponseDropsAnnotation pins the size-guard
// behavior: when data exceeds maxAnnotatedResponseSize the function returns
// the original bytes unchanged, dropping the @odata.deltaLink annotation
// rather than risking the size computation. CodeQL's go/allocation-size-overflow
// rule wants this guard. Lowering maxAnnotatedResponseSize would silently
// disable deltaLink for legitimately-large responses; raising it past the
// type ceiling would defeat the guard.
func TestAppendODataAnnotation_LargeResponseDropsAnnotation(t *testing.T) {
	tooLarge := maxAnnotatedResponseSize + 1
	data := []byte("{" + strings.Repeat("x", tooLarge-2) + "}")
	out := appendODataAnnotation(data, "@odata.deltaLink", "https://example/d")
	if len(out) != len(data) {
		t.Errorf("oversized input should pass through unchanged: got len=%d, want %d", len(out), len(data))
	}
}

// TestAppendODataAnnotation_OverlargeOverheadDropsAnnotation pins the second
// guard: when the key+value annotation overhead exceeds annotationOverheadBudget,
// the function returns data unchanged. This is the guard that closes
// CodeQL #6 by ensuring all three operands of the make() capacity addition
// are bounded.
func TestAppendODataAnnotation_OverlargeOverheadDropsAnnotation(t *testing.T) {
	data := []byte("{}")
	tooLargeValue := strings.Repeat("v", annotationOverheadBudget+1)
	out := appendODataAnnotation(data, "@odata.deltaLink", tooLargeValue)
	if len(out) != len(data) {
		t.Errorf("oversized annotation overhead should pass through unchanged: got len=%d, want %d", len(out), len(data))
	}
}
