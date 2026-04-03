// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"strings"
	"testing"
)

// Fuzz sanitizeTarget: must never panic, result must be alphanumeric + underscore only.
func FuzzSanitizeTarget(f *testing.F) {
	f.Add("raw.events")
	f.Add("staging.orders")
	f.Add("")
	f.Add("a.b.c")
	f.Add("has spaces.and-dashes")
	f.Add("UPPER.lower")
	f.Add("special!@#$%^&*()")

	f.Fuzz(func(t *testing.T, target string) {
		result := sanitizeTarget(target)

		// Property: only alphanumeric and underscore in output
		for _, c := range result {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
				t.Errorf("sanitizeTarget(%q) = %q, contains invalid char %q", target, result, string(c))
			}
		}

		// Property: no dots in output
		if strings.Contains(result, ".") {
			t.Errorf("sanitizeTarget(%q) = %q, contains dot", target, result)
		}
	})
}
