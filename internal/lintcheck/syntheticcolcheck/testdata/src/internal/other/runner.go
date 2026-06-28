// Test fixture: NOT named materialize.go, so the analyzer must ignore it even
// though it contains the otherwise-flagged pattern (file-suffix scoping).
package other

import "fmt"

func buildInsert(target string) string {
	// Same shape as a violation, but outside internal/execute/materialize.go.
	return fmt.Sprintf("INSERT INTO %s BY NAME SELECT v AS valid_from_snapshot FROM tmp", target)
}
