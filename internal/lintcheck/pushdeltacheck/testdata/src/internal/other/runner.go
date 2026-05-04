package other

import "internal/parser"

// Other files in execute pkg can reference Kind freely — only
// push_delta.go is restricted.
func runnerUsesKind(m *parser.Model) string {
	return m.Kind
}
