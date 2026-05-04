package execute

import "internal/parser"

func usesKindInPushDelta(m *parser.Model) {
	_ = m.Kind // want `model.Kind reference inside push_delta.go`
}

// String comparison still flags — any read of .Kind triggers.
func compareKind(m *parser.Model) bool {
	return m.Kind == "table" // want `model.Kind reference inside push_delta.go`
}
