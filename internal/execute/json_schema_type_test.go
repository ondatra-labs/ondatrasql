package execute

import "testing"

func TestToJSONSchemaType(t *testing.T) {
	t.Parallel()
	tests := []struct {
		normalized string
		want       string
	}{
		{"string", "string"},
		{"integer", "integer"},
		{"float", "number"},
		{"decimal", "number"},
		{"boolean", "boolean"},
		{"date", "string"},
		{"time", "string"},
		{"timestamp", "string"},
		{"interval", "string"},
		{"uuid", "string"},
		{"blob", "string"},
		{"bit", "string"},
		{"json", "array"},
		{"list", "array"},
		{"map", "object"},
		{"struct", "object"},
		{"union", "string"},
		{"variant", "string"},
		{"enum", "string"},
		{"unknown_type", "string"},
	}
	for _, tt := range tests {
		t.Run(tt.normalized, func(t *testing.T) {
			got := toJSONSchemaType(tt.normalized)
			if got != tt.want {
				t.Errorf("toJSONSchemaType(%q) = %q, want %q", tt.normalized, got, tt.want)
			}
		})
	}
}

func TestNormalizeType_IncludesJSONSchemaType(t *testing.T) {
	t.Parallel()
	tests := []struct {
		duckdb   string
		wantType string
		wantJSON string
	}{
		{"VARCHAR", "string", "string"},
		{"INTEGER", "integer", "integer"},
		{"DOUBLE", "float", "number"},
		{"BOOLEAN", "boolean", "boolean"},
		{"DATE", "date", "string"},
		{"JSON", "json", "array"},
		{"UUID", "uuid", "string"},
	}
	for _, tt := range tests {
		t.Run(tt.duckdb, func(t *testing.T) {
			result := normalizeType(tt.duckdb)
			if result["type"] != tt.wantType {
				t.Errorf("type = %v, want %v", result["type"], tt.wantType)
			}
			if result["json_schema_type"] != tt.wantJSON {
				t.Errorf("json_schema_type = %v, want %v", result["json_schema_type"], tt.wantJSON)
			}
		})
	}
}
