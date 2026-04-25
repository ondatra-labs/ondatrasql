package libregistry

import (
	"strings"
	"testing"
)

func TestSupportedKinds_FetchRejectsUnsupported(t *testing.T) {
	t.Parallel()
	code := `
API = {
    "base_url": "https://api.example.com",
    "fetch": {
        "args": [],
        "supported_kinds": ["table"],
    },
}

def fetch(page):
    pass
`
	lf, err := parseLibFile("kind_check", "lib/kind_check.star", code)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(lf.SupportedKinds) != 1 || lf.SupportedKinds[0] != "table" {
		t.Errorf("SupportedKinds = %v, want [table]", lf.SupportedKinds)
	}
}

func TestSupportedKinds_PushRejectsUnsupported(t *testing.T) {
	t.Parallel()
	code := `
API = {
    "base_url": "https://api.example.com",
    "push": {
        "supported_kinds": ["table", "append"],
        "batch_size": 100,
    },
}

def push(rows):
    pass
`
	lf, err := parseLibFile("push_kind", "lib/push_kind.star", code)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if lf.SinkConfig == nil {
		t.Fatal("expected SinkConfig")
	}
	if len(lf.SinkConfig.SupportedKinds) != 2 {
		t.Errorf("SupportedKinds = %v, want [table append]", lf.SinkConfig.SupportedKinds)
	}
}

func TestSupportedKinds_PushArgsMustBeDeclared(t *testing.T) {
	t.Parallel()
	code := `
API = {
    "base_url": "https://api.example.com",
    "push": {
        "args": ["spreadsheet_id"],
        "batch_size": 100,
    },
}

def push(rows):
    pass
`
	_, err := parseLibFile("missing_arg", "lib/missing_arg.star", code)
	if err == nil {
		t.Fatal("expected error for push() missing declared arg 'spreadsheet_id'")
	}
	if !strings.Contains(err.Error(), "spreadsheet_id") {
		t.Errorf("unexpected error: %v", err)
	}
}
