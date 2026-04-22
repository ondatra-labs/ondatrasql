// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package libregistry

import (
	"testing"
)

func TestAPIDict_FetchOnly(t *testing.T) {
	t.Parallel()
	code := `
API = {
    "base_url": "https://api.example.com",
    "fetch": {
        "args": ["resource"],
        "columns": {
            "id": {"type": "BIGINT"},
            "name": {"type": "VARCHAR"},
        },
    },
}

def fetch(save, resource):
    pass
`
	lf, err := parseLibFile("example_api", "lib/example_api.star", code)
	if err != nil {
		t.Fatal(err)
	}
	if lf == nil {
		t.Fatal("expected lib func")
	}
	if lf.IsSink {
		t.Error("expected IsSink=false for fetch-only API")
	}
	if lf.FuncName != "fetch" {
		t.Errorf("FuncName = %q, want fetch", lf.FuncName)
	}
	if len(lf.Args) != 1 || lf.Args[0] != "resource" {
		t.Errorf("Args = %v, want [resource]", lf.Args)
	}
	if len(lf.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(lf.Columns))
	}
	if lf.SinkConfig != nil {
		t.Error("expected SinkConfig=nil for fetch-only API")
	}
	if lf.APIConfig == nil {
		t.Fatal("expected APIConfig")
	}
	if lf.APIConfig.BaseURL != "https://api.example.com" {
		t.Errorf("BaseURL = %q, want https://api.example.com", lf.APIConfig.BaseURL)
	}
}

func TestAPIDict_PushOnly(t *testing.T) {
	t.Parallel()
	code := `
API = {
    "base_url": "https://api.example.com",
    "push": {
        "batch_size": 50,
        "batch_mode": "atomic",
    },
}

def push(rows):
    pass
`
	lf, err := parseLibFile("example_push", "lib/example_push.star", code)
	if err != nil {
		t.Fatal(err)
	}
	if lf == nil {
		t.Fatal("expected lib func")
	}
	if !lf.IsSink {
		t.Error("expected IsSink=true for push-only API")
	}
	if lf.FuncName != "push" {
		t.Errorf("FuncName = %q, want push", lf.FuncName)
	}
	if lf.SinkConfig == nil {
		t.Fatal("expected SinkConfig")
	}
	if lf.SinkConfig.BatchSize != 50 {
		t.Errorf("BatchSize = %d, want 50", lf.SinkConfig.BatchSize)
	}
	if lf.SinkConfig.BatchMode != "atomic" {
		t.Errorf("BatchMode = %q, want atomic", lf.SinkConfig.BatchMode)
	}
	if lf.APIConfig == nil {
		t.Fatal("expected APIConfig")
	}
}

func TestAPIDict_Both(t *testing.T) {
	t.Parallel()
	code := `
API = {
    "base_url": "https://api.hubapi.com",
    "fetch": {
        "args": ["object_type"],
        "dynamic_columns": True,
    },
    "push": {
        "batch_size": 100,
        "batch_mode": "sync",
    },
}

def fetch(save, object_type):
    pass

def push(rows):
    pass
`
	lf, err := parseLibFile("hubspot", "lib/hubspot.star", code)
	if err != nil {
		t.Fatal(err)
	}
	if lf == nil {
		t.Fatal("expected lib func")
	}
	// With both, registered as source (IsSink=false)
	if lf.IsSink {
		t.Error("expected IsSink=false for API with both fetch+push")
	}
	if lf.FuncName != "fetch" {
		t.Errorf("FuncName = %q, want fetch", lf.FuncName)
	}
	if !lf.DynamicColumns {
		t.Error("expected DynamicColumns=true")
	}
	if lf.SinkConfig == nil {
		t.Fatal("expected SinkConfig for API with push section")
	}
	if lf.SinkConfig.BatchSize != 100 {
		t.Errorf("BatchSize = %d, want 100", lf.SinkConfig.BatchSize)
	}
	if lf.APIConfig == nil {
		t.Fatal("expected APIConfig")
	}
	if lf.APIConfig.BaseURL != "https://api.hubapi.com" {
		t.Errorf("BaseURL = %q", lf.APIConfig.BaseURL)
	}
}

func TestAPIDict_SharedConfig(t *testing.T) {
	t.Parallel()
	code := `
API = {
    "base_url": "https://api.example.com",
    "auth": {"provider": "example", "env": "API_KEY"},
    "headers": {"Accept": "application/json", "X-Version": "2024-01"},
    "timeout": 60,
    "retry": 5,
    "backoff": 3,
    "fetch": {
        "args": ["q"],
    },
}

def fetch(save, q):
    pass
`
	lf, err := parseLibFile("cfg_test", "lib/cfg_test.star", code)
	if err != nil {
		t.Fatal(err)
	}
	if lf == nil {
		t.Fatal("expected lib func")
	}
	cfg := lf.APIConfig
	if cfg == nil {
		t.Fatal("expected APIConfig")
	}
	if cfg.BaseURL != "https://api.example.com" {
		t.Errorf("BaseURL = %q", cfg.BaseURL)
	}
	if cfg.Auth == nil {
		t.Fatal("expected Auth")
	}
	if cfg.Auth["provider"] != "example" {
		t.Errorf("Auth.provider = %v", cfg.Auth["provider"])
	}
	if cfg.Auth["env"] != "API_KEY" {
		t.Errorf("Auth.env = %v", cfg.Auth["env"])
	}
	if cfg.Headers == nil {
		t.Fatal("expected Headers")
	}
	if cfg.Headers["Accept"] != "application/json" {
		t.Errorf("Headers.Accept = %q", cfg.Headers["Accept"])
	}
	if cfg.Headers["X-Version"] != "2024-01" {
		t.Errorf("Headers.X-Version = %q", cfg.Headers["X-Version"])
	}
	if cfg.Timeout != 60 {
		t.Errorf("Timeout = %d, want 60", cfg.Timeout)
	}
	if cfg.Retry != 5 {
		t.Errorf("Retry = %d, want 5", cfg.Retry)
	}
	if cfg.Backoff != 3 {
		t.Errorf("Backoff = %d, want 3", cfg.Backoff)
	}
}

func TestAPIDict_RateLimit(t *testing.T) {
	t.Parallel()
	code := `
API = {
    "base_url": "https://api.example.com",
    "rate_limit": {"requests": 100, "per": "10s"},
    "fetch": {
        "args": [],
        "rate_limit": {"requests": 200, "per": "1m"},
    },
    "push": {
        "batch_size": 10,
        "rate_limit": {"requests": 50, "per": "10s"},
    },
}

def fetch(save):
    pass

def push(rows):
    pass
`
	lf, err := parseLibFile("rl_test", "lib/rl_test.star", code)
	if err != nil {
		t.Fatal(err)
	}
	if lf == nil {
		t.Fatal("expected lib func")
	}

	// Fetch direction: overridden rate_limit on APIConfig
	cfg := lf.APIConfig
	if cfg == nil {
		t.Fatal("expected APIConfig")
	}
	if cfg.RateLimit == nil {
		t.Fatal("expected RateLimit on APIConfig (fetch override)")
	}
	if cfg.RateLimit.Requests != 200 {
		t.Errorf("APIConfig.RateLimit.Requests = %d, want 200", cfg.RateLimit.Requests)
	}
	if cfg.RateLimit.Per != "1m" {
		t.Errorf("APIConfig.RateLimit.Per = %q, want 1m", cfg.RateLimit.Per)
	}

	// Push direction: overridden rate_limit on SinkConfig
	sink := lf.SinkConfig
	if sink == nil {
		t.Fatal("expected SinkConfig")
	}
	if sink.RateLimit == nil {
		t.Fatal("expected RateLimit on SinkConfig (push override)")
	}
	if sink.RateLimit.Requests != 50 {
		t.Errorf("SinkConfig.RateLimit.Requests = %d, want 50", sink.RateLimit.Requests)
	}
	if sink.RateLimit.Per != "10s" {
		t.Errorf("SinkConfig.RateLimit.Per = %q, want 10s", sink.RateLimit.Per)
	}
}

func TestAPIDict_RateLimit_Inherited(t *testing.T) {
	t.Parallel()
	code := `
API = {
    "base_url": "https://api.example.com",
    "rate_limit": {"requests": 100, "per": "10s"},
    "fetch": {
        "args": [],
    },
    "push": {
        "batch_size": 10,
    },
}

def fetch(save):
    pass

def push(rows):
    pass
`
	lf, err := parseLibFile("rl_inherit", "lib/rl_inherit.star", code)
	if err != nil {
		t.Fatal(err)
	}

	// Fetch direction: inherits top-level
	cfg := lf.APIConfig
	if cfg.RateLimit == nil {
		t.Fatal("expected inherited RateLimit on APIConfig")
	}
	if cfg.RateLimit.Requests != 100 {
		t.Errorf("APIConfig.RateLimit.Requests = %d, want 100", cfg.RateLimit.Requests)
	}

	// Push direction: inherits top-level
	sink := lf.SinkConfig
	if sink.RateLimit == nil {
		t.Fatal("expected inherited RateLimit on SinkConfig")
	}
	if sink.RateLimit.Requests != 100 {
		t.Errorf("SinkConfig.RateLimit.Requests = %d, want 100", sink.RateLimit.Requests)
	}
}

func TestAPIDict_MissingFetchFunc(t *testing.T) {
	t.Parallel()
	code := `
API = {
    "base_url": "https://api.example.com",
    "fetch": {
        "args": ["q"],
    },
}
# No fetch() function
`
	_, err := parseLibFile("no_fetch", "lib/no_fetch.star", code)
	if err == nil {
		t.Fatal("expected error for missing fetch()")
	}
}

func TestAPIDict_MissingPushFunc(t *testing.T) {
	t.Parallel()
	code := `
API = {
    "base_url": "https://api.example.com",
    "push": {
        "batch_size": 10,
    },
}
# No push() function
`
	_, err := parseLibFile("no_push", "lib/no_push.star", code)
	if err == nil {
		t.Fatal("expected error for missing push()")
	}
}

func TestAPIDict_DynamicColumnsWithColumns(t *testing.T) {
	t.Parallel()
	code := `
API = {
    "base_url": "https://api.example.com",
    "fetch": {
        "args": [],
        "columns": {"id": {"type": "BIGINT"}},
        "dynamic_columns": True,
    },
}

def fetch(save):
    pass
`
	_, err := parseLibFile("bad_cols", "lib/bad_cols.star", code)
	if err == nil {
		t.Fatal("expected error for dynamic_columns + columns")
	}
}

func TestAPIDict_NoFetchNoPush(t *testing.T) {
	t.Parallel()
	code := `
API = {
    "base_url": "https://api.example.com",
}

def fetch(save):
    pass
`
	_, err := parseLibFile("empty_api", "lib/empty_api.star", code)
	if err == nil {
		t.Fatal("expected error for API with neither fetch nor push")
	}
}

func TestAPIDict_InvalidPushBatchMode(t *testing.T) {
	t.Parallel()
	code := `
API = {
    "base_url": "https://api.example.com",
    "push": {
        "batch_mode": "invalid",
    },
}

def push(rows):
    pass
`
	_, err := parseLibFile("bad_mode", "lib/bad_mode.star", code)
	if err == nil {
		t.Fatal("expected error for invalid batch_mode")
	}
}
