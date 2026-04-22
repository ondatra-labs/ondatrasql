// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/libregistry"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
)

func syncSink(batchMode string) *libregistry.LibFunc {
	return &libregistry.LibFunc{
		Name:     "test_push",
		IsSink:   true,
		FuncName: "push",
		SinkConfig: &libregistry.SinkConfig{
			BatchSize:     100,
			BatchMode:     batchMode,
			MaxConcurrent: 1,
		},
	}
}

func sinkWithRateLimit(per string) *libregistry.LibFunc {
	s := syncSink("sync")
	s.SinkConfig.RateLimit = &libregistry.RateLimitConfig{Requests: 10, Per: per}
	return s
}

func sinkWithPoll(interval, timeout string) *libregistry.LibFunc {
	s := syncSink("async")
	s.SinkConfig.PollInterval = interval
	s.SinkConfig.PollTimeout = timeout
	return s
}

func sinkWithConcurrency(mode string, concurrent int) *libregistry.LibFunc {
	s := syncSink(mode)
	s.SinkConfig.MaxConcurrent = concurrent
	return s
}

func makeReg(funcs map[string]*libregistry.LibFunc) *libregistry.Registry {
	// Use Scan-compatible registry by building via exported methods
	// Since Registry.funcs is unexported, we use a test helper
	return libregistry.NewRegistryForTest(funcs)
}

func TestValidate_TableAsyncAllowed(t *testing.T) {
	t.Parallel()
	reg := makeReg(map[string]*libregistry.LibFunc{"async_push": syncSink("async")})
	models := []*parser.Model{{Target: "sync.contacts", Kind: "table", Sink: "async_push"}}

	if err := ValidateModelSinkCompat(models, reg); err != nil {
		t.Errorf("table + async should be allowed, got: %v", err)
	}
}

func TestValidate_MergeWithoutUniqueKey(t *testing.T) {
	t.Parallel()
	reg := makeReg(map[string]*libregistry.LibFunc{"push": syncSink("sync")})
	models := []*parser.Model{{Target: "sync.contacts", Kind: "merge", Sink: "push"}}

	err := ValidateModelSinkCompat(models, reg)
	if err == nil {
		t.Fatal("expected error for merge + sink without unique_key")
	}
	if !strings.Contains(err.Error(), "unique_key") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_TrackedWithoutUniqueKey(t *testing.T) {
	t.Parallel()
	reg := makeReg(map[string]*libregistry.LibFunc{"push": syncSink("sync")})
	models := []*parser.Model{{Target: "sync.invoices", Kind: "tracked", Sink: "push"}}

	err := ValidateModelSinkCompat(models, reg)
	if err == nil {
		t.Fatal("expected error for tracked + sink without unique_key")
	}
	if !strings.Contains(err.Error(), "unique_key") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_PollWithNonAsync(t *testing.T) {
	t.Parallel()
	s := syncSink("sync")
	s.SinkConfig.PollInterval = "30s"
	reg := makeReg(map[string]*libregistry.LibFunc{"push": s})
	models := []*parser.Model{{Target: "sync.x", Kind: "merge", Sink: "push", UniqueKey: "id"}}

	err := ValidateModelSinkCompat(models, reg)
	if err == nil {
		t.Fatal("expected error for poll_interval with non-async")
	}
	if !strings.Contains(err.Error(), "poll_interval") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_MaxConcurrentWithAtomic(t *testing.T) {
	t.Parallel()
	reg := makeReg(map[string]*libregistry.LibFunc{"push": sinkWithConcurrency("atomic", 4)})
	models := []*parser.Model{{Target: "sync.x", Kind: "merge", Sink: "push", UniqueKey: "id"}}

	err := ValidateModelSinkCompat(models, reg)
	if err == nil {
		t.Fatal("expected error for max_concurrent > 1 with atomic")
	}
	if !strings.Contains(err.Error(), "max_concurrent") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_MaxConcurrentWithAsync(t *testing.T) {
	t.Parallel()
	s := sinkWithConcurrency("async", 4)
	s.SinkConfig.PollInterval = "30s"
	s.SinkConfig.PollTimeout = "1h"
	reg := makeReg(map[string]*libregistry.LibFunc{"push": s})
	models := []*parser.Model{{Target: "sync.x", Kind: "merge", Sink: "push", UniqueKey: "id"}}

	err := ValidateModelSinkCompat(models, reg)
	if err == nil {
		t.Fatal("expected error for max_concurrent > 1 with async")
	}
	if !strings.Contains(err.Error(), "max_concurrent") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_InvalidPollInterval(t *testing.T) {
	t.Parallel()
	reg := makeReg(map[string]*libregistry.LibFunc{"push": sinkWithPoll("30sec", "1h")})
	models := []*parser.Model{{Target: "sync.x", Kind: "merge", Sink: "push", UniqueKey: "id"}}

	err := ValidateModelSinkCompat(models, reg)
	if err == nil {
		t.Fatal("expected error for invalid poll_interval")
	}
	if !strings.Contains(err.Error(), "poll_interval") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_InvalidPollTimeout(t *testing.T) {
	t.Parallel()
	reg := makeReg(map[string]*libregistry.LibFunc{"push": sinkWithPoll("30s", "2hours")})
	models := []*parser.Model{{Target: "sync.x", Kind: "merge", Sink: "push", UniqueKey: "id"}}

	err := ValidateModelSinkCompat(models, reg)
	if err == nil {
		t.Fatal("expected error for invalid poll_timeout")
	}
	if !strings.Contains(err.Error(), "poll_timeout") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_InvalidRateLimitPer(t *testing.T) {
	t.Parallel()
	reg := makeReg(map[string]*libregistry.LibFunc{"push": sinkWithRateLimit("10seconds")})
	models := []*parser.Model{{Target: "sync.x", Kind: "merge", Sink: "push", UniqueKey: "id"}}

	err := ValidateModelSinkCompat(models, reg)
	if err == nil {
		t.Fatal("expected error for invalid rate_limit.per")
	}
	if !strings.Contains(err.Error(), "rate_limit") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_SinkNotFound(t *testing.T) {
	t.Parallel()
	reg := makeReg(map[string]*libregistry.LibFunc{"other_push": syncSink("sync")})
	models := []*parser.Model{{Target: "sync.x", Kind: "merge", Sink: "missing_push", UniqueKey: "id"}}

	err := ValidateModelSinkCompat(models, reg)
	if err == nil {
		t.Fatal("expected error for missing sink")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_EmptyRegistryWithSinkModel(t *testing.T) {
	t.Parallel()
	reg := makeReg(map[string]*libregistry.LibFunc{})
	models := []*parser.Model{{Target: "sync.x", Kind: "merge", Sink: "push", UniqueKey: "id"}}

	err := ValidateModelSinkCompat(models, reg)
	if err == nil {
		t.Fatal("expected error for empty registry with sink model")
	}
	if !strings.Contains(err.Error(), "no lib functions") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_NilRegistryWithSinkModel(t *testing.T) {
	t.Parallel()
	models := []*parser.Model{{Target: "sync.x", Kind: "merge", Sink: "push", UniqueKey: "id"}}

	err := ValidateModelSinkCompat(models, nil)
	if err == nil {
		t.Fatal("expected error for nil registry with sink model")
	}
}

func TestValidate_NoSinkDict(t *testing.T) {
	t.Parallel()
	noDict := &libregistry.LibFunc{Name: "push", IsSink: true, FuncName: "push"}
	reg := makeReg(map[string]*libregistry.LibFunc{"push": noDict})
	models := []*parser.Model{{Target: "sync.x", Kind: "merge", Sink: "push", UniqueKey: "id"}}

	err := ValidateModelSinkCompat(models, reg)
	if err == nil {
		t.Fatal("expected error for sink without SINK dict")
	}
	if !strings.Contains(err.Error(), "no SINK dict") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_ValidConfig(t *testing.T) {
	t.Parallel()
	reg := makeReg(map[string]*libregistry.LibFunc{"push": syncSink("sync")})
	models := []*parser.Model{{Target: "sync.contacts", Kind: "merge", Sink: "push", UniqueKey: "id"}}

	if err := ValidateModelSinkCompat(models, reg); err != nil {
		t.Errorf("unexpected error for valid config: %v", err)
	}
}

func TestValidate_NoSinkModels(t *testing.T) {
	t.Parallel()
	models := []*parser.Model{{Target: "staging.orders", Kind: "table"}}

	if err := ValidateModelSinkCompat(models, nil); err != nil {
		t.Errorf("unexpected error for no-sink models: %v", err)
	}
}

func TestValidate_TableMaxConcurrentAllowed(t *testing.T) {
	t.Parallel()
	reg := makeReg(map[string]*libregistry.LibFunc{"push": sinkWithConcurrency("sync", 4)})
	models := []*parser.Model{{Target: "sync.contacts", Kind: "table", Sink: "push"}}

	if err := ValidateModelSinkCompat(models, reg); err != nil {
		t.Errorf("table + max_concurrent > 1 should be allowed, got: %v", err)
	}
}
