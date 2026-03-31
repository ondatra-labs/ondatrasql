// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"testing"
	"time"
)

func TestConvertTraceToSteps_Empty(t *testing.T) {
	t.Parallel()
	steps := ConvertTraceToSteps(nil)
	if len(steps) != 0 {
		t.Errorf("got %d steps, want 0", len(steps))
	}
}

func TestConvertTraceToSteps_SkipsGaps(t *testing.T) {
	t.Parallel()
	trace := []TraceStep{
		{Name: "step1", Duration: 100 * time.Millisecond, Status: "ok"},
		{Name: "_gap", Duration: 5 * time.Millisecond, Status: "ok"},
		{Name: "step2", Duration: 200 * time.Millisecond, Status: "ok"},
	}

	steps := ConvertTraceToSteps(trace)
	if len(steps) != 2 {
		t.Fatalf("got %d steps, want 2", len(steps))
	}
	if steps[0].Name != "step1" {
		t.Errorf("step[0].Name = %q, want %q", steps[0].Name, "step1")
	}
	if steps[1].Name != "step2" {
		t.Errorf("step[1].Name = %q, want %q", steps[1].Name, "step2")
	}
}

func TestConvertTraceToSteps_Duration(t *testing.T) {
	t.Parallel()
	trace := []TraceStep{
		{Name: "step1", Duration: 1500 * time.Millisecond, Status: "ok"},
		{Name: "step2", Duration: 250 * time.Millisecond, Status: "error"},
	}

	steps := ConvertTraceToSteps(trace)
	if steps[0].DurationMs != 1500 {
		t.Errorf("step[0].DurationMs = %d, want 1500", steps[0].DurationMs)
	}
	if steps[1].DurationMs != 250 {
		t.Errorf("step[1].DurationMs = %d, want 250", steps[1].DurationMs)
	}
	if steps[1].Status != "error" {
		t.Errorf("step[1].Status = %q, want %q", steps[1].Status, "error")
	}
}

func TestConvertTraceToSteps_AllGaps(t *testing.T) {
	t.Parallel()
	trace := []TraceStep{
		{Name: "_gap", Duration: 5 * time.Millisecond, Status: "ok"},
		{Name: "_gap", Duration: 10 * time.Millisecond, Status: "ok"},
	}

	steps := ConvertTraceToSteps(trace)
	if len(steps) != 0 {
		t.Errorf("got %d steps, want 0 (all gaps)", len(steps))
	}
}
