// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/backfill"
)

// TraceStep represents a single step in model execution with timing.
type TraceStep struct {
	Name     string        `json:"name"`
	Duration time.Duration `json:"duration"`
	Status   string        `json:"status"` // "ok", "skip", "error"
}

// trace adds a trace step and records any gap since the previous step.
func (r *Runner) trace(result *Result, name string, start time.Time, status string) {
	now := time.Now()
	duration := now.Sub(start)

	// Add gap if there was time between last trace end and this step's start
	if !result.lastTraceEnd.IsZero() {
		gap := start.Sub(result.lastTraceEnd)
		if gap > 100*time.Microsecond { // Only show gaps > 0.1ms
			result.Trace = append(result.Trace, TraceStep{
				Name:     "_gap",
				Duration: gap,
				Status:   "ok",
			})
		}
	}

	result.Trace = append(result.Trace, TraceStep{
		Name:     name,
		Duration: duration,
		Status:   status,
	})
	result.lastTraceEnd = now
}

// ConvertTraceToSteps converts TraceStep slice to StepInfo slice for commit metadata.
func ConvertTraceToSteps(trace []TraceStep) []backfill.StepInfo {
	var steps []backfill.StepInfo

	for _, t := range trace {
		// Skip gap markers
		if t.Name == "_gap" {
			continue
		}

		steps = append(steps, backfill.StepInfo{
			Name:       t.Name,
			DurationMs: t.Duration.Milliseconds(),
			Status:     t.Status,
		})
	}

	return steps
}
