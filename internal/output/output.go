// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package output provides structured JSON output support.
// When --json is active, human-readable output goes to stderr
// and JSON lines go to stdout for machine consumption.
package output

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

var (
	// Human is where human-readable output is written.
	// Defaults to stdout; redirected to stderr when --json is active.
	Human io.Writer = os.Stdout

	// JSONEnabled indicates whether JSON output mode is active.
	JSONEnabled bool
)

// Init configures output mode. When jsonFlag is true,
// human output goes to stderr and JSON lines go to stdout.
func Init(jsonFlag bool) {
	if jsonFlag {
		JSONEnabled = true
		Human = os.Stderr
	}
}

// ModelResult is the JSON structure emitted after each model run.
type ModelResult struct {
	Model        string   `json:"model"`
	Kind         string   `json:"kind"`
	RunType      string   `json:"run_type"`
	RunReason    string   `json:"run_reason,omitempty"`
	RowsAffected int64    `json:"rows_affected"`
	DurationMs   int64    `json:"duration_ms"`
	Status       string   `json:"status"`
	Errors       []string `json:"errors,omitempty"`
	Warnings     []string `json:"warnings,omitempty"`
	DagRunID     string   `json:"dag_run_id,omitempty"`
	Sandbox      bool     `json:"sandbox,omitempty"`
}

// EmitJSON writes a value as a single JSON line to stdout.
// Only emits when --json mode is active.
func EmitJSON(v any) {
	if !JSONEnabled {
		return
	}
	json.NewEncoder(os.Stdout).Encode(v)
}

// Fprintf writes formatted output to the Human writer.
func Fprintf(format string, a ...any) {
	fmt.Fprintf(Human, format, a...)
}

// Println writes a line to the Human writer.
func Println(a ...any) {
	fmt.Fprintln(Human, a...)
}
