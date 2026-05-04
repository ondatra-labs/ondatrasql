// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
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

// Init configures output mode. When jsonFlag is true, human output goes to
// stderr and JSON lines go to stdout. When jsonFlag is false, JSONEnabled is
// reset (so a previous --json invocation in the same process doesn't leak)
// but Human is left alone — tests that have swapped Human to a buffer
// expect it to survive the call.
func Init(jsonFlag bool) {
	JSONEnabled = jsonFlag
	if jsonFlag {
		Human = os.Stderr
	}
}

// Reset returns Human and JSONEnabled to their pristine defaults. Intended
// for test setup; production code should use Init.
func Reset() {
	JSONEnabled = false
	Human = os.Stdout
}

// ModelResultSchemaVersion is the JSON schema version for the
// machine-readable model-run output emitted via `--json run`. Bump
// on breaking shape changes (field rename, type change, OR a key
// going from omitempty → always-emit since clients that previously
// branched on key presence will now always see the key).
// Adding a new optional field with omitempty is a non-breaking
// change and does NOT require a bump.
//
// Version history:
//   - 1: initial release with snake_case field names
//   - 2: errors and warnings always emitted as `[]` instead of being
//        omitempty (R10 #1) — clients that branched on key presence
//        no longer see absent keys for clean runs.
const ModelResultSchemaVersion = 2

// ModelResult is the JSON structure emitted after each model run.
//
// Always-emitted fields (clients can decode unconditionally without
// branching on key presence): schema_version, model, kind, run_type,
// rows_affected, duration_ms, status, errors, warnings.
//
// Optional (omitempty): run_reason, dag_run_id, sandbox.
type ModelResult struct {
	// SchemaVersion lets typed clients detect breaking changes.
	// Always emitted, never omitempty.
	SchemaVersion int    `json:"schema_version"`
	Model         string `json:"model"`
	Kind          string `json:"kind"`
	RunType       string `json:"run_type"`
	RunReason     string `json:"run_reason,omitempty"`
	RowsAffected  int64  `json:"rows_affected"`
	DurationMs    int64  `json:"duration_ms"`
	Status        string `json:"status"`
	// Errors and Warnings are ALWAYS emitted (`[]` when empty) for
	// shape parity with history/stats/query/sql --json envelopes.
	// Pre-R10 these had `omitempty`, so a clean run produced an
	// envelope without these keys while a failing run produced one
	// with them — typed clients had to branch on key presence to
	// decode safely. (R10 #1.) Callers MUST initialise the slices
	// non-nil before encoding so a clean run emits `[]` rather than
	// `null` (a nil slice marshals to `null` in encoding/json).
	Errors   []string `json:"errors"`
	Warnings []string `json:"warnings"`
	DagRunID string   `json:"dag_run_id,omitempty"`
	Sandbox  bool     `json:"sandbox,omitempty"`
}

// EmitJSON writes a value as a single JSON line to stdout.
// Only emits when --json mode is active.
func EmitJSON(v any) {
	if !JSONEnabled {
		return
	}
	if err := json.NewEncoder(os.Stdout).Encode(v); err != nil {
		fmt.Fprintf(os.Stderr, "WARNING: JSON encode error: %v\n", err)
	}
}

// Fprintf writes formatted output to the Human writer. Write errors are
// intentionally dropped: Human is the user-facing terminal/log destination,
// and if the write fails (broken pipe, closed stderr) there is no
// alternative channel to report it on. Callers should use os.Stdout
// directly when the write outcome must be checked.
func Fprintf(format string, a ...any) {
	_, _ = fmt.Fprintf(Human, format, a...)
}

// Println writes a line to the Human writer. Same write-error semantics
// as Fprintf — see that function's comment for the rationale.
func Println(a ...any) {
	_, _ = fmt.Fprintln(Human, a...)
}
