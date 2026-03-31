// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package output

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
)

func TestInit_JSONDisabled(t *testing.T) {
	oldJSON := JSONEnabled
	oldHuman := Human
	defer func() {
		JSONEnabled = oldJSON
		Human = oldHuman
	}()

	JSONEnabled = false
	Human = os.Stdout

	Init(false)

	if JSONEnabled {
		t.Error("JSONEnabled should be false")
	}
	if Human != os.Stdout {
		t.Error("Human should remain os.Stdout")
	}
}

func TestInit_JSONEnabled(t *testing.T) {
	// Reset state after test
	defer func() {
		JSONEnabled = false
		Human = os.Stdout
	}()

	Init(true)

	if !JSONEnabled {
		t.Error("JSONEnabled should be true")
	}
	if Human != os.Stderr {
		t.Error("Human should be os.Stderr when JSON is enabled")
	}
}

func TestFprintf(t *testing.T) {
	var buf bytes.Buffer
	old := Human
	Human = &buf
	defer func() { Human = old }()

	Fprintf("hello %s %d", "world", 42)

	got := buf.String()
	if got != "hello world 42" {
		t.Errorf("got %q, want %q", got, "hello world 42")
	}
}

func TestPrintln(t *testing.T) {
	var buf bytes.Buffer
	old := Human
	Human = &buf
	defer func() { Human = old }()

	Println("hello", "world")

	got := buf.String()
	if got != "hello world\n" {
		t.Errorf("got %q, want %q", got, "hello world\n")
	}
}

func TestEmitJSON_Disabled(t *testing.T) {
	old := JSONEnabled
	oldStdout := os.Stdout
	defer func() {
		JSONEnabled = old
		os.Stdout = oldStdout
	}()

	JSONEnabled = false

	// Capture stdout to verify nothing is written
	r, w, _ := os.Pipe()
	defer r.Close()
	os.Stdout = w

	EmitJSON(map[string]string{"key": "value"})

	w.Close()
	out, _ := io.ReadAll(r)
	if len(out) != 0 {
		t.Errorf("EmitJSON with JSONEnabled=false should produce no output, got %q", string(out))
	}
}

func TestEmitJSON_Enabled(t *testing.T) {
	old := JSONEnabled
	oldStdout := os.Stdout
	defer func() {
		JSONEnabled = old
		os.Stdout = oldStdout
	}()

	// Capture stdout
	r, w, _ := os.Pipe()
	defer r.Close()
	os.Stdout = w
	JSONEnabled = true

	EmitJSON(map[string]string{"key": "value"})

	w.Close()
	out, _ := io.ReadAll(r)

	var result map[string]string
	if err := json.Unmarshal(out, &result); err != nil {
		t.Fatalf("invalid JSON output: %v", err)
	}
	if result["key"] != "value" {
		t.Errorf("got key=%q, want %q", result["key"], "value")
	}
}

func TestModelResult_JSON(t *testing.T) {
	t.Parallel()
	mr := ModelResult{
		Model:        "staging.orders",
		Kind:         "table",
		RunType:      "backfill",
		RowsAffected: 100,
		DurationMs:   250,
		Status:       "ok",
		Errors:       []string{"err1"},
		Warnings:     []string{"warn1"},
		DagRunID:     "run-123",
		Sandbox:      true,
	}

	data, err := json.Marshal(mr)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	s := string(data)
	for _, want := range []string{
		`"model":"staging.orders"`,
		`"kind":"table"`,
		`"run_type":"backfill"`,
		`"rows_affected":100`,
		`"duration_ms":250`,
		`"status":"ok"`,
		`"errors":["err1"]`,
		`"warnings":["warn1"]`,
		`"dag_run_id":"run-123"`,
		`"sandbox":true`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("JSON missing %s in %s", want, s)
		}
	}
}

func TestModelResult_StatusError(t *testing.T) {
	t.Parallel()
	mr := ModelResult{
		Model:  "staging.orders",
		Kind:   "table",
		Status: "error",
		Errors: []string{"NOT NULL failed: id has 1 NULL values"},
	}

	data, err := json.Marshal(mr)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var result map[string]json.RawMessage
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Verify status field
	var status string
	json.Unmarshal(result["status"], &status)
	if status != "error" {
		t.Errorf("status = %q, want error", status)
	}

	// Verify errors array present and non-empty
	var errors []string
	json.Unmarshal(result["errors"], &errors)
	if len(errors) == 0 {
		t.Error("errors should be present when status is error")
	}
	if !strings.Contains(errors[0], "NOT NULL failed") {
		t.Errorf("error message = %q, want descriptive constraint failure", errors[0])
	}
}

func TestModelResult_StatusOk_NoErrors(t *testing.T) {
	t.Parallel()
	mr := ModelResult{
		Model:        "staging.orders",
		Kind:         "table",
		Status:       "ok",
		RowsAffected: 100,
	}

	data, err := json.Marshal(mr)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	s := string(data)
	if !strings.Contains(s, `"status":"ok"`) {
		t.Errorf("expected status ok, got: %s", s)
	}
	if strings.Contains(s, `"errors"`) {
		t.Error("errors should be omitted when status is ok")
	}
}

func TestModelResult_JSON_OmitEmpty(t *testing.T) {
	t.Parallel()
	mr := ModelResult{
		Model:  "main.test",
		Kind:   "table",
		Status: "ok",
	}

	data, err := json.Marshal(mr)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	s := string(data)
	if strings.Contains(s, "errors") {
		t.Error("errors should be omitted when empty")
	}
	if strings.Contains(s, "warnings") {
		t.Error("warnings should be omitted when empty")
	}
	if strings.Contains(s, "dag_run_id") {
		t.Error("dag_run_id should be omitted when empty")
	}
	if strings.Contains(s, "sandbox") {
		t.Error("sandbox should be omitted when false")
	}
}
