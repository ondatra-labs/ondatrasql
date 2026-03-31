// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/execute"
	"github.com/ondatra-labs/ondatrasql/internal/output"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
)

func TestUnique(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input []string
		want  []string
	}{
		{"empty", nil, nil},
		{"no dupes", []string{"a", "b", "c"}, []string{"a", "b", "c"}},
		{"with dupes", []string{"a", "b", "a", "c", "b"}, []string{"a", "b", "c"}},
		{"all same", []string{"x", "x", "x"}, []string{"x"}},
		{"single", []string{"a"}, []string{"a"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := unique(tt.input)
			if len(got) != len(tt.want) {
				t.Errorf("unique(%v) = %v, want %v", tt.input, got, tt.want)
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("unique(%v)[%d] = %q, want %q", tt.input, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestTruncateStr(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input  string
		maxLen int
		want   string
	}{
		{"hello", 10, "hello"},
		{"hello world", 5, "he..."},
		{"", 5, ""},
		{"ab", 2, "ab"},
		{"abc", 3, "abc"},
		{"abcd", 3, "..."},
		{"日本語テスト", 4, "日..."},
	}
	for _, tt := range tests {
		got := truncateStr(tt.input, tt.maxLen)
		if got != tt.want {
			t.Errorf("truncateStr(%q, %d) = %q, want %q", tt.input, tt.maxLen, got, tt.want)
		}
	}
}

func TestPrintResult_NilIsNoop(t *testing.T) {
	// printResult(nil) should not panic
	printResult(nil)
}

func TestPrintResult_OK(t *testing.T) {
	// Capture output
	old := output.Human
	defer func() { output.Human = old }()

	var buf strings.Builder
	output.Human = &buf

	r := &execute.Result{
		Target:       "staging.orders",
		Kind:         "table",
		RunType:      "full",
		RowsAffected: 42,
		Duration:     150 * time.Millisecond,
	}
	printResult(r)

	got := buf.String()
	if !strings.Contains(got, "[OK]") {
		t.Errorf("expected [OK] in output, got: %s", got)
	}
	if !strings.Contains(got, "staging.orders") {
		t.Errorf("expected target in output, got: %s", got)
	}
}

func TestPrintResult_Failed(t *testing.T) {
	old := output.Human
	defer func() { output.Human = old }()

	var buf strings.Builder
	output.Human = &buf

	r := &execute.Result{
		Target:       "staging.orders",
		Kind:         "table",
		RunType:      "full",
		RowsAffected: 0,
		Duration:     50 * time.Millisecond,
		Errors:       []string{"constraint failed: id NOT NULL"},
	}
	printResult(r)

	got := buf.String()
	if !strings.Contains(got, "[FAILED]") {
		t.Errorf("expected [FAILED] in output, got: %s", got)
	}
	if !strings.Contains(got, "ERROR:") {
		t.Errorf("expected ERROR in output, got: %s", got)
	}
}

func TestPrintResult_Warnings(t *testing.T) {
	old := output.Human
	defer func() { output.Human = old }()

	var buf strings.Builder
	output.Human = &buf

	r := &execute.Result{
		Target:       "mart.revenue",
		Kind:         "table",
		RunType:      "incremental",
		RowsAffected: 10,
		Duration:     200 * time.Millisecond,
		Errors:       []string{"constraint failed: amount >= 0"},
		Warnings:     []string{"row_count dropped by 15%", "null ratio high on email"},
	}
	printResult(r)

	got := buf.String()
	if !strings.Contains(got, "[FAILED]") {
		t.Errorf("expected [FAILED] in output, got: %s", got)
	}
	if !strings.Contains(got, "ERROR:") {
		t.Errorf("expected ERROR in output, got: %s", got)
	}
	if !strings.Contains(got, "WARN:") {
		t.Errorf("expected WARN in output, got: %s", got)
	}
	if !strings.Contains(got, "row_count dropped by 15%") {
		t.Errorf("expected first warning in output, got: %s", got)
	}
	if !strings.Contains(got, "null ratio high on email") {
		t.Errorf("expected second warning in output, got: %s", got)
	}
}

func TestPrintResult_WarningsOnly(t *testing.T) {
	old := output.Human
	defer func() { output.Human = old }()

	var buf strings.Builder
	output.Human = &buf

	r := &execute.Result{
		Target:       "staging.users",
		Kind:         "table",
		RunType:      "full",
		RowsAffected: 100,
		Duration:     50 * time.Millisecond,
		Warnings:     []string{"row_count below threshold"},
	}
	printResult(r)

	got := buf.String()
	if !strings.Contains(got, "[OK]") {
		t.Errorf("expected [OK] (no errors), got: %s", got)
	}
	if !strings.Contains(got, "WARN:") {
		t.Errorf("expected WARN in output, got: %s", got)
	}
	if !strings.Contains(got, "row_count below threshold") {
		t.Errorf("expected warning text in output, got: %s", got)
	}
}

func TestEmitModelResultJSON_NilIsNoop(t *testing.T) {
	t.Parallel()
	// Should not panic
	emitModelResultJSON(nil, "run-123", false)
}

func TestEmitModelResultJSON_WithResult(t *testing.T) {
	// Save and restore global state
	oldEnabled := output.JSONEnabled
	defer func() { output.JSONEnabled = oldEnabled }()

	// Enable JSON mode
	output.JSONEnabled = true

	// Capture stdout
	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w

	result := &execute.Result{
		Target:       "staging.orders",
		Kind:         "table",
		RunType:      "incremental",
		RowsAffected: 42,
		Duration:     150 * time.Millisecond,
		Warnings:     []string{"row_count low"},
	}
	emitModelResultJSON(result, "run-abc", true)

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	buf.ReadFrom(r)

	// Parse the JSON output
	var got output.ModelResult
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON output: %v\nraw: %s", err, buf.String())
	}

	if got.Model != "staging.orders" {
		t.Errorf("Model = %q, want %q", got.Model, "staging.orders")
	}
	if got.Kind != "table" {
		t.Errorf("Kind = %q, want %q", got.Kind, "table")
	}
	if got.RunType != "incremental" {
		t.Errorf("RunType = %q, want %q", got.RunType, "incremental")
	}
	if got.RowsAffected != 42 {
		t.Errorf("RowsAffected = %d, want %d", got.RowsAffected, 42)
	}
	if got.DurationMs != 150 {
		t.Errorf("DurationMs = %d, want %d", got.DurationMs, 150)
	}
	if got.Status != "ok" {
		t.Errorf("Status = %q, want %q", got.Status, "ok")
	}
	if got.DagRunID != "run-abc" {
		t.Errorf("DagRunID = %q, want %q", got.DagRunID, "run-abc")
	}
	if !got.Sandbox {
		t.Error("Sandbox = false, want true")
	}
	if len(got.Warnings) != 1 || got.Warnings[0] != "row_count low" {
		t.Errorf("Warnings = %v, want [row_count low]", got.Warnings)
	}
}

func TestEmitModelResultJSON_ErrorStatus(t *testing.T) {
	oldEnabled := output.JSONEnabled
	defer func() { output.JSONEnabled = oldEnabled }()

	output.JSONEnabled = true

	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w

	result := &execute.Result{
		Target:       "raw.events",
		Kind:         "append",
		RunType:      "full",
		RowsAffected: 0,
		Duration:     10 * time.Millisecond,
		Errors:       []string{"constraint failed"},
	}
	emitModelResultJSON(result, "run-xyz", false)

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	buf.ReadFrom(r)

	var got output.ModelResult
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON output: %v\nraw: %s", err, buf.String())
	}

	if got.Status != "error" {
		t.Errorf("Status = %q, want %q", got.Status, "error")
	}
	if len(got.Errors) != 1 {
		t.Errorf("Errors = %v, want 1 error", got.Errors)
	}
	if got.Sandbox {
		t.Error("Sandbox = true, want false")
	}
}

func TestShowValidationStatus_WithConstraints(t *testing.T) {
	old := output.Human
	defer func() { output.Human = old }()

	var buf strings.Builder
	output.Human = &buf

	model := &parser.Model{
		Constraints: []string{"id PRIMARY KEY", "name NOT NULL"},
		Audits:      []string{"row_count >= 0"},
		Warnings:    []string{"row_count >= 10"},
	}

	result := &execute.Result{
		Target:   "staging.orders",
		Warnings: []string{"warning: row_count dropped to 5 rows"},
	}

	showValidationStatus(model, result)

	got := buf.String()
	if !strings.Contains(got, "Constraints: 2 passed") {
		t.Errorf("expected constraints count in output, got: %s", got)
	}
	if !strings.Contains(got, "Audits: 1 passed") {
		t.Errorf("expected audits count in output, got: %s", got)
	}
	if !strings.Contains(got, "1 triggered") {
		t.Errorf("expected warning triggered count in output, got: %s", got)
	}
	if !strings.Contains(got, "row_count dropped") {
		t.Errorf("expected warning detail in output, got: %s", got)
	}
}

func TestShowValidationStatus_NoValidations(t *testing.T) {
	old := output.Human
	defer func() { output.Human = old }()

	var buf strings.Builder
	output.Human = &buf

	model := &parser.Model{}
	result := &execute.Result{}

	showValidationStatus(model, result)

	got := buf.String()
	if got != "" {
		t.Errorf("expected no output for model without validations, got: %s", got)
	}
}

func TestShowValidationStatus_WarningsNotTriggered(t *testing.T) {
	old := output.Human
	defer func() { output.Human = old }()

	var buf strings.Builder
	output.Human = &buf

	model := &parser.Model{
		Warnings: []string{"row_count >= 10"},
	}

	result := &execute.Result{
		Target: "staging.orders",
	}

	showValidationStatus(model, result)

	got := buf.String()
	if !strings.Contains(got, "1 checked, 0 triggered") {
		t.Errorf("expected '1 checked, 0 triggered' in output, got: %s", got)
	}
}
