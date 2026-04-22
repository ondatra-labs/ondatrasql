// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package output

import (
	"bytes"
	"os"
	"testing"
)

// --- EmitJSON: encoding errors logged instead of swallowed ---

func TestEmitJSON_EncodingError(t *testing.T) {
	// Before fix: encoding errors were silently swallowed.
	// After fix: errors are logged to stderr.

	// Save and restore state
	oldJSON := JSONEnabled
	oldHuman := Human
	oldStdout := os.Stdout
	oldStderr := os.Stderr
	defer func() {
		JSONEnabled = oldJSON
		Human = oldHuman
		os.Stdout = oldStdout
		os.Stderr = oldStderr
	}()

	JSONEnabled = true
	Human = &bytes.Buffer{}

	// Redirect stdout (JSON output) to a pipe
	rOut, wOut, _ := os.Pipe()
	os.Stdout = wOut

	// Redirect stderr to capture warning
	rErr, wErr, _ := os.Pipe()
	os.Stderr = wErr

	// Channels are not JSON-serializable — should trigger an error
	ch := make(chan int)
	EmitJSON(ch)

	wOut.Close()
	wErr.Close()

	var bufOut, bufErr bytes.Buffer
	bufOut.ReadFrom(rOut)
	bufErr.ReadFrom(rErr)

	// stderr should contain the warning
	if bufErr.Len() == 0 {
		t.Error("EmitJSON should log encoding error to stderr")
	}
}

func TestEmitJSON_ValidData(t *testing.T) {
	// Valid data should still work
	oldJSON := JSONEnabled
	oldStdout := os.Stdout
	defer func() {
		JSONEnabled = oldJSON
		os.Stdout = oldStdout
	}()

	JSONEnabled = true

	r, w, _ := os.Pipe()
	os.Stdout = w

	EmitJSON(map[string]int{"count": 42})

	w.Close()
	var buf bytes.Buffer
	buf.ReadFrom(r)

	if buf.Len() == 0 {
		t.Error("EmitJSON should produce output for valid data")
	}
}
