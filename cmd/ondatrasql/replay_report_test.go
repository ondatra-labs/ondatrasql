// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"bytes"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/httpcassette"
	"github.com/ondatra-labs/ondatrasql/internal/output"
)

func TestReportReplay(t *testing.T) {
	old := output.Human
	defer func() { output.Human = old; output.Reset() }()
	var buf bytes.Buffer
	output.Human = &buf
	output.JSONEnabled = false

	// Off: nothing.
	t.Setenv(httpcassette.EnvDir, "")
	t.Setenv(httpcassette.EnvMode, "")
	httpcassette.Reset()
	t.Cleanup(httpcassette.Reset)
	reportReplay()
	if buf.Len() != 0 {
		t.Fatalf("off: printed %q", buf.String())
	}

	// Replay with no request made: zero, no cassette lines.
	dir := t.TempDir()
	t.Setenv(httpcassette.EnvDir, dir)
	t.Setenv(httpcassette.EnvMode, "replay")
	httpcassette.Reset()
	reportReplay()
	if want := "http replay: 0 requests answered from " + dir + "\n"; buf.String() != want {
		t.Fatalf("replay: printed %q, want %q", buf.String(), want)
	}
}
