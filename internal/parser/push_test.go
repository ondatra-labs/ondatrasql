// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package parser

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeSinkModel(t *testing.T, dir, relPath, content string) string {
	t.Helper()
	full := filepath.Join(dir, "models", relPath)
	if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return full
}

func TestParsePushDirective(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	path := writeSinkModel(t, dir, "sync/hubspot.sql", `-- @kind: merge
-- @unique_key: customer_id
-- @push: hubspot_push

SELECT customer_id, email FROM mart.customers
`)

	model, err := ParseModel(path, dir)
	if err != nil {
		t.Fatal(err)
	}
	if model.Push != "hubspot_push" {
		t.Errorf("sink = %q, want %q", model.Push, "hubspot_push")
	}
	if model.Kind != "merge" {
		t.Errorf("kind = %q, want merge", model.Kind)
	}
}

func TestParseSinkDetectDeletesRemoved(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	path := writeSinkModel(t, dir, "sync/mirror.sql", `-- @kind: merge
-- @unique_key: id
-- @push: crm_push
-- @sink_detect_deletes: true

SELECT id, name FROM source
`)

	_, err := ParseModel(path, dir)
	if err == nil {
		t.Fatal("expected error for removed @sink_detect_deletes directive")
	}
	if !strings.Contains(err.Error(), "was removed") {
		t.Errorf("error should mention 'was removed', got: %v", err)
	}
}

func TestParseSinkDeleteThresholdRemoved(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	path := writeSinkModel(t, dir, "sync/safe.sql", `-- @kind: merge
-- @unique_key: id
-- @push: crm_push
-- @sink_delete_threshold: 0.1

SELECT id FROM source
`)

	_, err := ParseModel(path, dir)
	if err == nil {
		t.Fatal("expected error for removed @sink_delete_threshold directive")
	}
	if !strings.Contains(err.Error(), "was removed") {
		t.Errorf("error should mention 'was removed', got: %v", err)
	}
}

func TestParsePushRejectsEventsKind(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	path := writeSinkModel(t, dir, "sync/bad.sql", `-- @kind: events
-- @push: crm_push

SELECT id FROM source
`)

	_, err := ParseModel(path, dir)
	if err == nil {
		t.Fatal("expected error for @sink with events kind")
	}
	if !strings.Contains(err.Error(), "not supported for events") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestParsePushAllowsSCD2Syntax(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	path := writeSinkModel(t, dir, "sync/scd2.sql", `-- @kind: scd2
-- @unique_key: id
-- @push: crm_push

SELECT id FROM source
`)

	// Parser allows scd2 + @sink (syntax is valid).
	// Runtime's validate_sink blocks it later with a clear migration message.
	_, err := ParseModel(path, dir)
	if err != nil {
		t.Fatalf("parser should allow scd2 + @sink syntax (runtime validates later), got: %v", err)
	}
}

