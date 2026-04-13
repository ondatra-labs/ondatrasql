// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"testing"

	"go.starlark.net/starlark"

	"github.com/ondatra-labs/ondatrasql/internal/backfill"
)

func TestIncrementalModuleDirect(t *testing.T) {
	t.Parallel()
	state := &backfill.IncrementalState{
		IsBackfill:   false,
		Cursor:       "updated_at",
		LastValue:    "2024-06-01",
		LastRun:      "2024-06-01T12:00:00Z",
		InitialValue: "2024-01-01",
	}

	s := incrementalModule(state)

	isBackfill, _ := s.Attr("is_backfill")
	if isBackfill != starlark.False {
		t.Error("is_backfill should be false")
	}

	cursor, _ := s.Attr("cursor")
	if cs, _ := starlark.AsString(cursor); cs != "updated_at" {
		t.Errorf("cursor = %v", cursor)
	}

	lastValue, _ := s.Attr("last_value")
	if lv, _ := starlark.AsString(lastValue); lv != "2024-06-01" {
		t.Errorf("last_value = %v", lastValue)
	}

	lastRun, _ := s.Attr("last_run")
	if lr, _ := starlark.AsString(lastRun); lr != "2024-06-01T12:00:00Z" {
		t.Errorf("last_run = %v", lastRun)
	}

	initialValue, _ := s.Attr("initial_value")
	if iv, _ := starlark.AsString(initialValue); iv != "2024-01-01" {
		t.Errorf("initial_value = %v", initialValue)
	}
}

func TestIncrementalModuleNilDirect(t *testing.T) {
	t.Parallel()
	s := incrementalModule(nil)

	isBackfill, _ := s.Attr("is_backfill")
	if isBackfill != starlark.True {
		t.Error("is_backfill should be true when nil")
	}

	cursor, _ := s.Attr("cursor")
	if cs, _ := starlark.AsString(cursor); cs != "" {
		t.Errorf("cursor = %v, want empty", cursor)
	}

	lastValue, _ := s.Attr("last_value")
	if lv, _ := starlark.AsString(lastValue); lv != "" {
		t.Errorf("last_value = %v, want empty", lastValue)
	}

	lastRun, _ := s.Attr("last_run")
	if lr, _ := starlark.AsString(lastRun); lr != "" {
		t.Errorf("last_run = %v, want empty", lastRun)
	}

	initialValue, _ := s.Attr("initial_value")
	if iv, _ := starlark.AsString(initialValue); iv != "" {
		t.Errorf("initial_value = %v, want empty", initialValue)
	}
}

func TestIncrementalModuleBackfillTrue(t *testing.T) {
	t.Parallel()
	state := &backfill.IncrementalState{IsBackfill: true}
	s := incrementalModule(state)

	isBackfill, _ := s.Attr("is_backfill")
	if isBackfill != starlark.True {
		t.Error("is_backfill should be true")
	}
}
