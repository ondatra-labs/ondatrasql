// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package sql

import (
	"strings"
	"testing"
)

func TestLoadCommitSQL(t *testing.T) {
	content, err := Load("execute/commit.sql")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if strings.Contains(content, "{{catalog}}") {
		t.Error("{{catalog}} placeholder should not appear in native SQL")
	}
	// set_commit_message resolves via USE — no catalog prefix needed
	if !strings.Contains(content, "CALL set_commit_message") {
		t.Error("expected bare set_commit_message call")
	}
}

func TestSetCatalogAliasIgnoresEmpty(t *testing.T) {
	SetCatalogAlias("custom")
	SetCatalogAlias("")
	defer SetCatalogAlias("lake")

	// Verify empty string didn't override
	catalogMu.RLock()
	alias := catalogAlias
	catalogMu.RUnlock()
	if alias != "custom" {
		t.Errorf("empty string should not override alias, got %q", alias)
	}
}

func TestMustFormatAppliesArgs(t *testing.T) {
	SetCatalogAlias("lake")

	// commit.sql now takes four substitution slots:
	//   1. main SQL, 2. pre-commit checks (audit error() wrapper or empty),
	//   3. model target, 4. extra info JSON.
	result := MustFormat("execute/commit.sql", "INSERT INTO t VALUES (1)", "", "schema.table", "{}")
	if !strings.Contains(result, "INSERT INTO t VALUES (1)") {
		t.Error("SQL not interpolated")
	}
	if !strings.Contains(result, "schema.table") {
		t.Error("target not interpolated")
	}
}

func TestLoadQueryHistory(t *testing.T) {
	content, err := LoadQuery("history")
	if err != nil {
		t.Fatalf("LoadQuery: %v", err)
	}

	if strings.Contains(content, "{{catalog}}") {
		t.Error("{{catalog}} placeholder should not appear in native SQL")
	}
	// snapshots() resolves via USE — no catalog prefix needed
	if !strings.Contains(content, "FROM snapshots()") {
		t.Error("expected bare snapshots() call")
	}
}

func TestLoadQueryStatsBasic(t *testing.T) {
	content, err := LoadQuery("stats_basic")
	if err != nil {
		t.Fatalf("LoadQuery: %v", err)
	}
	if strings.Contains(content, "{{catalog}}") {
		t.Error("{{catalog}} placeholder should not appear in native SQL")
	}
	if !strings.Contains(content, "FROM snapshots()") {
		t.Error("expected bare snapshots() call")
	}
	if !strings.Contains(content, "COUNT(DISTINCT") {
		t.Error("expected COUNT(DISTINCT in query")
	}
}

func TestLoadQueryStatsKindBreakdown(t *testing.T) {
	content, err := LoadQuery("stats_kind_breakdown")
	if err != nil {
		t.Fatalf("LoadQuery: %v", err)
	}
	if strings.Contains(content, "{{catalog}}") {
		t.Error("{{catalog}} placeholder should not appear in native SQL")
	}
	if !strings.Contains(content, "GROUP BY") {
		t.Error("expected GROUP BY in query")
	}
}

func TestLoadQueryStatsAllModels(t *testing.T) {
	content, err := LoadQuery("stats_all_models")
	if err != nil {
		t.Fatalf("LoadQuery: %v", err)
	}
	if strings.Contains(content, "{{catalog}}") {
		t.Error("{{catalog}} placeholder should not appear in native SQL")
	}
	if !strings.Contains(content, "ROW_NUMBER()") {
		t.Error("expected ROW_NUMBER() in query")
	}
	// Regression: PARTITION BY must lowercase the model name so case-variant
	// commits dedup to a single "latest" row, matching the case-insensitive
	// lookup story used by GetModelCommitInfo, ondatra_get_downstream, etc.
	if !strings.Contains(content, "PARTITION BY LOWER(commit_extra_info->>'model')") {
		t.Error("expected PARTITION BY LOWER(...) for case-insensitive dedup")
	}
}

// Regression: stats_basic and stats_kind_breakdown count distinct models
// using LOWER() so case-variant commits don't double-count.
func TestLoadQueryStats_CaseInsensitiveModelCount(t *testing.T) {
	SetCatalogAlias("lake")

	for _, name := range []string{"stats_basic", "stats_kind_breakdown"} {
		content, err := LoadQuery(name)
		if err != nil {
			t.Fatalf("LoadQuery(%q): %v", name, err)
		}
		if !strings.Contains(content, "COUNT(DISTINCT LOWER(commit_extra_info->>'model'))") {
			t.Errorf("%s: expected COUNT(DISTINCT LOWER(model)) for case-insensitive dedup", name)
		}
	}
}

func TestLoadQueryLineageAllModels(t *testing.T) {
	content, err := LoadQuery("lineage_all_models")
	if err != nil {
		t.Fatalf("LoadQuery: %v", err)
	}
	if strings.Contains(content, "{{catalog}}") {
		t.Error("{{catalog}} placeholder should not appear in native SQL")
	}
	// The query now uses ROW_NUMBER() OVER (PARTITION BY LOWER(...)) instead
	// of SELECT DISTINCT to dedup case-variant model names while keeping the
	// original-case display value from the most recent commit.
	if !strings.Contains(content, "ROW_NUMBER") {
		t.Error("expected ROW_NUMBER dedup in query")
	}
	if !strings.Contains(content, "LOWER(") {
		t.Error("expected LOWER() for case-insensitive dedup")
	}
}

func TestLoadQueryWithNoArgs(t *testing.T) {
	SetCatalogAlias("lake")

	// LoadQuery with no args should still work
	content, err := LoadQuery("history")
	if err != nil {
		t.Fatalf("LoadQuery: %v", err)
	}
	if content == "" {
		t.Error("expected non-empty content")
	}
}

func TestLoadQueryNonexistent(t *testing.T) {
	_, err := LoadQuery("nonexistent_query")
	if err == nil {
		t.Error("expected error for nonexistent query")
	}
}

func TestMustFormatPanicsOnBadPath(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for nonexistent file")
		}
	}()
	MustFormat("nonexistent/file.sql")
}

func TestLoadNonexistent(t *testing.T) {
	_, err := Load("nonexistent/file.sql")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestLoadQueryWithArgSubstitution(t *testing.T) {
	// Create a simple query file for testing arg substitution
	// We use history which has no $N placeholders, then verify args don't break it
	SetCatalogAlias("lake")

	// LoadQuery with args but no placeholders should return unchanged content
	content, err := LoadQuery("history", "unused_arg")
	if err != nil {
		t.Fatalf("LoadQuery: %v", err)
	}
	if content == "" {
		t.Error("expected non-empty content")
	}
}

func TestLoadQueryArgEscaping(t *testing.T) {
	// Verify that the arg escaping logic works even if template has no placeholders.
	// The function replaces $1, $2, etc. with escaped values.
	SetCatalogAlias("lake")

	// Test with multiple args — no error expected
	_, err := LoadQuery("history", "arg with 'quotes'", "another; arg")
	if err != nil {
		t.Fatalf("LoadQuery with special args: %v", err)
	}
}
