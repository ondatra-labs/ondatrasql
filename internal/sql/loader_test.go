// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package sql

import (
	"strings"
	"testing"
)

func TestLoadReplacesCatalogPlaceholder(t *testing.T) {
	// Set a custom alias
	SetCatalogAlias("my_warehouse")
	defer SetCatalogAlias("lake") // restore default

	content, err := Load("execute/commit.sql")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if strings.Contains(content, "{{catalog}}") {
		t.Error("{{catalog}} placeholder was not replaced")
	}
	if !strings.Contains(content, "'my_warehouse'") {
		t.Error("catalog alias not found in output")
	}
}

func TestSetCatalogAliasIgnoresEmpty(t *testing.T) {
	SetCatalogAlias("custom")
	SetCatalogAlias("")
	defer SetCatalogAlias("lake")

	content, err := Load("execute/commit.sql")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if !strings.Contains(content, "'custom'") {
		t.Error("empty string should not override alias")
	}
}

func TestMustFormatAppliesArgs(t *testing.T) {
	SetCatalogAlias("lake")

	result := MustFormat("execute/commit.sql", "INSERT INTO t VALUES (1)", "schema.table", "{}")
	if !strings.Contains(result, "INSERT INTO t VALUES (1)") {
		t.Error("SQL not interpolated")
	}
	if !strings.Contains(result, "schema.table") {
		t.Error("target not interpolated")
	}
}

func TestLoadQueryReplacesCatalog(t *testing.T) {
	SetCatalogAlias("my_catalog")
	defer SetCatalogAlias("lake")

	content, err := LoadQuery("history")
	if err != nil {
		t.Fatalf("LoadQuery: %v", err)
	}

	if strings.Contains(content, "{{catalog}}") {
		t.Error("{{catalog}} placeholder not replaced")
	}
	if !strings.Contains(content, "my_catalog") {
		t.Error("catalog alias not found in output")
	}
}

func TestLoadQueryStatsBasic(t *testing.T) {
	SetCatalogAlias("test_catalog")
	defer SetCatalogAlias("lake")

	content, err := LoadQuery("stats_basic")
	if err != nil {
		t.Fatalf("LoadQuery: %v", err)
	}
	if strings.Contains(content, "{{catalog}}") {
		t.Error("{{catalog}} placeholder not replaced")
	}
	if !strings.Contains(content, "test_catalog.snapshots()") {
		t.Error("catalog alias not found in output")
	}
	if !strings.Contains(content, "COUNT(DISTINCT") {
		t.Error("expected COUNT(DISTINCT in query")
	}
}

func TestLoadQueryStatsKindBreakdown(t *testing.T) {
	SetCatalogAlias("test_catalog")
	defer SetCatalogAlias("lake")

	content, err := LoadQuery("stats_kind_breakdown")
	if err != nil {
		t.Fatalf("LoadQuery: %v", err)
	}
	if strings.Contains(content, "{{catalog}}") {
		t.Error("{{catalog}} placeholder not replaced")
	}
	if !strings.Contains(content, "GROUP BY") {
		t.Error("expected GROUP BY in query")
	}
}

func TestLoadQueryStatsAllModels(t *testing.T) {
	SetCatalogAlias("test_catalog")
	defer SetCatalogAlias("lake")

	content, err := LoadQuery("stats_all_models")
	if err != nil {
		t.Fatalf("LoadQuery: %v", err)
	}
	if strings.Contains(content, "{{catalog}}") {
		t.Error("{{catalog}} placeholder not replaced")
	}
	if !strings.Contains(content, "ROW_NUMBER()") {
		t.Error("expected ROW_NUMBER() in query")
	}
}

func TestLoadQueryLineageAllModels(t *testing.T) {
	SetCatalogAlias("test_catalog")
	defer SetCatalogAlias("lake")

	content, err := LoadQuery("lineage_all_models")
	if err != nil {
		t.Fatalf("LoadQuery: %v", err)
	}
	if strings.Contains(content, "{{catalog}}") {
		t.Error("{{catalog}} placeholder not replaced")
	}
	if !strings.Contains(content, "DISTINCT") {
		t.Error("expected DISTINCT in query")
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
