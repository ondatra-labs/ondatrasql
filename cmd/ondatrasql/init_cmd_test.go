// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"strings"
	"testing"
)

func TestInitTemplates_NonEmpty(t *testing.T) {
	t.Parallel()
	// Every init template function should return non-empty content
	templates := map[string]func() string{
		"initDotEnv":     initDotEnv,
		"initGitignore":  initGitignore,
		"initCatalog":    initCatalog,
		"initExtensions": initExtensions,
		"initMacros":     initMacros,
		"initVariables":  initVariables,
		"initSources":    initSources,
		"initSecrets":    initSecrets,
		"initSettings":   initSettings,
		"initMerge":      initMerge,
		"initExpire":     initExpire,
		"initCleanup":    initCleanup,
		"initOrphaned":   initOrphaned,
	}

	for name, fn := range templates {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			content := fn()
			if content == "" {
				t.Errorf("%s() returned empty string", name)
			}
		})
	}
}

func TestInitTemplatesWithName(t *testing.T) {
	t.Parallel()
	name := "my_project"

	readme := initReadmeMD(name)
	if !strings.Contains(readme, name) {
		t.Error("initReadmeMD should contain project name")
	}
}

func TestInitCatalog_ContainsDuckLake(t *testing.T) {
	t.Parallel()
	content := initCatalog()
	if !strings.Contains(content, "ducklake") {
		t.Error("catalog template should reference ducklake")
	}
	if !strings.Contains(content, "ATTACH") {
		t.Error("catalog template should contain ATTACH statement")
	}
}

func TestInitMerge_ContainsCall(t *testing.T) {
	t.Parallel()
	content := initMerge()
	if !strings.Contains(content, "CALL") {
		t.Error("merge template should contain CALL statement")
	}
	if !strings.Contains(content, "ducklake_merge_adjacent_files") {
		t.Error("merge template should reference ducklake_merge_adjacent_files")
	}
}

func TestInitGitignore_ContainsEnv(t *testing.T) {
	t.Parallel()
	content := initGitignore()
	if !strings.Contains(content, ".env") {
		t.Error("gitignore should exclude .env")
	}
	if !strings.Contains(content, "secrets.sql") {
		t.Error("gitignore should exclude secrets.sql")
	}
}
