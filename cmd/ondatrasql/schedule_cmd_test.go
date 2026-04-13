// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/config"
)

func TestProjectName_Unique(t *testing.T) {
	// Two different projects must produce different names because each gets
	// its own .ondatra/project-id.
	a := projectName(&config.Config{ProjectDir: t.TempDir()})
	b := projectName(&config.Config{ProjectDir: t.TempDir()})
	if a == b {
		t.Errorf("collision between separate temp dirs: %q", a)
	}
}

func TestProjectName_StableAcrossRename(t *testing.T) {
	// Renaming the project directory must NOT change the schedule identity.
	src := t.TempDir()
	idDir := filepath.Join(src, ".ondatra")
	if err := os.MkdirAll(idDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(idDir, "project-id"), []byte("cafebabe\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	a := projectName(&config.Config{ProjectDir: src})

	// Simulate a rename: copy the project-id to a dir with a completely
	// different basename. The schedule identity must remain identical.
	dst := filepath.Join(t.TempDir(), "totally-different-name")
	dstID := filepath.Join(dst, ".ondatra")
	_ = os.MkdirAll(dstID, 0o755)
	_ = os.WriteFile(filepath.Join(dstID, "project-id"), []byte("cafebabe\n"), 0o644)

	b := projectName(&config.Config{ProjectDir: dst})

	if a != b {
		t.Errorf("rename changed schedule identity: %q vs %q", a, b)
	}
}

func TestProjectName_StableAcrossCalls(t *testing.T) {
	dir := t.TempDir()
	a := projectName(&config.Config{ProjectDir: dir})
	b := projectName(&config.Config{ProjectDir: dir})
	if a != b {
		t.Errorf("not deterministic for same project: %q vs %q", a, b)
	}
}


func TestGetOrCreateProjectID_PersistsAcrossCalls(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{ProjectDir: dir}

	id1 := getOrCreateProjectID(cfg)
	id2 := getOrCreateProjectID(cfg)
	if id1 != id2 {
		t.Errorf("id changed between calls: %q vs %q", id1, id2)
	}

	// Verify file was created
	idPath := filepath.Join(dir, ".ondatra", "project-id")
	if _, err := os.Stat(idPath); err != nil {
		t.Errorf("project-id file not created: %v", err)
	}
}

func TestGetOrCreateProjectID_ReadOnlyFallback(t *testing.T) {
// On a read-only filesystem, persistence fails. The fallback must be
	// deterministic so two CLI invocations from the same path get the same id.
	dir := t.TempDir()

	// Make the project directory read-only so .ondatra/ cannot be created
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	cfg := &config.Config{ProjectDir: dir}

	id1 := getOrCreateProjectID(cfg)
	id2 := getOrCreateProjectID(cfg)
	id3 := getOrCreateProjectID(cfg)

	if id1 != id2 || id2 != id3 {
		t.Errorf("read-only fallback not deterministic: %q %q %q", id1, id2, id3)
	}

	// Verify nothing was actually written
	if _, err := os.Stat(filepath.Join(dir, ".ondatra")); err == nil {
		t.Error(".ondatra/ was created on read-only fs (should not be possible)")
	}

	// And projectName must also be stable across calls
	a := projectName(cfg)
	b := projectName(cfg)
	if a != b {
		t.Errorf("projectName not stable on read-only fs: %q vs %q", a, b)
	}
}

func TestGetOrCreateProjectID_ReadOnly_DifferentPaths(t *testing.T) {
// Two different read-only project paths must produce different (but each
	// individually deterministic) ids — the path-hash fallback handles this.
	a := t.TempDir()
	b := t.TempDir()
	if err := os.Chmod(a, 0o555); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(b, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(a, 0o755)
		_ = os.Chmod(b, 0o755)
	})

	idA := getOrCreateProjectID(&config.Config{ProjectDir: a})
	idB := getOrCreateProjectID(&config.Config{ProjectDir: b})
	if idA == idB {
		t.Errorf("collision on read-only fs: %q", idA)
	}
}
