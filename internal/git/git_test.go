// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package git

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNormalizeGitURL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "HTTPS URL unchanged",
			input: "https://github.com/user/repo",
			want:  "https://github.com/user/repo",
		},
		{
			name:  "HTTPS URL with .git suffix",
			input: "https://github.com/user/repo.git",
			want:  "https://github.com/user/repo",
		},
		{
			name:  "SSH URL converted to HTTPS",
			input: "git@github.com:user/repo.git",
			want:  "https://github.com/user/repo",
		},
		{
			name:  "SSH URL without .git suffix",
			input: "git@github.com:user/repo",
			want:  "https://github.com/user/repo",
		},
		{
			name:  "GitLab SSH URL",
			input: "git@gitlab.com:user/project.git",
			want:  "https://gitlab.com/user/project",
		},
		{
			name:  "Bitbucket SSH URL",
			input: "git@bitbucket.org:user/repo.git",
			want:  "https://bitbucket.org/user/repo",
		},
		{
			name:  "URL with whitespace",
			input: "  https://github.com/user/repo.git  ",
			want:  "https://github.com/user/repo",
		},
		{
			name:  "Empty string",
			input: "",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := normalizeGitURL(tt.input)
			if got != tt.want {
				t.Errorf("normalizeGitURL(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestGetInfo_EmptyDir(t *testing.T) {
	t.Parallel()
	// Test with non-git directory - should return empty Info
	info := GetInfo(t.TempDir())

	if info.Commit != "" {
		t.Errorf("expected empty Commit, got %q", info.Commit)
	}
	if info.Branch != "" {
		t.Errorf("expected empty Branch, got %q", info.Branch)
	}
	if info.RepoURL != "" {
		t.Errorf("expected empty RepoURL, got %q", info.RepoURL)
	}
}

func TestGetInfo_GitRepo(t *testing.T) {
	t.Parallel()
	// Use a temp dir with git init to create a real git repo
	dir := t.TempDir()

	// Initialize a git repo
	if _, err := runGit(dir, "init"); err != nil {
		t.Skip("git not available")
	}
	if _, err := runGit(dir, "config", "user.email", "test@test.com"); err != nil {
		t.Skip("cannot configure git")
	}
	if _, err := runGit(dir, "config", "user.name", "Test"); err != nil {
		t.Skip("cannot configure git")
	}

	// Create a commit so HEAD exists
	if _, err := runGit(dir, "commit", "--allow-empty", "-m", "initial"); err != nil {
		t.Fatalf("commit: %v", err)
	}

	info := GetInfo(dir)

	if info.Commit == "" {
		t.Error("expected non-empty Commit")
	}
	if info.Branch == "" {
		t.Error("expected non-empty Branch")
	}
	// No remote configured, so RepoURL should be empty
	if info.RepoURL != "" {
		t.Errorf("expected empty RepoURL, got %q", info.RepoURL)
	}
	// Clean repo, so not dirty
	if info.IsDirty {
		t.Error("expected clean repo (not dirty)")
	}
}

func TestGetInfo_WithRemote(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	if _, err := runGit(dir, "init"); err != nil {
		t.Skip("git not available")
	}
	runGit(dir, "config", "user.email", "test@test.com")
	runGit(dir, "config", "user.name", "Test")
	runGit(dir, "commit", "--allow-empty", "-m", "initial")
	runGit(dir, "remote", "add", "origin", "git@github.com:user/repo.git")

	info := GetInfo(dir)
	if info.RepoURL != "https://github.com/user/repo" {
		t.Errorf("RepoURL = %q, want https://github.com/user/repo", info.RepoURL)
	}
}

func TestGetInfo_DirtyRepo(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	if _, err := runGit(dir, "init"); err != nil {
		t.Skip("git not available")
	}
	runGit(dir, "config", "user.email", "test@test.com")
	runGit(dir, "config", "user.name", "Test")
	runGit(dir, "commit", "--allow-empty", "-m", "initial")

	// Create an untracked file to make repo dirty
	if err := os.WriteFile(filepath.Join(dir, "dirty.txt"), []byte("dirty"), 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	info := GetInfo(dir)
	if !info.IsDirty {
		t.Error("expected dirty repo")
	}
}

func TestRunGit_InvalidDir(t *testing.T) {
	t.Parallel()
	_, err := runGit("/nonexistent/path", "status")
	if err == nil {
		t.Error("expected error for invalid directory")
	}
}
