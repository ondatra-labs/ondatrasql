// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package git provides utilities for extracting Git repository metadata.
package git

import (
	"os/exec"
	"strings"
)

// Info contains Git repository metadata.
type Info struct {
	Commit  string // Current commit SHA (short)
	Branch  string // Current branch name
	RepoURL string // Remote origin URL
	IsDirty bool   // True if there are uncommitted changes
}

// GetInfo extracts Git metadata from the current directory.
// Returns empty Info if not a Git repository or on any error.
func GetInfo(dir string) Info {
	info := Info{}

	// Get current commit SHA (short)
	if out, err := runGit(dir, "rev-parse", "--short", "HEAD"); err == nil {
		info.Commit = out
	}

	// Get current branch name
	if out, err := runGit(dir, "rev-parse", "--abbrev-ref", "HEAD"); err == nil {
		info.Branch = out
	}

	// Get remote origin URL
	if out, err := runGit(dir, "config", "--get", "remote.origin.url"); err == nil {
		info.RepoURL = normalizeGitURL(out)
	}

	// Check if working directory is dirty
	if out, err := runGit(dir, "status", "--porcelain"); err == nil {
		info.IsDirty = out != ""
	}

	return info
}

// runGit executes a git command and returns trimmed output.
func runGit(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// normalizeGitURL converts SSH URLs to HTTPS format for consistency.
// git@github.com:user/repo.git -> https://github.com/user/repo
func normalizeGitURL(url string) string {
	url = strings.TrimSpace(url)

	// Remove .git suffix
	url = strings.TrimSuffix(url, ".git")

	// Convert SSH to HTTPS
	if strings.HasPrefix(url, "git@") {
		// git@github.com:user/repo -> https://github.com/user/repo
		url = strings.Replace(url, ":", "/", 1)
		url = strings.Replace(url, "git@", "https://", 1)
	}

	return url
}
