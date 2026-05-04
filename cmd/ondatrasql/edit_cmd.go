// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/config"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/backfill"
)

// runEdit opens the model source file in $EDITOR (default: vim).
func runEdit(cfg *config.Config, target string) error {
	var sourceFile string

	// Handle special targets
	switch {
	case target == "env" || target == ".env":
		sourceFile = ".env"
	case strings.HasPrefix(target, "macros/") || strings.HasPrefix(target, "variables/"):
		// Dynamic: macros/<name> and variables/<name> open config/<target>.sql
		// Validate: name part must not contain path separators or ".."
		name := target[strings.Index(target, "/")+1:]
		if name == "" || strings.Contains(name, "/") || strings.Contains(name, "\\") || strings.Contains(name, "..") {
			return fmt.Errorf("invalid target: %q", target)
		}
		fullPath := filepath.Join(cfg.ConfigPath, target+".sql")
		if _, err := os.Stat(fullPath); err != nil {
			// Show relative path in error for readability
			rel, _ := filepath.Rel(cfg.ProjectDir, fullPath)
			if rel == "" {
				rel = fullPath
			}
			return fmt.Errorf("file not found: %s", rel)
		}
		// Store relative path for editor (runEdit joins with ProjectDir later)
		sourceFile, _ = filepath.Rel(cfg.ProjectDir, fullPath)
		if sourceFile == "" {
			sourceFile = fullPath
		}
	case target == "sources" || target == "sources.sql":
		sourceFile = filepath.Join("config", "sources.sql")
	case target == "catalog" || target == "catalog.sql":
		sourceFile = filepath.Join("config", "catalog.sql")
	case target == "extensions" || target == "extensions.sql":
		sourceFile = filepath.Join("config", "extensions.sql")
	case target == "secrets" || target == "secrets.sql":
		sourceFile = filepath.Join("config", "secrets.sql")
	case target == "settings" || target == "settings.sql":
		sourceFile = filepath.Join("config", "settings.sql")
	default:
		// Reject path traversal attempts
		if strings.Contains(target, "..") || strings.ContainsAny(target, `/\`) {
			return fmt.Errorf("invalid target: %q", target)
		}
		// Find the source file for the model
		var err error
		sourceFile, err = findModelSourceFile(cfg, target)
		if err != nil {
			return err
		}
	}

	// Get editor from environment, try common editors as fallback
	editor := os.Getenv("EDITOR")
	if editor == "" {
		// Try common editors in order of preference
		editors := []string{"nano", "vi", "vim", "code", "gedit"}
		for _, e := range editors {
			if _, err := exec.LookPath(e); err == nil {
				editor = e
				break
			}
		}
		if editor == "" {
			return fmt.Errorf("no editor found. Set $EDITOR environment variable")
		}
	}

	// Validate editor is a real executable
	editorPath, err := exec.LookPath(editor)
	if err != nil {
		return fmt.Errorf("editor %q not found in PATH", editor)
	}

	// Open the file in editor
	fullPath := filepath.Join(cfg.ProjectDir, sourceFile)
	cmd := exec.Command(editorPath, fullPath)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

// findModelSourceFile finds the source file for a model target.
func findModelSourceFile(cfg *config.Config, target string) (string, error) {
	// First try to get from DuckLake metadata
	sess, err := duckdb.NewSession("")
	if err == nil {
		defer closeSessionOrLog(sess)

		if err := sess.InitWithCatalog(cfg.ConfigPath); err == nil {
			commitInfo, err := backfill.GetModelCommitInfo(sess, target)
			if err == nil && commitInfo != nil && commitInfo.SourceFile != "" {
				return commitInfo.SourceFile, nil
			}
		}
	}

	// Fallback: search in models directory
	// Try common patterns: schema/name.sql
	parts := splitTarget(target)
	if len(parts) == 2 {
		patterns := []string{
			filepath.Join(cfg.ModelsPath, parts[0], parts[1]+".sql"),
			filepath.Join(cfg.ModelsPath, parts[0]+"."+parts[1]+".sql"),
		}
		for _, pattern := range patterns {
			relPath, _ := filepath.Rel(cfg.ProjectDir, pattern)
			if _, err := os.Stat(pattern); err == nil {
				return relPath, nil
			}
		}
	}

	return "", fmt.Errorf("source file for model %q not found", target)
}

// splitTarget splits "schema.table" or "schema.table.sql" into ["schema", "table"].
// Known extensions (.sql) are stripped before splitting.
func splitTarget(target string) []string {
	lower := strings.ToLower(target)
	for _, ext := range []string{".sql"} {
		if strings.HasSuffix(lower, ext) {
			target = target[:len(target)-len(ext)]
			break
		}
	}
	for i := len(target) - 1; i >= 0; i-- {
		if target[i] == '.' {
			return []string{target[:i], target[i+1:]}
		}
	}
	return []string{target}
}
