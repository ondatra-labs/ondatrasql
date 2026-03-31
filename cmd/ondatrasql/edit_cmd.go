// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
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
	switch target {
	case "env", ".env":
		sourceFile = ".env"
	// sql/ folder files
	case "macros", "macros.sql":
		sourceFile = filepath.Join(cfg.ConfigPath, "macros.sql")
	case "variables", "variables.sql":
		sourceFile = filepath.Join(cfg.ConfigPath, "variables.sql")
	case "sources", "sources.sql":
		sourceFile = filepath.Join(cfg.ConfigPath, "sources.sql")
	case "catalog", "catalog.sql":
		sourceFile = filepath.Join(cfg.ConfigPath, "catalog.sql")
	case "extensions", "extensions.sql":
		sourceFile = filepath.Join(cfg.ConfigPath, "extensions.sql")
	case "secrets", "secrets.sql":
		sourceFile = filepath.Join(cfg.ConfigPath, "secrets.sql")
	case "settings", "settings.sql":
		sourceFile = filepath.Join(cfg.ConfigPath, "settings.sql")
	default:
		// Reject path traversal attempts
		for _, p := range strings.Split(target, ".") {
			if p == ".." || strings.ContainsAny(p, `/\`) {
				return fmt.Errorf("invalid target: %q", target)
			}
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
		defer sess.Close()

		if err := sess.InitWithCatalog(cfg.ConfigPath); err == nil {
			commitInfo, err := backfill.GetModelCommitInfo(sess, target)
			if err == nil && commitInfo != nil && commitInfo.SourceFile != "" {
				return commitInfo.SourceFile, nil
			}
		}
	}

	// Fallback: search in models directory
	// Try common patterns: schema/name.sql, schema/name.star
	parts := splitTarget(target)
	if len(parts) == 2 {
		patterns := []string{
			filepath.Join(cfg.ModelsPath, parts[0], parts[1]+".sql"),
			filepath.Join(cfg.ModelsPath, parts[0], parts[1]+".star"),
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

// splitTarget splits "schema.table" into ["schema", "table"].
func splitTarget(target string) []string {
	for i := len(target) - 1; i >= 0; i-- {
		if target[i] == '.' {
			return []string{target[:i], target[i+1:]}
		}
	}
	return []string{target}
}
