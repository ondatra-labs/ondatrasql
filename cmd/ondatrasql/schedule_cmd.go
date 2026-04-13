// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/config"
	"github.com/ondatra-labs/ondatrasql/internal/output"
	"github.com/ondatra-labs/ondatrasql/internal/schedule"
)

// runSchedule dispatches the `schedule` subcommand:
//
//	ondatrasql schedule "*/5 * * * *"   install
//	ondatrasql schedule                 status
//	ondatrasql schedule remove          remove
func runSchedule(cfg *config.Config, args []string) error {
	if len(args) == 0 {
		return showSchedule(cfg)
	}
	if args[0] == "remove" || args[0] == "uninstall" {
		return removeSchedule(cfg)
	}
	return installSchedule(cfg, args[0])
}

// projectName returns a unique, stable identifier for this project's schedule unit.
//
// The identifier is the 8-char project id from .ondatra/project-id alone
// (no basename prefix). This makes the schedule identity stable across:
//   - Project relocations (mv /work/old /work/new)
//   - Project renames (basename change)
//   - Symlinked checkouts (different absolute paths, same project)
//   - Slug normalization collisions (foo.bar vs foo_bar)
//
// The project-id file should be committed to git so all collaborators share
// the same identity.
func projectName(cfg *config.Config) string {
	return getOrCreateProjectID(cfg)
}

// getOrCreateProjectID returns the project's stable identifier, creating it if missing.
//
// Lookup order:
//  1. Read .ondatra/project-id if it exists (committed to git, follows the project)
//  2. Generate a random id and try to persist it. If persistence succeeds, return it.
//  3. If persistence fails (read-only filesystem etc.), fall back to a deterministic
//     hash of the absolute project path. This guarantees the same id on subsequent
//     CLI invocations from the same path, even though it loses portability.
func getOrCreateProjectID(cfg *config.Config) string {
	idPath := filepath.Join(cfg.ProjectDir, ".ondatra", "project-id")

	if data, err := os.ReadFile(idPath); err == nil {
		id := strings.TrimSpace(string(data))
		if len(id) >= 8 {
			return id[:8]
		}
	}

	// Generate a new random id
	var b [4]byte
	if _, err := rand.Read(b[:]); err == nil {
		id := hex.EncodeToString(b[:])

		// Try to persist
		if err := os.MkdirAll(filepath.Dir(idPath), 0o755); err == nil {
			if err := os.WriteFile(idPath, []byte(id+"\n"), 0o644); err == nil {
				return id
			}
		}
		// Persistence failed → fall through to deterministic fallback so that
		// the next CLI invocation from the same path gets the same id.
	}

	// Deterministic fallback: hash absolute path
	abs, err := filepath.Abs(cfg.ProjectDir)
	if err != nil {
		abs = cfg.ProjectDir
	}
	sum := sha256.Sum256([]byte(abs))
	return hex.EncodeToString(sum[:4])
}

func installSchedule(cfg *config.Config, cronExpr string) error {
	cronExpr, err := schedule.ValidateCron(cronExpr)
	if err != nil {
		return fmt.Errorf("invalid cron expression: %w", err)
	}

	backend, err := schedule.Detect()
	if err != nil {
		return err
	}

	binary, err := os.Executable()
	if err != nil {
		return fmt.Errorf("locate binary: %w", err)
	}

	name := projectName(cfg)
	unit, err := backend.Install(name, cfg.ProjectDir, cronExpr, binary)
	if err != nil {
		return fmt.Errorf("install schedule: %w", err)
	}

	output.Fprintf("Installed schedule for %q\n", name)
	output.Fprintf("  Cron:    %s  (%s)\n", cronExpr, schedule.Describe(cronExpr))
	output.Fprintf("  Backend: %s\n", backend.Name())
	output.Fprintf("  Unit:    %s\n", unit)
	output.Fprintf("\nView status with: ondatrasql schedule\n")
	output.Fprintf("Remove with:      ondatrasql schedule remove\n")

	switch backend.Name() {
	case "systemd":
		output.Fprintf("Logs:             journalctl --user -u %s -f\n", unit)
	case "launchd":
		home, _ := os.UserHomeDir()
		output.Fprintf("Logs:             tail -f %s\n", filepath.Join(home, "Library", "LaunchAgents", unit+".log"))
	}

	return nil
}

func removeSchedule(cfg *config.Config) error {
	backend, err := schedule.Detect()
	if err != nil {
		return err
	}
	name := projectName(cfg)
	if err := backend.Remove(name); err != nil {
		return fmt.Errorf("remove schedule: %w", err)
	}
	output.Fprintf("Removed schedule for %q\n", name)
	return nil
}

func showSchedule(cfg *config.Config) error {
	backend, err := schedule.Detect()
	if err != nil {
		return err
	}
	name := projectName(cfg)
	st, err := backend.Status(name)
	if err != nil {
		return err
	}

	if !st.Installed {
		output.Fprintf("No schedule installed for %q\n", name)
		output.Fprintf("\nInstall with: ondatrasql schedule \"*/5 * * * *\"\n")
		return nil
	}

	output.Fprintf("Schedule for %q\n", name)
	output.Fprintf("  Backend:  %s\n", st.Backend)
	output.Fprintf("  Unit:     %s\n", st.UnitName)
	if st.Cron != "" {
		output.Fprintf("  Cron:     %s  (%s)\n", st.Cron, schedule.Describe(st.Cron))
	}
	if st.Active {
		output.Fprintf("  Status:   active\n")
	} else {
		output.Fprintf("  Status:   inactive\n")
	}
	if st.LastRun != "" {
		output.Fprintf("  Last run: %s\n", st.LastRun)
	}
	if st.NextRun != "" {
		output.Fprintf("  Next run: %s\n", st.NextRun)
	}
	return nil
}
