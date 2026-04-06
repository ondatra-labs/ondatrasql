// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package schedule generates OS-native scheduler config (systemd timer,
// launchd plist, Windows Task Scheduler XML) for running ondatrasql on cron.
package schedule

import (
	"fmt"
	"runtime"
)

// Status describes the current scheduler state for a project.
type Status struct {
	Installed bool
	Active    bool
	Cron      string
	NextRun   string
	LastRun   string
	UnitName  string
	Backend   string // "systemd", "launchd", "task-scheduler"
}

// Backend is the OS-native scheduler interface.
type Backend interface {
	// Install creates and activates a schedule for the given project.
	Install(projectName, projectDir, cronExpr, binaryPath string) (string, error)
	// Remove deactivates and removes the schedule.
	Remove(projectName string) error
	// Status returns the current schedule state.
	Status(projectName string) (*Status, error)
	// Name returns the backend name (systemd/launchd/task-scheduler).
	Name() string
}

// Detect returns the appropriate backend for the current OS.
func Detect() (Backend, error) {
	switch runtime.GOOS {
	case "linux":
		return &systemdBackend{}, nil
	case "darwin":
		return &launchdBackend{}, nil
	case "windows":
		return &taskBackend{}, nil
	default:
		return nil, fmt.Errorf("unsupported OS: %s", runtime.GOOS)
	}
}
