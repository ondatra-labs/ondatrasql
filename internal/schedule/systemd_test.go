// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package schedule

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSystemdRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	unitDir := t.TempDir() // isolated unit dir — never touches real ~/.config

	binaryPath := "/usr/bin/true"
	projectName := "ondatrasql-test"

	// unitDirOverride disables systemctl calls and redirects file writes.
	b := &systemdBackend{unitDirOverride: unitDir}

	// Install
	unit, err := b.Install(projectName, tmpDir, "*/5 * * * *", binaryPath)
	if err != nil {
		t.Fatalf("install: %v", err)
	}
	if unit == "" {
		t.Fatal("expected unit name, got empty")
	}

	// Verify both files exist with correct content
	timerPath := filepath.Join(unitDir, unit+".timer")
	servicePath := filepath.Join(unitDir, unit+".service")

	timerData, err := os.ReadFile(timerPath)
	if err != nil {
		t.Fatalf("timer file missing: %v", err)
	}
	if !strings.Contains(string(timerData), "OnCalendar=*-*-* *:0/5:00") {
		t.Errorf("timer missing OnCalendar:\n%s", timerData)
	}
	if !strings.Contains(string(timerData), "(*/5 * * * *)") {
		t.Errorf("timer missing cron in description:\n%s", timerData)
	}

	serviceData, err := os.ReadFile(servicePath)
	if err != nil {
		t.Fatalf("service file missing: %v", err)
	}
	if !strings.Contains(string(serviceData), `ExecStart="/usr/bin/true" run`) {
		t.Errorf("service missing ExecStart:\n%s", serviceData)
	}
	if !strings.Contains(string(serviceData), "WorkingDirectory="+tmpDir) {
		t.Errorf("service missing WorkingDirectory:\n%s", serviceData)
	}

	// Status — must round-trip cron from timer file
	st, err := b.Status(projectName)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if !st.Installed {
		t.Error("status: not installed")
	}
	if st.Cron != "*/5 * * * *" {
		t.Errorf("status cron = %q, want %q", st.Cron, "*/5 * * * *")
	}
	if st.Backend != "systemd" {
		t.Errorf("backend = %q", st.Backend)
	}

	// Remove
	if err := b.Remove(projectName); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, err := os.Stat(timerPath); !os.IsNotExist(err) {
		t.Error("timer file still exists after remove")
	}
	if _, err := os.Stat(servicePath); !os.IsNotExist(err) {
		t.Error("service file still exists after remove")
	}

	// Status after remove
	st, err = b.Status(projectName)
	if err != nil {
		t.Fatalf("status after remove: %v", err)
	}
	if st.Installed {
		t.Error("status: still installed after remove")
	}
}
