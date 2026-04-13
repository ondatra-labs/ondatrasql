// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package schedule

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCronToLaunchd(t *testing.T) {
	tests := []struct {
		name        string
		cron        string
		wantXML     string // substring to find in calendar XML
		wantSec     int
		wantErr     bool
		errContains string
	}{
		// StartInterval patterns
		{"every minute", "* * * * *", "", 60, false, ""},
		{"every 5 minutes", "*/5 * * * *", "", 300, false, ""},
		{"every 30 minutes", "*/30 * * * *", "", 1800, false, ""},

		// StartCalendarInterval patterns
		{"daily 09:00", "0 9 * * *", "<key>Hour</key>", 0, false, ""},
		{"monthly day 1 at 00:00", "0 0 1 * *", "<key>Day</key>", 0, false, ""},
		{"weekly Sunday", "0 0 * * 0", "<key>Weekday</key>", 0, false, ""},

		// Patterns launchd cannot represent — must be rejected
		{"every 2 hours", "0 */2 * * *", "", 0, true, "step values"},
		{"weekdays", "0 22 * * 1-5", "", 0, true, "ranges"},
		{"comma list", "0 9,17 * * *", "", 0, true, "comma lists"},
		{"hour range", "0 9-17 * * *", "", 0, true, "ranges"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			xml, sec, err := cronToLaunchd(tt.cron)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got xml=%q sec=%d", xml, sec)
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantSec > 0 {
				if sec != tt.wantSec {
					t.Errorf("interval = %d, want %d", sec, tt.wantSec)
				}
				if xml != "" {
					t.Errorf("expected interval mode but got xml = %q", xml)
				}
				return
			}
			if sec != 0 {
				t.Errorf("expected calendar mode but got interval = %d", sec)
			}
			if !strings.Contains(xml, tt.wantXML) {
				t.Errorf("xml does not contain %q: %s", tt.wantXML, xml)
			}
		})
	}
}

func TestLaunchdRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	plistDir := t.TempDir()
	projectName := "ondatrasql-test"

	b := &launchdBackend{plistDirOverride: plistDir}

	// Install with interval pattern
	label, err := b.Install(projectName, tmpDir, "*/5 * * * *", "/usr/bin/true")
	if err != nil {
		t.Fatalf("install: %v", err)
	}
	if label == "" {
		t.Fatal("expected label")
	}

	// Verify plist file
	plistPath := filepath.Join(plistDir, label+".plist")
	data, err := os.ReadFile(plistPath)
	if err != nil {
		t.Fatalf("plist missing: %v", err)
	}
	if !strings.Contains(string(data), "<integer>300</integer>") {
		t.Errorf("plist missing StartInterval=300:\n%s", data)
	}
	if !strings.Contains(string(data), "<!-- cron: */5 * * * * -->") {
		t.Errorf("plist missing cron comment:\n%s", data)
	}

	// Status round-trips cron
	st, _ := b.Status(projectName)
	if !st.Installed {
		t.Error("not installed")
	}
	if st.Cron != "*/5 * * * *" {
		t.Errorf("cron = %q, want %q", st.Cron, "*/5 * * * *")
	}

	// Remove
	if err := b.Remove(projectName); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, err := os.Stat(plistPath); !os.IsNotExist(err) {
		t.Error("plist still exists after remove")
	}
}

func TestLaunchdRoundTrip_Calendar(t *testing.T) {
	tmpDir := t.TempDir()
	plistDir := t.TempDir()
	b := &launchdBackend{plistDirOverride: plistDir}

	if _, err := b.Install("test", tmpDir, "0 9 * * *", "/usr/bin/true"); err != nil {
		t.Fatalf("install: %v", err)
	}
	st, _ := b.Status("test")
	if st.Cron != "0 9 * * *" {
		t.Errorf("cron = %q, want %q", st.Cron, "0 9 * * *")
	}
}
