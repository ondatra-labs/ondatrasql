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

func TestBuildTrigger(t *testing.T) {
	tests := []struct {
		name        string
		cron        string
		wantContain string
		wantErr     bool
		errContains string
	}{
		// TimeTrigger (every N minutes)
		{"every minute", "* * * * *", "<Interval>PT1M</Interval>", false, ""},
		{"every 5 min", "*/5 * * * *", "<Interval>PT5M</Interval>", false, ""},
		{"every 15 min", "*/15 * * * *", "<Interval>PT15M</Interval>", false, ""},

		// CalendarTrigger ScheduleByDay (daily)
		{"daily 09:00", "0 9 * * *", "<ScheduleByDay>", false, ""},
		{"daily 14:30", "30 14 * * *", "T14:30:00", false, ""},

		// CalendarTrigger ScheduleByWeek (weekly)
		{"weekly Sunday", "0 0 * * 0", "<Sunday />", false, ""},
		{"weekly Monday", "0 0 * * 1", "<Monday />", false, ""},
		{"weekdays", "0 22 * * 1-5", "<Monday />", false, ""},

		// CalendarTrigger ScheduleByMonth (monthly)
		{"monthly day 1", "0 0 1 * *", "<ScheduleByMonth>", false, ""},
		{"monthly day 15 at 12:00", "0 12 15 * *", "<Day>15</Day>", false, ""},

		// Patterns Task Scheduler cannot represent
		{"every 2 hours", "0 */2 * * *", "", true, "literal hour"},
		{"specific month only", "0 9 * 6 *", "", true, "cannot represent"},
		{"day-of-month + day-of-week", "0 9 1 * 1", "", true, "cannot represent"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields := strings.Fields(tt.cron)
			trigger, err := buildTrigger(fields[0], fields[1], fields[2], fields[3], fields[4])
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got: %s", trigger)
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !strings.Contains(trigger, tt.wantContain) {
				t.Errorf("trigger does not contain %q:\n%s", tt.wantContain, trigger)
			}
		})
	}
}

func TestWindowsRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	xmlDir := t.TempDir()

	b := &taskBackend{xmlDirOverride: xmlDir}
	projectName := "myproject"

	taskName, err := b.Install(projectName, tmpDir, "*/5 * * * *", `C:\Windows\System32\where.exe`)
	if err != nil {
		t.Fatalf("install: %v", err)
	}
	if taskName == "" {
		t.Fatal("expected task name")
	}

	// Verify XML file exists with correct UTF-16 encoding
	xmlPath := filepath.Join(xmlDir, taskName+".xml")
	data, err := os.ReadFile(xmlPath)
	if err != nil {
		t.Fatalf("XML file missing: %v", err)
	}
	if len(data) < 2 || data[0] != 0xFF || data[1] != 0xFE {
		t.Errorf("XML missing UTF-16 LE BOM: %x", data[:2])
	}
	// Decode and verify content
	text := decodeUTF16LE(data)
	if !strings.Contains(text, "*/5 * * * *") {
		t.Errorf("XML missing cron in description")
	}
	if !strings.Contains(text, "<Interval>PT5M</Interval>") {
		t.Errorf("XML missing trigger interval")
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
	if _, err := os.Stat(xmlPath); !os.IsNotExist(err) {
		t.Error("XML still exists after remove")
	}
}

func TestEncodeUTF16LE(t *testing.T) {
	out := encodeUTF16LE("Hi")
	// Expected: BOM (FF FE) + 'H' (48 00) + 'i' (69 00)
	want := []byte{0xFF, 0xFE, 0x48, 0x00, 0x69, 0x00}
	if len(out) != len(want) {
		t.Fatalf("length = %d, want %d", len(out), len(want))
	}
	for i, b := range want {
		if out[i] != b {
			t.Errorf("byte %d = %#x, want %#x", i, out[i], b)
		}
	}
}

func TestEncodeUTF16LE_BOM(t *testing.T) {
	out := encodeUTF16LE("any text")
	if len(out) < 2 || out[0] != 0xFF || out[1] != 0xFE {
		t.Errorf("missing UTF-16 LE BOM: %x", out[:2])
	}
}

func TestWeekdayElements(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"0", []string{"Sunday"}},
		{"1", []string{"Monday"}},
		{"7", []string{"Sunday"}},
		{"1-5", []string{"Monday", "Tuesday", "Wednesday", "Thursday", "Friday"}},
		{"1,3,5", []string{"Monday", "Wednesday", "Friday"}},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			out, err := weekdayElements(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for _, name := range tt.want {
				if !strings.Contains(out, "<"+name+" />") {
					t.Errorf("output missing %s:\n%s", name, out)
				}
			}
		})
	}
}
