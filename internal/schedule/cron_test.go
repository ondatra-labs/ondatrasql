// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package schedule

import "testing"

func TestValidateCron(t *testing.T) {
	tests := []struct {
		expr    string
		wantErr bool
	}{
		{"* * * * *", false},
		{"*/5 * * * *", false},
		{"0 9 * * *", false},
		{"0 0 1 * *", false},
		{"0 0 * * 0", false},
		{"15 14 1 * *", false},
		{"0 22 * * 1-5", false},
		{"", true},
		{"* * * *", true},
		{"60 * * * *", true},
		{"* 24 * * *", true},
		{"* * 32 * *", true},
		{"* * * 13 *", true},
		{"* * * * 8", true},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			_, err := ValidateCron(tt.expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateCron(%q) err = %v, wantErr = %v", tt.expr, err, tt.wantErr)
			}
		})
	}
}

func TestToSystemdOnCalendar(t *testing.T) {
	tests := []struct {
		cron string
		want string
	}{
		{"*/5 * * * *", "*-*-* *:0/5:00"},
		{"0 9 * * *", "*-*-* 09:00:00"},
		{"30 14 1 * *", "*-*-01 14:30:00"},
		{"0 0 * * 0", "Sun *-*-* 00:00:00"},
	}
	for _, tt := range tests {
		t.Run(tt.cron, func(t *testing.T) {
			got, err := ToSystemdOnCalendar(tt.cron)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("ToSystemdOnCalendar(%q) = %q, want %q", tt.cron, got, tt.want)
			}
		})
	}
}

func TestDescribe(t *testing.T) {
	if Describe("*/5 * * * *") != "every 5 minutes" {
		t.Error("Describe failed for */5")
	}
	if Describe("0 9 * * *") != "daily at 09:00" {
		t.Error("Describe failed for 0 9")
	}
	if Describe("13 7 * * 3") != "13 7 * * 3" {
		t.Error("Describe should pass through unknown patterns")
	}
}
