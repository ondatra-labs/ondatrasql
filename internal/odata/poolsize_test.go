// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package odata

import (
	"runtime"
	"strings"
	"testing"
)

// TestLoadPoolSize pins the env-var contract for ONDATRA_ODATA_POOL_SIZE.
// Empty falls back to the default; valid positive integer is used as-is;
// anything else (zero, negative, non-numeric) refuses to start the server
// rather than silently falling back. The fail-closed semantics mirror
// loadDeltaMaxAge — a typoed env shouldn't disable the operator's
// configuration intent.
func TestLoadPoolSize(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		env     string
		want    int
		wantErr bool
	}{
		{"empty falls back to default", "", defaultPoolSize(), false},
		{"valid 1", "1", 1, false},
		{"valid 4", "4", 4, false},
		{"valid 100 (above the default cap)", "100", 100, false},
		{"whitespace trimmed", "  16  ", 16, false},
		{"zero rejected", "0", 0, true},
		{"negative rejected", "-1", 0, true},
		{"non-numeric rejected", "lots", 0, true},
		{"hex rejected", "0x10", 0, true},
		{"float rejected", "4.5", 0, true},
		{"trailing junk rejected", "4abc", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := loadPoolSize(tc.env)
			if (err != nil) != tc.wantErr {
				t.Fatalf("loadPoolSize(%q) err = %v, wantErr = %v", tc.env, err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("loadPoolSize(%q) = %d, want %d", tc.env, got, tc.want)
			}
			if tc.wantErr && err != nil && !strings.Contains(err.Error(), "ONDATRA_ODATA_POOL_SIZE") {
				t.Errorf("error should mention env var name, got: %v", err)
			}
		})
	}
}

// TestDefaultPoolSize pins the default's contract: min(GOMAXPROCS, 8),
// floor 1. The contract is documented in odata-api.md; this test catches
// drift if someone changes the cap or floor without updating docs.
func TestDefaultPoolSize(t *testing.T) {
	t.Parallel()
	got := defaultPoolSize()
	max := runtime.GOMAXPROCS(0)
	if max > 8 {
		max = 8
	}
	if max < 1 {
		max = 1
	}
	if got != max {
		t.Errorf("defaultPoolSize() = %d, want %d (min(GOMAXPROCS=%d, 8), floor 1)",
			got, max, runtime.GOMAXPROCS(0))
	}
	if got < 1 {
		t.Errorf("defaultPoolSize() = %d, must be >= 1", got)
	}
	if got > 8 {
		t.Errorf("defaultPoolSize() = %d, must be <= 8 (the cap documented in odata-api.md)", got)
	}
}
