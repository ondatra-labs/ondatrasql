// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package clock

import (
	"testing"
	"time"
)

func TestFrozen(t *testing.T) {
	t.Setenv(EnvNow, "")
	if _, ok, err := Frozen(); ok || err != nil {
		t.Fatalf("unset: ok=%v err=%v", ok, err)
	}

	t.Setenv(EnvNow, "2026-03-01T08:30:00+01:00")
	got, ok, err := Frozen()
	if !ok || err != nil {
		t.Fatalf("set: ok=%v err=%v", ok, err)
	}
	if want := time.Date(2026, 3, 1, 7, 30, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	t.Setenv(EnvNow, "2026-03-01")
	if _, ok, err := Frozen(); !ok || err == nil {
		t.Fatalf("malformed: ok=%v err=%v, want ok and an error", ok, err)
	}
	if Validate() == nil {
		t.Fatal("Validate accepted a malformed value")
	}
}
