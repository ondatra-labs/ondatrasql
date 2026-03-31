// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"testing"
)

func TestBasicAuth(t *testing.T) {
	t.Parallel()
	got := BasicAuth("user", "pass")
	// base64("user:pass") = "dXNlcjpwYXNz"
	want := "Basic dXNlcjpwYXNz"
	if got != want {
		t.Errorf("BasicAuth = %q, want %q", got, want)
	}
}

func TestBasicAuthSpecialChars(t *testing.T) {
	t.Parallel()
	got := BasicAuth("user@domain.com", "p@ss:word!")
	if got[:6] != "Basic " {
		t.Errorf("BasicAuth should start with 'Basic ', got %q", got)
	}
}
