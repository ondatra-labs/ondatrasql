// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package clock reads ONDATRA_NOW, the frozen instant Starlark's
// time.now() returns when a blueprint is run for a deterministic test.
package clock

import (
	"fmt"
	"os"
	"time"
)

// EnvNow is the variable that freezes the clock, as an RFC 3339 timestamp.
const EnvNow = "ONDATRA_NOW"

// Frozen returns the instant ONDATRA_NOW names. ok is false when the
// variable is unset or empty; err is set when it does not parse.
func Frozen() (t time.Time, ok bool, err error) {
	v := os.Getenv(EnvNow)
	if v == "" {
		return time.Time{}, false, nil
	}
	t, err = time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return time.Time{}, true, fmt.Errorf("%s=%q: not an RFC 3339 timestamp (e.g. 2026-01-31T12:00:00Z)", EnvNow, v)
	}
	return t, true, nil
}

// Validate reports an ONDATRA_NOW that does not parse.
func Validate() error {
	_, _, err := Frozen()
	return err
}
