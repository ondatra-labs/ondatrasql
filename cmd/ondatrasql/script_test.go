// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package main

import (
	"os"
	"testing"

	"github.com/rogpeppe/go-internal/testscript"
)

func TestMain(m *testing.M) {
	os.Exit(testscript.RunMain(m, map[string]func() int{
		"ondatrasql": func() int {
			if err := run(os.Args[1:]); err != nil {
				os.Stderr.WriteString("error: " + err.Error() + "\n")
				return 1
			}
			return 0
		},
	}))
}

func TestScript(t *testing.T) {
	testscript.Run(t, testscript.Params{
		Dir: "testdata/scripts",
		Setup: func(env *testscript.Env) error {
			// DuckDB requires a valid HOME directory
			env.Setenv("HOME", env.WorkDir)
			return nil
		},
	})
}
