// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
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
			// A fresh HOME has no extension cache, so every script downloads
			// ducklake. testscript starts from an empty environment, which
			// drops the proxy a sandboxed host can only reach the network
			// through; without it DuckDB 2.0 spins on the download until
			// the package times out instead of failing.
			for _, name := range []string{"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy"} {
				if v, ok := os.LookupEnv(name); ok {
					env.Setenv(name, v)
				}
			}
			return nil
		},
	})
}
