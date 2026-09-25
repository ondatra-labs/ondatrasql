// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package configenv expands environment variables in config SQL and keeps
// track of the ones that were not set.
//
// os.ExpandEnv turns an unset variable into an empty string without a word.
// In a connection string that hides the real problem behind a misleading
// one: `host=${PG_HOST} port=5432` with PG_HOST unset becomes
// `host= port=5432`, libpq takes "port=5432" as the host name, and the error
// talks about resolving "port=5432". Naming the unset variable next to the
// error puts the actual cause on screen.
package configenv

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

// envRef matches what a config author meant as a variable: a name that
// starts like one. That keeps DuckDB's own $1 and $$ out, and keeps shell
// syntax Go does not support (${PG_HOST:-localhost}, which expands to "") in:
// reporting that as unset is exactly the hint its author needs.
var envRef = regexp.MustCompile(`^[A-Za-z_]`)

// Expand replaces ${VAR} and $VAR exactly as os.ExpandEnv does, and also
// returns the names of referenced variables that are not set at all, sorted
// and without duplicates. A variable set to the empty string counts as set:
// that is a deliberate value, not a missing one.
//
// Names that appear only in `--` comments are not reported: they are expanded
// like everything else, but a commented-out example cannot be why a statement
// failed, and naming it would point at the wrong thing. A `--` inside a
// string literal is not a comment, so a connection string with one in its
// password keeps the names after it.
func Expand(s string) (string, []string) {
	out := os.Expand(s, os.Getenv)

	seen := map[string]bool{}
	var unset []string
	for _, line := range strings.Split(s, "\n") {
		os.Expand(StripLineComment(line), func(name string) string {
			if _, ok := os.LookupEnv(name); !ok && envRef.MatchString(name) && !seen[name] {
				seen[name] = true
				unset = append(unset, name)
			}
			return ""
		})
	}
	sort.Strings(unset)
	return out, unset
}

// Annotate prefixes err with the unset variables, when there are any. Use it
// on the error from executing the SQL that Expand produced: the unset
// variable is usually why it failed. An error that already carries a note is
// returned as it is, so a caller wrapping a load error does not add a second
// one that names another file's variables.
//
// The note goes first. After the error it would sit next to whatever the
// error ends with — often `password=` with the empty value of the very
// variable that is missing — and the credential redaction the CLI applies
// before printing would take the note for the password.
func Annotate(err error, unset []string) error {
	if err == nil || len(unset) == 0 {
		return err
	}
	var already *annotatedError
	if errors.As(err, &already) {
		return err
	}
	msg := fmt.Sprintf("not set in the environment: %s; %s", strings.Join(unset, ", "), err.Error())
	return &annotatedError{msg: msg, err: err}
}

type annotatedError struct {
	msg string
	err error
}

func (e *annotatedError) Error() string { return e.msg }
func (e *annotatedError) Unwrap() error { return e.err }

// StripLineComment removes everything from the first `--` outside a
// single-quoted string literal to the end of the line. A `--` inside a
// literal (a connection string whose password contains one) stays, and a
// doubled quote inside a literal does not end it.
func StripLineComment(line string) string {
	inString := false
	for i := 0; i < len(line); i++ {
		ch := line[i]
		if ch == '\'' {
			// SQL doubles quotes to escape: '' inside a literal stays
			// inside. Toggle on the boundary single quote.
			if i+1 < len(line) && line[i+1] == '\'' {
				i++ // skip both quotes (escaped quote)
				continue
			}
			inString = !inString
			continue
		}
		if !inString && ch == '-' && i+1 < len(line) && line[i+1] == '-' {
			return line[:i]
		}
	}
	return line
}
