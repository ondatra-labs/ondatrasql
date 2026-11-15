// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/deprecation"
)

// MinVersionVariable is the variable config/settings.sql sets to the
// oldest ondatrasql the project runs on:
//
//	SET VARIABLE ondatrasql_min_version = '0.48.0';
const MinVersionVariable = "ondatrasql_min_version"

var minVersionRe = regexp.MustCompile(`(?i)\bSET\s+VARIABLE\s+` + MinVersionVariable + `\s*(?:=|\bTO\b)\s*([^;]*)`)

// MinVersion returns the version config/settings.sql pins with
// MinVersionVariable, or "" when the file or the statement is absent. It
// reads the file as text, before any DuckDB session exists.
func MinVersion(projectDir string) (string, error) {
	content, err := os.ReadFile(filepath.Join(projectDir, "config", "settings.sql"))
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read config/settings.sql: %w", err)
	}
	m := minVersionRe.FindStringSubmatch(stripSQLComments(string(content)))
	if m == nil {
		return "", nil
	}
	raw := strings.TrimSpace(m[1])
	v := strings.TrimSuffix(strings.TrimPrefix(raw, "'"), "'")
	if len(raw) < 2 || raw[0] != '\'' || raw[len(raw)-1] != '\'' {
		return "", fmt.Errorf("config/settings.sql: %s must be a quoted version such as '0.48.0', got %s", MinVersionVariable, raw)
	}
	if _, err := deprecation.ParseVersion(v); err != nil {
		return "", fmt.Errorf("config/settings.sql: %s: %w", MinVersionVariable, err)
	}
	return v, nil
}

// CheckMinVersion refuses a project whose config/settings.sql pins a newer
// ondatrasql than running. A running version that does not parse (a dev
// build) is not checked.
func CheckMinVersion(projectDir, running string) error {
	pin, err := MinVersion(projectDir)
	if err != nil || pin == "" {
		return err
	}
	cur, err := deprecation.ParseVersion(running)
	if err != nil {
		return nil
	}
	want, _ := deprecation.ParseVersion(pin) // MinVersion parsed it
	if cur.Less(want) {
		return fmt.Errorf("this project needs ondatrasql %s or newer (%s in config/settings.sql), but this binary is %s: upgrade with `curl -fsSL https://ondatra.sh/install.sh | sh`",
			pin, MinVersionVariable, running)
	}
	return nil
}

// stripSQLComments blanks `--` line comments and `/* ... */` block comments
// outside single-quoted literals, so a commented-out pin is not enforced. A
// comment becomes a space, which keeps the tokens around it apart. An
// unterminated block comment runs to the end of the text, as in DuckDB.
func stripSQLComments(sql string) string {
	var b strings.Builder
	inString := false
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		switch {
		case inString:
			b.WriteByte(ch)
			if ch == '\'' {
				// A doubled quote stays inside the literal.
				if i+1 < len(sql) && sql[i+1] == '\'' {
					b.WriteByte('\'')
					i++
				} else {
					inString = false
				}
			}
		case ch == '\'':
			inString = true
			b.WriteByte(ch)
		case ch == '-' && i+1 < len(sql) && sql[i+1] == '-':
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			b.WriteByte(' ')
			if i < len(sql) {
				b.WriteByte('\n')
			}
		case ch == '/' && i+1 < len(sql) && sql[i+1] == '*':
			end := strings.Index(sql[i+2:], "*/")
			if end < 0 {
				i = len(sql)
			} else {
				i += 2 + end + 1
			}
			b.WriteByte(' ')
		default:
			b.WriteByte(ch)
		}
	}
	return b.String()
}
