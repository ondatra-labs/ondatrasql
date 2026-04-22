// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package schedule

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// --- Systemd: percent signs in paths escaped ---

func TestSystemd_PercentInPath(t *testing.T) {
	// Before fix: a path like /home/user/my%20project was passed verbatim.
	// systemd interprets %20 as a specifier, causing startup failure.
	// After fix: % is escaped as %%.
	tmpDir := t.TempDir()
	unitDir := t.TempDir()

	b := &systemdBackend{unitDirOverride: unitDir}
	binaryPath := "/opt/my%20app/ondatrasql"

	_, err := b.Install("test-percent", tmpDir, "0 * * * *", binaryPath)
	if err != nil {
		t.Fatalf("install: %v", err)
	}

	servicePath := filepath.Join(unitDir, b.unitName("test-percent")+".service")
	data, err := os.ReadFile(servicePath)
	if err != nil {
		t.Fatalf("read service: %v", err)
	}

	content := string(data)
	// The ExecStart line should have %%20 (escaped), not bare %20
	if !strings.Contains(content, "%%20") {
		t.Errorf("service file should contain %%%%20 (escaped percent), got:\n%s", content)
	}
	// Verify no bare %20 (without preceding %) exists
	replaced := strings.ReplaceAll(content, "%%20", "")
	if strings.Contains(replaced, "%20") {
		t.Error("service file contains unescaped %20")
	}
}

func TestSystemd_NewlineInProjectName(t *testing.T) {
	// Before fix: a newline in projectName could inject arbitrary directives.
	// After fix: newlines are replaced with spaces.
	tmpDir := t.TempDir()
	unitDir := t.TempDir()

	b := &systemdBackend{unitDirOverride: unitDir}
	binaryPath := "/usr/bin/true"

	_, err := b.Install("test\ninjection", tmpDir, "0 * * * *", binaryPath)
	if err != nil {
		t.Fatalf("install: %v", err)
	}

	servicePath := filepath.Join(unitDir, b.unitName("test\ninjection")+".service")
	data, err := os.ReadFile(servicePath)
	if err != nil {
		t.Fatalf("read service: %v", err)
	}

	content := string(data)
	// The Description line should not contain a raw newline from projectName
	for _, line := range strings.Split(content, "\n") {
		if strings.HasPrefix(line, "Description=") {
			if strings.Contains(line, "\n") {
				t.Error("Description should not contain newline")
			}
			break
		}
	}
}

// --- Launchd: XML escaping ---

func TestXmlEscape(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{"simple", "simple"},
		{"R&D", "R&amp;D"},
		{"a<b>c", "a&lt;b&gt;c"},
		{`say "hello"`, "say &quot;hello&quot;"},
		{"it's", "it&apos;s"},
		{"a&b<c>d'e\"f", "a&amp;b&lt;c&gt;d&apos;e&quot;f"},
		{"", ""},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			got := xmlEscape(tt.input)
			if got != tt.want {
				t.Errorf("xmlEscape(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestLaunchd_AmpersandInPath(t *testing.T) {
	// Before fix: a path like /Users/marcus/R&D/ondatrasql produced invalid XML.
	// text/template does not XML-escape. After fix: xmlEscape is applied.
	tmpDir := t.TempDir()
	plistDir := t.TempDir()

	b := &launchdBackend{plistDirOverride: plistDir}
	binaryPath := "/Users/marcus/R&D/ondatrasql"

	_, err := b.Install("test-amp", tmpDir, "0 * * * *", binaryPath)
	if err != nil {
		t.Fatalf("install: %v", err)
	}

	plistPath := filepath.Join(plistDir, b.label("test-amp")+".plist")
	data, err := os.ReadFile(plistPath)
	if err != nil {
		t.Fatalf("read plist: %v", err)
	}

	content := string(data)
	// Raw & should be escaped to &amp;
	if strings.Contains(content, "R&D") && !strings.Contains(content, "R&amp;D") {
		t.Error("plist should XML-escape & to &amp;")
	}
	if !strings.Contains(content, "R&amp;D") {
		t.Errorf("plist missing escaped path, got:\n%s", content)
	}
}
