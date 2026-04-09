// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package schedule

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type systemdBackend struct {
	// unitDirOverride lets tests redirect unit files to a temp directory.
	// When set, systemctl commands are also skipped.
	unitDirOverride string
}

func (s *systemdBackend) Name() string { return "systemd" }

func (s *systemdBackend) unitDir() (string, error) {
	if s.unitDirOverride != "" {
		return s.unitDirOverride, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(home, ".config", "systemd", "user")
	return dir, nil
}

// useSystemctl returns true if we should call systemctl (false in test mode).
func (s *systemdBackend) useSystemctl() bool {
	return s.unitDirOverride == ""
}

func (s *systemdBackend) unitName(projectName string) string {
	return "ondatrasql-" + sanitize(projectName)
}

func (s *systemdBackend) Install(projectName, projectDir, cronExpr, binaryPath string) (string, error) {
	onCalendar, err := ToSystemdOnCalendar(cronExpr)
	if err != nil {
		return "", fmt.Errorf("translate cron: %w", err)
	}

	unitDir, err := s.unitDir()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(unitDir, 0o755); err != nil {
		return "", fmt.Errorf("mkdir %s: %w", unitDir, err)
	}

	unit := s.unitName(projectName)

	servicePath := filepath.Join(unitDir, unit+".service")
	timerPath := filepath.Join(unitDir, unit+".timer")

	service := fmt.Sprintf(`[Unit]
Description=OndatraSQL run — %s

[Service]
Type=oneshot
WorkingDirectory=%s
ExecStart="%s" run
StandardOutput=journal
StandardError=journal
`, projectName, projectDir, binaryPath)

	timer := fmt.Sprintf(`[Unit]
Description=OndatraSQL schedule — %s (%s)

[Timer]
OnCalendar=%s
Persistent=true
Unit=%s.service

[Install]
WantedBy=timers.target
`, projectName, cronExpr, onCalendar, unit)

	if err := os.WriteFile(servicePath, []byte(service), 0o644); err != nil {
		return "", fmt.Errorf("write service: %w", err)
	}
	if err := os.WriteFile(timerPath, []byte(timer), 0o644); err != nil {
		return "", fmt.Errorf("write timer: %w", err)
	}

	// Reload + enable + start (skipped in test mode)
	if s.useSystemctl() {
		if err := runCmd("systemctl", "--user", "daemon-reload"); err != nil {
			return "", fmt.Errorf("systemctl daemon-reload: %w", err)
		}
		if err := runCmd("systemctl", "--user", "enable", "--now", unit+".timer"); err != nil {
			return "", fmt.Errorf("systemctl enable: %w", err)
		}
	}

	return unit, nil
}

func (s *systemdBackend) Remove(projectName string) error {
	unitDir, err := s.unitDir()
	if err != nil {
		return err
	}
	unit := s.unitName(projectName)

	if s.useSystemctl() {
		// Disable + stop (best-effort)
		_ = runCmd("systemctl", "--user", "disable", "--now", unit+".timer")
	}

	// Remove files
	_ = os.Remove(filepath.Join(unitDir, unit+".service"))
	_ = os.Remove(filepath.Join(unitDir, unit+".timer"))

	if s.useSystemctl() {
		_ = runCmd("systemctl", "--user", "daemon-reload")
	}
	return nil
}

func (s *systemdBackend) Status(projectName string) (*Status, error) {
	unitDir, err := s.unitDir()
	if err != nil {
		return nil, err
	}
	unit := s.unitName(projectName)
	timerPath := filepath.Join(unitDir, unit+".timer")

	st := &Status{
		UnitName: unit + ".timer",
		Backend:  "systemd",
	}

	data, err := os.ReadFile(timerPath)
	if err != nil {
		return st, nil
	}
	st.Installed = true

	// Extract cron from description line
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "Description=OndatraSQL schedule") {
			if i := strings.Index(line, "("); i >= 0 {
				if j := strings.LastIndex(line, ")"); j > i {
					st.Cron = line[i+1 : j]
				}
			}
		}
	}

	if s.useSystemctl() {
		// Query systemctl for active state and next run
		out, _ := exec.Command("systemctl", "--user", "is-active", unit+".timer").CombinedOutput()
		st.Active = strings.TrimSpace(string(out)) == "active"

		if st.Active {
			out, _ := exec.Command("systemctl", "--user", "list-timers", unit+".timer", "--no-pager", "--no-legend").CombinedOutput()
			fields := strings.Fields(string(out))
			if len(fields) >= 3 {
				st.NextRun = fields[0] + " " + fields[1] + " " + fields[2]
			}
			out, _ = exec.Command("systemctl", "--user", "show", unit+".service", "--property=ExecMainStartTimestamp", "--value").CombinedOutput()
			st.LastRun = strings.TrimSpace(string(out))
		}
	}

	return st, nil
}

func runCmd(name string, args ...string) error {
	out, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %w (%s)", name, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// sanitize converts a project name to a safe systemd/launchd unit name component.
func sanitize(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(name) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_' || r == '.':
			b.WriteRune('-')
		}
	}
	if b.Len() == 0 {
		return "project"
	}
	return b.String()
}
