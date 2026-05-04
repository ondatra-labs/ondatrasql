// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
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

	// Sanitize project name (no newlines that could inject directives)
	safeName := strings.ReplaceAll(projectName, "\n", " ")

	// Build service unit file. Use string concatenation instead of fmt.Sprintf
	// because systemd requires % to be escaped as %%, but Sprintf would
	// consume the %% escaping.
	service := "[Unit]\nDescription=OndatraSQL run — " + safeName + "\n\n" +
		"[Service]\nType=oneshot\n" +
		"WorkingDirectory=" + systemdEscape(projectDir) + "\n" +
		"ExecStart=" + systemdEscape(binaryPath) + " run\n" +
		"StandardOutput=journal\nStandardError=journal\n"

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
		// Service was written but timer failed — undo the service write
		// so a partial install doesn't leave an orphan .service behind.
		_ = os.Remove(servicePath)
		return "", fmt.Errorf("write timer: %w", err)
	}

	// Reload + enable + start (skipped in test mode). On any failure
	// here, roll back the unit files so the user can retry from a
	// clean slate — without rollback a stale .service/.timer pair is
	// left visible to systemctl, which masks the real error on a retry
	// and can keep the timer subtly active under an old config.
	if s.useSystemctl() {
		if err := runCmd("systemctl", "--user", "daemon-reload"); err != nil {
			_ = os.Remove(servicePath)
			_ = os.Remove(timerPath)
			return "", fmt.Errorf("systemctl daemon-reload: %w", err)
		}
		if err := runCmd("systemctl", "--user", "enable", "--now", unit+".timer"); err != nil {
			_ = os.Remove(servicePath)
			_ = os.Remove(timerPath)
			// Best-effort second daemon-reload so systemctl forgets the
			// units we just removed; ignore its error since the primary
			// failure is what we want to surface.
			_ = runCmd("systemctl", "--user", "daemon-reload")
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
		// Disable + stop. Fails are non-fatal here (the unit may already
		// be disabled, or the timer may not exist), but we surface them
		// to stderr so a permission-denied or broken systemd doesn't
		// silently leave the timer active when the user thinks Remove
		// succeeded.
		if err := runCmd("systemctl", "--user", "disable", "--now", unit+".timer"); err != nil {
			fmt.Fprintf(os.Stderr, "warning: systemctl disable %s.timer failed: %v\n", unit, err)
		}
	}

	// Remove files. Surface unexpected errors (a real ENOENT is
	// expected on a never-installed unit; permission denied or I/O
	// errors are not).
	for _, suffix := range []string{".service", ".timer"} {
		path := filepath.Join(unitDir, unit+suffix)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "warning: remove %s: %v\n", path, err)
		}
	}

	if s.useSystemctl() {
		if err := runCmd("systemctl", "--user", "daemon-reload"); err != nil {
			fmt.Fprintf(os.Stderr, "warning: systemctl daemon-reload failed: %v\n", err)
		}
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

// systemdEscape escapes % as %% for systemd unit files.
// systemd interprets % followed by a letter as a specifier (e.g. %n, %i).
func systemdEscape(s string) string {
	return strings.ReplaceAll(s, "%", "%%")
}
