// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package schedule

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"
)

type launchdBackend struct {
	// plistDirOverride lets tests redirect plist files to a temp directory.
	// When set, launchctl commands are also skipped.
	plistDirOverride string
}

func (l *launchdBackend) Name() string { return "launchd" }

func (l *launchdBackend) plistDir() (string, error) {
	if l.plistDirOverride != "" {
		return l.plistDirOverride, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "Library", "LaunchAgents"), nil
}

// useLaunchctl returns true if we should call launchctl (false in test mode).
func (l *launchdBackend) useLaunchctl() bool {
	return l.plistDirOverride == ""
}

func (l *launchdBackend) label(projectName string) string {
	return "sh.ondatra." + sanitize(projectName)
}

func (l *launchdBackend) plistPath(projectName string) (string, error) {
	dir, err := l.plistDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, l.label(projectName)+".plist"), nil
}

func (l *launchdBackend) Install(projectName, projectDir, cronExpr, binaryPath string) (string, error) {
	if _, err := ValidateCron(cronExpr); err != nil {
		return "", err
	}

	calendar, interval, err := cronToLaunchd(cronExpr)
	if err != nil {
		return "", err
	}

	dir, err := l.plistDir()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}

	label := l.label(projectName)
	path, _ := l.plistPath(projectName)

	tmpl := template.Must(template.New("plist").Funcs(template.FuncMap{
		"xml": xmlEscape,
	}).Parse(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<!-- cron: {{.Cron | xml}} -->
<dict>
    <key>Label</key>
    <string>{{.Label | xml}}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{{.Binary | xml}}</string>
        <string>run</string>
    </array>
    <key>WorkingDirectory</key>
    <string>{{.Dir | xml}}</string>
    <key>StandardOutPath</key>
    <string>{{.LogPath | xml}}</string>
    <key>StandardErrorPath</key>
    <string>{{.LogPath | xml}}</string>
    {{.Schedule}}
</dict>
</plist>
`))

	var schedule string
	if interval > 0 {
		schedule = fmt.Sprintf("<key>StartInterval</key>\n    <integer>%d</integer>", interval)
	} else {
		schedule = "<key>StartCalendarInterval</key>\n    " + calendar
	}

	logPath := filepath.Join(dir, label+".log")

	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	// Don't `defer f.Close()` — that swallows the Close error which
	// for a write handle indicates flush/data-loss. Close explicitly
	// after Execute and propagate the error. (Per
	// https://www.joeshaw.org/dont-defer-close-on-writable-files/)
	if execErr := tmpl.Execute(f, map[string]string{
		"Label":    label,
		"Binary":   binaryPath,
		"Dir":      projectDir,
		"LogPath":  logPath,
		"Schedule": schedule,
		"Cron":     cronExpr,
	}); execErr != nil {
		_ = f.Close() // primary error already set
		return "", execErr
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("close plist file: %w", err)
	}

	// Load (skipped in test mode). The pre-emptive unload covers the
	// case where the same plist label is already loaded — typical when
	// re-installing with a new cron expression. unload fails are
	// expected (plist not loaded) so we don't surface them; load fails
	// are real and propagated.
	//
	// On load failure, roll back the plist file we just wrote. Without
	// rollback the on-disk plist is visible to `launchctl list` (after
	// next login) but never actually loaded, which leaves the user with
	// a confusingly-half-installed schedule that Status reports as
	// "installed but inactive".
	if l.useLaunchctl() {
		_ = exec.Command("launchctl", "unload", path).Run() //nolint:errcheck // unload errors are expected on first install (plist not yet loaded); the subsequent load() surfaces real failures.
		if err := runCmd("launchctl", "load", path); err != nil {
			_ = os.Remove(path)
			return "", err
		}
	}

	return label, nil
}

func (l *launchdBackend) Remove(projectName string) error {
	path, err := l.plistPath(projectName)
	if err != nil {
		return err
	}
	if l.useLaunchctl() {
		_ = exec.Command("launchctl", "unload", path).Run()
	}
	_ = os.Remove(path)
	return nil
}

func (l *launchdBackend) Status(projectName string) (*Status, error) {
	path, err := l.plistPath(projectName)
	if err != nil {
		return nil, err
	}
	st := &Status{
		UnitName: l.label(projectName),
		Backend:  "launchd",
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return st, nil
	}
	st.Installed = true

	// Plist files written by Install() encode the original cron expression as a
	// trailing comment. Look for it.
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "<!-- cron:") {
			cron := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(line, "<!-- cron:"), "-->"))
			st.Cron = cron
			break
		}
	}

	if l.useLaunchctl() {
		// `launchctl list <label>` exits 0 with a plist-formatted dump
		// when the job is loaded, exits non-zero with "Could not find
		// service" on stderr when it isn't, and fails entirely (no
		// such binary, etc.) when launchctl is missing or broken. We
		// must distinguish the second case from the third: ignoring
		// the run error meant a missing launchctl produced empty
		// stdout, and the original `!Contains(..., "Could not find
		// service")` check then reported Active=true incorrectly.
		// (R6 finding.)
		cmd := exec.Command("launchctl", "list", l.label(projectName))
		out, runErr := cmd.CombinedOutput()
		switch {
		case runErr != nil && cmd.ProcessState != nil && cmd.ProcessState.Exited():
			// launchctl ran and reported the job is not loaded.
			st.Active = false
		case runErr != nil:
			// launchctl is missing/broken — we can't determine state.
			// Surface the failure on stderr so the operator notices
			// rather than silently assuming inactive.
			fmt.Fprintf(os.Stderr, "warning: launchctl list %s failed: %v\n", l.label(projectName), runErr)
			st.Active = false
		default:
			st.Active = !strings.Contains(string(out), "Could not find service")
		}
	}
	return st, nil
}

// cronToLaunchd converts a cron expression to launchd config.
// Returns (calendarXML, intervalSeconds, error). Only one is non-zero/empty.
//
// launchd has two scheduling mechanisms:
//   - StartInterval: fixed seconds between runs (used for "*/N * * * *")
//   - StartCalendarInterval: specific calendar slots (used for fixed times like "0 9 * * *")
//
// Patterns that cannot be expressed in either are rejected explicitly to avoid
// silently installing a wrong schedule (e.g. "0 */2 * * *" cannot become a
// daily 00:00 trigger).
func cronToLaunchd(cron string) (string, int, error) {
	fields := strings.Fields(cron)
	if len(fields) != 5 {
		return "", 0, fmt.Errorf("invalid cron")
	}
	minute, hour, day, month, weekday := fields[0], fields[1], fields[2], fields[3], fields[4]

	// Pattern A: every N minutes — "*/N * * * *" or "* * * * *"
	if hour == "*" && day == "*" && month == "*" && weekday == "*" {
		if strings.HasPrefix(minute, "*/") {
			n, err := strconv.Atoi(minute[2:])
			if err == nil && n > 0 {
				return "", n * 60, nil
			}
		}
		if minute == "*" {
			return "", 60, nil
		}
	}

	// Pattern B: StartCalendarInterval — only literal integers per field.
	// Reject ranges, lists, and steps (launchd cannot represent them).
	if err := requireLiteralOrStar("minute", minute); err != nil {
		return "", 0, err
	}
	if err := requireLiteralOrStar("hour", hour); err != nil {
		return "", 0, err
	}
	if err := requireLiteralOrStar("day", day); err != nil {
		return "", 0, err
	}
	if err := requireLiteralOrStar("month", month); err != nil {
		return "", 0, err
	}
	if err := requireLiteralOrStar("weekday", weekday); err != nil {
		return "", 0, err
	}

	var entries []string
	addField := func(key, val string) {
		if val == "*" {
			return
		}
		if n, err := strconv.Atoi(val); err == nil {
			entries = append(entries, fmt.Sprintf("<key>%s</key>\n        <integer>%d</integer>", key, n))
		}
	}
	addField("Minute", minute)
	addField("Hour", hour)
	addField("Day", day)
	addField("Month", month)
	addField("Weekday", weekday)

	if len(entries) == 0 {
		return "", 0, fmt.Errorf("cron expression %q produces no schedule", cron)
	}

	xml := "<dict>\n        " + strings.Join(entries, "\n        ") + "\n    </dict>"
	return xml, 0, nil
}

// requireLiteralOrStar rejects step/range/list patterns that launchd's
// StartCalendarInterval cannot express.
func requireLiteralOrStar(name, val string) error {
	if val == "*" {
		return nil
	}
	if strings.HasPrefix(val, "*/") {
		return fmt.Errorf("launchd does not support step values (%s=%q); use plain integer or *", name, val)
	}
	if strings.Contains(val, ",") {
		return fmt.Errorf("launchd does not support comma lists (%s=%q); install one schedule per value", name, val)
	}
	if strings.Contains(val, "-") {
		return fmt.Errorf("launchd does not support ranges (%s=%q)", name, val)
	}
	if _, err := strconv.Atoi(val); err != nil {
		return fmt.Errorf("invalid %s value %q", name, val)
	}
	return nil
}

// xmlEscape escapes special characters for XML string values.
func xmlEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "'", "&apos;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	return s
}
