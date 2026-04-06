// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package schedule

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"unicode/utf16"
)

type taskBackend struct {
	// xmlDirOverride lets tests redirect the XML output dir and skip schtasks calls.
	xmlDirOverride string
}

func (t *taskBackend) Name() string { return "task-scheduler" }

func (t *taskBackend) taskName(projectName string) string {
	return "OndatraSQL-" + sanitize(projectName)
}

func (t *taskBackend) useSchtasks() bool {
	return t.xmlDirOverride == ""
}

func (t *taskBackend) tempDir() string {
	if t.xmlDirOverride != "" {
		return t.xmlDirOverride
	}
	return os.TempDir()
}

func (t *taskBackend) Install(projectName, projectDir, cronExpr, binaryPath string) (string, error) {
	if _, err := ValidateCron(cronExpr); err != nil {
		return "", err
	}

	taskName := t.taskName(projectName)

	// Generate XML
	xmlPath := filepath.Join(t.tempDir(), taskName+".xml")
	xml, err := buildTaskXML(projectName, projectDir, cronExpr, binaryPath)
	if err != nil {
		return "", err
	}
	// Task Scheduler requires UTF-16 LE with BOM (the XML declaration says encoding="UTF-16").
	encoded := encodeUTF16LE(xml)
	if err := os.WriteFile(xmlPath, encoded, 0o644); err != nil {
		return "", err
	}

	if t.useSchtasks() {
		defer os.Remove(xmlPath)

		// Delete existing task (best-effort)
		_ = exec.Command("schtasks", "/delete", "/tn", taskName, "/f").Run()

		// Create from XML
		if err := runCmd("schtasks", "/create", "/tn", taskName, "/xml", xmlPath); err != nil {
			return "", err
		}
	}

	return taskName, nil
}

func (t *taskBackend) Remove(projectName string) error {
	taskName := t.taskName(projectName)
	if t.useSchtasks() {
		_ = exec.Command("schtasks", "/delete", "/tn", taskName, "/f").Run()
	} else {
		// Test mode: clean up the XML file
		_ = os.Remove(filepath.Join(t.tempDir(), taskName+".xml"))
	}
	return nil
}

func (t *taskBackend) Status(projectName string) (*Status, error) {
	taskName := t.taskName(projectName)
	st := &Status{
		UnitName: taskName,
		Backend:  "task-scheduler",
	}

	// Test mode: read from the XML file directly.
	if !t.useSchtasks() {
		xmlPath := filepath.Join(t.tempDir(), taskName+".xml")
		raw, err := os.ReadFile(xmlPath)
		if err != nil {
			return st, nil
		}
		st.Installed = true
		// Decode UTF-16 LE back to a string for parsing
		text := decodeUTF16LE(raw)
		// Description: "OndatraSQL run for X (cron)"
		if a := strings.Index(text, "("); a >= 0 {
			if b := strings.Index(text[a+1:], ")"); b >= 0 {
				st.Cron = text[a+1 : a+1+b]
			}
		}
		return st, nil
	}

	out, err := exec.Command("schtasks", "/query", "/tn", taskName, "/fo", "list", "/v").CombinedOutput()
	if err != nil {
		return st, nil
	}
	st.Installed = true
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Status:") {
			st.Active = strings.Contains(line, "Ready") || strings.Contains(line, "Running")
		}
		if strings.HasPrefix(line, "Next Run Time:") {
			st.NextRun = strings.TrimSpace(strings.TrimPrefix(line, "Next Run Time:"))
		}
		if strings.HasPrefix(line, "Last Run Time:") {
			st.LastRun = strings.TrimSpace(strings.TrimPrefix(line, "Last Run Time:"))
		}
		// Description includes cron expression in parentheses
		if strings.HasPrefix(line, "Comment:") || strings.HasPrefix(line, "Description:") {
			val := line
			if i := strings.Index(line, ":"); i >= 0 {
				val = strings.TrimSpace(line[i+1:])
			}
			if a := strings.Index(val, "("); a >= 0 {
				if b := strings.LastIndex(val, ")"); b > a {
					st.Cron = val[a+1 : b]
				}
			}
		}
	}
	return st, nil
}

func buildTaskXML(projectName, projectDir, cronExpr, binaryPath string) (string, error) {
	fields := strings.Fields(cronExpr)
	minute, hour, day, month, weekday := fields[0], fields[1], fields[2], fields[3], fields[4]

	trigger, err := buildTrigger(minute, hour, day, month, weekday)
	if err != nil {
		return "", err
	}

	xml := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>OndatraSQL run for %s (%s)</Description>
  </RegistrationInfo>
  <Triggers>
    %s
  </Triggers>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
  </Settings>
  <Actions>
    <Exec>
      <Command>%s</Command>
      <Arguments>run</Arguments>
      <WorkingDirectory>%s</WorkingDirectory>
    </Exec>
  </Actions>
</Task>`, projectName, cronExpr, trigger, binaryPath, projectDir)

	return xml, nil
}

// buildTrigger generates a Task Scheduler trigger XML element for a cron pattern.
// Supports four trigger families:
//
//   - TimeTrigger with Repetition: every N minutes ("*/N * * * *")
//   - CalendarTrigger ScheduleByDay: daily at HH:MM ("M H * * *")
//   - CalendarTrigger ScheduleByWeek: weekly on day(s) at HH:MM ("M H * * D")
//   - CalendarTrigger ScheduleByMonth: monthly on day D at HH:MM ("M H D * *")
//
// Anything else (steps in hour/day/month, ranges, comma lists) is rejected
// rather than silently degraded — Task Scheduler does not have a generic
// cron-equivalent and a wrong schedule is worse than no schedule.
func buildTrigger(minute, hour, day, month, weekday string) (string, error) {
	// Pattern A: every N minutes — "*/N * * * *"
	if hour == "*" && day == "*" && month == "*" && weekday == "*" {
		if strings.HasPrefix(minute, "*/") {
			n, err := strconv.Atoi(minute[2:])
			if err != nil || n <= 0 {
				return "", fmt.Errorf("invalid step minute: %s", minute)
			}
			return fmt.Sprintf(`<TimeTrigger>
      <Repetition>
        <Interval>PT%dM</Interval>
        <StopAtDurationEnd>false</StopAtDurationEnd>
      </Repetition>
      <StartBoundary>2026-01-01T00:00:00</StartBoundary>
      <Enabled>true</Enabled>
    </TimeTrigger>`, n), nil
		}
		if minute == "*" {
			return `<TimeTrigger>
      <Repetition>
        <Interval>PT1M</Interval>
        <StopAtDurationEnd>false</StopAtDurationEnd>
      </Repetition>
      <StartBoundary>2026-01-01T00:00:00</StartBoundary>
      <Enabled>true</Enabled>
    </TimeTrigger>`, nil
		}
	}

	// Patterns B-D require literal minute and hour
	m, err := strconv.Atoi(minute)
	if err != nil {
		return "", fmt.Errorf("Task Scheduler requires a literal minute (got %q)", minute)
	}
	h, err := strconv.Atoi(hour)
	if err != nil {
		return "", fmt.Errorf("Task Scheduler requires a literal hour (got %q)", hour)
	}

	// Pattern D: monthly on day D — "M H D * *"
	if day != "*" && month == "*" && weekday == "*" {
		d, err := strconv.Atoi(day)
		if err != nil {
			return "", fmt.Errorf("Task Scheduler requires a literal day (got %q)", day)
		}
		return fmt.Sprintf(`<CalendarTrigger>
      <StartBoundary>2026-01-01T%02d:%02d:00</StartBoundary>
      <Enabled>true</Enabled>
      <ScheduleByMonth>
        <DaysOfMonth>
          <Day>%d</Day>
        </DaysOfMonth>
        <Months>
          <January /><February /><March /><April /><May /><June />
          <July /><August /><September /><October /><November /><December />
        </Months>
      </ScheduleByMonth>
    </CalendarTrigger>`, h, m, d), nil
	}

	// Pattern C: weekly on day(s) — "M H * * D"
	if day == "*" && month == "*" && weekday != "*" {
		days, err := weekdayElements(weekday)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf(`<CalendarTrigger>
      <StartBoundary>2026-01-01T%02d:%02d:00</StartBoundary>
      <Enabled>true</Enabled>
      <ScheduleByWeek>
        <WeeksInterval>1</WeeksInterval>
        <DaysOfWeek>
%s
        </DaysOfWeek>
      </ScheduleByWeek>
    </CalendarTrigger>`, h, m, days), nil
	}

	// Pattern B: daily at HH:MM — "M H * * *"
	if day == "*" && month == "*" && weekday == "*" {
		return fmt.Sprintf(`<CalendarTrigger>
      <StartBoundary>2026-01-01T%02d:%02d:00</StartBoundary>
      <Enabled>true</Enabled>
      <ScheduleByDay>
        <DaysInterval>1</DaysInterval>
      </ScheduleByDay>
    </CalendarTrigger>`, h, m), nil
	}

	// Combinations like specific day-of-month + day-of-week, or month restrictions, are not supported.
	return "", fmt.Errorf("Task Scheduler cannot represent cron pattern %q (use simpler schedule)",
		fmt.Sprintf("%s %s %s %s %s", minute, hour, day, month, weekday))
}

// decodeUTF16LE returns the string from UTF-16 LE bytes (BOM optional).
func decodeUTF16LE(b []byte) string {
	if len(b) >= 2 && b[0] == 0xFF && b[1] == 0xFE {
		b = b[2:]
	}
	if len(b)%2 != 0 {
		return ""
	}
	u16 := make([]uint16, len(b)/2)
	for i := range u16 {
		u16[i] = uint16(b[2*i]) | uint16(b[2*i+1])<<8
	}
	return string(utf16.Decode(u16))
}

// encodeUTF16LE returns the UTF-16 LE bytes (with BOM) of s.
// Task Scheduler refuses XML files that don't match their declared encoding.
func encodeUTF16LE(s string) []byte {
	runes := utf16.Encode([]rune(s))
	buf := new(bytes.Buffer)
	buf.Write([]byte{0xFF, 0xFE}) // UTF-16 LE BOM
	for _, r := range runes {
		_ = binary.Write(buf, binary.LittleEndian, r)
	}
	return buf.Bytes()
}

func weekdayElements(weekday string) (string, error) {
	names := map[string]string{
		"0": "Sunday", "1": "Monday", "2": "Tuesday", "3": "Wednesday",
		"4": "Thursday", "5": "Friday", "6": "Saturday", "7": "Sunday",
	}
	var elems []string
	for _, p := range strings.Split(weekday, ",") {
		// Expand a-b ranges
		if strings.Contains(p, "-") {
			rng := strings.SplitN(p, "-", 2)
			a, err1 := strconv.Atoi(rng[0])
			b, err2 := strconv.Atoi(rng[1])
			if err1 != nil || err2 != nil || a > b {
				return "", fmt.Errorf("invalid weekday range %q", p)
			}
			for i := a; i <= b; i++ {
				name, ok := names[strconv.Itoa(i)]
				if !ok {
					return "", fmt.Errorf("invalid weekday %d", i)
				}
				elems = append(elems, "          <"+name+" />")
			}
			continue
		}
		name, ok := names[p]
		if !ok {
			return "", fmt.Errorf("invalid weekday %q", p)
		}
		elems = append(elems, "          <"+name+" />")
	}
	return strings.Join(elems, "\n"), nil
}
