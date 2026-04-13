// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package schedule

import (
	"fmt"
	"strconv"
	"strings"
)

// ValidateCron validates a 5-field cron expression (minute hour dom month dow).
// Returns the trimmed expression or an error.
func ValidateCron(expr string) (string, error) {
	expr = strings.TrimSpace(expr)
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return "", fmt.Errorf("expected 5 cron fields (minute hour day month weekday), got %d", len(fields))
	}
	bounds := []struct {
		name     string
		min, max int
	}{
		{"minute", 0, 59},
		{"hour", 0, 23},
		{"day", 1, 31},
		{"month", 1, 12},
		{"weekday", 0, 7},
	}
	for i, f := range fields {
		if err := validateField(f, bounds[i].min, bounds[i].max); err != nil {
			return "", fmt.Errorf("%s: %w", bounds[i].name, err)
		}
	}
	return strings.Join(fields, " "), nil
}

func validateField(f string, min, max int) error {
	if f == "*" {
		return nil
	}
	// */N step
	if strings.HasPrefix(f, "*/") {
		n, err := strconv.Atoi(f[2:])
		if err != nil || n <= 0 {
			return fmt.Errorf("invalid step %q", f)
		}
		return nil
	}
	// Comma-separated list
	for _, part := range strings.Split(f, ",") {
		// Range a-b
		if strings.Contains(part, "-") {
			rng := strings.SplitN(part, "-", 2)
			if len(rng) != 2 {
				return fmt.Errorf("invalid range %q", part)
			}
			a, err1 := strconv.Atoi(rng[0])
			b, err2 := strconv.Atoi(rng[1])
			if err1 != nil || err2 != nil || a < min || b > max || a > b {
				return fmt.Errorf("invalid range %q", part)
			}
			continue
		}
		n, err := strconv.Atoi(part)
		if err != nil || n < min || n > max {
			return fmt.Errorf("invalid value %q", part)
		}
	}
	return nil
}

// ToSystemdOnCalendar translates a cron expression to systemd OnCalendar format.
// Cron:    minute hour day month weekday
// Systemd: DayOfWeek Year-Month-Day Hour:Minute:Second
func ToSystemdOnCalendar(cron string) (string, error) {
	cron, err := ValidateCron(cron)
	if err != nil {
		return "", err
	}
	fields := strings.Fields(cron)
	minute, hour, day, month, weekday := fields[0], fields[1], fields[2], fields[3], fields[4]

	// Day of week
	dow := translateDOW(weekday)

	// Date part
	monthStr := translateMonth(month)
	dayStr := translateDay(day)

	// Time part
	hourStr := translateHourMinute(hour)
	minStr := translateHourMinute(minute)

	if dow == "" {
		return fmt.Sprintf("*-%s-%s %s:%s:00", monthStr, dayStr, hourStr, minStr), nil
	}
	return fmt.Sprintf("%s *-%s-%s %s:%s:00", dow, monthStr, dayStr, hourStr, minStr), nil
}

func translateHourMinute(f string) string {
	if f == "*" {
		return "*"
	}
	if strings.HasPrefix(f, "*/") {
		return "0/" + f[2:]
	}
	// Pad single-digit numbers (cron uses 0-23/0-59, systemd accepts both but pad for clarity)
	if n, err := strconv.Atoi(f); err == nil {
		return fmt.Sprintf("%02d", n)
	}
	return f
}

func translateDay(f string) string {
	if f == "*" {
		return "*"
	}
	if n, err := strconv.Atoi(f); err == nil {
		return fmt.Sprintf("%02d", n)
	}
	return f
}

func translateMonth(f string) string {
	if f == "*" {
		return "*"
	}
	if n, err := strconv.Atoi(f); err == nil {
		return fmt.Sprintf("%02d", n)
	}
	return f
}

func translateDOW(f string) string {
	if f == "*" {
		return ""
	}
	names := map[string]string{
		"0": "Sun", "1": "Mon", "2": "Tue", "3": "Wed",
		"4": "Thu", "5": "Fri", "6": "Sat", "7": "Sun",
	}
	var parts []string
	for _, p := range strings.Split(f, ",") {
		if name, ok := names[p]; ok {
			parts = append(parts, name)
		} else {
			return f // pass through unknown patterns
		}
	}
	return strings.Join(parts, ",")
}

// Describe returns a human-readable description of a cron expression.
func Describe(cron string) string {
	cron = strings.TrimSpace(cron)
	switch cron {
	case "* * * * *":
		return "every minute"
	case "*/5 * * * *":
		return "every 5 minutes"
	case "*/10 * * * *":
		return "every 10 minutes"
	case "*/15 * * * *":
		return "every 15 minutes"
	case "*/30 * * * *":
		return "every 30 minutes"
	case "0 * * * *":
		return "every hour"
	case "0 */2 * * *":
		return "every 2 hours"
	case "0 0 * * *":
		return "daily at midnight"
	case "0 9 * * *":
		return "daily at 09:00"
	case "0 0 * * 0":
		return "weekly on Sunday"
	case "0 0 1 * *":
		return "monthly on the 1st"
	}
	return cron
}
