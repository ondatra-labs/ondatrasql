// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package validate

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

// OutputFormat selects the rendering format for a validate Report.
type OutputFormat string

const (
	// OutputHuman is the default TTY-friendly format.
	OutputHuman OutputFormat = "human"
	// OutputJSON is a single buffered JSON object — the full report.
	OutputJSON OutputFormat = "json"
	// OutputNDJSON is one line per file plus a trailing summary line.
	// Suitable for streaming large projects.
	OutputNDJSON OutputFormat = "ndjson"
)

// Render writes the report to w in the requested format.
func Render(w io.Writer, r *Report, format OutputFormat) error {
	switch format {
	case OutputJSON:
		return renderJSON(w, r)
	case OutputNDJSON:
		return renderNDJSON(w, r)
	case OutputHuman, "":
		return renderHuman(w, r)
	default:
		return fmt.Errorf("unknown output format: %q", format)
	}
}

func renderJSON(w io.Writer, r *Report) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// renderNDJSON emits one JSON object per file, then a final summary object
// with key "summary". Stream-friendly for CI.
//
// Every per-file record carries schema_version inline so streaming
// consumers can pick the right parser for each line without buffering
// the whole stream and finding the version on the trailing summary
// (Codex round 4 finding: NDJSON consumers couldn't detect the schema
// from a single per-file record before).
func renderNDJSON(w io.Writer, r *Report) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for _, f := range r.Files {
		envelope := map[string]any{
			"schema_version": r.SchemaVersion,
			"path":           f.Path,
			"status":         f.Status,
		}
		if len(f.Checks) > 0 {
			envelope["checks"] = f.Checks
		}
		if len(f.Findings) > 0 {
			envelope["findings"] = f.Findings
		}
		if err := enc.Encode(envelope); err != nil {
			return err
		}
	}
	return enc.Encode(map[string]any{
		"schema_version": r.SchemaVersion,
		"summary":        r.Summary,
	})
}

// renderHuman renders a TTY-friendly view. Files are listed in scan order;
// findings are ordered by line number then severity (BLOCKER → WARN → INFO).
func renderHuman(w io.Writer, r *Report) error {
	for _, f := range r.Files {
		if _, err := fmt.Fprintln(w, f.Path); err != nil {
			return err
		}
		if len(f.Findings) == 0 {
			// Print check-by-check status for clean files.
			keys := make([]string, 0, len(f.Checks))
			for k := range f.Checks {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				if _, err := fmt.Fprintf(w, "  ✓ %s\n", k); err != nil {
					return err
				}
			}
			continue
		}
		findings := append([]Finding(nil), f.Findings...)
		sort.SliceStable(findings, func(i, j int) bool {
			if findings[i].Line != findings[j].Line {
				return findings[i].Line < findings[j].Line
			}
			return severityOrder(findings[i].Severity) < severityOrder(findings[j].Severity)
		})
		for _, fi := range findings {
			marker := markerFor(fi.Severity)
			loc := ""
			if fi.Line > 0 {
				loc = fmt.Sprintf(":%d", fi.Line)
			}
			if _, err := fmt.Fprintf(w, "  %s%s [%s %s] %s\n", marker, loc, fi.Severity, fi.Rule, fi.Message); err != nil {
				return err
			}
		}
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	_, err := fmt.Fprintf(w, "Summary: %d files, %d BLOCKER, %d WARN, %d INFO, %d clean\n",
		r.Summary.Files, r.Summary.Blockers, r.Summary.Warns, r.Summary.Infos, r.Summary.Clean)
	return err
}

func markerFor(s Severity) string {
	switch s {
	case SeverityBlocker:
		return "✗"
	case SeverityWarn:
		return "⚠"
	case SeverityInfo:
		return "ℹ"
	}
	return "•"
}

func severityOrder(s Severity) int {
	switch s {
	case SeverityBlocker:
		return 0
	case SeverityWarn:
		return 1
	case SeverityInfo:
		return 2
	}
	return 3
}

// ParseFormat returns the OutputFormat for a string flag value, defaulting
// to human if empty. Unknown values return an error.
func ParseFormat(s string) (OutputFormat, error) {
	switch strings.ToLower(s) {
	case "", "human":
		return OutputHuman, nil
	case "json":
		return OutputJSON, nil
	case "ndjson":
		return OutputNDJSON, nil
	}
	return "", fmt.Errorf("unknown output format: %q (want human|json|ndjson)", s)
}
