// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package validate is the static validation engine behind the `validate` CLI
// command. It runs the full set of compile-time checks (parser, strict-fetch,
// strict-push, DAG, blueprint, cross-AST) without ever opening a DuckDB
// session, and emits findings tagged with stable rule-IDs and severities.
//
// Public contract: rule-ID strings AND severity values are part of the API
// contract — changing either requires a SchemaVersion bump.
package validate

// SchemaVersion is the validate output schema version. Bump on any change
// to the JSON output structure, rule-ID set, or severity assignments —
// including additive ones, so typed clients have a signal that the
// rule set has grown and their switch-on-rule logic may need updating.
//
// Version history:
//   - 1: initial release (parser.*, strict_fetch.*, strict_push.*, read_only.*,
//     dag.*, blueprint.*, push.*, cross_ast.*)
//   - 2: added validate.* environmental diagnostics
//     (validate.extensions_load_failed, validate.blueprint_load_failed,
//     validate.builtin_introspection_failed)
//   - 3: removed parser.expose_on_non_table (OData support removed in v0.32)
const SchemaVersion = 3

// Severity classifies a Finding by how much attention it demands.
type Severity string

const (
	// SeverityBlocker means the model would fail at `run` today. Default
	// classification for any check that mirrors a run-time rejection.
	SeverityBlocker Severity = "BLOCKER"
	// SeverityWarn signals that validate's own analysis was degraded
	// (extension load failed, blueprint parse error, walk skipped,
	// builtin introspection failed, lineage extract failed). Triggers
	// exit code 1 unconditionally — a degraded run can't be treated
	// as clean by CI, regardless of `--strict`. Deprecated directives
	// are NOT WARN; they are BLOCKER (parser rejects them outright at
	// run-time, see severityOverrides comment in rules.go).
	SeverityWarn Severity = "WARN"
	// SeverityInfo means the validator could not verify a property without
	// extern state (e.g. external table references, helper-libs without
	// API dict). Never affects exit code.
	SeverityInfo Severity = "INFO"
)

// Finding is a single validation result for a single source location.
//
// The JSON encoding is part of the public output contract:
//
//	{"path": "...", "line": 5, "severity": "BLOCKER", "rule": "...", "message": "..."}
//
// Field order matches struct order so encoding/json gives deterministic
// output across runs (G5).
type Finding struct {
	Path     string   `json:"path"`
	Line     int      `json:"line,omitempty"`
	Severity Severity `json:"severity"`
	Rule     RuleID   `json:"rule"`
	Message  string   `json:"message"`
}

// FileResult aggregates findings for a single source file plus a per-check
// status map ("parser": "ok", "strict-fetch": "fail") for the human renderer.
//
//lintcheck:nojsonversion nested under Report.Files; renderNDJSON wraps each instance with schema_version inline so per-line consumers don't lose the contract.
type FileResult struct {
	Path     string            `json:"path"`
	Status   string            `json:"status"` // "ok" | "fail"
	Checks   map[string]string `json:"checks,omitempty"`
	Findings []Finding         `json:"findings,omitempty"`
}

// Report is the top-level validate output. SchemaVersion is always emitted
// so consumers can detect breaking changes.
type Report struct {
	SchemaVersion int          `json:"schema_version"`
	Files         []FileResult `json:"files"`
	Summary       Summary      `json:"summary"`
}

// Summary aggregates findings counts across all files.
type Summary struct {
	Files    int `json:"files"`
	Blockers int `json:"blockers"`
	Warns    int `json:"warns"`
	Infos    int `json:"infos"`
	Clean    int `json:"clean"`
}

// HasBlockers returns true if any file in the report has BLOCKER findings.
func (r *Report) HasBlockers() bool {
	return r.Summary.Blockers > 0
}

// HasWarns returns true if any file has WARN findings.
func (r *Report) HasWarns() bool {
	return r.Summary.Warns > 0
}

// AddFile appends a FileResult to the report and updates Summary counts.
// Use this rather than mutating Files directly so counts stay in sync.
func (r *Report) AddFile(fr FileResult) {
	r.Files = append(r.Files, fr)
	r.Summary.Files++
	if len(fr.Findings) == 0 {
		r.Summary.Clean++
		return
	}
	for _, f := range fr.Findings {
		switch f.Severity {
		case SeverityBlocker:
			r.Summary.Blockers++
		case SeverityWarn:
			r.Summary.Warns++
		case SeverityInfo:
			r.Summary.Infos++
		}
	}
}

// NewReport returns an empty Report with the current SchemaVersion set.
func NewReport() *Report {
	return &Report{SchemaVersion: SchemaVersion}
}
