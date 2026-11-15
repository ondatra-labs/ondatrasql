// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package deprecation is the one way a user-facing feature (a directive, a
// lib function or module member, a CLI flag or command, a config setting)
// leaves OndatraSQL. A feature is registered in registry.go with the minor
// version that deprecated it; the running version decides its level:
//
//   - WarnMinors minors from that version on, the feature still works and
//     every use is reported as a warning naming the replacement;
//   - ErrorMinors minors after that, a use fails with the same message;
//   - after that the code goes, and the guard test in this package fails
//     the build until it has.
//
// Each feature has one call site, Use, at the place that still accepts the
// old form. Policy and list: docs/support/deprecations.md.
package deprecation

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// WarnMinors and ErrorMinors are the lengths of the warning and error
// phases in minor versions.
const (
	WarnMinors  = 3
	ErrorMinors = 3
)

// DocsURL is the page that explains every registered feature; a feature's
// Anchor is a heading on it.
const DocsURL = "https://ondatra.sh/support/deprecations/"

// ID names a registered feature. Stable: it appears in validate output.
type ID string

// Kind is the sort of user-facing feature.
type Kind string

const (
	KindDirective Kind = "directive"
	KindLib       Kind = "lib"
	KindCLI       Kind = "cli"
	KindConfig    Kind = "config"
)

// Feature is one registered deprecation.
type Feature struct {
	ID          ID
	Kind        Kind
	Old         string // the deprecated form as the user writes it, e.g. "@sink"
	Replacement string // what to write instead, e.g. "@push"
	Since       string // minor version that deprecated it, e.g. "0.48"
	Anchor      string // heading anchor on DocsURL
}

// Level is a feature's phase at a given version.
type Level int

const (
	LevelWarn Level = iota
	LevelError
	LevelRemoved
)

func (l Level) String() string {
	switch l {
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	default:
		return "removed"
	}
}

// Version is a parsed MAJOR.MINOR[.PATCH] with an optional leading "v";
// a pre-release or build suffix is ignored.
type Version struct{ Major, Minor, Patch int }

// ParseVersion parses "0.47", "0.47.1", "v0.47.1" or "0.48.0-rc1".
func ParseVersion(s string) (Version, error) {
	core := strings.TrimPrefix(strings.TrimSpace(s), "v")
	if i := strings.IndexAny(core, "-+"); i >= 0 {
		core = core[:i]
	}
	parts := strings.Split(core, ".")
	if len(parts) < 2 || len(parts) > 3 {
		return Version{}, fmt.Errorf("version %q is not MAJOR.MINOR[.PATCH]", s)
	}
	nums := make([]int, 3)
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return Version{}, fmt.Errorf("version %q is not MAJOR.MINOR[.PATCH]", s)
		}
		nums[i] = n
	}
	return Version{nums[0], nums[1], nums[2]}, nil
}

// Less reports whether v is an older version than w.
func (v Version) Less(w Version) bool {
	if v.Major != w.Major {
		return v.Major < w.Major
	}
	if v.Minor != w.Minor {
		return v.Minor < w.Minor
	}
	return v.Patch < w.Patch
}

func (v Version) String() string { return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch) }

func (v Version) minorString() string { return fmt.Sprintf("%d.%d", v.Major, v.Minor) }

func (f Feature) since() Version {
	v, err := ParseVersion(f.Since)
	if err != nil {
		// The registry test rejects a bad Since; keep running regardless.
		return Version{}
	}
	return v
}

// ErrorIn is the minor version from which a use fails.
func (f Feature) ErrorIn() string {
	v := f.since()
	return Version{v.Major, v.Minor + WarnMinors, 0}.minorString()
}

// RemovedIn is the minor version that no longer has the code.
func (f Feature) RemovedIn() string {
	v := f.since()
	return Version{v.Major, v.Minor + WarnMinors + ErrorMinors, 0}.minorString()
}

// LevelAt derives f's level at the given running version. A version that
// does not parse (a dev build stamped "dev") or is older than Since gives
// LevelWarn: an unknown version never breaks a project.
func (f Feature) LevelAt(running string) Level {
	cur, err := ParseVersion(running)
	if err != nil {
		return LevelWarn
	}
	since := f.since()
	if cur.Major > since.Major {
		return LevelRemoved
	}
	if cur.Major < since.Major {
		return LevelWarn
	}
	switch d := cur.Minor - since.Minor; {
	case d < WarnMinors:
		return LevelWarn
	case d < WarnMinors+ErrorMinors:
		return LevelError
	default:
		return LevelRemoved
	}
}

// Message is the text a use of f is reported with at the given level. The
// warning and the error name the same replacement and docs anchor.
func (f Feature) Message(l Level) string {
	link := DocsURL + "#" + f.Anchor
	if l == LevelWarn {
		return fmt.Sprintf("%s is deprecated since %s and stops working in %s; replace it with %s (%s)",
			f.Old, f.since().minorString(), f.ErrorIn(), f.Replacement, link)
	}
	return fmt.Sprintf("%s no longer works since %s (deprecated in %s, removed in %s); replace it with %s (%s)",
		f.Old, f.ErrorIn(), f.since().minorString(), f.RemovedIn(), f.Replacement, link)
}

// Error is returned by Use for a feature in its error phase (or past it, in
// a binary that still has the code).
type Error struct {
	Feature Feature
	Level   Level
}

func (e *Error) Error() string { return e.Feature.Message(e.Level) }

// IsError reports whether err holds a deprecation Error.
func IsError(err error) bool {
	var de *Error
	return errors.As(err, &de)
}

// Usage is one recorded use of a feature.
type Usage struct {
	Feature Feature
	Level   Level
	Path    string // project-relative file, "" for a CLI flag or command
	Line    int    // 0 when unknown
}

// Message is the report text for u.
func (u Usage) Message() string { return u.Feature.Message(u.Level) }

var (
	mu       sync.Mutex
	running  string
	features = map[ID]Feature{}
	usages   []Usage
	reported = map[ID]bool{}
	reporter func(Usage)
)

func init() {
	for _, f := range registry {
		features[f.ID] = f
	}
}

// Features returns the registered features in registry order.
func Features() []Feature {
	return append([]Feature(nil), registry...)
}

// SetVersion sets the running version that levels are derived from. main
// calls it with the binary's version before any command runs.
func SetVersion(v string) {
	mu.Lock()
	defer mu.Unlock()
	running = v
}

// SetReporter installs the function a warning-phase use is reported to, at
// most once per feature until the next Reset. nil reports nothing (the
// uses are still recorded for Drain). Error-phase uses are not reported:
// Use returns them and the call site fails with the message.
func SetReporter(fn func(Usage)) {
	mu.Lock()
	defer mu.Unlock()
	reporter = fn
}

// Reset forgets recorded and reported uses; main calls it at the start of
// every command so one in-process run does not leak into the next.
func Reset() {
	mu.Lock()
	defer mu.Unlock()
	usages = nil
	reported = map[ID]bool{}
}

// Drain returns the uses recorded since the last Drain or Reset and
// forgets them.
func Drain() []Usage {
	mu.Lock()
	defer mu.Unlock()
	out := usages
	usages = nil
	return out
}

// Use is the single call site of a deprecated feature, placed where the old
// form is still accepted. It records the use and returns nil while the
// feature is in its warning phase; after that it returns an *Error the
// caller must fail with. path is the project file the old form is in ("" for
// a CLI flag or command), line its 1-based line or 0.
//
// An ID that is not registered is a programming error and panics; the
// registry test catches it before a release.
func Use(id ID, path string, line int) error {
	mu.Lock()
	f, ok := features[id]
	if !ok {
		mu.Unlock()
		panic(fmt.Sprintf("deprecation: %q is not registered", id))
	}
	u := Usage{Feature: f, Level: f.LevelAt(running), Path: path, Line: line}
	usages = append(usages, u)
	var report func(Usage)
	if u.Level == LevelWarn && !reported[id] {
		reported[id] = true
		report = reporter
	}
	mu.Unlock()
	if report != nil {
		report(u)
	}
	if u.Level != LevelWarn {
		return &Error{Feature: f, Level: u.Level}
	}
	return nil
}

// Swap replaces the registry with fs and returns a function that restores
// it. For tests in other packages that need a feature with a known Since;
// production code never calls it.
func Swap(fs []Feature) (restore func()) {
	mu.Lock()
	defer mu.Unlock()
	saved := features
	features = map[ID]Feature{}
	for _, f := range fs {
		features[f.ID] = f
	}
	return func() {
		mu.Lock()
		defer mu.Unlock()
		features = saved
	}
}
