// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build blueprints || e2e

package blueprinttest

// The blueprint harness. It runs the ondatrasql binary, built once from
// this checkout or named by ONDATRASQL_BIN, against a copy of a blueprint
// in a temporary project:
//
//   - check: validate --strict is clean, the page's frontmatter agrees
//     with --json describe blueprint, .env.example names the auth
//     variables, and the page shows every code file;
//   - replay: each run listed in <recording>/expect.yaml is replayed from
//     <recording>/cassettes/ with that run's ONDATRA_NOW, and its tables,
//     row counts, sample rows, run types and request count asserted;
//   - record: the same runs against the live API, writing the cassettes.
//
// Every function returns its findings as an error instead of failing the
// test, so the self-test can show that a broken blueprint is caught.

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/goccy/go-yaml"
)

// EnvTestsDir names the directory holding the private recordings:
// <dir>/<blueprint>/cassettes/ and <dir>/<blueprint>/expect.yaml.
const EnvTestsDir = "BLUEPRINT_TESTS_DIR"

// EnvBinary names an ondatrasql binary to run instead of building
// cmd/ondatrasql: CI sets it to the latest released binary.
const EnvBinary = "ONDATRASQL_BIN"

var (
	binOnce sync.Once
	binDir  string
	binPath string
	binErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if binDir != "" {
		_ = os.RemoveAll(binDir) // best-effort removal of the built binary
	}
	os.Exit(code)
}

// ondatrasqlBinary builds cmd/ondatrasql from this checkout once per test
// process, so the harness runs the CLI exactly as a user would. With
// ONDATRASQL_BIN set it returns that binary instead.
func ondatrasqlBinary(t testing.TB) string {
	t.Helper()
	binOnce.Do(func() {
		if bin := os.Getenv(EnvBinary); bin != "" {
			binPath, binErr = filepath.Abs(bin)
			return
		}
		binDir, binErr = os.MkdirTemp("", "ondatrasql-bp-")
		if binErr != nil {
			return
		}
		binPath = filepath.Join(binDir, "ondatrasql")
		cmd := exec.Command("go", "build", "-o", binPath, "./cmd/ondatrasql")
		cmd.Dir = "../.."
		if out, err := cmd.CombinedOutput(); err != nil {
			binErr = fmt.Errorf("go build ./cmd/ondatrasql: %v\n%s", err, out)
		}
	})
	if binErr != nil {
		t.Fatal(binErr)
	}
	return binPath
}

// harness runs one ondatrasql binary against blueprints that take their
// local config from shared (blueprints/_shared). mainVersion is the
// version in cmd/ondatrasql/main.go, vX.Y.Z: no min_version is above it.
type harness struct {
	bin         string
	shared      string
	mainVersion string
}

// ---------------------------------------------------------------------
// Versions: min_version, main's and the binary's
// ---------------------------------------------------------------------

// versionRe is vX.Y.Z, the form of a release tag and of min_version.
var versionRe = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

func validVersion(v string) bool { return versionRe.MatchString(v) }

// compareVersions compares two valid versions like strings.Compare.
func compareVersions(a, b string) int {
	parse := func(v string) []int {
		var n []int
		for _, s := range versionRe.FindStringSubmatch(v)[1:] {
			i, _ := strconv.Atoi(s) // versionRe matched digits
			n = append(n, i)
		}
		return n
	}
	return slices.Compare(parse(a), parse(b))
}

var mainVersionRe = regexp.MustCompile(`(?m)^var version = "([^"]+)"$`)

// mainVersion reads the version cmd/ondatrasql/main.go carries on this
// checkout, as vX.Y.Z.
func mainVersion(t testing.TB) string {
	t.Helper()
	data, err := os.ReadFile("../../cmd/ondatrasql/main.go")
	if err != nil {
		t.Fatal(err)
	}
	m := mainVersionRe.FindSubmatch(data)
	if m == nil || !validVersion("v"+string(m[1])) {
		t.Fatal(`cmd/ondatrasql/main.go: no var version = "X.Y.Z"`)
	}
	return "v" + string(m[1])
}

// checkMinVersion: min_version is required, vX.Y.Z, and at most main's
// version, so a blueprint never needs a version that does not exist.
func checkMinVersion(minVersion, mainVersion string) error {
	switch {
	case minVersion == "":
		return errors.New("frontmatter min_version is missing (vX.Y.Z, the oldest release that runs the blueprint)")
	case !validVersion(minVersion):
		return fmt.Errorf("frontmatter min_version %q is not vX.Y.Z", minVersion)
	case compareVersions(minVersion, mainVersion) > 0:
		return fmt.Errorf("frontmatter min_version %s is above %s, the version in cmd/ondatrasql/main.go; a blueprint waits for the release it needs and never bumps the version (blueprints/README.md)", minVersion, mainVersion)
	}
	return nil
}

// ---------------------------------------------------------------------
// Projects and commands
// ---------------------------------------------------------------------

func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
}

// project is a blueprint copied into a fresh directory with the shared
// config, as a user gets it after ondatrasql init and copying lib/ and
// models/ in.
type project struct {
	dir string
	env []string // the variables every command of this project gets
}

func (h *harness) newProject(t testing.TB, bpDir string) (*project, error) {
	t.Helper()
	dir := t.TempDir()
	if err := copyTree(h.shared, dir); err != nil {
		return nil, err
	}
	for _, sub := range []string{"lib", "models"} {
		if err := copyTree(filepath.Join(bpDir, sub), filepath.Join(dir, sub)); err != nil {
			return nil, err
		}
	}
	key := make([]byte, 32)
	_, _ = rand.Read(key) // crypto/rand.Read never returns an error
	return &project{dir: dir, env: []string{"ONDATRA_STATE_KEY=" + base64.StdEncoding.EncodeToString(key)}}, nil
}

// cliEnv is the environment of the test process without the variables
// that change what ondatrasql does (record/replay, clock, state key) and
// without drop, plus extra.
func cliEnv(drop []string, extra []string) []string {
	skip := map[string]bool{
		"ONDATRA_HTTP_CASSETTE": true, "ONDATRA_HTTP_MODE": true,
		"ONDATRA_NOW": true, "ONDATRA_STATE_KEY": true,
	}
	for _, d := range drop {
		skip[d] = true
	}
	var env []string
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		if !skip[name] {
			env = append(env, kv)
		}
	}
	return append(env, extra...)
}

// exec runs the binary in the project with env (from cliEnv) and returns
// stdout, stderr and the error.
func (h *harness) exec(p *project, env []string, args ...string) (string, string, error) {
	cmd := exec.Command(h.bin, args...)
	cmd.Dir = p.dir
	cmd.Env = append(append([]string(nil), env...), p.env...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	return out.String(), errb.String(), err
}

// sqlRows runs a query with --json sql and returns its rows.
func (h *harness) sqlRows(p *project, env []string, query string) ([]map[string]any, error) {
	stdout, stderr, err := h.exec(p, env, "--json", "sql", query)
	if err != nil {
		return nil, fmt.Errorf("sql %q: %v\n%s%s", query, err, stdout, stderr)
	}
	var res struct {
		Rows []map[string]any `json:"rows"`
	}
	if err := json.Unmarshal([]byte(stdout), &res); err != nil {
		return nil, fmt.Errorf("sql %q: decode %v: %s", query, err, stdout)
	}
	return res.Rows, nil
}

// ---------------------------------------------------------------------
// Static check: validate, page, describe blueprint
// ---------------------------------------------------------------------

// frontmatter is the Hugo frontmatter of a blueprint's index.md.
type frontmatter struct {
	Title       string   `yaml:"title"`
	Description string   `yaml:"description"`
	Vendor      string   `yaml:"vendor"`
	Tags        []string `yaml:"tags"`
	AuthMethod  string   `yaml:"auth_method"`
	SourceType  string   `yaml:"source_type"`
	ModelKind   string   `yaml:"model_kind"`
	Args        []string `yaml:"args"`
	MinVersion  string   `yaml:"min_version"`
}

// describedBlueprint is the part of --json describe blueprint <name> the
// page is compared with.
type describedBlueprint struct {
	Name  string         `json:"name"`
	Auth  map[string]any `json:"auth"`
	Fetch *struct {
		Args           []string `json:"args"`
		SupportedKinds []string `json:"supported_kinds"`
	} `json:"fetch"`
	Push *struct {
		Args           []string `json:"args"`
		SupportedKinds []string `json:"supported_kinds"`
	} `json:"push"`
}

var bpFileRef = regexp.MustCompile(`\{\{<\s*bp-file\s+"([^"]+)"\s*>\}\}`)

func readPage(bpDir string) (frontmatter, string, error) {
	var fm frontmatter
	data, err := os.ReadFile(filepath.Join(bpDir, "index.md"))
	if err != nil {
		return fm, "", err
	}
	s := strings.ReplaceAll(string(data), "\r\n", "\n")
	if !strings.HasPrefix(s, "---\n") {
		return fm, "", errors.New("index.md: no YAML frontmatter")
	}
	head, body, ok := strings.Cut(s[4:], "\n---\n")
	if !ok {
		return fm, "", errors.New("index.md: frontmatter not closed with ---")
	}
	if err := yaml.UnmarshalWithOptions([]byte(head), &fm, yaml.Strict()); err != nil {
		return fm, "", fmt.Errorf("index.md frontmatter: %w", err)
	}
	return fm, body, nil
}

// authMethod is the frontmatter value for an API dict's auth.
func authMethod(auth map[string]any) string {
	switch {
	case len(auth) == 0:
		return "none"
	case auth["provider"] != nil:
		return "oauth"
	case auth["service_account"] != nil:
		return "service_account"
	case auth["user"] != nil || auth["pass"] != nil:
		return "basic"
	case auth["env"] != nil:
		return "api_key"
	}
	return "unknown"
}

// authEnvVars lists the environment variables an API dict's auth reads.
func authEnvVars(auth map[string]any) []string {
	var out []string
	var walk func(v any)
	walk = func(v any) {
		if m, ok := v.(map[string]any); ok {
			if s, ok := m["env"].(string); ok {
				out = append(out, s)
			}
			for _, sub := range m {
				walk(sub)
			}
		}
	}
	walk(auth)
	if p, ok := auth["provider"].(string); ok {
		out = append(out, "ONDATRA_OAUTH_TOKEN_"+strings.ToUpper(strings.ReplaceAll(p, "-", "_")))
	}
	sort.Strings(out)
	return slices.Compact(out)
}

// envExampleKeys reads the variable names of a .env.example and reports
// any that has a value: the file names what is required, never a value.
func envExampleKeys(bpDir string) ([]string, error) {
	data, err := os.ReadFile(filepath.Join(bpDir, ".env.example"))
	if err != nil {
		return nil, err
	}
	var keys []string
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, val, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf(".env.example: %q is not NAME=", line)
		}
		if strings.TrimSpace(val) != "" {
			return nil, fmt.Errorf(".env.example: %s has a value; leave it empty", name)
		}
		keys = append(keys, strings.TrimSpace(name))
	}
	return keys, sc.Err()
}

var (
	kindDirective  = regexp.MustCompile(`(?m)^--\s*@kind:\s*(\w+)`)
	fetchDirective = regexp.MustCompile(`(?m)^--\s*@fetch\s*$`)
)

// libModelKinds returns, per model file that calls lib (a @fetch model
// with lib( in it, or @push: lib), its @kind.
func libModelKinds(bpDir, lib string) (map[string]string, error) {
	pushRe := regexp.MustCompile(`(?m)^--\s*@push:\s*` + regexp.QuoteMeta(lib) + `\s*$`)
	callRe := regexp.MustCompile(`\b` + regexp.QuoteMeta(lib) + `\s*\(`)
	out := map[string]string{}
	err := filepath.WalkDir(filepath.Join(bpDir, "models"), func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".sql") {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		s := string(data)
		if pushRe.MatchString(s) || (fetchDirective.MatchString(s) && callRe.MatchString(s)) {
			kind := "table"
			if m := kindDirective.FindStringSubmatch(s); m != nil {
				kind = m[1]
			}
			rel, _ := filepath.Rel(bpDir, path) // path is under bpDir
			out[filepath.ToSlash(rel)] = kind
		}
		return nil
	})
	return out, err
}

// check is the offline check every blueprint gets on every PR.
func (h *harness) check(t testing.TB, bpDir string) error {
	t.Helper()
	var errs []error
	fail := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	p, err := h.newProject(t, bpDir)
	if err != nil {
		return err
	}
	env := cliEnv(nil, nil)

	// validate --strict: 0 BLOCKER, 0 WARN.
	stdout, stderr, err := h.exec(p, env, "validate", "--strict", "--output=json")
	var rep struct {
		Files []struct {
			Path   string            `json:"path"`
			Status string            `json:"status"`
			Checks map[string]string `json:"checks"`
		} `json:"files"`
		Summary struct {
			Files    int `json:"files"`
			Blockers int `json:"blockers"`
			Warns    int `json:"warns"`
		} `json:"summary"`
	}
	if jerr := json.Unmarshal([]byte(stdout), &rep); jerr != nil {
		fail("validate --strict: %v (%v)\n%s%s", err, jerr, stdout, stderr)
	} else if err != nil || rep.Summary.Blockers != 0 || rep.Summary.Warns != 0 || rep.Summary.Files == 0 {
		fail("validate --strict: %d files, %d BLOCKER, %d WARN (exit %v)\n%s%s", rep.Summary.Files, rep.Summary.Blockers, rep.Summary.Warns, err, stdout, stderr)
	}

	// The page.
	fm, body, err := readPage(bpDir)
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	for field, v := range map[string]string{"title": fm.Title, "description": fm.Description, "vendor": fm.Vendor,
		"auth_method": fm.AuthMethod, "source_type": fm.SourceType, "model_kind": fm.ModelKind} {
		if strings.TrimSpace(v) == "" {
			fail("index.md: frontmatter %s is empty", field)
		}
	}
	if len(fm.Tags) == 0 {
		fail("index.md: frontmatter tags is empty")
	}
	if fm.Args == nil {
		fail("index.md: frontmatter args is missing (use [] for none)")
	}
	if err := checkMinVersion(fm.MinVersion, h.mainVersion); err != nil {
		fail("index.md: %v", err)
	}

	// Every code file is shown on the page, and every reference exists.
	refs := map[string]bool{}
	for _, m := range bpFileRef.FindAllStringSubmatch(body, -1) {
		refs[m[1]] = true
		if _, err := os.Stat(filepath.Join(bpDir, filepath.FromSlash(m[1]))); err != nil {
			fail("index.md: bp-file %q does not exist", m[1])
		}
	}
	for _, sub := range []string{"lib", "models"} {
		_ = filepath.WalkDir(filepath.Join(bpDir, sub), func(path string, d fs.DirEntry, err error) error {
			if err == nil && !d.IsDir() {
				rel, _ := filepath.Rel(bpDir, path) // path is under bpDir
				if !refs[filepath.ToSlash(rel)] {
					fail("index.md: %s is not shown; add {{< bp-file %q >}}", filepath.ToSlash(rel), filepath.ToSlash(rel))
				}
			}
			return err
		}) // a walk error leaves files unlisted, which the checks above report
	}

	// describe blueprint: exactly one lib with an API dict.
	stdout, stderr, err = h.exec(p, env, "--json", "describe", "blueprint")
	var list struct {
		Blueprints []struct {
			Name string `json:"name"`
		} `json:"blueprints"`
	}
	if err != nil || json.Unmarshal([]byte(stdout), &list) != nil {
		return errors.Join(append(errs, fmt.Errorf("describe blueprint: %v\n%s%s", err, stdout, stderr))...)
	}
	if len(list.Blueprints) != 1 {
		return errors.Join(append(errs, fmt.Errorf("describe blueprint lists %d libs, want exactly 1: %s", len(list.Blueprints), stdout))...)
	}
	lib := list.Blueprints[0].Name
	stdout, stderr, err = h.exec(p, env, "--json", "describe", "blueprint", lib)
	var desc describedBlueprint
	if err != nil || json.Unmarshal([]byte(stdout), &desc) != nil {
		return errors.Join(append(errs, fmt.Errorf("describe blueprint %s: %v\n%s%s", lib, err, stdout, stderr))...)
	}

	var args, kinds []string
	switch {
	case desc.Fetch != nil && desc.Push != nil:
		fail("describe blueprint %s: both fetch and push; a blueprint page describes one", lib)
	case desc.Fetch != nil:
		args, kinds = desc.Fetch.Args, desc.Fetch.SupportedKinds
		if fm.SourceType != "fetch" {
			fail("index.md: source_type %q, describe blueprint %s has a fetch section", fm.SourceType, lib)
		}
	case desc.Push != nil:
		args, kinds = desc.Push.Args, desc.Push.SupportedKinds
		if fm.SourceType != "push" {
			fail("index.md: source_type %q, describe blueprint %s has a push section", fm.SourceType, lib)
		}
	default:
		fail("describe blueprint %s: neither fetch nor push", lib)
	}
	if !slices.Equal(fm.Args, args) && !(len(fm.Args) == 0 && len(args) == 0) {
		fail("index.md: args %q, describe blueprint %s has %q", fm.Args, lib, args)
	}
	if want := authMethod(desc.Auth); fm.AuthMethod != want {
		fail("index.md: auth_method %q, describe blueprint %s has auth %v (%s)", fm.AuthMethod, lib, desc.Auth, want)
	}
	if len(kinds) > 0 && !slices.Contains(kinds, fm.ModelKind) {
		fail("index.md: model_kind %q is not in describe blueprint %s supported_kinds %q", fm.ModelKind, lib, kinds)
	}
	models, err := libModelKinds(bpDir, lib)
	if err != nil {
		fail("models: %v", err)
	}
	if len(models) == 0 {
		fail("models: none calls %s", lib)
	}
	for path, kind := range models {
		if kind != fm.ModelKind {
			fail("index.md: model_kind %q, %s is @kind: %s", fm.ModelKind, path, kind)
		}
	}

	// .env.example names every variable the auth reads.
	keys, err := envExampleKeys(bpDir)
	if err != nil {
		fail("%v", err)
	}
	for _, v := range authEnvVars(desc.Auth) {
		if !slices.Contains(keys, v) && !strings.HasPrefix(v, "ONDATRA_OAUTH_TOKEN_") {
			fail(".env.example: %s (read by the API dict's auth) is missing", v)
		}
	}
	return errors.Join(errs...)
}

// ---------------------------------------------------------------------
// expect.yaml
// ---------------------------------------------------------------------

// expectFile is <recording>/expect.yaml: the runs to make, in order, each
// in the same project, and what each must leave behind.
type expectFile struct {
	Runs []expectRun `yaml:"runs"`
}

type expectRun struct {
	// Now is the run's ONDATRA_NOW (RFC 3339), the one it was recorded with.
	Now string `yaml:"now"`
	// Cassettes is the subdirectory of cassettes/ this run replays from,
	// for a later run that makes requests the first did not; empty means
	// cassettes/ itself.
	Cassettes string `yaml:"cassettes,omitempty"`
	// Requests is how many recorded interactions the run must use.
	Requests *int `yaml:"requests,omitempty"`
	// Models maps a model to its run type and rows affected.
	Models map[string]expectModel `yaml:"models,omitempty"`
	// Tables lists every table the project has after the run.
	Tables map[string]expectTable `yaml:"tables"`
}

type expectModel struct {
	RunType string `yaml:"run_type,omitempty"`
	Rows    *int64 `yaml:"rows,omitempty"`
	// Warnings the model may print, each matched as a substring. Any
	// other warning fails the run: a push batch the recording does not
	// match ends as a warning, not an error.
	Warnings []string `yaml:"warnings,omitempty"`
}

type expectTable struct {
	// Columns are "name TYPE", in table order.
	Columns []string `yaml:"columns,omitempty"`
	Rows    *int     `yaml:"rows"`
	// Sample rows must each be in the table: values as DuckDB prints
	// them cast to VARCHAR, null for NULL. A row may name a subset of
	// the columns.
	Sample []map[string]any `yaml:"sample,omitempty"`
}

func readExpect(path string) (*expectFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var e expectFile
	if err := yaml.UnmarshalWithOptions(data, &e, yaml.Strict()); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if len(e.Runs) == 0 {
		return nil, fmt.Errorf("%s: no runs", path)
	}
	for i, r := range e.Runs {
		if _, err := time.Parse(time.RFC3339, r.Now); err != nil {
			return nil, fmt.Errorf("%s: run %d: now %q is not RFC 3339", path, i+1, r.Now)
		}
		if r.Cassettes != "" && (i == 0 || !filepath.IsLocal(r.Cassettes)) {
			return nil, fmt.Errorf("%s: run %d: cassettes %q must be a subdirectory, and only on a later run", path, i+1, r.Cassettes)
		}
		for name, tb := range r.Tables {
			if tb.Rows == nil {
				return nil, fmt.Errorf("%s: run %d: table %s has no rows count", path, i+1, name)
			}
		}
	}
	return &e, nil
}

// ---------------------------------------------------------------------
// Running: replay and record
// ---------------------------------------------------------------------

// runOutcome is what one ondatrasql --json run printed.
type runOutcome struct {
	models   map[string]modelResult
	requests int
	sawCount bool
	stdout   string
	stderr   string
}

type modelResult struct {
	Model        string   `json:"model"`
	Kind         string   `json:"kind"`
	RunType      string   `json:"run_type"`
	RowsAffected int64    `json:"rows_affected"`
	Status       string   `json:"status"`
	Errors       []string `json:"errors"`
	Warnings     []string `json:"warnings"`
}

// doRun makes one ondatrasql --json run in mode (record or replay) with
// cassette dir cas and ONDATRA_NOW now. drop are variables to unset.
func (h *harness) doRun(p *project, mode, cas, now string, drop []string) (*runOutcome, error) {
	env := cliEnv(drop, []string{
		"ONDATRA_HTTP_MODE=" + mode,
		"ONDATRA_HTTP_CASSETTE=" + cas,
		"ONDATRA_NOW=" + now,
	})
	stdout, stderr, err := h.exec(p, env, "--json", "run")
	out := &runOutcome{models: map[string]modelResult{}, stdout: stdout, stderr: stderr}
	for _, line := range strings.Split(stdout, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var head struct {
			Kind     string `json:"kind"`
			Model    string `json:"model"`
			Requests int    `json:"requests"`
		}
		if json.Unmarshal([]byte(line), &head) != nil {
			continue
		}
		switch {
		case head.Kind == "http_replay":
			out.requests, out.sawCount = head.Requests, true
		case head.Model != "":
			var r modelResult
			if json.Unmarshal([]byte(line), &r) == nil {
				out.models[r.Model] = r
			}
		}
	}
	var errs []error
	if err != nil {
		errs = append(errs, fmt.Errorf("ondatrasql run: %v", err))
	}
	for _, name := range sortedKeys(out.models) {
		if r := out.models[name]; (r.Status != "ok" && r.Status != "skip") || len(r.Errors) > 0 {
			errs = append(errs, fmt.Errorf("%s: status %s: %s", name, r.Status, strings.Join(r.Errors, "; ")))
		}
	}
	if len(errs) > 0 {
		return out, fmt.Errorf("%w\n--- stdout\n%s--- stderr\n%s", errors.Join(errs...), stdout, stderr)
	}
	return out, nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// tableState is one table as the harness sees it after a run.
type tableState struct {
	columns []string
	rows    []map[string]any // every row, values cast to VARCHAR
}

// tables reads every user table of the project: columns with types and
// all rows with values cast to VARCHAR, ordered by every column.
func (h *harness) tables(p *project) (map[string]*tableState, error) {
	env := cliEnv(nil, nil)
	cols, err := h.sqlRows(p, env, `SELECT table_schema || '.' || table_name AS t, column_name AS c, data_type AS ty
		FROM information_schema.columns
		WHERE table_catalog = 'lake' AND table_schema <> 'main'
		ORDER BY table_schema, table_name, ordinal_position`)
	if err != nil {
		return nil, err
	}
	out := map[string]*tableState{}
	var order []string
	for _, r := range cols {
		name := fmt.Sprint(r["t"])
		if out[name] == nil {
			out[name] = &tableState{}
			order = append(order, name)
		}
		out[name].columns = append(out[name].columns, fmt.Sprint(r["c"])+" "+fmt.Sprint(r["ty"]))
	}
	for _, name := range order {
		ts := out[name]
		var sel, ord []string
		for i, c := range ts.columns {
			col, _, _ := strings.Cut(c, " ")
			q := `"` + strings.ReplaceAll(col, `"`, `""`) + `"`
			sel = append(sel, q+"::VARCHAR AS "+q)
			ord = append(ord, fmt.Sprint(i+1))
		}
		rows, err := h.sqlRows(p, env, "SELECT "+strings.Join(sel, ", ")+" FROM "+name+" ORDER BY "+strings.Join(ord, ", "))
		if err != nil {
			return nil, err
		}
		ts.rows = rows
	}
	return out, nil
}

func sampleValue(v any) any {
	if v == nil {
		return nil
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

func rowMatches(row, want map[string]any) bool {
	for k, v := range want {
		got, ok := row[k]
		if !ok || sampleValue(got) != sampleValue(v) {
			return false
		}
	}
	return true
}

// compareRun asserts one run's outcome and tables against its expect.
func compareRun(i int, want expectRun, got *runOutcome, tables map[string]*tableState) []error {
	var errs []error
	fail := func(format string, a ...any) {
		errs = append(errs, fmt.Errorf("run %d (now %s): %s", i+1, want.Now, fmt.Sprintf(format, a...)))
	}
	if want.Requests != nil {
		switch {
		case !got.sawCount:
			fail("no http_replay summary in the run output")
		case got.requests != *want.Requests:
			fail("%d requests replayed, want %d", got.requests, *want.Requests)
		}
	}
	for _, name := range sortedKeys(got.models) {
		allowed := want.Models[name].Warnings
		for _, w := range got.models[name].Warnings {
			if !slices.ContainsFunc(allowed, func(a string) bool { return strings.Contains(w, a) }) {
				fail("model %s: warning: %s", name, w)
			}
		}
	}
	for _, name := range sortedKeys(want.Models) {
		wm := want.Models[name]
		r, ok := got.models[name]
		if !ok {
			fail("model %s did not run", name)
			continue
		}
		if wm.RunType != "" && r.RunType != wm.RunType {
			fail("model %s: run_type %s, want %s", name, r.RunType, wm.RunType)
		}
		if wm.Rows != nil && r.RowsAffected != *wm.Rows {
			fail("model %s: %d rows affected, want %d", name, r.RowsAffected, *wm.Rows)
		}
	}
	for _, name := range sortedKeys(tables) {
		if _, ok := want.Tables[name]; !ok {
			fail("table %s exists but expect.yaml does not list it", name)
		}
	}
	for _, name := range sortedKeys(want.Tables) {
		wt := want.Tables[name]
		ts, ok := tables[name]
		if !ok {
			fail("table %s is missing", name)
			continue
		}
		if wt.Columns != nil && !slices.Equal(wt.Columns, ts.columns) {
			fail("table %s: columns %q, want %q", name, ts.columns, wt.Columns)
		}
		if len(ts.rows) != *wt.Rows {
			fail("table %s: %d rows, want %d", name, len(ts.rows), *wt.Rows)
		}
		for _, s := range wt.Sample {
			if !slices.ContainsFunc(ts.rows, func(r map[string]any) bool { return rowMatches(r, s) }) {
				fail("table %s: no row matches sample %v", name, s)
			}
		}
	}
	if i == 0 {
		for name, wt := range want.Tables {
			if wt.Columns == nil {
				fail("table %s: the first run must list its columns", name)
			}
		}
	}
	return errs
}

// replay runs the blueprint as rec/expect.yaml says, every request
// answered from rec/cassettes/, in one fresh project, and returns what
// does not match. drop are the blueprint's env keys, unset so a replay
// proves it needs none.
func (h *harness) replay(t testing.TB, bpDir, rec string, drop []string) error {
	t.Helper()
	exp, err := readExpect(filepath.Join(rec, "expect.yaml"))
	if err != nil {
		return err
	}
	casRoot := filepath.Join(rec, "cassettes")
	if st, err := os.Stat(casRoot); err != nil || !st.IsDir() {
		return fmt.Errorf("no recording: %s is not a directory", casRoot)
	}
	// Replay from a copy: nothing the run does can touch the recording.
	casCopy := t.TempDir()
	if err := copyTree(casRoot, casCopy); err != nil {
		return err
	}
	p, err := h.newProject(t, bpDir)
	if err != nil {
		return err
	}
	var errs []error
	for i, r := range exp.Runs {
		out, err := h.doRun(p, "replay", filepath.Join(casCopy, r.Cassettes), r.Now, drop)
		if err != nil {
			return errors.Join(append(errs, fmt.Errorf("run %d (now %s): %w", i+1, r.Now, err))...)
		}
		tables, err := h.tables(p)
		if err != nil {
			return errors.Join(append(errs, fmt.Errorf("run %d: %w", i+1, err))...)
		}
		errs = append(errs, compareRun(i, r, out, tables)...)
	}
	return errors.Join(errs...)
}

// replayBlueprint replays blueprint name (in bpDir) from its recording in
// testsDir/name, the directory BLUEPRINT_TESTS_DIR names, with the
// blueprint's keys unset. A missing recording is an error naming it.
func (h *harness) replayBlueprint(t testing.TB, name, bpDir, testsDir string) error {
	t.Helper()
	dir := filepath.Join(testsDir, name)
	if st, err := os.Stat(dir); err != nil || !st.IsDir() {
		return fmt.Errorf("%s: no recording in %s=%s (want %s/cassettes/ and %s/expect.yaml)", name, EnvTestsDir, testsDir, dir, dir)
	}
	keys, err := envExampleKeys(bpDir)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if err := h.replay(t, bpDir, dir, keys); err != nil {
		return fmt.Errorf("%s: replay of %s: %w", name, dir, err)
	}
	return nil
}

// record runs the blueprint against the live API as rec/expect.yaml says
// (or, without one, two runs at the current time), writing the requests
// to rec/cassettes/ (replaced). It then replays the recording and
// returns the expect.yaml that replay produced, as a draft for review;
// it writes it to rec/expect.yaml only when there is none.
func (h *harness) record(t testing.TB, bpDir, rec string) ([]byte, error) {
	t.Helper()
	keys, err := envExampleKeys(bpDir)
	if err != nil {
		return nil, err
	}
	var missing []string
	for _, k := range keys {
		if os.Getenv(k) == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("recording needs %s set (see .env.example)", strings.Join(missing, ", "))
	}

	expPath := filepath.Join(rec, "expect.yaml")
	exp, err := readExpect(expPath)
	haveExpect := err == nil
	switch {
	case errors.Is(err, fs.ErrNotExist):
		now := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)
		exp = &expectFile{Runs: []expectRun{{Now: now}, {Now: now}}}
	case err != nil:
		return nil, err
	}

	casRoot := filepath.Join(rec, "cassettes")
	if err := os.RemoveAll(casRoot); err != nil {
		return nil, err
	}
	p, err := h.newProject(t, bpDir)
	if err != nil {
		return nil, err
	}
	for i, r := range exp.Runs {
		dir := filepath.Join(casRoot, r.Cassettes)
		if i > 0 && r.Cassettes == "" {
			// A later run replays from cassettes/ too; what it records
			// would replace the first run's files, so it must record none.
			dir = filepath.Join(t.TempDir(), "later-run")
		}
		if _, err := h.doRun(p, "record", dir, r.Now, nil); err != nil {
			return nil, fmt.Errorf("record run %d: %w", i+1, err)
		}
		if i > 0 && r.Cassettes == "" {
			if entries, _ := os.ReadDir(dir); len(entries) > 0 {
				return nil, fmt.Errorf("record run %d made requests; give it cassettes: run%d in expect.yaml so they are kept apart", i+1, i+1)
			}
		}
	}

	draft, err := h.draftExpect(t, bpDir, casRoot, exp)
	if err != nil {
		return nil, err
	}
	if !haveExpect {
		if err := os.WriteFile(expPath, draft, 0o644); err != nil {
			return nil, err
		}
	}
	return draft, nil
}

// draftExpect replays the runs of exp from casRoot and writes down what
// they produced: an expect.yaml to review, not to accept blindly.
func (h *harness) draftExpect(t testing.TB, bpDir, casRoot string, exp *expectFile) ([]byte, error) {
	t.Helper()
	p, err := h.newProject(t, bpDir)
	if err != nil {
		return nil, err
	}
	keys, _ := envExampleKeys(bpDir) // read without error by record
	out := expectFile{}
	for i, r := range exp.Runs {
		res, err := h.doRun(p, "replay", filepath.Join(casRoot, r.Cassettes), r.Now, keys)
		if err != nil {
			return nil, fmt.Errorf("replay run %d: %w", i+1, err)
		}
		tables, err := h.tables(p)
		if err != nil {
			return nil, err
		}
		n := res.requests
		d := expectRun{Now: r.Now, Cassettes: r.Cassettes, Requests: &n, Models: map[string]expectModel{}, Tables: map[string]expectTable{}}
		for name, m := range res.models {
			rows := m.RowsAffected
			d.Models[name] = expectModel{RunType: m.RunType, Rows: &rows, Warnings: m.Warnings}
		}
		for name, ts := range tables {
			count := len(ts.rows)
			et := expectTable{Rows: &count}
			if i == 0 {
				et.Columns = ts.columns
				// first and last row as samples
				if count > 0 {
					et.Sample = append(et.Sample, ts.rows[0])
				}
				if count > 1 {
					et.Sample = append(et.Sample, ts.rows[count-1])
				}
			}
			d.Tables[name] = et
		}
		out.Runs = append(out.Runs, d)
	}
	return yaml.MarshalWithOptions(out, yaml.IndentSequence(true))
}
