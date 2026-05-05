// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"go.starlark.net/syntax"

	"github.com/ondatra-labs/ondatrasql/internal/config"
	"github.com/ondatra-labs/ondatrasql/internal/libregistry"
	"github.com/ondatra-labs/ondatrasql/internal/output"
)

// describeBlueprintListSchemaVersion is the JSON schema version for
// the LISTING form (`describe blueprint` with no name) — payload shape
// `{schema_version, blueprints: [...], parse_errors: [...]}`.
//
// describeBlueprintDetailSchemaVersion is the version for the
// DETAIL form (`describe blueprint <name>`) — payload shape
// `BlueprintDescription`. Two distinct schemas need distinct versions
// so consumers can dispatch on `output.kind` + `schema_version`.
const (
	describeBlueprintListSchemaVersion   = 1
	describeBlueprintDetailSchemaVersion = 1
)

// BlueprintDescription is the JSON shape returned by `describe blueprint`.
//
// Field order matches struct order so encoding/json gives deterministic
// output across runs (G5).
type BlueprintDescription struct {
	SchemaVersion int    `json:"schema_version"`
	// Kind discriminates the payload shape ("detail" vs "list" in the
	// listing form). Always "detail" here. Listing output is a separate
	// shape with its own schema_version.
	Kind          string            `json:"kind"`
	Name          string            `json:"name"`
	Path          string            `json:"path"`
	Fetch         *fetchSection     `json:"fetch,omitempty"`
	Push          *pushSection      `json:"push,omitempty"`
	Auth          map[string]any    `json:"auth,omitempty"`
	BaseURL       string            `json:"base_url,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
	Timeout       int               `json:"timeout,omitempty"`
	Retry         int               `json:"retry,omitempty"`
	Backoff       int               `json:"backoff,omitempty"`
	Endpoints     []endpoint        `json:"endpoints,omitempty"`
	IsSink        bool              `json:"is_sink,omitempty"`
	RateLimit     *rateLimit        `json:"rate_limit,omitempty"`
}

type fetchSection struct {
	Args           []string            `json:"args"`
	PageSize       int                 `json:"page_size,omitempty"`
	SupportedKinds []string            `json:"supported_kinds,omitempty"`
	Mode           string              `json:"mode"`
	Functions      map[string][]string `json:"functions,omitempty"`
	PollInterval   string              `json:"poll_interval,omitempty"`
	PollTimeout    string              `json:"poll_timeout,omitempty"`
	PollBackoff    int                 `json:"poll_backoff,omitempty"`
	// Note: per-model @group_key is a SQL directive on `tracked` models,
	// not a blueprint-level property. The design's example output put it
	// inside fetch but reviewing libregistry.LibFunc shows no such field.
	// Surface it via `describe <model>` instead.
}

type pushSection struct {
	Args           []string            `json:"args"`
	SupportedKinds []string            `json:"supported_kinds,omitempty"`
	BatchSize      int                 `json:"batch_size,omitempty"`
	BatchMode      string              `json:"batch_mode,omitempty"`
	MaxConcurrent  int                 `json:"max_concurrent,omitempty"`
	PollInterval   string              `json:"poll_interval,omitempty"`
	PollTimeout    string              `json:"poll_timeout,omitempty"`
	Functions      map[string][]string `json:"functions,omitempty"`
}

type endpoint struct {
	Method   string `json:"method"`
	Path     string `json:"path"`
	Function string `json:"function,omitempty"`
}

type rateLimit struct {
	Requests int    `json:"requests"`
	Per      string `json:"per"`
}

// runDescribeBlueprint dispatches `describe blueprint [name] [--fields=...]`.
//
// With no name argument, lists all available blueprints.
// With a name, returns the full blueprint description (subject to --fields).
func runDescribeBlueprint(cfg *config.Config, args []string) error {
	var name string
	var fields string

	for _, a := range args {
		switch {
		case a == "--json":
			// Already handled globally by main.go.
		case strings.HasPrefix(a, "--fields="):
			fields = strings.TrimPrefix(a, "--fields=")
			if fields == "" {
				return &invocationErr{fmt.Errorf("--fields requires a non-empty comma-separated path list (e.g. --fields=fetch.args,fetch.mode)")}
			}
			// Reject `--fields=,` and `--fields= , , ` (whitespace-
			// or comma-only). Pre-R11 these passed the empty-string
			// check and slipped through splitFieldMask which drops
			// empties — projection then produced an envelope with
			// only schema_version+kind, hiding the user's typo.
			// (R11 #5.)
			if len(splitFieldMask(fields)) == 0 {
				return &invocationErr{fmt.Errorf("--fields contains no valid paths (got %q) — use comma-separated dotted paths like fetch.args,fetch.mode", fields)}
			}
		case strings.HasPrefix(a, "--"):
			return &invocationErr{fmt.Errorf("unknown flag: %s", a)}
		default:
			if name != "" {
				return &invocationErr{fmt.Errorf("usage: ondatrasql describe blueprint [<name>] [--fields=...]")}
			}
			name = a
		}
	}

	// ScanLenient: don't let a broken sibling blueprint block describe
	// for healthy ones. Broken files appear nowhere in the partial
	// registry, so describe-by-name on a broken blueprint still falls
	// through to "blueprint not found"-with-suggestions correctly.
	reg, scanErrs := libregistry.ScanLenient(cfg.ProjectDir)

	if name == "" {
		// Listing mode does not support --fields. Reject explicitly
		// rather than silently ignoring the projection — typed callers
		// would get a different shape than they asked for.
		if fields != "" {
			return &invocationErr{fmt.Errorf("--fields is only supported for `describe blueprint <name>`, not the listing form")}
		}
		// Listing mode: surface broken blueprints both on stderr (for
		// human visibility) AND in the JSON output as a structured
		// `parse_errors` field, so machine-readable clients can detect
		// that the list is partial without parsing log text.
		for fileName, scanErr := range scanErrs {
			fmt.Fprintf(os.Stderr, "warning: skipped broken blueprint lib/%s: %v\n", fileName, scanErr)
		}
		return listBlueprints(reg, scanErrs)
	}

	if !isValidBlueprintName(name) {
		return &invocationErr{fmt.Errorf("invalid blueprint name: %q", name)}
	}

	lf := reg.Get(name)
	if lf == nil {
		// Directory-level scan failure: surface that lib/ itself
		// couldn't be read, rather than misleading "blueprint not
		// found" — there might BE a matching blueprint, we just
		// couldn't enumerate the directory.
		if scanErr, ok := scanErrs["<lib-dir>"]; ok {
			return fmt.Errorf("blueprint discovery failed: %w (cannot determine whether %q exists)", scanErr, name)
		}
		// Distinguish "exists but failed to parse" from "no such blueprint".
		if scanErr, ok := scanErrs[name+".star"]; ok {
			return fmt.Errorf("blueprint %q failed to parse: %w", name, scanErr)
		}
		return blueprintNotFoundError(reg, name)
	}

	desc, err := buildBlueprintDescription(cfg.ProjectDir, lf)
	if err != nil {
		return fmt.Errorf("build description: %w", err)
	}

	if output.JSONEnabled {
		return emitWithFieldMask(desc, fields)
	}

	if fields != "" {
		// In human mode, --fields is treated as JSON-only.
		return &invocationErr{fmt.Errorf("--fields requires --json")}
	}
	printBlueprintHuman(desc)
	return nil
}

// listBlueprints prints all blueprints (sorted by name).
//
// JSON output keeps a stable shape regardless of registry size: the
// `blueprints` field is always an array of {name, path, is_sink}-objects.
// Empty registries get [] (empty array) rather than null or a different
// type so typed clients can decode unconditionally.
//
// `parse_errors` is included in the JSON output as an array of
// {file, error}-objects when ScanLenient skipped any blueprints. The
// field is always emitted (empty array when none) so typed clients
// can rely on its presence and distinguish "complete" from "partial"
// list output.
func listBlueprints(reg *libregistry.Registry, scanErrs map[string]error) error {
	all := reg.List()

	if output.JSONEnabled {
		entries := make([]map[string]any, 0, len(all))
		for _, lf := range all {
			entries = append(entries, map[string]any{
				"name":    lf.Name,
				"path":    lf.FilePath,
				"is_sink": lf.IsSink,
			})
		}
		errEntries := make([]map[string]any, 0, len(scanErrs))
		// Sort error file names for deterministic output.
		errFiles := make([]string, 0, len(scanErrs))
		for f := range scanErrs {
			errFiles = append(errFiles, f)
		}
		sort.Strings(errFiles)
		for _, f := range errFiles {
			// `<lib-dir>` is the directory-level sentinel ScanLenient
			// uses when ReadDir itself failed. Surface it as `lib/`
			// (not `lib/<lib-dir>`) so JSON consumers can distinguish
			// "the lib directory couldn't be read" from "this specific
			// blueprint failed to parse".
			path := filepath.Join("lib", f)
			if f == "<lib-dir>" {
				path = "lib/"
			}
			errEntries = append(errEntries, map[string]any{
				"file":  path,
				"error": scanErrs[f].Error(),
			})
		}
		output.EmitJSON(map[string]any{
			"schema_version": describeBlueprintListSchemaVersion,
			"kind":           "list",
			"blueprints":     entries,
			"parse_errors":   errEntries,
		})
		return nil
	}

	if len(all) == 0 {
		output.Println("no blueprints found in lib/")
		return nil
	}

	output.Println("Available blueprints:")
	for _, lf := range all {
		kind := "fetch"
		if lf.IsSink {
			kind = "push"
		}
		output.Fprintf("  %-24s  %s  (%s)\n", lf.Name, kind, lf.FilePath)
	}
	return nil
}

func blueprintNotFoundError(reg *libregistry.Registry, name string) error {
	all := reg.List()
	if len(all) == 0 {
		return &invocationErr{fmt.Errorf("blueprint %q not found (no blueprints in lib/)", name)}
	}
	names := make([]string, 0, len(all))
	for _, lf := range all {
		names = append(names, lf.Name)
	}
	return &invocationErr{fmt.Errorf("blueprint %q not found (available: %s)", name, strings.Join(names, ", "))}
}

// buildBlueprintDescription assembles the description struct for a blueprint.
func buildBlueprintDescription(projectDir string, lf *libregistry.LibFunc) (*BlueprintDescription, error) {
	desc := &BlueprintDescription{
		SchemaVersion: describeBlueprintDetailSchemaVersion,
		Kind:          "detail",
		Name:          lf.Name,
		Path:          lf.FilePath,
		IsSink:        lf.IsSink,
	}

	if lf.APIConfig != nil {
		desc.Auth = lf.APIConfig.Auth
		desc.BaseURL = lf.APIConfig.BaseURL
		desc.Headers = lf.APIConfig.Headers
		desc.Timeout = lf.APIConfig.Timeout
		desc.Retry = lf.APIConfig.Retry
		desc.Backoff = lf.APIConfig.Backoff
		if lf.APIConfig.RateLimit != nil {
			desc.RateLimit = &rateLimit{
				Requests: lf.APIConfig.RateLimit.Requests,
				Per:      lf.APIConfig.RateLimit.Per,
			}
		}
	}

	// Parse the Starlark file once for signature extraction + endpoint walk.
	src, err := os.ReadFile(filepath.Join(projectDir, lf.FilePath))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", lf.FilePath, err)
	}
	// Match runtime's internal/script.blueprintFileOptions so describe
	// surfaces accept exactly what runtime would load.
	opts := &syntax.FileOptions{
		Set: true, While: true, TopLevelControl: false,
		GlobalReassign: false, Recursion: true,
	}
	f, err := opts.Parse(lf.FilePath, src, 0)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", lf.FilePath, err)
	}

	signatures := libregistry.ExtractSignatures(f)

	if !lf.IsSink {
		desc.Fetch = &fetchSection{
			Args:           lf.Args,
			PageSize:       lf.PageSize,
			SupportedKinds: lf.SupportedKinds,
			Mode:           fetchMode(lf),
			Functions:      pickFetchSignatures(lf, signatures),
		}
		if lf.APIConfig != nil {
			desc.Fetch.PollInterval = lf.APIConfig.FetchPollInterval
			desc.Fetch.PollTimeout = lf.APIConfig.FetchPollTimeout
			desc.Fetch.PollBackoff = lf.APIConfig.FetchPollBackoff
		}
	} else if lf.PushConfig != nil {
		desc.Push = &pushSection{
			Args:           lf.PushConfig.Args,
			SupportedKinds: lf.PushConfig.SupportedKinds,
			BatchSize:      lf.PushConfig.BatchSize,
			BatchMode:      lf.PushConfig.BatchMode,
			MaxConcurrent:  lf.PushConfig.MaxConcurrent,
			PollInterval:   lf.PushConfig.PollInterval,
			PollTimeout:    lf.PushConfig.PollTimeout,
			Functions:      map[string][]string{"push": signatures["push"]},
		}
	}

	desc.Endpoints = extractEndpoints(f)
	return desc, nil
}

func fetchMode(lf *libregistry.LibFunc) string {
	if lf.FetchMode == "async" {
		return "async"
	}
	return "sync"
}

func pickFetchSignatures(lf *libregistry.LibFunc, sigs map[string][]string) map[string][]string {
	if lf.FetchMode == "async" {
		out := map[string][]string{}
		for _, name := range []string{"submit", "check", "fetch_result"} {
			if params, ok := sigs[name]; ok {
				out[name] = params
			}
		}
		return out
	}
	if lf.FuncName != "" {
		if params, ok := sigs[lf.FuncName]; ok {
			return map[string][]string{lf.FuncName: params}
		}
	}
	if params, ok := sigs["fetch"]; ok {
		return map[string][]string{"fetch": params}
	}
	return nil
}

// extractEndpoints walks the Starlark AST for `http.<method>(...)` calls and
// records (method, path, enclosing-function-name). Returns []endpoint sorted
// by method then path for deterministic output.
//
// Path is the literal first argument; if it is a non-string expression
// (concat, format, etc.) we emit "<computed>" rather than guessing.
func extractEndpoints(f *syntax.File) []endpoint {
	if f == nil {
		return nil
	}
	var out []endpoint
	httpMethods := map[string]string{
		"get":    "GET",
		"post":   "POST",
		"put":    "PUT",
		"patch":  "PATCH",
		"delete": "DELETE",
		"upload": "POST",
	}

	// Walk the WHOLE file (not just DefStmts) so http.* calls inside
	// top-level if/for blocks are still recorded. Collect every def's
	// span first, then for each http.* call resolve the smallest
	// enclosing def by position so calls inside a nested def are
	// attributed to that inner def — not the outer scope.
	//
	// (R6 finding: pre-fix walk seeded only top-level DefStmts and
	// captured fnName once per seed, dropping non-def-scoped calls
	// AND misattributing nested-def calls to the outer name.)
	type defSpan struct {
		name       string
		start, end syntax.Position
	}
	var defs []defSpan
	for _, stmt := range f.Stmts {
		syntax.Walk(stmt, func(n syntax.Node) bool {
			if def, ok := n.(*syntax.DefStmt); ok {
				start, end := def.Span()
				defs = append(defs, defSpan{name: def.Name.Name, start: start, end: end})
			}
			return true
		})
	}
	// enclosingFn picks the smallest def-span containing pos. Nested
	// Starlark defs always start strictly after their parent's start,
	// so among containing candidates the one with the LATEST start is
	// the innermost. This avoids the int64-flattening overflow and
	// negative-on-malformed-span hazard the previous spanSize helper
	// had on very long lines.
	enclosingFn := func(pos syntax.Position) string {
		var bestName string
		var haveBest bool
		var bestStart syntax.Position
		for _, d := range defs {
			if !inSpan(pos, d.start, d.end) {
				continue
			}
			if !haveBest || posLess(bestStart, d.start) {
				haveBest = true
				bestStart = d.start
				bestName = d.name
			}
		}
		return bestName
	}
	for _, stmt := range f.Stmts {
		syntax.Walk(stmt, func(n syntax.Node) bool {
			call, ok := n.(*syntax.CallExpr)
			if !ok {
				return true
			}
			dot, ok := call.Fn.(*syntax.DotExpr)
			if !ok {
				return true
			}
			recv, ok := dot.X.(*syntax.Ident)
			if !ok || recv.Name != "http" {
				return true
			}
			method, ok := httpMethods[dot.Name.Name]
			if !ok {
				return true
			}
			path := "<computed>"
			if len(call.Args) > 0 {
				if lit, ok := call.Args[0].(*syntax.Literal); ok {
					if s, ok := lit.Value.(string); ok {
						path = s
					}
				}
			}
			callPos, _ := call.Span()
			out = append(out, endpoint{Method: method, Path: path, Function: enclosingFn(callPos)})
			return true
		})
	}

	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Method != out[j].Method {
			return out[i].Method < out[j].Method
		}
		return out[i].Path < out[j].Path
	})
	return out
}

// inSpan reports whether pos is between start (inclusive) and end
// (exclusive). Compares (line, col) tuples since Starlark Positions
// don't expose a single comparable offset.
func inSpan(pos, start, end syntax.Position) bool {
	if posLess(pos, start) {
		return false
	}
	if !posLess(pos, end) {
		return false
	}
	return true
}

// posLess reports whether a strictly precedes b in (line, col) order.
func posLess(a, b syntax.Position) bool {
	if a.Line != b.Line {
		return a.Line < b.Line
	}
	return a.Col < b.Col
}

// emitWithFieldMask renders the description as JSON, optionally projected
// through a comma-separated dotted-path field mask (G2). Returns an error
// for unknown field paths so main()'s error path can route exit codes.
//
// schema_version is always emitted, even under field-mask projection,
// so typed clients can detect breaking output changes regardless of
// which specific fields they requested.
func emitWithFieldMask(desc *BlueprintDescription, fields string) error {
	if fields == "" {
		output.EmitJSON(desc)
		return nil
	}
	all := descToMap(desc)
	paths := splitFieldMask(fields)
	projected, err := projectFields(all, paths)
	if err != nil {
		// Unknown field path is an invocation error (the user typed
		// something the public schema doesn't support), so route it
		// to exit code 2 like the other --fields invocation failures.
		return &invocationErr{err}
	}
	projected["schema_version"] = describeBlueprintDetailSchemaVersion
	projected["kind"] = "detail"
	// Emit with top-level keys in canonical struct-declaration order
	// instead of encoding/json's default alphabetical ordering for
	// maps. Without this, a `--fields` projection shifts schema_version
	// from first (struct path) to mid-object (map path), breaking
	// streaming JSON parsers that key off field position. (R6 finding.)
	output.EmitJSON(orderedProjection{m: projected, order: blueprintSchemaTopLevelOrder})
	return nil
}

// orderedProjection is a JSON-encodable map[string]any with a
// caller-controlled top-level key order. Keys present in m but
// missing from order are appended alphabetically at the end so the
// output remains deterministic even if the projection contains a key
// the canonical order doesn't enumerate.
type orderedProjection struct {
	m     map[string]any
	order []string
}

func (o orderedProjection) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	first := true
	seen := make(map[string]bool, len(o.m))
	emit := func(k string) error {
		v, ok := o.m[k]
		if !ok {
			return nil
		}
		if !first {
			buf.WriteByte(',')
		}
		first = false
		kj, err := json.Marshal(k)
		if err != nil {
			return err
		}
		buf.Write(kj)
		buf.WriteByte(':')
		vj, err := json.Marshal(v)
		if err != nil {
			return err
		}
		buf.Write(vj)
		seen[k] = true
		return nil
	}
	for _, k := range o.order {
		if err := emit(k); err != nil {
			return nil, err
		}
	}
	rest := make([]string, 0, len(o.m))
	for k := range o.m {
		if !seen[k] {
			rest = append(rest, k)
		}
	}
	sort.Strings(rest)
	for _, k := range rest {
		if err := emit(k); err != nil {
			return nil, err
		}
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// descToMap encodes the description through JSON to get a map[string]any
// view suitable for field-mask projection. This guarantees the projection
// uses the exact JSON field names from struct tags.
func descToMap(desc *BlueprintDescription) map[string]any {
	// json.Marshal + Unmarshal into map keeps tag handling consistent.
	// Implementation deferred to a small helper to avoid panic noise.
	return mustEncodeAsMap(desc)
}

func splitFieldMask(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// projectFields returns a new map containing only the requested dotted-paths
// from src. Schema validity is checked against blueprintSchemaPaths, which
// is computed once via reflection on BlueprintDescription's struct tags.
// Paths NOT in the schema return an error ("unknown field path"); paths
// that ARE in the schema but absent from src (because their struct field
// has omitempty and was zero-valued) return null in the projection.
//
// This separation is necessary so a request like `--fields=fetch.poll_interval`
// against a sync blueprint (which has empty poll_interval) returns
// `{"fetch":{"poll_interval":null}}` rather than an "unknown path" error.
//
// Round-trip note (R8 #7): projecting every schema path produces a JSON
// shape that includes `null` leaves for any omitempty-zero field, while
// the unprojected detail JSON omits those keys via `omitempty`. The two
// shapes are NOT byte-equivalent. This is intentional: --fields is a
// query language, and a null answer ("path is valid, value is empty")
// carries strictly more information than an absent key. Clients that
// need the unprojected shape should call `describe blueprint <name>`
// without `--fields`.
func projectFields(src map[string]any, paths []string) (map[string]any, error) {
	out := map[string]any{}
	for _, p := range paths {
		if !isValidBlueprintFieldPath(p) {
			return nil, fmt.Errorf("unknown field path: %s", p)
		}
		val, _ := lookupPath(src, p) // missing-but-valid → nil
		setPath(out, p, val)
	}
	return out, nil
}

// lookupPath walks a dotted path into a JSON-shaped map. Returns (nil, true)
// for null leaves so callers can distinguish "not present" from "explicitly null".
func lookupPath(m map[string]any, path string) (any, bool) {
	parts := strings.Split(path, ".")
	var cur any = m
	for _, p := range parts {
		obj, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		v, ok := obj[p]
		if !ok {
			return nil, false
		}
		cur = v
	}
	return cur, true
}

// setPath inserts val at a dotted path inside m, creating intermediate maps.
func setPath(m map[string]any, path string, val any) {
	parts := strings.Split(path, ".")
	cur := m
	for i, p := range parts {
		if i == len(parts)-1 {
			cur[p] = val
			return
		}
		next, ok := cur[p].(map[string]any)
		if !ok {
			next = map[string]any{}
			cur[p] = next
		}
		cur = next
	}
}

// mustEncodeAsMap roundtrips a struct through JSON to get a map[string]any
// view that respects struct tags. Panics on encode/decode error since the
// inputs are always our own structs (programmer bug, not user input).
func mustEncodeAsMap(v any) map[string]any {
	data, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("encode blueprint description: %v", err))
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		panic(fmt.Sprintf("decode blueprint description: %v", err))
	}
	return out
}

// isValidBlueprintName mirrors the identifier rules the parser applies to
// model names: ASCII letters, digits, underscore. Rejects path separators,
// control characters, and shell metachars.
func isValidBlueprintName(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		if i == 0 {
			if !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') && r != '_' {
				return false
			}
			continue
		}
		if !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') &&
			!(r >= '0' && r <= '9') && r != '_' {
			return false
		}
	}
	return true
}

// printBlueprintHuman renders the description as a TTY-friendly view.
func printBlueprintHuman(desc *BlueprintDescription) {
	output.Fprintf("%s (%s)\n\n", desc.Name, desc.Path)
	if desc.Fetch != nil {
		output.Println("  Fetch:")
		output.Fprintf("    Args:           %s\n", strings.Join(desc.Fetch.Args, ", "))
		if desc.Fetch.PageSize > 0 {
			output.Fprintf("    Page size:      %d\n", desc.Fetch.PageSize)
		}
		if len(desc.Fetch.SupportedKinds) > 0 {
			output.Fprintf("    Supported:      %s\n", strings.Join(desc.Fetch.SupportedKinds, ", "))
		}
		output.Fprintf("    Mode:           %s\n", desc.Fetch.Mode)
		if len(desc.Fetch.Functions) > 0 {
			output.Println("    Functions:")
			names := make([]string, 0, len(desc.Fetch.Functions))
			for name := range desc.Fetch.Functions {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				output.Fprintf("      %s(%s)\n", name, strings.Join(desc.Fetch.Functions[name], ", "))
			}
		}
		output.Println()
	}
	if desc.Push != nil {
		output.Println("  Push:")
		output.Fprintf("    Args:           %s\n", strings.Join(desc.Push.Args, ", "))
		if desc.Push.BatchSize > 0 {
			output.Fprintf("    Batch size:     %d\n", desc.Push.BatchSize)
		}
		if desc.Push.BatchMode != "" {
			output.Fprintf("    Batch mode:     %s\n", desc.Push.BatchMode)
		}
		if len(desc.Push.SupportedKinds) > 0 {
			output.Fprintf("    Supported:      %s\n", strings.Join(desc.Push.SupportedKinds, ", "))
		}
		output.Println()
	}
	if desc.BaseURL != "" {
		output.Fprintf("  Base URL:       %s\n", desc.BaseURL)
	}
	if desc.Auth != nil {
		if env, ok := desc.Auth["env"].(string); ok {
			output.Fprintf("  Auth:           env %s\n", env)
		} else if provider, ok := desc.Auth["provider"].(string); ok {
			output.Fprintf("  Auth:           %s\n", provider)
		}
	}
	if desc.Timeout > 0 {
		output.Fprintf("  Timeout:        %ds\n", desc.Timeout)
	}
	if desc.Retry > 0 {
		output.Fprintf("  Retry:          %d\n", desc.Retry)
	}
	if len(desc.Endpoints) > 0 {
		output.Println("\n  HTTP endpoints:")
		for _, ep := range desc.Endpoints {
			fn := ""
			if ep.Function != "" {
				fn = "  (" + ep.Function + ")"
			}
			output.Fprintf("    %-6s %s%s\n", ep.Method, ep.Path, fn)
		}
	}
}

// blueprintSchemaPaths enumerates every dotted JSON path that's part of
// the BlueprintDescription contract. Computed once via reflection so the
// set stays in sync with the struct definition.
//
// Used by isValidBlueprintFieldPath to distinguish "valid field that
// happens to be absent in this blueprint" (e.g. fetch.poll_interval on
// sync blueprints) from "user typo" — the former gets null in the
// projected output, the latter gets an error.
var blueprintSchemaPaths = computeBlueprintSchemaPaths()

// blueprintSchemaTopLevelOrder lists the top-level JSON field names of
// BlueprintDescription in struct-declaration order so the --fields
// projection can emit deterministic output that matches the
// unprojected struct's key order. (R6 finding.)
var blueprintSchemaTopLevelOrder = computeBlueprintTopLevelOrder()

func computeBlueprintSchemaPaths() map[string]bool {
	paths := map[string]bool{}
	walkSchemaType(reflect.TypeOf(BlueprintDescription{}), "", paths)
	return paths
}

func computeBlueprintTopLevelOrder() []string {
	t := reflect.TypeOf(BlueprintDescription{})
	order := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		tag := t.Field(i).Tag.Get("json")
		if tag == "" || tag == "-" {
			continue
		}
		name := strings.SplitN(tag, ",", 2)[0]
		if name == "" {
			continue
		}
		order = append(order, name)
	}
	return order
}

func walkSchemaType(t reflect.Type, prefix string, paths map[string]bool) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := f.Tag.Get("json")
		if tag == "" || tag == "-" {
			continue
		}
		name := strings.SplitN(tag, ",", 2)[0]
		if name == "" {
			continue
		}
		path := name
		if prefix != "" {
			path = prefix + "." + name
		}
		paths[path] = true
		// Recurse into nested struct fields (and pointer-to-struct).
		ft := f.Type
		if ft.Kind() == reflect.Ptr {
			ft = ft.Elem()
		}
		if ft.Kind() == reflect.Struct {
			walkSchemaType(ft, path, paths)
		}
	}
}

// isValidBlueprintFieldPath reports whether path is one of the documented
// fields in BlueprintDescription's JSON shape. Used by --fields-mask
// projection to error on unknown paths while still allowing valid-but-
// absent paths to project as null.
//
// Paths under map-typed fields (auth, headers) are accepted at exactly
// one level of nesting since their value type is map[string]<scalar>
// (or map[string]any). A path like `auth.provider.foo` is rejected
// because the leaf is a string/any, not a map — accepting it would
// let `lookupPath` walk into a scalar and project a fabricated nested
// object. (R6 finding.)
func isValidBlueprintFieldPath(path string) bool {
	if blueprintSchemaPaths[path] {
		return true
	}
	// Map-typed parents: exactly one nested key allowed (e.g.
	// `auth.bearer`, `headers.X-API-Key`). Two or more dots after the
	// prefix means descending into a leaf scalar — reject.
	for _, mapField := range []string{"auth.", "headers."} {
		rest, ok := strings.CutPrefix(path, mapField)
		if !ok {
			continue
		}
		if rest == "" || strings.Contains(rest, ".") {
			return false
		}
		return true
	}
	return false
}
