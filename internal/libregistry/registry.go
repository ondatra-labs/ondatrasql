// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package libregistry

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"go.starlark.net/syntax"
)

// Column describes a column that a lib function can produce.
type Column struct {
	Name string // column name (e.g. "AD_UNIT_NAME")
	Type string // DuckDB type (e.g. "VARCHAR", "BIGINT")
}

// PushConfig holds SINK-specific configuration extracted from the SINK dict.
type PushConfig struct {
	Args            []string          // parameter names mapped from @push: name('arg1', 'arg2')
	SupportedKinds  []string          // optional whitelist of valid model kinds (validated at startup)
	BatchSize    int               // rows per push call (default 1)
	BatchMode    string            // "sync", "atomic", "async" (default "sync")
	RateLimit    *RateLimitConfig  // proactive rate limiting (nil = unlimited)
	MaxConcurrent int              // concurrent push workers (default 1)
	PollInterval string            // async only: duration string (e.g. "30s")
	PollTimeout  string            // async only: duration string (e.g. "1h")
}

// RateLimitConfig holds rate limit settings from SINK dict.
type RateLimitConfig struct {
	Requests int    // max requests per window
	Per      string // window duration (e.g. "10s", "1m", "1h")
}

// APIConfig holds shared configuration from the API dict.
// Injected into the http module at runtime (base_url, auth, headers, etc.).
type APIConfig struct {
	BaseURL           string            // prepended to relative URLs
	Auth              map[string]any    // auth config (provider, env, cert, etc.)
	Headers           map[string]string // default headers for all requests
	Timeout           int               // default request timeout (seconds)
	Retry             int               // default retry count
	Backoff           int               // default backoff multiplier
	RateLimit         *RateLimitConfig  // default rate limit
	FetchPollInterval string            // async fetch: min wait between check() calls (e.g. "5s")
	FetchPollTimeout  string            // async fetch: max total poll duration (e.g. "5m")
	FetchPollBackoff  int               // async fetch: backoff multiplier for poll interval
}

// LibFunc describes a Starlark lib function registered as a SQL source or sink.
type LibFunc struct {
	Name           string      // function name = file stem (e.g. "gam_fetch")
	FilePath       string      // relative path (e.g. "lib/gam_fetch.star")
	Args           []string    // positional arg names from TABLE/SINK dict
	Columns        []Column    // output columns (TABLE only)
	DynamicColumns bool        // if true, any column name accepted (TABLE only)
	IsSink         bool        // true for SINK libs, false for TABLE libs
	FuncName       string      // Starlark function name ("fetch" or "push")
	PushConfig     *PushConfig // SINK-specific config (nil for TABLE libs)
	APIConfig      *APIConfig  // shared API config (nil for legacy TABLE/SINK)
	PageSize         int         // fetch pagination: rows per page (0 = single call)
	FetchMode        string      // "sync" (default) or "async"
	SupportedColumns []string    // optional whitelist of valid column names (validated at startup)
	SupportedKinds   []string    // optional whitelist of valid model kinds (validated at startup)
}

// Registry holds all discovered lib functions.
type Registry struct {
	funcs map[string]*LibFunc
}

// Scan reads all .star files in lib/ and extracts API dicts.
// Files without an API dict are silently skipped (helper libraries).
func Scan(projectDir string) (*Registry, error) {
	reg := &Registry{funcs: make(map[string]*LibFunc)}
	libDir := filepath.Join(projectDir, "lib")

	// Symlink containment: reject lib/ if it resolves outside projectDir.
	// Without this, a malicious or accidental external symlink could
	// inject blueprints from outside the project as if they were
	// trusted lib/*.star files. Same hardening as ScanLenient.
	absProj := resolveProjectDir(projectDir)
	if real, symErr := filepath.EvalSymlinks(libDir); symErr == nil {
		if !pathInsideProject(absProj, real) {
			return nil, fmt.Errorf("lib/ resolves outside project directory (%s → %s)", libDir, real)
		}
	}

	entries, err := os.ReadDir(libDir)
	if err != nil {
		if os.IsNotExist(err) {
			return reg, nil
		}
		return nil, fmt.Errorf("read lib/: %w", err)
	}

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".star") {
			continue
		}

		path := filepath.Join(libDir, e.Name())
		// Per-file containment: each `lib/foo.star` must also resolve
		// inside projectDir. Otherwise an individual file symlink can
		// point at /etc/passwd or external tooling code.
		if real, symErr := filepath.EvalSymlinks(path); symErr == nil {
			if !pathInsideProject(absProj, real) {
				return nil, fmt.Errorf("lib/%s resolves outside project directory (%s → %s)", e.Name(), path, real)
			}
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read lib/%s: %w", e.Name(), err)
		}

		name := strings.TrimSuffix(e.Name(), ".star")
		relPath := filepath.Join("lib", e.Name())

		lf, err := parseLibFile(name, relPath, string(data))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", relPath, err)
		}
		if lf != nil {
			reg.funcs[name] = lf
		}
	}

	return reg, nil
}

// NewRegistryForTest creates a registry from a map of lib functions (test helper).
func NewRegistryForTest(funcs map[string]*LibFunc) *Registry {
	return &Registry{funcs: funcs}
}

// ScanLenient is like Scan but never aborts on a single bad blueprint.
// Returns a registry containing every lib that parsed successfully,
// plus a per-file error map for the ones that didn't.
//
// Used by `describe <model>` so that one broken `.star` file doesn't
// silently strip the blueprint cross-link from describe output for
// every model in the project. The runtime path keeps using Scan and
// the strict abort-on-error contract.
func ScanLenient(projectDir string) (*Registry, map[string]error) {
	reg := &Registry{funcs: make(map[string]*LibFunc)}
	libDir := filepath.Join(projectDir, "lib")

	// Resolve symlinks on lib/ and verify the real path stays within
	// projectDir. Without this, `lib/` could be a symlink to an external
	// tree and validate/describe/describe-blueprint would silently
	// ingest blueprints from outside the project as if they were
	// trusted lib/*.star files.
	absProj := resolveProjectDir(projectDir)
	if real, symErr := filepath.EvalSymlinks(libDir); symErr == nil {
		if !pathInsideProject(absProj, real) {
			return reg, map[string]error{"<lib-dir>": fmt.Errorf("lib/ resolves outside project directory (%s → %s)", libDir, real)}
		}
	}

	entries, err := os.ReadDir(libDir)
	if err != nil {
		if os.IsNotExist(err) {
			return reg, nil
		}
		// Use a sentinel filename for the directory-level error so
		// consumers (which key by base filename for in-scope dedup)
		// don't get a malformed absolute-path key.
		return reg, map[string]error{"<lib-dir>": fmt.Errorf("read %s: %w", libDir, err)}
	}

	var errs map[string]error
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".star") {
			continue
		}
		path := filepath.Join(libDir, e.Name())
		// Per-file containment: skip individual file symlinks that
		// resolve outside projectDir. Reported as a per-file error so
		// the user sees the rejection rather than wondering why their
		// blueprint disappeared.
		if real, symErr := filepath.EvalSymlinks(path); symErr == nil {
			if !pathInsideProject(absProj, real) {
				if errs == nil {
					errs = map[string]error{}
				}
				errs[e.Name()] = fmt.Errorf("rejected: file resolves outside project directory (%s → %s)", path, real)
				continue
			}
		}
		data, err := os.ReadFile(path)
		if err != nil {
			if errs == nil {
				errs = map[string]error{}
			}
			errs[e.Name()] = err
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".star")
		relPath := filepath.Join("lib", e.Name())
		lf, err := parseLibFile(name, relPath, string(data))
		if err != nil {
			if errs == nil {
				errs = map[string]error{}
			}
			errs[e.Name()] = err
			continue
		}
		if lf != nil {
			reg.funcs[name] = lf
		}
	}
	return reg, errs
}

// Get returns the lib function with the given name, or nil.
func (r *Registry) Get(name string) *LibFunc {
	if r == nil {
		return nil
	}
	return r.funcs[name]
}

// IsLibFunction returns true if name matches a registered lib function.
func (r *Registry) IsLibFunction(name string) bool {
	return r.Get(name) != nil
}

// TableFuncs returns all TABLE lib functions.
func (r *Registry) TableFuncs() []*LibFunc {
	var result []*LibFunc
	for _, lf := range r.funcs {
		if !lf.IsSink {
			result = append(result, lf)
		}
	}
	return result
}

// PushFuncs returns all SINK lib functions.
func (r *Registry) PushFuncs() []*LibFunc {
	var result []*LibFunc
	for _, lf := range r.funcs {
		if lf.IsSink {
			result = append(result, lf)
		}
	}
	return result
}

// Empty returns true if no lib functions were found.
func (r *Registry) Empty() bool {
	return r == nil || len(r.funcs) == 0
}

// List returns all registered lib functions sorted by name.
// Order is deterministic so callers (e.g. `describe blueprint` listing) get stable output.
func (r *Registry) List() []*LibFunc {
	if r == nil {
		return nil
	}
	names := make([]string, 0, len(r.funcs))
	for name := range r.funcs {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]*LibFunc, 0, len(names))
	for _, name := range names {
		result = append(result, r.funcs[name])
	}
	return result
}

// SQLExecutor can execute SQL statements. Satisfied by *duckdb.Session.
type SQLExecutor interface {
	Exec(sql string) error
}

// RegisterMacros registers dummy DuckDB table macros for each TABLE lib function
// so that json_serialize_sql() accepts FROM lib_func(...) syntax.
func (r *Registry) RegisterMacros(sess SQLExecutor) error {
	if r == nil {
		return nil
	}
	for _, lf := range r.funcs {
		if lf.IsSink {
			continue
		}
		sql := buildMacroSQL(lf)
		if err := sess.Exec(sql); err != nil {
			return fmt.Errorf("register macro %s: %w", lf.Name, err)
		}
	}
	return nil
}

// buildMacroSQL generates a CREATE TEMP MACRO statement for a lib function.
func buildMacroSQL(lf *LibFunc) string {
	var b strings.Builder

	// Macro signature: CREATE OR REPLACE TEMP MACRO name(arg1, arg2) AS TABLE
	b.WriteString("CREATE OR REPLACE TEMP MACRO ")
	fmt.Fprintf(&b, "\"%s\"", lf.Name)
	b.WriteString("(")
	for i, arg := range lf.Args {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "\"%s\"", arg) // quote to handle reserved words
	}
	b.WriteString(") AS TABLE SELECT ")

	if lf.DynamicColumns || len(lf.Columns) == 0 {
		// Dynamic columns: single placeholder column
		b.WriteString("NULL AS _dynamic")
	} else {
		// Typed columns from TABLE.columns
		for i, col := range lf.Columns {
			if i > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "NULL::%s AS \"%s\"", col.Type, col.Name)
		}
	}

	b.WriteString(" WHERE false")
	return b.String()
}

// ParseLibFile parses a single .star file and extracts the API dict.
//
// Returns (nil, nil) if the file has no API dict (helper library — Scan()
// silently skips these). Returns an error for malformed Starlark or an
// API dict that fails validation.
//
// Exposed for the `validate` CLI command, which needs to surface the
// parse error per file rather than abort the whole scan.
func ParseLibFile(name, relPath, code string) (*LibFunc, error) {
	return parseLibFile(name, relPath, code)
}

// parseLibFile parses a single .star file and extracts the API dict.
// Returns nil if the file has no API dict (helper library, silently skipped).
func parseLibFile(name, relPath, code string) (*LibFunc, error) {
	opts := &syntax.FileOptions{
		Set:             true,
		While:           true,
		TopLevelControl: true,
		GlobalReassign:  true,
		Recursion:       true,
	}

	f, err := opts.Parse(relPath, code, 0)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	// Find API assignment
	var dictExpr *syntax.DictExpr
	for _, stmt := range f.Stmts {
		assign, ok := stmt.(*syntax.AssignStmt)
		if !ok {
			continue
		}
		ident, ok := assign.LHS.(*syntax.Ident)
		if !ok {
			continue
		}
		if ident.Name == "API" {
			if d, ok := assign.RHS.(*syntax.DictExpr); ok {
				dictExpr = d
			}
		}
		if dictExpr != nil {
			break
		}
	}

	if dictExpr == nil {
		return nil, nil // not a lib function
	}

	return parseAPIDict(name, relPath, dictExpr, f)
}

// parseAPIDict parses an API = {...} dict and produces one or two LibFunc registrations.
func parseAPIDict(name, relPath string, dictExpr *syntax.DictExpr, f *syntax.File) (*LibFunc, error) {
	// Parse shared top-level config
	apiCfg := &APIConfig{
		Timeout: 30,
		Retry:   0,
		Backoff: 1,
	}

	var fetchDict, pushDict *syntax.DictExpr

	for _, entry := range dictExpr.List {
		kv, ok := entry.(*syntax.DictEntry)
		if !ok {
			continue
		}
		key := literalString(kv.Key)
		if key == "" {
			continue
		}

		switch key {
		case "base_url":
			apiCfg.BaseURL = literalString(kv.Value)
		case "auth":
			apiCfg.Auth = extractAnyDict(kv.Value)
		case "headers":
			apiCfg.Headers = extractStringDict(kv.Value)
		case "timeout":
			if v := literalInt(kv.Value); v > 0 {
				apiCfg.Timeout = v
			}
		case "retry":
			if v := literalInt(kv.Value); v >= 0 {
				apiCfg.Retry = v
			}
		case "backoff":
			if v := literalInt(kv.Value); v >= 0 {
				apiCfg.Backoff = v
			}
		case "rate_limit":
			rl, err := extractRateLimit(kv.Value)
			if err != nil {
				return nil, fmt.Errorf("API.rate_limit: %w", err)
			}
			apiCfg.RateLimit = rl
		case "fetch":
			if d, ok := kv.Value.(*syntax.DictExpr); ok {
				fetchDict = d
			} else {
				return nil, fmt.Errorf("API.fetch: expected dict")
			}
		case "push":
			if d, ok := kv.Value.(*syntax.DictExpr); ok {
				pushDict = d
			} else {
				return nil, fmt.Errorf("API.push: expected dict")
			}
		}
	}

	if fetchDict == nil && pushDict == nil {
		return nil, fmt.Errorf("API dict must have at least one of \"fetch\" or \"push\"")
	}

	lf := &LibFunc{
		Name:     name,
		FilePath: relPath,
	}

	// Clone apiCfg for the LibFunc (will be modified with per-direction overrides)
	lfAPI := cloneAPIConfig(apiCfg)
	lf.APIConfig = lfAPI

	if fetchDict != nil {
		lf.IsSink = false
		lf.FuncName = "fetch"

		// Parse fetch-specific fields
		for _, entry := range fetchDict.List {
			kv, ok := entry.(*syntax.DictEntry)
			if !ok {
				continue
			}
			key := literalString(kv.Key)
			switch key {
			case "args":
				lf.Args = extractStringList(kv.Value)
			case "supported_columns":
				lf.SupportedColumns = extractStringList(kv.Value)
			case "supported_kinds":
				lf.SupportedKinds = extractStringList(kv.Value)
			case "columns":
				return nil, fmt.Errorf("API.fetch.columns was removed — SQL controls the schema. Use supported_columns for validation")
			case "dynamic_columns":
				return nil, fmt.Errorf("API.fetch.dynamic_columns was removed — API dict blueprints are always dynamic. Remove the field")
			case "page_size":
				if v := literalInt(kv.Value); v > 0 {
					lf.PageSize = v
				}
			case "mode":
				if v := literalString(kv.Value); v != "" {
					lf.FetchMode = v
				}
			case "async":
				if literalBool(kv.Value) {
					lf.FetchMode = "async"
				}
			case "poll_interval":
				if v := literalString(kv.Value); v != "" {
					lfAPI.FetchPollInterval = v
				}
			case "poll_timeout":
				if v := literalString(kv.Value); v != "" {
					lfAPI.FetchPollTimeout = v
				}
			case "poll_backoff":
				if v := literalInt(kv.Value); v > 0 {
					lfAPI.FetchPollBackoff = v
				}
			// Inheritable overrides
			case "headers":
				lfAPI.Headers = extractStringDict(kv.Value)
			case "timeout":
				if v := literalInt(kv.Value); v > 0 {
					lfAPI.Timeout = v
				}
			case "retry":
				if v := literalInt(kv.Value); v >= 0 {
					lfAPI.Retry = v
				}
			case "backoff":
				if v := literalInt(kv.Value); v >= 0 {
					lfAPI.Backoff = v
				}
			case "rate_limit":
				rl, err := extractRateLimit(kv.Value)
				if err != nil {
					return nil, fmt.Errorf("API.fetch.rate_limit: %w", err)
				}
				lfAPI.RateLimit = rl
			}
		}

		// API dict blueprints are always dynamic — SQL controls the schema
		lf.DynamicColumns = true

		// Validate function(s) exist
		if lf.FetchMode == "async" {
			// Async requires submit/check/fetch_result — not fetch
			for _, fn := range []string{"submit", "check", "fetch_result"} {
				if !hasFuncDef(f, fn) {
					return nil, fmt.Errorf("async fetch requires %s() function", fn)
				}
			}
			if hasFuncDef(f, "fetch") {
				return nil, fmt.Errorf("async fetch uses submit/check/fetch_result, not fetch")
			}
			// Validate submit() accepts all declared args
			for _, arg := range lf.Args {
				if err := validateAsyncParam(f, "submit", arg); err != nil {
					return nil, err
				}
			}
			// Validate check() accepts job_ref
			if err := validateAsyncParam(f, "check", "job_ref"); err != nil {
				return nil, err
			}
			// Validate fetch_result() accepts result_ref and page
			if err := validateAsyncParam(f, "fetch_result", "result_ref"); err != nil {
				return nil, err
			}
			if err := validateAsyncParam(f, "fetch_result", "page"); err != nil {
				return nil, err
			}
		} else {
			if err := validateFunc(lf, false, f); err != nil {
				return nil, err
			}
		}
	}

	if pushDict != nil {
		sinkCfg := &PushConfig{
			BatchSize:     1,
			BatchMode:     "sync",
			MaxConcurrent: 1,
		}

		// Start with top-level rate_limit as default for push
		if apiCfg.RateLimit != nil {
			sinkCfg.RateLimit = &RateLimitConfig{
				Requests: apiCfg.RateLimit.Requests,
				Per:      apiCfg.RateLimit.Per,
			}
		}

		for _, entry := range pushDict.List {
			kv, ok := entry.(*syntax.DictEntry)
			if !ok {
				continue
			}
			key := literalString(kv.Key)
			switch key {
			case "args":
				sinkCfg.Args = extractStringList(kv.Value)
			case "supported_kinds":
				sinkCfg.SupportedKinds = extractStringList(kv.Value)
			case "batch_size":
				if v := literalInt(kv.Value); v > 0 {
					sinkCfg.BatchSize = v
				}
			case "batch_mode":
				if v := literalString(kv.Value); v != "" {
					switch v {
					case "sync", "atomic", "async":
						sinkCfg.BatchMode = v
					default:
						return nil, fmt.Errorf("API.push.batch_mode: invalid %q (must be sync, atomic, or async)", v)
					}
				}
			case "max_concurrent":
				if v := literalInt(kv.Value); v > 0 {
					sinkCfg.MaxConcurrent = v
				}
			case "poll_interval":
				if v := literalString(kv.Value); v != "" {
					sinkCfg.PollInterval = v
				}
			case "poll_timeout":
				if v := literalString(kv.Value); v != "" {
					sinkCfg.PollTimeout = v
				}
			case "rate_limit":
				rl, err := extractRateLimit(kv.Value)
				if err != nil {
					return nil, fmt.Errorf("API.push.rate_limit: %w", err)
				}
				sinkCfg.RateLimit = rl
			case "headers":
				// push-direction header overrides -- store on a push-specific APIConfig if needed
			case "timeout":
				// push-direction timeout override
			case "retry":
				// push-direction retry override
			case "backoff":
				// push-direction backoff override
			}
		}

		lf.PushConfig = sinkCfg

		// Validate push() function exists
		pushFound := false
		for _, stmt := range f.Stmts {
			def, ok := stmt.(*syntax.DefStmt)
			if !ok || def.Name.Name != "push" {
				continue
			}
			pushFound = true
			var paramNames []string
			for _, p := range def.Params {
				switch v := p.(type) {
				case *syntax.Ident:
					paramNames = append(paramNames, v.Name)
				case *syntax.BinaryExpr:
					if ident, ok := v.X.(*syntax.Ident); ok {
						paramNames = append(paramNames, ident.Name)
					}
				case *syntax.UnaryExpr:
					if ident, ok := v.X.(*syntax.Ident); ok {
						paramNames = append(paramNames, ident.Name)
					}
				}
			}
			validPushParams := map[string]bool{"rows": true, "batch_number": true, "kind": true, "key_columns": true, "columns": true}
			for _, arg := range sinkCfg.Args {
				validPushParams[arg] = true
			}
			for _, p := range paramNames {
				if !validPushParams[p] {
					return nil, fmt.Errorf("push() has unknown parameter %q", p)
				}
			}
			if len(paramNames) == 0 {
				return nil, fmt.Errorf("push() must declare at least 'rows' parameter")
			}
			// Verify push() declares all sink args
			hasKwargs := false
			for _, p := range def.Params {
				if _, ok := p.(*syntax.UnaryExpr); ok {
					hasKwargs = true
				}
			}
			if !hasKwargs {
				for _, arg := range sinkCfg.Args {
					found := false
					for _, p := range paramNames {
						if p == arg {
							found = true
							break
						}
					}
					if !found {
						return nil, fmt.Errorf("push() must declare parameter %q (declared in push.args)", arg)
					}
				}
			}
			break
		}
		if !pushFound {
			return nil, fmt.Errorf("API dict has \"push\" section but no push() function defined")
		}

		// If no fetch section, this is a push-only API (sink)
		if fetchDict == nil {
			lf.IsSink = true
			lf.FuncName = "push"
		}
	}

	return lf, nil
}

// validateAsyncParam checks that an async function has a required parameter.
func validateAsyncParam(f *syntax.File, funcName, paramName string) error {
	for _, stmt := range f.Stmts {
		def, ok := stmt.(*syntax.DefStmt)
		if !ok || def.Name.Name != funcName {
			continue
		}
		for _, p := range def.Params {
			switch v := p.(type) {
			case *syntax.Ident:
				if v.Name == paramName {
					return nil
				}
			case *syntax.BinaryExpr:
				if ident, ok := v.X.(*syntax.Ident); ok && ident.Name == paramName {
					return nil
				}
			case *syntax.UnaryExpr:
				return nil // **kwargs accepts anything
			}
		}
		return fmt.Errorf("%s() must accept %q parameter", funcName, paramName)
	}
	return fmt.Errorf("no %s() function defined", funcName)
}

// hasFuncDef checks if a function definition exists in the Starlark file.
func hasFuncDef(f *syntax.File, name string) bool {
	for _, stmt := range f.Stmts {
		def, ok := stmt.(*syntax.DefStmt)
		if ok && def.Name.Name == name {
			return true
		}
	}
	return false
}

// validateFunc validates that the expected function exists and its params match.
func validateFunc(lf *LibFunc, isSink bool, f *syntax.File) error {
	expectedFunc := lf.FuncName
	funcFound := false
	for _, stmt := range f.Stmts {
		def, ok := stmt.(*syntax.DefStmt)
		if !ok || def.Name.Name != expectedFunc {
			continue
		}
		funcFound = true

		// Extract param names
		var paramNames []string
		for _, p := range def.Params {
			switch v := p.(type) {
			case *syntax.Ident:
				paramNames = append(paramNames, v.Name)
			case *syntax.BinaryExpr: // default value: name=value
				if ident, ok := v.X.(*syntax.Ident); ok {
					paramNames = append(paramNames, ident.Name)
				}
			case *syntax.UnaryExpr: // **kwargs or *args — skip, they accept any parameter
				continue
			}
		}

		// Validate args match function params (fetch only).
		if !isSink {
			// API dict: skip "page" (runtime-injected kwarg).
			// Legacy TABLE: first param is "save", skip it.
			if lf.PageSize > 0 {
				// API paginated: skip "page" (last param)
				if len(paramNames) > 0 && paramNames[len(paramNames)-1] == "page" {
					paramNames = paramNames[:len(paramNames)-1]
				}
			} else if lf.APIConfig != nil {
				// API non-paginated: page is still injected with size=0
				if len(paramNames) > 0 && paramNames[len(paramNames)-1] == "page" {
					paramNames = paramNames[:len(paramNames)-1]
				}
			} else {
				// Legacy TABLE: first param is "save"
				if len(paramNames) > 0 {
					paramNames = paramNames[1:] // skip save
				}
			}

			for _, arg := range lf.Args {
				found := false
				for _, p := range paramNames {
					if p == arg {
						found = true
						break
					}
				}
				if !found {
					return fmt.Errorf("%s() missing parameter %q declared in args",
						expectedFunc, arg)
				}
			}
			// Check for undeclared params (excluding runtime-injected ones like save/page)
			for _, p := range paramNames {
				if p == "save" || p == "page" || p == "target" || p == "columns" ||
					p == "is_backfill" || p == "last_value" || p == "last_run" ||
					p == "cursor" || p == "initial_value" {
					continue
				}
				found := false
				for _, arg := range lf.Args {
					if p == arg {
						found = true
						break
					}
				}
				if !found {
					return fmt.Errorf("%s() has undeclared parameter %q (not in args list)",
						expectedFunc, p)
				}
			}
		} else {
			// SINK: validate push takes rows + optional runtime kwargs + sink args
			validPushParams := map[string]bool{"rows": true, "batch_number": true, "kind": true, "key_columns": true, "columns": true}
			if lf.PushConfig != nil {
				for _, arg := range lf.PushConfig.Args {
					validPushParams[arg] = true
				}
			}
			for _, p := range paramNames {
				if !validPushParams[p] {
					return fmt.Errorf("push() has unknown parameter %q", p)
				}
			}
			if len(paramNames) == 0 {
				return fmt.Errorf("push() must declare at least 'rows' parameter")
			}
		}
		break
	}

	if !funcFound {
		return fmt.Errorf("no %s() function defined", expectedFunc)
	}

	return nil
}

// ExtractSignatures walks all top-level def-statements in a parsed Starlark
// file and returns a map from function name to its parameter-name list.
//
// Used by `describe blueprint` to surface fetch/submit/check/fetch_result/push
// signatures without re-running validateFunc/validateAsyncParam (which only
// return error/nil and don't expose the parameter list).
//
// Parameter handling matches validateFunc:
//   - *syntax.Ident          → bare name
//   - *syntax.BinaryExpr     → name with default value (only the name is kept)
//   - *syntax.UnaryExpr      → *args / **kwargs (skipped)
func ExtractSignatures(f *syntax.File) map[string][]string {
	if f == nil {
		return nil
	}
	out := make(map[string][]string)
	for _, stmt := range f.Stmts {
		def, ok := stmt.(*syntax.DefStmt)
		if !ok {
			continue
		}
		params := make([]string, 0, len(def.Params))
		for _, p := range def.Params {
			switch v := p.(type) {
			case *syntax.Ident:
				params = append(params, v.Name)
			case *syntax.BinaryExpr:
				if ident, ok := v.X.(*syntax.Ident); ok {
					params = append(params, ident.Name)
				}
			case *syntax.UnaryExpr:
				continue
			}
		}
		out[def.Name.Name] = params
	}
	return out
}

// cloneAPIConfig creates a shallow copy of an APIConfig.
func cloneAPIConfig(src *APIConfig) *APIConfig {
	dst := *src
	if src.Auth != nil {
		dst.Auth = make(map[string]any, len(src.Auth))
		for k, v := range src.Auth {
			dst.Auth[k] = v
		}
	}
	if src.Headers != nil {
		dst.Headers = make(map[string]string, len(src.Headers))
		for k, v := range src.Headers {
			dst.Headers[k] = v
		}
	}
	if src.RateLimit != nil {
		rl := *src.RateLimit
		dst.RateLimit = &rl
	}
	return &dst
}

// extractStringDict extracts a dict of string→string from a Starlark dict expression.
func extractStringDict(expr syntax.Expr) map[string]string {
	dict, ok := expr.(*syntax.DictExpr)
	if !ok {
		return nil
	}
	result := make(map[string]string)
	for _, entry := range dict.List {
		kv, ok := entry.(*syntax.DictEntry)
		if !ok {
			continue
		}
		k := literalString(kv.Key)
		v := literalString(kv.Value)
		if k != "" {
			result[k] = v
		}
	}
	return result
}

// extractAnyDict extracts a dict of string→any from a Starlark dict expression.
// Values are extracted recursively: nested dicts become map[string]any, lists become []any,
// and leaf values are strings, ints, or bools.
func extractAnyDict(expr syntax.Expr) map[string]any {
	dict, ok := expr.(*syntax.DictExpr)
	if !ok {
		return nil
	}
	result := make(map[string]any)
	for _, entry := range dict.List {
		kv, ok := entry.(*syntax.DictEntry)
		if !ok {
			continue
		}
		k := literalString(kv.Key)
		if k == "" {
			continue
		}
		if v, ok := literalValue(kv.Value); ok {
			result[k] = v
		}
	}
	return result
}

// literalValue extracts a typed value from a Starlark literal expression.
// Handles string/int/bool literals, nested dicts (recursively), and lists.
func literalValue(expr syntax.Expr) (any, bool) {
	// Check for bool (True/False are Ident nodes, not Literal)
	if ident, ok := expr.(*syntax.Ident); ok {
		switch ident.Name {
		case "True":
			return true, true
		case "False":
			return false, true
		}
		return nil, false
	}

	// Nested dict → recursive extraction
	if dict, ok := expr.(*syntax.DictExpr); ok {
		m := make(map[string]any)
		for _, entry := range dict.List {
			kv, ok := entry.(*syntax.DictEntry)
			if !ok {
				continue
			}
			k := literalString(kv.Key)
			if k == "" {
				continue
			}
			if v, ok := literalValue(kv.Value); ok {
				m[k] = v
			}
		}
		return m, true
	}

	// List → recursive extraction
	if list, ok := expr.(*syntax.ListExpr); ok {
		items := make([]any, 0, len(list.List))
		for _, item := range list.List {
			if v, ok := literalValue(item); ok {
				items = append(items, v)
			}
		}
		return items, true
	}

	lit, ok := expr.(*syntax.Literal)
	if !ok {
		return nil, false
	}
	switch lit.Token {
	case syntax.STRING:
		s, _ := lit.Value.(string)
		return s, true // empty string is valid
	case syntax.INT:
		v, ok := lit.Value.(int64)
		if ok {
			return int(v), true // 0 is valid
		}
		return nil, false
	}
	return nil, false
}


// literalString extracts a string value from a Starlark literal expression.
func literalString(expr syntax.Expr) string {
	lit, ok := expr.(*syntax.Literal)
	if !ok || lit.Token != syntax.STRING {
		return ""
	}
	s, _ := lit.Value.(string)
	return s
}

// literalInt extracts an int value from a Starlark literal expression.
func literalInt(expr syntax.Expr) int {
	lit, ok := expr.(*syntax.Literal)
	if !ok || lit.Token != syntax.INT {
		return 0
	}
	v, ok := lit.Value.(int64)
	if ok {
		return int(v)
	}
	return 0
}

// extractRateLimit extracts rate limit config from a Starlark dict expression.
// Expected format: {"requests": 100, "per": "10s"}
func extractRateLimit(expr syntax.Expr) (*RateLimitConfig, error) {
	dict, ok := expr.(*syntax.DictExpr)
	if !ok {
		return nil, fmt.Errorf("expected dict, got %T", expr)
	}
	rl := &RateLimitConfig{}
	for _, entry := range dict.List {
		kv, ok := entry.(*syntax.DictEntry)
		if !ok {
			continue
		}
		key := literalString(kv.Key)
		switch key {
		case "requests":
			rl.Requests = literalInt(kv.Value)
		case "per":
			rl.Per = literalString(kv.Value)
		}
	}
	if rl.Requests <= 0 {
		return nil, fmt.Errorf("requests must be > 0")
	}
	if rl.Per == "" {
		return nil, fmt.Errorf("per is required (e.g. \"10s\", \"1m\", \"1h\")")
	}
	return rl, nil
}

// literalBool extracts a bool value from a Starlark expression (True/False ident).
func literalBool(expr syntax.Expr) bool {
	ident, ok := expr.(*syntax.Ident)
	if !ok {
		return false
	}
	return ident.Name == "True"
}

// resolveProjectDir returns the canonicalised projectDir suitable for
// path-containment comparison. Falls through `EvalSymlinks → Abs → ""`
// so a missing or unresolvable projectDir doesn't crash the scanner.
//
// Required because on macOS `/var → /private/var` symlinks make
// `t.TempDir()` produce `/var/folders/...` paths whose
// `EvalSymlinks` form is `/private/var/folders/...`. Without
// canonicalising the projectDir the same way the resolved file
// paths are canonicalised, every legitimate path would be flagged
// as "resolves outside project directory" — which is exactly the
// macOS unit-test failure observed on the v0.31 release tag push.
func resolveProjectDir(projectDir string) string {
	if real, err := filepath.EvalSymlinks(projectDir); err == nil {
		if abs, err := filepath.Abs(real); err == nil {
			return abs
		}
	}
	abs, _ := filepath.Abs(projectDir)
	return abs
}

// pathInsideProject reports whether `realPath` (already canonicalised
// via EvalSymlinks) is contained inside `absProject` (already
// canonicalised via resolveProjectDir).
func pathInsideProject(absProject, realPath string) bool {
	if absProject == "" {
		return true // can't validate; let callers proceed
	}
	absReal, err := filepath.Abs(realPath)
	if err != nil {
		return false
	}
	if absReal == absProject {
		return true
	}
	return strings.HasPrefix(absReal+string(filepath.Separator), absProject+string(filepath.Separator))
}

// extractStringList extracts a list of strings from a Starlark list expression.
func extractStringList(expr syntax.Expr) []string {
	list, ok := expr.(*syntax.ListExpr)
	if !ok {
		return nil
	}
	var result []string
	for _, item := range list.List {
		if s := literalString(item); s != "" {
			result = append(result, s)
		}
	}
	return result
}
