// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package libregistry

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.starlark.net/syntax"
)

// Column describes a column that a lib function can produce.
type Column struct {
	Name     string // column name (e.g. "AD_UNIT_NAME")
	Type     string // DuckDB type (e.g. "VARCHAR", "BIGINT")
	Category string // optional: "dimension" or "metric"
}

// SinkConfig holds SINK-specific configuration extracted from the SINK dict.
type SinkConfig struct {
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
	BaseURL   string            // prepended to relative URLs
	Auth      map[string]any    // auth config (provider, env, cert, etc.)
	Headers   map[string]string // default headers for all requests
	Timeout   int               // default request timeout (seconds)
	Retry     int               // default retry count
	Backoff   int               // default backoff multiplier
	RateLimit *RateLimitConfig  // default rate limit
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
	SinkConfig     *SinkConfig // SINK-specific config (nil for TABLE libs)
	APIConfig      *APIConfig  // shared API config (nil for legacy TABLE/SINK)
	PageSize       int         // fetch pagination: rows per page (0 = single call)
	FetchMode      string      // "sync" (default) or "async"
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
		data, err := os.ReadFile(path)
		if err != nil {
			continue
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

// SinkFuncs returns all SINK lib functions.
func (r *Registry) SinkFuncs() []*LibFunc {
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
			case "columns":
				cols, err := extractColumns(kv.Value)
				if err != nil {
					return nil, fmt.Errorf("API.fetch.columns: %w", err)
				}
				lf.Columns = cols
			case "dynamic_columns":
				lf.DynamicColumns = literalBool(kv.Value)
			case "page_size":
				if v := literalInt(kv.Value); v > 0 {
					lf.PageSize = v
				}
			case "mode":
				if v := literalString(kv.Value); v != "" {
					lf.FetchMode = v
				}
			case "poll_interval":
				// stored for runtime
			case "poll_timeout":
				// stored for runtime
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

		// Validate: dynamic_columns + columns together is an error
		if lf.DynamicColumns && len(lf.Columns) > 0 {
			return nil, fmt.Errorf("API.fetch: dynamic_columns and columns are mutually exclusive")
		}

		// Validate fetch() function exists
		if err := validateFunc(lf, false, f); err != nil {
			return nil, err
		}
	}

	if pushDict != nil {
		sinkCfg := &SinkConfig{
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

		lf.SinkConfig = sinkCfg

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
			if len(paramNames) != 1 {
				return nil, fmt.Errorf("push() must take exactly one parameter (rows), got %d", len(paramNames))
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

		// Extract param names (skip "save" for fetch, "rows" for push)
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

		// Validate TABLE.args match function params (TABLE only).
		// SINK push functions take only (rows) -- auth is via oauth.token()/env.get() inside push.
		if !isSink {
			// Legacy TABLE: first param is "save", skip it.
			// API dict with page_size: first param is first arg, last is "page", skip "page".
			// API dict without page_size: same as legacy (first param is "save").
			if lf.PageSize > 0 {
				// API paginated: skip "page" (last param)
				if len(paramNames) > 0 && paramNames[len(paramNames)-1] == "page" {
					paramNames = paramNames[:len(paramNames)-1]
				}
			} else if lf.APIConfig != nil {
				// API non-paginated: first param is "page" with size=0, skip it
				// Actually: non-paginated API fetch has (page) as first arg too
				// since all API fetches use the return-value contract.
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
				if p == "save" || p == "page" || p == "target" || p == "columns" {
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
			// SINK: validate push takes exactly one param (rows)
			if len(paramNames) != 1 {
				return fmt.Errorf("push() must take exactly one parameter (rows), got %d", len(paramNames))
			}
		}
		break
	}

	if !funcFound {
		return fmt.Errorf("no %s() function defined", expectedFunc)
	}

	return nil
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
// Values are extracted as strings or ints depending on their literal type.
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
// Returns (value, true) for string/int/bool literals, (nil, false) for others.
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

// extractColumns extracts column definitions from a TABLE.columns dict expression.
func extractColumns(expr syntax.Expr) ([]Column, error) {
	dict, ok := expr.(*syntax.DictExpr)
	if !ok {
		return nil, fmt.Errorf("expected dict, got %T", expr)
	}

	var cols []Column
	for _, entry := range dict.List {
		kv, ok := entry.(*syntax.DictEntry)
		if !ok {
			continue
		}
		colName := literalString(kv.Key)
		if colName == "" {
			continue
		}

		col := Column{Name: colName}

		// Value is a dict: {"type": "VARCHAR", "category": "dimension"}
		if valDict, ok := kv.Value.(*syntax.DictExpr); ok {
			for _, ve := range valDict.List {
				vkv, ok := ve.(*syntax.DictEntry)
				if !ok {
					continue
				}
				vkey := literalString(vkv.Key)
				vval := literalString(vkv.Value)
				switch vkey {
				case "type":
					col.Type = vval
				case "category":
					col.Category = vval
				}
			}
		}

		if col.Type == "" {
			col.Type = "VARCHAR" // default
		}

		cols = append(cols, col)
	}

	return cols, nil
}
