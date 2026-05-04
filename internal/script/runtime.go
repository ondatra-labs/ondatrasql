// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package script provides a Starlark-based scripting runtime for data pipelines.
// Scripts can fetch data from REST APIs, transform it, and save to DuckLake.
package script

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"go.starlark.net/starlark"
	starlarkjson "go.starlark.net/lib/json"
	starlarktime "go.starlark.net/lib/time"
	"go.starlark.net/syntax"

	"github.com/ondatra-labs/ondatrasql/internal/backfill"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
)

// recoverScriptPanic converts a panic into an error written through
// errOut. Used as `defer recoverScriptPanic(&err)` at the top of every
// public Runtime entry point so a Go-side builtin/helper panic
// surfaces as a normal error rather than crashing the host process.
//
// Returned error has a STABLE prefix `starlark execution panicked: `
// (callers can `strings.HasPrefix` on it). The suffix is the panic
// value rendered via panicValueString below — explicitly NOT plain
// `%v` so a struct/map/error panic produces a deterministic suffix
// instead of Go's default formatter (which emits non-canonical
// braces, quotes, and field-order-dependent text). The stack trace
// is written separately to stderr because:
//
//   - debug.Stack() output isn't a stable cross-Go-version contract;
//     embedding it in the error baked instability into the public
//     surface.
//   - Stack frames carry absolute build-host paths, which leak when
//     an error is logged to a downstream system or surfaced
//     verbatim in a JSON envelope.
//
// Operators still get the stack on stderr where it belongs as
// diagnostic output, separate from the error contract.
func recoverScriptPanic(errOut *error) {
	if r := recover(); r != nil {
		fmt.Fprintf(os.Stderr, "starlark execution panicked: %s\n%s", panicValueString(r), debug.Stack())
		*errOut = fmt.Errorf("starlark execution panicked: %s", panicValueString(r))
	}
}

// panicValueString renders a recovered panic value with a stable
// suffix shape:
//
//   - error    → its Error() text
//   - string   → the string itself
//   - other    → "<type>: <%v formatting>" so the type prefix
//                disambiguates structurally-different values that
//                happen to format the same, AND non-string panic
//                values still produce traceable diagnostic text.
//
// (R9 #7: pre-fix used bare `%v`, so a struct/map panic value emitted
// non-canonical braces and field-order-dependent text — incompatible
// with the documented "stable prefix" contract.)
func panicValueString(r any) string {
	switch v := r.(type) {
	case error:
		return v.Error()
	case string:
		return v
	default:
		return fmt.Sprintf("%T: %v", r, r)
	}
}

// AbortError is returned when a script calls abort() for a clean early exit.
type AbortError struct{}

func (e *AbortError) Error() string {
	return "script aborted"
}

// Runtime executes Starlark scripts with built-in modules for data pipelines.
type Runtime struct {
	sess       *duckdb.Session
	incrState  *backfill.IncrementalState
	projectDir string
	ingestDir  string // When set, uses Badger for durable save() buffering
}

// NewRuntime creates a new script runtime with DuckDB access.
// If incrState is provided, the incremental module will be available to scripts.
// An optional projectDir enables load() for importing shared Starlark modules.
func NewRuntime(sess *duckdb.Session, incrState *backfill.IncrementalState, projectDir ...string) *Runtime {
	rt := &Runtime{sess: sess, incrState: incrState}
	if len(projectDir) > 0 {
		rt.projectDir = projectDir[0]
	}
	return rt
}

// SetIngestDir enables durable Badger-backed save() buffering.
// When set, save.row() writes to Badger at ingestDir instead of in-memory.
func (r *Runtime) SetIngestDir(dir string) {
	r.ingestDir = dir
}

// parseEmptyResult reads the optional "empty_result" key from a fetch
// return dict and returns the parsed intent, falling back to defaultIntent
// when the key is missing or carries an unknown string. Unknown values
// are silently coerced rather than rejected — the runner treats this as
// soft metadata, not a contract that should fail the run.
func parseEmptyResult(retDict *starlark.Dict, defaultIntent EmptyResultIntent) EmptyResultIntent {
	v, found, _ := retDict.Get(starlark.String("empty_result"))
	if !found {
		return defaultIntent
	}
	s, ok := starlark.AsString(v)
	if !ok {
		return defaultIntent
	}
	switch EmptyResultIntent(s) {
	case EmptyNoChange:
		return EmptyNoChange
	case EmptyDeleteMissing:
		return EmptyDeleteMissing
	default:
		return defaultIntent
	}
}

// EmptyResultIntent describes what a 0-row fetch return means in the context
// of a tracked-kind model. The lib declares this per-call via its fetch
// return dict; the runner uses it to decide whether to delete groups that
// disappeared from the source or preserve the existing target.
type EmptyResultIntent string

const (
	// EmptyNoChange means "the source genuinely has no new data; target
	// state is still authoritative". Tracked materialize must NOT delete
	// groups missing from this empty result. Default for lib-backed tracked.
	EmptyNoChange EmptyResultIntent = "no_change"
	// EmptyDeleteMissing means "the source is fully enumerated, and the
	// 0-row response represents a real empty state". Tracked materialize
	// proceeds with delete-on-missing semantics. Opt-in.
	EmptyDeleteMissing EmptyResultIntent = "delete_missing"
)

// Result contains the outcome of script execution.
type Result struct {
	TempTable   string            // Name of temp table with collected data
	RowCount    int64             // Number of rows collected
	Duration    time.Duration     // Script execution time
	ClaimIDs    []string          // Badger claim IDs (non-nil only when Badger is used)
	EmptyResult EmptyResultIntent // Lib's intent when RowCount==0; defaults to EmptyNoChange
	collector   *saveCollector    // internal: deferred temp table creation (in-memory mode)
	badger      *badgerCollector  // internal: durable mode
}

// blueprintFileOptions returns the strict Starlark syntax options used
// when loading lib/*.star blueprint files. v0.30.0 tightens two toggles
// to enforce a clean module structure:
//
//   - GlobalReassign=false → a top-level name (API, fetch, push, helpers)
//     can be defined exactly once. Catches `API = {...}; API = {...}`
//     mutations that silently overwrite the dict the runtime introspects.
//   - TopLevelControl=false → top-level if/for/while is rejected. The
//     module structure becomes purely declarative (constants, dict
//     literals, function defs). Conditional logic belongs inside a
//     function body, not at module load time.
//
// Recursion / While / Set stay enabled — those are widely used in
// fetch/push function bodies (parsing pagination cursors, retrying
// transient errors, building deduplication sets).
func blueprintFileOptions() *syntax.FileOptions {
	return &syntax.FileOptions{
		Set:             true,  // set() built-in
		While:           true,  // while loops (inside function bodies)
		Recursion:       true,  // recursive functions
		TopLevelControl: false, // no if/for/while at module top level
		GlobalReassign:  false, // each top-level name defined once
	}
}

// adhocFileOptions returns the permissive Starlark syntax options used
// for ad-hoc script execution via Runtime.Run (entrypoint for legacy
// @script models and the runtime test harness that exercises modules
// like time/json/csv directly with top-level statements). Blueprints
// loaded via load() still get the strict blueprintFileOptions.
func adhocFileOptions() *syntax.FileOptions {
	return &syntax.FileOptions{
		Set:             true,
		While:           true,
		Recursion:       true,
		TopLevelControl: true,
		GlobalReassign:  true,
	}
}

// libraryPredeclared returns the predeclared globals available to library modules.
// This excludes save (write side effects — must be passed explicitly as a parameter).
func (r *Runtime) libraryPredeclared(ctx context.Context, httpCfg ...*apiHTTPConfig) starlark.StringDict {
	var cfg *apiHTTPConfig
	if len(httpCfg) > 0 {
		cfg = httpCfg[0]
	}
	return starlark.StringDict{
		// Core I/O
		"http":        httpModule(ctx, cfg),
		"env":         envModule(),
		"xml":         xmlModule(),
		"csv":         csvModule(),
		"lib_helpers": libHelpersModule(),

		// DuckDB-backed builtins
		"glob":        globBuiltin(r.sess),
		"md5_file":    md5FileBuiltin(r.sess),
		"read_text":   readTextBuiltin(r.sess),
		"read_blob":   readBlobBuiltin(r.sess),
		"file_exists": fileExistsBuiltin(r.sess),
		"md5":         md5Builtin(r.sess),
		"sha256":      sha256Builtin(r.sess),
		"uuid":        uuidBuiltin(r.sess),
		"lookup":      lookupBuiltin(r.sess),

		// Crypto builtins (Go-native, not DuckDB)
		"hmac_sha256":   hmacSha256Builtin(),
		"base64_encode": base64EncodeBuiltin(),
		"base64_decode": base64DecodeBuiltin(),

		"time":        starlarktime.Module,
		"json":        starlarkjson.Module,
		"abort": starlark.NewBuiltin("abort", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 0); err != nil {
				return nil, err
			}
			return nil, &AbortError{}
		}),
		"sleep": starlark.NewBuiltin("sleep", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			var seconds starlark.Value
			if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &seconds); err != nil {
				return nil, err
			}
			var dur time.Duration
			switch v := seconds.(type) {
			case starlark.Int:
				i, ok := v.Int64()
				if !ok {
					return nil, fmt.Errorf("sleep: integer too large")
				}
				dur = time.Duration(i) * time.Second
			case starlark.Float:
				dur = time.Duration(float64(v) * float64(time.Second))
			default:
				return nil, fmt.Errorf("sleep: expected int or float, got %s", seconds.Type())
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(dur):
			}
			return starlark.None, nil
		}),
	}
}

// makeLoadFunc creates a Starlark load() handler that imports modules from the project directory.
// The cache is shared across all nested loads within a single Run() invocation, so each module
// is executed at most once regardless of how many modules import it. A nil cache entry signals
// that a module is currently being loaded, which detects import cycles.
func (r *Runtime) makeLoadFunc(ctx context.Context, libPredeclared starlark.StringDict, cache map[string]starlark.StringDict) func(*starlark.Thread, string) (starlark.StringDict, error) {
	return func(thread *starlark.Thread, module string) (starlark.StringDict, error) {
		if r.projectDir == "" {
			return nil, fmt.Errorf("load(%q): not available (no project directory)", module)
		}

		// Resolve real path (follow symlinks) and check for path traversal
		absProjectDir, err := filepath.EvalSymlinks(r.projectDir)
		if err != nil {
			return nil, fmt.Errorf("load(%q): resolve project dir: %w", module, err)
		}
		absProjectDir, err = filepath.Abs(absProjectDir)
		if err != nil {
			return nil, fmt.Errorf("load(%q): resolve project dir: %w", module, err)
		}
		joined := filepath.Join(r.projectDir, module)
		absPath, err := filepath.EvalSymlinks(joined)
		if err != nil {
			// EvalSymlinks fails if file doesn't exist — fall back to Abs for better error later
			absPath, err = filepath.Abs(joined)
			if err != nil {
				return nil, fmt.Errorf("load(%q): resolve path: %w", module, err)
			}
		} else {
			absPath, err = filepath.Abs(absPath)
			if err != nil {
				return nil, fmt.Errorf("load(%q): resolve path: %w", module, err)
			}
		}
		if !strings.HasPrefix(absPath, absProjectDir+string(filepath.Separator)) {
			return nil, fmt.Errorf("load(%q): path traversal not allowed", module)
		}

		// Return cached result if already loaded
		if cached, ok := cache[absPath]; ok {
			if cached == nil {
				// nil sentinel means this module is currently being loaded — cycle
				return nil, fmt.Errorf("load(%q): import cycle detected", module)
			}
			return cached, nil
		}

		// Mark as loading (nil sentinel for cycle detection)
		cache[absPath] = nil

		// Read the module file
		data, err := os.ReadFile(absPath)
		if err != nil {
			delete(cache, absPath)
			return nil, fmt.Errorf("load(%q): %w", module, err)
		}

		// Execute with library predeclared (no save/incremental)
		libThread := &starlark.Thread{
			Name: module,
			Print: func(_ *starlark.Thread, msg string) {
				fmt.Fprintln(os.Stderr, RedactSecrets(msg))
			},
		}
		libThread.SetLocal("ctx", ctx)
		libThread.SetLocal("sess", r.sess)
		// Share the same cache for nested loads
		libThread.Load = r.makeLoadFunc(ctx, libPredeclared, cache)

		globals, err := starlark.ExecFileOptions(blueprintFileOptions(), libThread, module, data, libPredeclared)
		if err != nil {
			delete(cache, absPath)
			return nil, fmt.Errorf("load(%q): %w", module, err)
		}

		cache[absPath] = globals
		return globals, nil
	}
}

// Run executes a Starlark script and returns a temp table with collected data.
// The executor should then use materialize() to handle backfill, schema evolution, etc.
func (r *Runtime) Run(ctx context.Context, target, code string) (result *Result, err error) {
	defer recoverScriptPanic(&err)
	start := time.Now()

	// Create collector: Badger-backed (durable) or in-memory
	var collector rowCollector
	var bc *badgerCollector
	if r.ingestDir != "" {
		var err error
		bc, err = newBadgerCollector(target, r.ingestDir, r.sess)
		if err != nil {
			return nil, fmt.Errorf("create ingest collector: %w", err)
		}
		collector = bc
	} else {
		collector = &saveCollector{
			target: target,
			sess:   r.sess,
		}
	}

	// Track whether we returned successfully so defer can close bc on error
	var succeeded bool
	if bc != nil {
		defer func() {
			if !succeeded {
				_ = bc.close() // close logs internally on failure; runtime cleanup path
			}
		}()
	}

	// Create a Starlark thread with context for cancellation
	thread := &starlark.Thread{
		Name: target,
		Print: func(_ *starlark.Thread, msg string) {
			fmt.Fprintln(os.Stderr, RedactSecrets(msg))
		},
	}
	thread.SetLocal("ctx", ctx)
	thread.SetLocal("sess", r.sess)

	// Set up context cancellation via thread.Cancel
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			thread.Cancel(ctx.Err().Error())
		case <-done:
		}
	}()

	// Build predeclared globals — library modules plus save (model-specific write builtin)
	libPredeclared := r.libraryPredeclared(ctx)
	predeclared := make(starlark.StringDict, len(libPredeclared)+1)
	for k, v := range libPredeclared {
		predeclared[k] = v
	}
	predeclared["save"] = saveModule(collector)

	// Enable load() for importing shared modules (shared cache across nested loads)
	loadCache := make(map[string]starlark.StringDict)
	thread.Load = r.makeLoadFunc(ctx, libPredeclared, loadCache)

	// Ad-hoc Run() uses permissive options (top-level if/for/while OK,
	// global reassign OK). Blueprints loaded via load() get the strict
	// blueprintFileOptions instead.
	_, err = starlark.ExecFileOptions(adhocFileOptions(), thread, target+".star", code, predeclared)
	if err != nil {
		return nil, fmt.Errorf("run script: %w", err)
	}

	// Return result with collector for deferred temp table creation.
	// The caller should call result.CreateTempTable() after resuming DuckDB.
	result = &Result{
		Duration: time.Since(start),
		RowCount: int64(collector.count()),
		// Script-kind models don't currently flow through the lib smart-skip
		// path, but defaulting to no_change keeps Result construction
		// consistent across all four entry points (Run / RunSource /
		// RunSourcePaginated / RunSourceAsync) and avoids a footgun if a
		// future code path routes script kind through allLibsReturnedNoChange.
		EmptyResult: EmptyNoChange,
		badger:      bc,
	}
	if sc, ok := collector.(*saveCollector); ok {
		result.collector = sc
	}

	succeeded = true
	return result, nil
}

// RunSource executes a lib/ blueprint by loading a source function from lib/ and
// calling it directly via starlark.Call(). This avoids generating Starlark source
// code as a string — config values are converted to Starlark values in Go and
// passed as keyword arguments.
func (r *Runtime) RunSource(ctx context.Context, target, source string, config map[string]any) (result *Result, err error) {
	defer recoverScriptPanic(&err)
	start := time.Now()

	// Create collector: Badger-backed (durable) or in-memory
	var collector rowCollector
	var bc *badgerCollector
	if r.ingestDir != "" {
		var err error
		bc, err = newBadgerCollector(target, r.ingestDir, r.sess)
		if err != nil {
			return nil, fmt.Errorf("create ingest collector: %w", err)
		}
		collector = bc
	} else {
		collector = &saveCollector{
			target: target,
			sess:   r.sess,
		}
	}

	// Track whether we returned successfully so defer can close bc on error
	var succeeded bool
	if bc != nil {
		defer func() {
			if !succeeded {
				_ = bc.close() // close logs internally on failure; runtime cleanup path
			}
		}()
	}

	// Build predeclared
	libPredeclared := r.libraryPredeclared(ctx)

	// Load the source module via the same load() mechanism
	loadCache := make(map[string]starlark.StringDict)
	loadFunc := r.makeLoadFunc(ctx, libPredeclared, loadCache)

	// Create a thread for the load
	thread := &starlark.Thread{
		Name: target,
		Print: func(_ *starlark.Thread, msg string) {
			fmt.Fprintln(os.Stderr, RedactSecrets(msg))
		},
		Load: loadFunc,
	}
	thread.SetLocal("ctx", ctx)
	thread.SetLocal("sess", r.sess)

	// Set up context cancellation
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			thread.Cancel(ctx.Err().Error())
		case <-done:
		}
	}()

	// Load the module: lib/<source>.star
	modulePath := "lib/" + source + ".star"
	globals, err := loadFunc(thread, modulePath)
	if err != nil {
		return nil, fmt.Errorf("load source %q: %w", source, err)
	}

	// Get the function by name — try "fetch" first (lib convention), then source name
	funcName := "fetch"
	fn, ok := globals[funcName]
	if !ok {
		// Fallback: try the source name (backwards compat)
		funcName = source
		fn, ok = globals[funcName]
		if !ok {
			return nil, fmt.Errorf("source %q: no fetch() or %s() function in %s", source, source, modulePath)
		}
	}
	callable, ok := fn.(starlark.Callable)
	if !ok {
		return nil, fmt.Errorf("source %q: %q is not a function (got %s)", source, source, fn.Type())
	}

	// Build positional args: (save,)
	// incremental is available as a global in library scope (read-only state)
	args := starlark.Tuple{
		saveModule(collector),
	}

	// Build keyword args from config
	var kwargs []starlark.Tuple
	for key, val := range config {
		sv, err := goToStarlark(val)
		if err != nil {
			return nil, fmt.Errorf("source %q: config key %q: %w", source, key, err)
		}
		kwargs = append(kwargs, starlark.Tuple{starlark.String(key), sv})
	}

	// Call the function
	_, err = starlark.Call(thread, callable, args, kwargs)
	if err != nil {
		return nil, fmt.Errorf("run source: %w", err)
	}

	result = &Result{
		Duration: time.Since(start),
		RowCount: int64(collector.count()),
		// Legacy save.row() libs have no contract for declaring empty
		// semantics — default to no_change so they get the same safe
		// preserve-target behavior as the lib-API path.
		EmptyResult: EmptyNoChange,
		badger:      bc,
	}
	if sc, ok := collector.(*saveCollector); ok {
		result.collector = sc
	}

	succeeded = true
	return result, nil
}

// RunSourcePaginated executes a paginated fetch from lib/<source>.star.
// Runtime calls fetch(page, **kwargs) per page until next is None.
// fetch() returns {"rows": [...], "next": cursor_or_none}.
func (r *Runtime) RunSourcePaginated(ctx context.Context, target, source string, config map[string]any, pageSize int, httpCfg ...*apiHTTPConfig) (result *Result, err error) {
	defer recoverScriptPanic(&err)
	start := time.Now()

	var cfg *apiHTTPConfig
	if len(httpCfg) > 0 {
		cfg = httpCfg[0]
	}

	// Create collector
	var collector rowCollector
	var bc *badgerCollector
	if r.ingestDir != "" {
		var err error
		bc, err = newBadgerCollector(target, r.ingestDir, r.sess)
		if err != nil {
			return nil, fmt.Errorf("create ingest collector: %w", err)
		}
		collector = bc
	} else {
		collector = &saveCollector{target: target, sess: r.sess}
	}

	// Track whether we returned successfully so defer can close bc on error
	var succeeded bool
	if bc != nil {
		defer func() {
			if !succeeded {
				_ = bc.close() // close logs internally on failure; runtime cleanup path
			}
		}()
	}

	libPredeclared := r.libraryPredeclared(ctx, cfg)

	loadCache := make(map[string]starlark.StringDict)
	loadFunc := r.makeLoadFunc(ctx, libPredeclared, loadCache)

	thread := &starlark.Thread{
		Name: target,
		Print: func(_ *starlark.Thread, msg string) {
			fmt.Fprintln(os.Stderr, RedactSecrets(msg))
		},
		Load: loadFunc,
	}
	thread.SetLocal("ctx", ctx)
	thread.SetLocal("sess", r.sess)

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			thread.Cancel(ctx.Err().Error())
		case <-done:
		}
	}()

	modulePath := "lib/" + source + ".star"
	globals, err := loadFunc(thread, modulePath)
	if err != nil {
		return nil, fmt.Errorf("load source %q: %w", source, err)
	}

	fn, ok := globals["fetch"]
	if !ok {
		return nil, fmt.Errorf("source %q: no fetch() function in %s", source, modulePath)
	}
	callable, ok := fn.(starlark.Callable)
	if !ok {
		return nil, fmt.Errorf("source %q: fetch is not a function (got %s)", source, fn.Type())
	}

	// Check for finalize_fetch
	var finalizeFn starlark.Callable
	if ff, ok := globals["finalize_fetch"]; ok {
		if fc, ok := ff.(starlark.Callable); ok {
			finalizeFn = fc
		}
	}

	// Build keyword args from config
	var kwargs []starlark.Tuple
	for key, val := range config {
		sv, err := goToStarlark(val)
		if err != nil {
			return nil, fmt.Errorf("source %q: config key %q: %w", source, key, err)
		}
		kwargs = append(kwargs, starlark.Tuple{starlark.String(key), sv})
	}

	// Pagination loop
	var cursor starlark.Value = starlark.None
	pageNum := 1 // internal counter for error messages only
	// Default for lib-backed tracked: 0 rows means "no change", preserve target.
	// Lib can override per-call by setting "empty_result" in the final fetch dict.
	emptyIntent := EmptyNoChange

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		page := pageModule(cursor, pageSize)

		// Call fetch(**kwargs, page=page) -- page passed as keyword arg
		allKwargs := make([]starlark.Tuple, len(kwargs)+1)
		copy(allKwargs, kwargs)
		allKwargs[len(kwargs)] = starlark.Tuple{starlark.String("page"), page}
		retVal, err := starlark.Call(thread, callable, nil, filterKwargs(callable, allKwargs))
		if err != nil {
			return nil, fmt.Errorf("fetch page %d: %w", pageNum, err)
		}

		// Parse return value: {"rows": [...], "next": cursor_or_none}
		retDict, ok := retVal.(*starlark.Dict)
		if !ok {
			return nil, fmt.Errorf("fetch page %d: must return dict {rows, next}, got %s", pageNum, retVal.Type())
		}

		// Extract rows
		rowsVal, found, _ := retDict.Get(starlark.String("rows"))
		if !found {
			return nil, fmt.Errorf("fetch page %d: return dict must include 'rows' key", pageNum)
		}
		rowsList, ok := rowsVal.(*starlark.List)
		if !ok {
			return nil, fmt.Errorf("fetch page %d: 'rows' must be a list, got %s", pageNum, rowsVal.Type())
		}

		// save.row() for each returned row
		for i := 0; i < rowsList.Len(); i++ {
			item := rowsList.Index(i)
			goVal, err := starlarkToGo(item)
			if err != nil {
				return nil, fmt.Errorf("fetch page %d row %d: convert: %w", pageNum, i, err)
			}
			rowMap, ok := goVal.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("fetch page %d row %d: expected dict, got %T", pageNum, i, goVal)
			}
			if addErr := collector.add(rowMap); addErr != nil {
				return nil, fmt.Errorf("fetch page %d row %d: %w", pageNum, i, addErr)
			}
		}

		// Extract next cursor. None, missing, or empty string = last page.
		nextVal, found, _ := retDict.Get(starlark.String("next"))
		if !found || nextVal == starlark.None {
			emptyIntent = parseEmptyResult(retDict, emptyIntent)
			break
		}
		if s, ok := nextVal.(starlark.String); ok && string(s) == "" {
			emptyIntent = parseEmptyResult(retDict, emptyIntent)
			break
		}
		cursor = nextVal
		pageNum++
	}

	// Call finalize_fetch(row_count) if defined
	if finalizeFn != nil {
		_, err := starlark.Call(thread, finalizeFn, starlark.Tuple{
			starlark.MakeInt64(int64(collector.count())),
		}, nil)
		if err != nil {
			return nil, fmt.Errorf("finalize_fetch: %w", err)
		}
	}

	result = &Result{
		Duration:    time.Since(start),
		RowCount:    int64(collector.count()),
		EmptyResult: emptyIntent,
		badger:      bc,
	}
	if sc, ok := collector.(*saveCollector); ok {
		result.collector = sc
	}

	succeeded = true
	return result, nil
}

// RunSourceAsync executes an async fetch blueprint: submit() → poll check() → fetch_result().
// Runtime handles the poll loop with interval, backoff, and timeout from API config.
func (r *Runtime) RunSourceAsync(ctx context.Context, target, source string, config map[string]any, pageSize int, pollInterval, pollTimeout time.Duration, pollBackoff int, httpCfg ...*apiHTTPConfig) (result *Result, err error) {
	defer recoverScriptPanic(&err)
	start := time.Now()
	var cfg *apiHTTPConfig
	if len(httpCfg) > 0 {
		cfg = httpCfg[0]
	}

	// Create collector: Badger-backed (durable) or in-memory
	var collector rowCollector
	var bc *badgerCollector
	if r.ingestDir != "" {
		var err error
		bc, err = newBadgerCollector(target, r.ingestDir, r.sess)
		if err != nil {
			return nil, fmt.Errorf("create ingest collector: %w", err)
		}
		collector = bc
	} else {
		collector = &saveCollector{target: target, sess: r.sess}
	}

	// Track whether we returned successfully so defer can close bc on error
	var succeeded bool
	if bc != nil {
		defer func() {
			if !succeeded {
				_ = bc.close() // close logs internally on failure; runtime cleanup path
			}
		}()
	}

	libPredeclared := r.libraryPredeclared(ctx, cfg)

	loadCache := make(map[string]starlark.StringDict)
	loadFunc := r.makeLoadFunc(ctx, libPredeclared, loadCache)

	thread := &starlark.Thread{
		Name: target,
		Print: func(_ *starlark.Thread, msg string) {
			fmt.Fprintln(os.Stderr, RedactSecrets(msg))
		},
		Load: loadFunc,
	}
	thread.SetLocal("ctx", ctx)
	thread.SetLocal("sess", r.sess)

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			thread.Cancel(ctx.Err().Error())
		case <-done:
		}
	}()

	modulePath := "lib/" + source + ".star"
	globals, err := loadFunc(thread, modulePath)
	if err != nil {
		return nil, fmt.Errorf("load source %q: %w", source, err)
	}

	submitFn, err := getCallable(globals, "submit", source)
	if err != nil {
		return nil, err
	}
	checkFn, err := getCallable(globals, "check", source)
	if err != nil {
		return nil, err
	}
	fetchResultFn, err := getCallable(globals, "fetch_result", source)
	if err != nil {
		return nil, err
	}

	// Build kwargs from config for submit()
	var submitKwargs []starlark.Tuple
	for key, val := range config {
		sv, err := goToStarlark(val)
		if err != nil {
			return nil, fmt.Errorf("source %q: config key %q: %w", source, key, err)
		}
		submitKwargs = append(submitKwargs, starlark.Tuple{starlark.String(key), sv})
	}

	// Phase 1: submit()
	submitRet, err := starlark.Call(thread, submitFn, nil, filterKwargs(submitFn, submitKwargs))
	if err != nil {
		return nil, fmt.Errorf("submit: %w", err)
	}
	if submitRet == nil || submitRet == starlark.None {
		// abort() in submit — 0 rows. Default to no_change so the runner's
		// smart-skip path preserves target rows; without this, an aborted
		// async fetch would silently fall through to destructive
		// delete-on-missing semantics for tracked models.
		return &Result{Duration: time.Since(start), EmptyResult: EmptyNoChange}, nil
	}

	// Phase 2: poll check() until done.
	checkKwargs := []starlark.Tuple{
		{starlark.String("job_ref"), submitRet},
	}
	const maxPollInterval = 30 * time.Second
	// Cap the initial pollInterval so a misconfigured blueprint with
	// `fetch_poll_interval: "10m"` can't stall a single iteration
	// past pollTimeout. Validation in runner.go already rejects
	// non-positive values, so we only need to clamp the upper bound.
	currentInterval := pollInterval
	if currentInterval > maxPollInterval {
		currentInterval = maxPollInterval
	}
	pollStart := time.Now()

	// Call check() IMMEDIATELY after submit (no initial sleep) —
	// pre-R10 the loop always slept currentInterval before the
	// first check, so when pollTimeout < clamped pollInterval
	// (e.g. timeout=10s, interval=30s) the loop timed out without
	// ever calling check. Most async APIs return submitted-but-not-
	// done initially, so the immediate first-check typically falls
	// through to the sleep+retry path below, but a fast-path
	// "already done" check is now correctly observed too.
	// (R10 #2 + R11 #9.)
	//
	// Observability note: timing/metrics that previously assumed
	// "first check happens after one pollInterval" need updating —
	// the first poll now happens at t=0 (post-submit), and
	// subsequent polls happen at t=currentInterval intervals from
	// the previous one. The loop calls check() at most ceil(
	// pollTimeout/currentInterval)+1 times in the worst case (the
	// +1 is the immediate first call).
	var resultRef starlark.Value
	{
		// Honour context cancellation before the very first check.
		// A caller that already cancelled the request between submit
		// and check (e.g. SIGINT lands during the submit RTT) should
		// not have one extra check fire on its way out. (R11 #1.)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// pollTimeout=0 is technically valid (caller wants
		// fail-fast). Honour it before calling check, so a
		// 0-budget request doesn't spin one check before failing.
		if time.Since(pollStart) > pollTimeout {
			return nil, fmt.Errorf("async fetch timed out after %v", pollTimeout)
		}
		checkRet, err := starlark.Call(thread, checkFn, nil, filterKwargs(checkFn, checkKwargs))
		if err != nil {
			return nil, fmt.Errorf("check: %w", err)
		}
		if checkRet != nil && checkRet != starlark.None {
			resultRef = checkRet
		}
	}

	for resultRef == nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Check timeout BEFORE sleeping — guarantees we don't sleep
		// through the entire pollTimeout budget when
		// currentInterval >= pollTimeout. Combined with the
		// pre-loop check() above, this means: on the first
		// iteration we've already called check once, so this
		// check-then-sleep is a normal poll cycle.
		if time.Since(pollStart) > pollTimeout {
			return nil, fmt.Errorf("async fetch timed out after %v", pollTimeout)
		}

		// Context-aware sleep — responds to cancellation during poll interval
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(currentInterval):
		}

		checkRet, err := starlark.Call(thread, checkFn, nil, filterKwargs(checkFn, checkKwargs))
		if err != nil {
			return nil, fmt.Errorf("check: %w", err)
		}

		if checkRet != nil && checkRet != starlark.None {
			resultRef = checkRet
			break
		}

		if time.Since(pollStart) > pollTimeout {
			return nil, fmt.Errorf("async fetch timed out after %v", pollTimeout)
		}

		if pollBackoff > 1 {
			// Cap BEFORE multiplying. Pre-fix the cap was applied
			// after the multiply, so a malformed blueprint with
			// pollBackoff in the millions could overflow
			// time.Duration (int64 nanoseconds) to a negative value
			// — `time.After(currentInterval)` then fires immediately
			// and the poll loop busy-spins until the outer timeout.
			// (R8 #8.) Capping pre-multiply also prevents a single
			// overflow from a huge currentInterval seed; the initial
			// clamp above (R9 #2) handles the seed itself.
			if currentInterval >= maxPollInterval/time.Duration(pollBackoff) {
				currentInterval = maxPollInterval
			} else {
				currentInterval = currentInterval * time.Duration(pollBackoff)
			}
		}
	}

	// Phase 3: fetch_result() with pagination
	var cursor starlark.Value = starlark.None
	pageNum := 1
	emptyIntent := EmptyNoChange

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		page := pageModule(cursor, pageSize)
		fetchKwargs := []starlark.Tuple{
			{starlark.String("result_ref"), resultRef},
			{starlark.String("page"), page},
		}

		retVal, err := starlark.Call(thread, fetchResultFn, nil, filterKwargs(fetchResultFn, fetchKwargs))
		if err != nil {
			return nil, fmt.Errorf("fetch_result page %d: %w", pageNum, err)
		}

		retDict, ok := retVal.(*starlark.Dict)
		if !ok {
			return nil, fmt.Errorf("fetch_result page %d: must return dict, got %s", pageNum, retVal.Type())
		}

		rowsVal, found, err := retDict.Get(starlark.String("rows"))
		if err != nil || !found {
			return nil, fmt.Errorf("fetch_result page %d: must include 'rows' key", pageNum)
		}
		rowsList, ok := rowsVal.(*starlark.List)
		if !ok {
			return nil, fmt.Errorf("fetch_result page %d: 'rows' must be a list", pageNum)
		}

		for i := 0; i < rowsList.Len(); i++ {
			goVal, err := starlarkToGo(rowsList.Index(i))
			if err != nil {
				return nil, fmt.Errorf("fetch_result page %d row %d: %w", pageNum, i, err)
			}
			m, ok := goVal.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("fetch_result page %d row %d: expected dict", pageNum, i)
			}
			if addErr := collector.add(m); addErr != nil {
				return nil, fmt.Errorf("fetch_result page %d row %d: %w", pageNum, i, addErr)
			}
		}

		nextVal, _, _ := retDict.Get(starlark.String("next"))
		if nextVal == nil || nextVal == starlark.None {
			emptyIntent = parseEmptyResult(retDict, emptyIntent)
			break
		}
		if s, ok := starlark.AsString(nextVal); ok && s == "" {
			emptyIntent = parseEmptyResult(retDict, emptyIntent)
			break
		}
		cursor = nextVal
		pageNum++
	}

	result = &Result{
		Duration:    time.Since(start),
		RowCount:    int64(collector.count()),
		EmptyResult: emptyIntent,
		badger:      bc,
	}
	if sc, ok := collector.(*saveCollector); ok {
		result.collector = sc
	}

	succeeded = true
	return result, nil
}

// getCallable finds a callable function in globals by name.
func getCallable(globals starlark.StringDict, name, source string) (starlark.Callable, error) {
	fn, ok := globals[name]
	if !ok {
		return nil, fmt.Errorf("source %q: no %s() function", source, name)
	}
	callable, ok := fn.(starlark.Callable)
	if !ok {
		return nil, fmt.Errorf("source %q: %s is not callable", source, name)
	}
	return callable, nil
}

// filterKwargs returns only the kwargs that the function accepts.
// If the function has **kwargs, all kwargs are passed through.
func filterKwargs(fn starlark.Callable, kwargs []starlark.Tuple) []starlark.Tuple {
	sf, ok := fn.(*starlark.Function)
	if !ok {
		return kwargs // built-in or wrapper — pass all
	}
	if sf.HasKwargs() {
		return kwargs // function accepts **kwargs — pass all
	}
	accepted := make(map[string]bool, sf.NumParams())
	for i := 0; i < sf.NumParams(); i++ {
		name, _ := sf.Param(i)
		accepted[name] = true
	}
	filtered := make([]starlark.Tuple, 0, len(kwargs))
	for _, kv := range kwargs {
		if name, ok := starlark.AsString(kv[0]); ok && accepted[name] {
			filtered = append(filtered, kv)
		}
	}
	return filtered
}

// PushResult holds the return value from a push() call.
type PushResult struct {
	// PerRow maps sync_key -> "ok" or "error: message" (sync mode).
	// Nil if push returned None (atomic mode).
	PerRow map[string]string

	// RawReturn holds the full Starlark return value converted to Go types.
	// Used by async mode for job references that may contain non-string values
	// (e.g. {"job_id": 123, "meta": {"cursor": "x"}}).
	RawReturn map[string]any
}

// RunPush executes a SINK push function from lib/<sink>.star.
// It calls push(rows, batch_number, kind, key_columns, columns, sink args...) with the given batch.
// Optional httpCfg injects API dict defaults (base_url, headers, etc.) into http module.
//
// `tableColumns` carries the materialized DuckLake table's schema as
// `[]map[string]any` with `name` (the materialized column name = the SQL
// alias the row dicts are keyed by) and `type` (the real DuckDB type
// from the table — DuckDB-native syntax: "VARCHAR", "DECIMAL(18,3)",
// etc.). Pass nil to fall back to deriving names from the row dicts
// (untyped); the caller does this only when the schema lookup fails.
func (r *Runtime) RunPush(ctx context.Context, sinkName string, rows []map[string]any, batchNumber int, kind string, uniqueKey string, tableColumns []map[string]any, sinkArgs map[string]string, httpCfg ...*apiHTTPConfig) (result *PushResult, err error) {
	defer recoverScriptPanic(&err)
	var cfg *apiHTTPConfig
	if len(httpCfg) > 0 {
		cfg = httpCfg[0]
	}
	libPredeclared := r.libraryPredeclared(ctx, cfg)

	// sink module removed — batch_number passed as kwarg to push()

	loadCache := make(map[string]starlark.StringDict)
	loadFunc := r.makeLoadFunc(ctx, libPredeclared, loadCache)

	thread := &starlark.Thread{
		Name: sinkName,
		Print: func(_ *starlark.Thread, msg string) {
			fmt.Fprintln(os.Stderr, RedactSecrets(msg))
		},
		Load: loadFunc,
	}
	thread.SetLocal("ctx", ctx)
	thread.SetLocal("sess", r.sess)

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			thread.Cancel(ctx.Err().Error())
		case <-done:
		}
	}()

	// Load the sink module: lib/<sink>.star
	modulePath := "lib/" + sinkName + ".star"
	globals, err := loadFunc(thread, modulePath)
	if err != nil {
		return nil, fmt.Errorf("load sink %q: %w", sinkName, err)
	}

	// Find push() function
	fn, ok := globals["push"]
	if !ok {
		return nil, fmt.Errorf("sink %q: no push() function in %s", sinkName, modulePath)
	}
	callable, ok := fn.(starlark.Callable)
	if !ok {
		return nil, fmt.Errorf("sink %q: push is not a function (got %s)", sinkName, fn.Type())
	}

	// Convert []map[string]any to []interface{} for goToStarlark compatibility
	rowsIface := make([]interface{}, len(rows))
	for i, r := range rows {
		m := make(map[string]interface{}, len(r))
		for k, v := range r {
			m[k] = v
		}
		rowsIface[i] = m
	}
	starlarkRows, err := goToStarlark(rowsIface)
	if err != nil {
		return nil, fmt.Errorf("sink %q: convert rows: %w", sinkName, err)
	}

	// Build the columns kwarg.
	//
	// Preferred: a pre-built list from the materialized DuckLake table's
	// schema (column_name + data_type). Names are the SQL aliases — the
	// keys the blueprint sees in `rows` — and types are the real DuckDB
	// types. Each entry is a {"name": ..., "type": ...} dict, mirroring
	// the @fetch contract.
	//
	// Fallback (tableColumns == nil): derive names from the row dicts,
	// untyped. Used only when the schema lookup failed (transient
	// catalog hiccup) so a push attempt is still made instead of
	// silently nacking.
	colsList := make([]starlark.Value, 0, len(tableColumns))
	if tableColumns != nil {
		for _, col := range tableColumns {
			d := starlark.NewDict(2)
			if name, ok := col["name"].(string); ok {
				_ = d.SetKey(starlark.String("name"), starlark.String(name))
			}
			if t, ok := col["type"]; ok {
				v, convErr := goToStarlark(t)
				if convErr == nil {
					_ = d.SetKey(starlark.String("type"), v)
				}
			}
			colsList = append(colsList, d)
		}
	} else {
		colSet := make(map[string]bool)
		for _, row := range rows {
			for k := range row {
				if !strings.HasPrefix(k, "_") {
					colSet[k] = true
				}
			}
		}
		var colNames []string
		for k := range colSet {
			colNames = append(colNames, k)
		}
		sort.Strings(colNames)
		for _, name := range colNames {
			d := starlark.NewDict(1)
			_ = d.SetKey(starlark.String("name"), starlark.String(name))
			colsList = append(colsList, d)
		}
	}

	// Build unique_key as list (handles composite keys like "region, year")
	var keyList []starlark.Value
	if uniqueKey != "" {
		for _, part := range strings.Split(uniqueKey, ",") {
			keyList = append(keyList, starlark.String(strings.TrimSpace(part)))
		}
	}

	// Call push(rows=..., batch_number=..., kind=..., key_columns=..., columns=..., sink_args...) — kwargs-only
	pushKwargs := []starlark.Tuple{
		{starlark.String("rows"), starlarkRows},
		{starlark.String("batch_number"), starlark.MakeInt(batchNumber)},
		{starlark.String("kind"), starlark.String(kind)},
		{starlark.String("key_columns"), starlark.NewList(keyList)},
		{starlark.String("columns"), starlark.NewList(colsList)},
	}
	// Add sink args as named kwargs
	for name, val := range sinkArgs {
		pushKwargs = append(pushKwargs, starlark.Tuple{starlark.String(name), starlark.String(val)})
	}
	retVal, err := starlark.Call(thread, callable, nil, filterKwargs(callable, pushKwargs))
	if err != nil {
		return nil, fmt.Errorf("push: %w", err)
	}

	// Parse return value
	result = &PushResult{}
	if retVal == nil || retVal == starlark.None {
		return result, nil
	}

	// Dict return: convert to Go types
	retDict, ok := retVal.(*starlark.Dict)
	if !ok {
		return result, nil
	}

	// Always populate RawReturn with full Go types (handles nested dicts, ints, etc.)
	rawGo, convErr := starlarkToGo(retDict)
	if convErr != nil {
		return nil, fmt.Errorf("push: cannot convert return value: %w", convErr)
	}
	if m, ok := rawGo.(map[string]any); ok {
		result.RawReturn = m
	}

	// Also populate PerRow for sync mode (string values only)
	result.PerRow = make(map[string]string)
	for _, item := range retDict.Items() {
		key, _ := starlark.AsString(item[0])
		if key == "" {
			continue
		}
		// Convert value to string — handles int, bool, etc.
		if s, ok := starlark.AsString(item[1]); ok {
			result.PerRow[key] = s
		} else {
			result.PerRow[key] = item[1].String()
		}
	}

	return result, nil
}

// RunPushFinalize calls the optional finalize(succeeded, failed) function after all batches.
// Returns nil if finalize() is not defined (no-op).
func (r *Runtime) RunPushFinalize(ctx context.Context, sinkName string, succeeded, failed int64, httpCfg ...*apiHTTPConfig) (err error) {
	defer recoverScriptPanic(&err)
	var cfg *apiHTTPConfig
	if len(httpCfg) > 0 {
		cfg = httpCfg[0]
	}
	libPredeclared := r.libraryPredeclared(ctx, cfg)

	loadCache := make(map[string]starlark.StringDict)
	loadFunc := r.makeLoadFunc(ctx, libPredeclared, loadCache)

	thread := &starlark.Thread{
		Name: sinkName + "_finalize",
		Print: func(_ *starlark.Thread, msg string) {
			fmt.Fprintln(os.Stderr, RedactSecrets(msg))
		},
		Load: loadFunc,
	}
	thread.SetLocal("ctx", ctx)
	thread.SetLocal("sess", r.sess)

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			thread.Cancel(ctx.Err().Error())
		case <-done:
		}
	}()

	modulePath := "lib/" + sinkName + ".star"
	globals, err := loadFunc(thread, modulePath)
	if err != nil {
		return fmt.Errorf("load sink %q: %w", sinkName, err)
	}

	fn, ok := globals["finalize"]
	if !ok {
		return nil // finalize is optional
	}
	callable, ok := fn.(starlark.Callable)
	if !ok {
		return fmt.Errorf("sink %q: finalize is not a function", sinkName)
	}

	finalizeKwargs := []starlark.Tuple{
		{starlark.String("succeeded"), starlark.MakeInt64(succeeded)},
		{starlark.String("failed"), starlark.MakeInt64(failed)},
	}
	_, err = starlark.Call(thread, callable, nil, filterKwargs(callable, finalizeKwargs))
	if err != nil {
		return fmt.Errorf("finalize: %w", err)
	}

	return nil
}

// RunPushPoll calls the poll() function for async batch mode.
func (r *Runtime) RunPushPoll(ctx context.Context, sinkName string, jobRef map[string]any, httpCfg ...*apiHTTPConfig) (done bool, perRow map[string]string, err error) {
	defer recoverScriptPanic(&err)
	var cfg *apiHTTPConfig
	if len(httpCfg) > 0 {
		cfg = httpCfg[0]
	}
	libPredeclared := r.libraryPredeclared(ctx, cfg)

	loadCache := make(map[string]starlark.StringDict)
	loadFunc := r.makeLoadFunc(ctx, libPredeclared, loadCache)

	thread := &starlark.Thread{
		Name: sinkName + "_poll",
		Print: func(_ *starlark.Thread, msg string) {
			fmt.Fprintln(os.Stderr, RedactSecrets(msg))
		},
		Load: loadFunc,
	}
	thread.SetLocal("ctx", ctx)
	thread.SetLocal("sess", r.sess)

	doneC := make(chan struct{})
	defer close(doneC)
	go func() {
		select {
		case <-ctx.Done():
			thread.Cancel(ctx.Err().Error())
		case <-doneC:
		}
	}()

	modulePath := "lib/" + sinkName + ".star"
	globals, err := loadFunc(thread, modulePath)
	if err != nil {
		return false, nil, fmt.Errorf("load sink %q: %w", sinkName, err)
	}

	fn, ok := globals["poll"]
	if !ok {
		return false, nil, fmt.Errorf("sink %q: no poll() function (required for async batch_mode)", sinkName)
	}
	callable, ok := fn.(starlark.Callable)
	if !ok {
		return false, nil, fmt.Errorf("sink %q: poll is not a function", sinkName)
	}

	jobRefVal, err := goToStarlark(jobRef)
	if err != nil {
		return false, nil, fmt.Errorf("convert job_ref: %w", err)
	}

	pollKwargs := []starlark.Tuple{
		{starlark.String("job_ref"), jobRefVal},
	}
	retVal, err := starlark.Call(thread, callable, nil, filterKwargs(callable, pollKwargs))
	if err != nil {
		return false, nil, fmt.Errorf("poll: %w", err)
	}

	retDict, ok := retVal.(*starlark.Dict)
	if !ok {
		return false, nil, fmt.Errorf("poll must return a dict, got %s", retVal.Type())
	}

	// Check "done" field -- must be a bool (True/False)
	doneVal, found, _ := retDict.Get(starlark.String("done"))
	if !found {
		return false, nil, fmt.Errorf("poll return must include 'done' key")
	}
	if doneVal != starlark.True && doneVal != starlark.False {
		return false, nil, fmt.Errorf("poll 'done' must be True or False, got %s (%s)", doneVal.String(), doneVal.Type())
	}

	if doneVal == starlark.False {
		return false, nil, nil
	}

	// Check "per_row" field
	perRowVal, found, _ := retDict.Get(starlark.String("per_row"))
	if !found {
		return true, nil, nil
	}
	perRowDict, ok := perRowVal.(*starlark.Dict)
	if !ok {
		return true, nil, nil
	}

	perRow = make(map[string]string)
	for _, item := range perRowDict.Items() {
		key, _ := starlark.AsString(item[0])
		if key == "" {
			continue
		}
		// Convert value to string — same logic as RunPush PerRow
		if s, ok := starlark.AsString(item[1]); ok {
			perRow[key] = s
		} else {
			perRow[key] = item[1].String()
		}
	}

	return true, perRow, nil
}

// CreateTempTable materializes collected script data into a DuckDB temp table.
// This is separated from Run() so the caller can suspend DuckDB during script
// execution and resume before creating the temp table.
func (r *Result) CreateTempTable() error {
	if r.badger != nil {
		// Durable mode: claim from Badger → temp table
		tmpTable, rowCount, claimIDs, err := r.badger.createTempTable()
		if err != nil {
			return err
		}
		r.TempTable = tmpTable
		r.RowCount = rowCount
		r.ClaimIDs = claimIDs
		return nil
	}

	// In-memory mode
	if r.collector == nil || len(r.collector.data) == 0 {
		return nil
	}
	tmpTable, err := r.collector.createTempTable()
	if err != nil {
		return err
	}
	r.TempTable = tmpTable
	return nil
}

// AckClaims acknowledges all Badger claims (successful DuckDB commit).
func (r *Result) AckClaims() error {
	if r.badger == nil || len(r.ClaimIDs) == 0 {
		return nil
	}
	return r.badger.ack(r.ClaimIDs)
}

// NackClaims returns all claimed events back to Badger (DuckDB commit failed).
func (r *Result) NackClaims() error {
	if r.badger == nil || len(r.ClaimIDs) == 0 {
		return nil
	}
	return r.badger.nack(r.ClaimIDs)
}

// Close releases resources (closes Badger store if used).
func (r *Result) Close() error {
	if r.badger != nil {
		return r.badger.close()
	}
	return nil
}
