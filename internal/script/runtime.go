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
	"strings"
	"time"

	starlarkre "github.com/magnetde/starlark-re"
	"go.starlark.net/starlark"
	starlarkjson "go.starlark.net/lib/json"
	starlarkmath "go.starlark.net/lib/math"
	starlarktime "go.starlark.net/lib/time"
	"go.starlark.net/syntax"

	"github.com/ondatra-labs/ondatrasql/internal/backfill"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
)

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

// Result contains the outcome of script execution.
type Result struct {
	TempTable string        // Name of temp table with collected data
	RowCount  int64         // Number of rows collected
	Duration  time.Duration // Script execution time
	ClaimIDs  []string      // Badger claim IDs (non-nil only when Badger is used)
	collector *saveCollector   // internal: deferred temp table creation (in-memory mode)
	badger    *badgerCollector // internal: durable mode
}

// fileOptions returns the shared Starlark syntax options (Python-like semantics).
func fileOptions() *syntax.FileOptions {
	return &syntax.FileOptions{
		Set:             true, // set() built-in
		While:           true, // while loops
		TopLevelControl: true, // if/for/while at top level
		GlobalReassign:  true, // reassign top-level names
		Recursion:       true, // recursive functions
	}
}

// libraryPredeclared returns the predeclared globals available to library modules.
// This excludes save (write side effects — must be passed explicitly as a parameter)
// but includes incremental (read-only state, safe as a global like env/http/json).
func (r *Runtime) libraryPredeclared(ctx context.Context) starlark.StringDict {
	return starlark.StringDict{
		"incremental": incrementalModule(r.incrState),
		"http":        httpModule(ctx),
		"env":         envModule(),
		"getvariable": getvariableBuiltin(r.sess),
		"query":       queryBuiltin(r.sess),
		"url":         urlModule(),
		"crypto":      cryptoModule(),
		"xml":         xmlModule(),
		"csv":         csvModule(),
		"oauth":       oauthModule(ctx, r.projectDir),
		"time":        starlarktime.Module,
		"math":        starlarkmath.Module,
		"json":        starlarkjson.Module,
		"re":          starlarkre.NewModule(),
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

		globals, err := starlark.ExecFileOptions(fileOptions(), libThread, module, data, libPredeclared)
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
func (r *Runtime) Run(ctx context.Context, target, code string) (*Result, error) {
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

	// Execute the script with Python-like semantics enabled
	_, err := starlark.ExecFileOptions(fileOptions(), thread, target+".star", code, predeclared)
	if err != nil {
		if bc != nil {
			bc.close()
		}
		return nil, fmt.Errorf("run script: %w", err)
	}

	// Return result with collector for deferred temp table creation.
	// The caller should call result.CreateTempTable() after resuming DuckDB.
	result := &Result{
		Duration: time.Since(start),
		RowCount: int64(collector.count()),
		badger:   bc,
	}
	if sc, ok := collector.(*saveCollector); ok {
		result.collector = sc
	}

	return result, nil
}

// RunSource executes a YAML model by loading a source function from lib/ and
// calling it directly via starlark.Call(). This avoids generating Starlark source
// code as a string — config values are converted to Starlark values in Go and
// passed as keyword arguments.
func (r *Runtime) RunSource(ctx context.Context, target, source string, config map[string]any) (*Result, error) {
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

	// Get the function by name
	fn, ok := globals[source]
	if !ok {
		return nil, fmt.Errorf("source %q: function %q not found in %s", source, source, modulePath)
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
		if bc != nil {
			bc.close()
		}
		return nil, fmt.Errorf("run source: %w", err)
	}

	result := &Result{
		Duration: time.Since(start),
		RowCount: int64(collector.count()),
		badger:   bc,
	}
	if sc, ok := collector.(*saveCollector); ok {
		result.collector = sc
	}

	return result, nil
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
