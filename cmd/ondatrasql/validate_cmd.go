// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/config"
	"github.com/ondatra-labs/ondatrasql/internal/dag"
	"github.com/ondatra-labs/ondatrasql/internal/duckast"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/execute"
	"github.com/ondatra-labs/ondatrasql/internal/libcall"
	"github.com/ondatra-labs/ondatrasql/internal/libregistry"
	"github.com/ondatra-labs/ondatrasql/internal/lineage"
	"github.com/ondatra-labs/ondatrasql/internal/output"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"github.com/ondatra-labs/ondatrasql/internal/validate"
)

// Exit-code contract (per cli-introspection.md, "Exit-koder"):
//
//	0 — clean (no BLOCKERs; WARN/INFO OK without --strict)
//	1 — findings (BLOCKER, or WARN with --strict) — also catches
//	    invocation errors in v0.31. Future: split invocation to 2.
//
// validateOpts is parsed from `validate [paths...]` arguments.
type validateOpts struct {
	paths  []string         // explicit file/dir args; empty = whole project scope
	format validate.OutputFormat
	strict bool
}

// runValidate dispatches the `validate` CLI subcommand.
//
// Returns a finding-style error (non-nil) when BLOCKERs are present, so
// main()'s default exit-1 path matches our contract. Invocation errors
// return a separate error type that main() can route to exit-2; we keep
// the simple "any error → exit 1" contract for now and surface invocation
// errors through stderr.
func runValidate(cfg *config.Config, args []string) error {
	opts, err := parseValidateArgs(args)
	if err != nil {
		return &invocationErr{err}
	}

	report, err := runValidatePipeline(cfg, opts)
	if err != nil {
		return err
	}

	// Render report. The global --json flag is the project-wide signal
	// that stdout must be machine-readable — it overrides --output=human
	// to JSON so a downstream JSON consumer sees structured output even
	// if the user combined the two flags by accident. The local --output
	// flag still picks between json/ndjson when --json is on.
	format := opts.format
	if output.JSONEnabled && format == validate.OutputHuman {
		format = validate.OutputJSON
	}
	w := output.Human
	if output.JSONEnabled || format == validate.OutputJSON || format == validate.OutputNDJSON {
		w = os.Stdout
	}
	if err := validate.Render(w, report, format); err != nil {
		return fmt.Errorf("render report: %w", err)
	}

	// Exit-code logic:
	//   - BLOCKER findings → exit 1 always
	//   - validate.* WARN  → exit 1 always (degraded run; cross-AST/
	//     unknown-lib coverage is unreliable, so machine-readable users
	//     must be able to detect this without --strict)
	//   - other WARN       → exit 1 only with --strict
	if report.HasBlockers() {
		return errFindings
	}
	if hasDegradedRunFinding(report) {
		return errFindings
	}
	if opts.strict && report.HasWarns() {
		return errFindings
	}
	return nil
}

// hasDegradedRunFinding reports whether the report contains any
// `validate.*`-rule finding. These signal that validate's own
// environment was incomplete (extension-load failure, blueprint scan
// errors, builtin introspection failure) and downstream code findings
// may be missed or false-positive. Always promote to exit 1 because a
// degraded run is not equivalent to a clean one.
func hasDegradedRunFinding(report *validate.Report) bool {
	for _, f := range report.Files {
		for _, finding := range f.Findings {
			if strings.HasPrefix(string(finding.Rule), "validate.") {
				return true
			}
		}
	}
	return false
}

// errFindings is a sentinel returned when BLOCKER/WARN findings should
// translate to a non-zero exit code. main() suppresses the error message
// for this sentinel because the report itself already rendered findings.
var errFindings = errors.New("validate: findings")

// invocationErr wraps an error that should map to exit code 2 (invocation
// error: bad flag, unknown blueprint, file not found). Implements
// exitCoder so main() routes it correctly.
type invocationErr struct{ err error }

func (e *invocationErr) Error() string  { return e.err.Error() }
func (e *invocationErr) Unwrap() error  { return e.err }
func (e *invocationErr) ExitCode() int  { return 2 }

func parseValidateArgs(args []string) (*validateOpts, error) {
	opts := &validateOpts{format: validate.OutputHuman}
	if output.JSONEnabled {
		opts.format = validate.OutputJSON
	}

	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--strict":
			opts.strict = true
		case a == "--output":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("--output requires a value (human|json|ndjson)")
			}
			i++
			f, err := validate.ParseFormat(args[i])
			if err != nil {
				return nil, err
			}
			opts.format = f
		case strings.HasPrefix(a, "--output="):
			f, err := validate.ParseFormat(strings.TrimPrefix(a, "--output="))
			if err != nil {
				return nil, err
			}
			opts.format = f
		case strings.HasPrefix(a, "--"):
			return nil, fmt.Errorf("unknown flag: %s", a)
		default:
			opts.paths = append(opts.paths, a)
		}
	}
	return opts, nil
}

// runValidatePipeline does the heavy lifting: scope discovery, file walk,
// per-file validation, finding aggregation.
func runValidatePipeline(cfg *config.Config, opts *validateOpts) (*validate.Report, error) {
	files, err := discoverFiles(cfg.ProjectDir, opts.paths)
	if err != nil {
		// Bad path / non-validatable extension → exit 2.
		return nil, &invocationErr{err}
	}

	// Vanilla DuckDB session for json_serialize_sql. No catalog attach
	// (we don't want to read DuckLake state), but DO load the project's
	// extensions.sql so that BuiltinTableFunctions can include
	// extension-provided table functions in its result set. Without this
	// step, validate would flag legitimate extension-loaded TABLE_FUNCTIONs
	// as cross_ast.unknown_lib false positives.
	sess, err := duckdb.NewSession("")
	if err != nil {
		return nil, fmt.Errorf("create session: %w", err)
	}
	defer closeSessionOrLog(sess)

	// Extension load is best-effort. Failures here only matter for
	// cross_ast.unknown_lib accuracy on extension-provided table
	// functions; ordinary SQL/blueprint validation works without it.
	// Track the failure so we can attach an INFO finding to the report
	// rather than aborting the whole run — ditto for offline CI where
	// the extension can't be downloaded.
	extensionLoadErr := loadProjectExtensions(sess, cfg.ConfigPath)

	// Use ScanLenient so a single broken lib/*.star doesn't strip the
	// registry empty for healthy blueprints. Per-file failures are
	// reported as WARN findings on each broken file (the report has
	// concrete file:line locations), and cross-AST + push-compat
	// validation runs against the partial registry.
	reg, scanErrs := libregistry.ScanLenient(cfg.ProjectDir)

	// Built-in table functions (read_csv, unnest, generate_series, etc.)
	// — fetched once per validate run since the set is constant for the
	// session. Used to filter unknown-lib detection so DuckDB built-ins
	// don't get flagged as typos. Track introspection errors so we can
	// surface them as a structured finding rather than silently
	// degrading cross_ast.unknown_lib accuracy.
	builtins, builtinsErr := libcall.BuiltinTableFunctions(sess)

	// `builtinsUnreliable` triggers narrow suppression of unknown-lib
	// findings on names that LOOK LIKE built-ins (read_*, pragma_*,
	// information_schema_*, duckdb_*). Set when either duckdb_functions()
	// failed or extensions.sql failed to load — both leave the allowlist
	// incomplete for legitimate built-in/extension table functions.
	//
	// We do NOT use this as a blanket "skip all unknown-lib" flag —
	// that would hide real typos in user code. Per-blueprint suppression
	// (via scanErrs) covers broken-sibling cases.
	builtinsUnreliable := builtinsErr != nil || extensionLoadErr != nil

	// Build a set of in-scope file paths so we can suppress duplicate
	// ScanLenient findings for blueprints that the walk will visit and
	// report on directly via validateBlueprintFile.
	//
	// Use symlink-resolved paths to match what `files` contains —
	// without this, a symlinked lib/foo.star would generate a finding
	// from BOTH ScanLenient (resolved path miss) and validateBlueprintFile.
	inScope := map[string]bool{}
	for _, f := range files {
		inScope[f] = true
	}

	report := validate.NewReport()

	// Surface walk-skipped entries (broken symlinks, unreadable subdirs)
	// as structured findings so JSON/NDJSON consumers can detect that
	// the scope was not fully validated. Each skipped path becomes a
	// WARN entry on its own — the user can decide whether to act.
	//
	// Dedup against ScanLenient findings on the same file: a broken
	// `lib/foo.star` symlink would otherwise emit BOTH walk_skipped
	// (from discovery) AND blueprint_load_failed (from ScanLenient),
	// which inflates the WARN count from one root cause.
	walkSkipPaths := map[string]bool{}
	for _, ws := range pendingWalkSkips {
		rel, _ := filepath.Rel(cfg.ProjectDir, ws.path)
		if rel == "" {
			rel = ws.path
		}
		walkSkipPaths[rel] = true
		report.AddFile(validate.FileResult{
			Path:   rel,
			Status: "fail",
			Findings: []validate.Finding{validate.NewFinding(
				rel, 0,
				validate.RuleValidateWalkSkipped,
				fmt.Sprintf("skipped during file walk: %v — file was not validated", ws.err),
			)},
		})
	}

	// Surface ScanLenient parse failures for blueprints that are NOT
	// in scope (e.g. scoped runs like `validate models/foo.sql` that
	// don't walk lib/). Without this, a broken lib/_x.star would
	// silently degrade cross-AST and push-compat coverage for the
	// scoped models without any signal in the report.
	for fileName, scanErr := range scanErrs {
		// Resolve through symlinks so the inScope lookup matches what
		// walkValidatable produced (which calls safeResolveSymlink).
		resolved, _ := filepath.EvalSymlinks(filepath.Join(cfg.ProjectDir, "lib", fileName))
		if resolved != "" && inScope[resolved] {
			continue // walk-time validateBlueprintFile will report this directly.
		}
		// Sentinel "<lib-dir>" indicates a directory-level read failure
		// (e.g. ReadDir permission denied) — attribute to lib/ rather
		// than to a fake filename so JSON consumers can tell it apart
		// from per-file blueprint parse errors.
		var relPath string
		if fileName == "<lib-dir>" {
			relPath = "lib/"
		} else {
			relPath = filepath.Join("lib", fileName)
		}
		report.AddFile(validate.FileResult{
			Path:   relPath,
			Status: "fail",
			Findings: []validate.Finding{validate.NewFinding(
				relPath, 0,
				validate.RuleValidateBlueprintLoadFailed,
				fmt.Sprintf("blueprint failed to parse: %v — cross-AST and push-compat coverage degraded for models that use this blueprint", scanErr),
			)},
		})
	}

	// Surface extension load failures as a structured WARN finding on
	// the extensions.sql path. JSON/NDJSON consumers see this in the
	// report — exit code reflects "findings present" so machine-readable
	// users can detect the degraded state. Without this, validate would
	// silently report "clean" while cross_ast.unknown_lib results were
	// unreliable.
	if extensionLoadErr != nil {
		extPath := filepath.Join(cfg.ConfigPath, "extensions.sql")
		relExtPath, _ := filepath.Rel(cfg.ProjectDir, extPath)
		report.AddFile(validate.FileResult{
			Path:   relExtPath,
			Status: "fail",
			Findings: []validate.Finding{validate.NewFinding(
				relExtPath, 0,
				validate.RuleValidateExtensionsLoadFailed,
				fmt.Sprintf("could not load extensions: %v — extension-provided table functions may show false-positive cross_ast.unknown_lib findings", extensionLoadErr),
			)},
		})
	}

	// Surface builtin-introspection failures the same way. If
	// duckdb_functions() couldn't be queried, the cross_ast.unknown_lib
	// check has no built-in allowlist and may flag every legitimate
	// DuckDB table function as a typo. The user needs to know.
	if builtinsErr != nil {
		report.AddFile(validate.FileResult{
			Path:   "<duckdb-builtins>",
			Status: "fail",
			Findings: []validate.Finding{validate.NewFinding(
				"<duckdb-builtins>", 0,
				validate.RuleValidateBuiltinIntrospectionFail,
				fmt.Sprintf("could not introspect DuckDB built-in table functions: %v — cross_ast.unknown_lib will produce false positives for read_csv, unnest, generate_series, and other built-ins", builtinsErr),
			)},
		})
	}

	models := make(map[string]*parser.Model)
	// Track which file each target came from so duplicate-target
	// collisions can be reported on both files.
	targetSource := make(map[string]string)
	for _, path := range files {
		fr := validateFile(sess, reg, builtins, scanErrs, builtinsUnreliable, cfg.ProjectDir, path)
		// Stash successfully-parsed models for the DAG pass.
		if strings.HasSuffix(path, ".sql") && fr.Checks["parser"] == "ok" {
			if model, err := parser.ParseModel(path, cfg.ProjectDir); err == nil {
				if existingPath, exists := targetSource[model.Target]; exists {
					// Two files map to the same target — typically
					// something like models/orders.sql vs models/main/orders.sql
					// or a copy-paste schema.table. Surface as BLOCKER
					// on the colliding file so users notice.
					fr.Findings = append(fr.Findings, validate.NewFinding(
						fr.Path, 0,
						validate.RuleParserDuplicateTarget,
						fmt.Sprintf("target %q already declared by %s — duplicate targets silently overwrite each other in the DAG; rename one", model.Target, existingPath),
					))
				} else {
					models[model.Target] = model
					targetSource[model.Target] = fr.Path
				}
			}
		}
		report.AddFile(fr)
	}

	// Cross-model DAG pass — detect cycles and flag external references.
	// Findings are attached to the source model's FileResult so users see
	// them next to their per-file output.
	if len(models) > 0 {
		runDAGValidation(sess, cfg.ProjectDir, models, report)
	}

	// Push-compat validation: kind support, unique-key for merge,
	// group-key for tracked, poll-interval/timeout format. Runs against
	// the full model set + registry; emits per-target findings.
	//
	// Runs even when the registry is empty — `ValidateModelPushCompat`
	// itself flags "unknown lib" and other no-registry cases as
	// findings rather than silently passing. Skipping based on registry
	// state would mask real issues exactly when the upstream blueprint
	// scan failed.
	if len(models) > 0 {
		runPushCompatValidation(models, reg, report)
	}

	return report, nil
}

// runPushCompatValidation invokes execute.ValidateModelPushCompat across
// all parsed models and attaches findings to the originating file.
//
// ValidateModelPushCompat returns a single error when any model is
// incompatible — to surface per-model findings we run it once per model
// in isolation.
func runPushCompatValidation(models map[string]*parser.Model, reg *libregistry.Registry, report *validate.Report) {
	for target, m := range models {
		if m.Push == "" {
			continue
		}
		if err := execute.ValidateModelPushCompat([]*parser.Model{m}, reg); err != nil {
			id := mapPushCompatRule(err.Error())
			attachDAGFinding(report, models, target,
				validate.NewFinding("", 0, id, err.Error()))
		}
	}
}

// mapPushCompatRule routes ValidateModelPushCompat error messages to
// stable rule-IDs. Phrasing in execute/validate_push.go is stable enough
// to match against; default falls back to push.other.
func mapPushCompatRule(msg string) validate.RuleID {
	switch {
	case strings.Contains(msg, "unique_key") || strings.Contains(msg, "unique key"):
		return validate.RulePushMissingUniqueKey
	case strings.Contains(msg, "group_key") || strings.Contains(msg, "group key"):
		return validate.RulePushMissingGroupKey
	case strings.Contains(msg, "poll_interval") || strings.Contains(msg, "poll interval"):
		return validate.RulePushInvalidPollInterval
	case strings.Contains(msg, "poll_timeout") || strings.Contains(msg, "poll timeout"):
		return validate.RulePushInvalidPollTimeout
	case strings.Contains(msg, "kind") || strings.Contains(msg, "supported"):
		return validate.RulePushKindNotSupported
	}
	return validate.RulePushOther
}

// runDAGValidation builds the dependency graph from parsed models, runs
// topological sort to surface cycles, and flags dependencies that don't
// resolve to a known model as INFO (external references that validate
// can't verify without catalog access).
func runDAGValidation(sess *duckdb.Session, projectDir string, models map[string]*parser.Model, report *validate.Report) {
	graph := dag.NewGraph(sess, projectDir)
	for _, m := range models {
		graph.Add(m)
	}

	if _, err := graph.Sort(); err != nil {
		// dag.Sort returns (nil, err) on the first cycle it sees, so we
		// can't enumerate cycle members from its return values. Attach
		// the finding to the named target only — the user follows its
		// dependency chain to find the loop. (Earlier code tried to
		// derive members from the partial sort; that was a bug because
		// the partial result is empty.)
		msg := err.Error()
		target := strings.TrimPrefix(msg, "circular dependency detected: ")
		attachDAGFinding(report, models, target,
			validate.NewFinding("", 0, validate.RuleDagCycleDetected,
				fmt.Sprintf("model is part of a circular dependency — follow %s's dependency chain to find the loop", target)))
	}

	// External-reference detection: any dep mentioned by a model that
	// doesn't resolve to a known model target is INFO.
	knownTargets := map[string]bool{}
	for t := range models {
		knownTargets[t] = true
	}
	for _, m := range models {
		// Skip lineage-based dep extraction for Starlark/script models.
		// Their deps come from query() calls inside the script, which
		// dag.Add already extracted via extractStarlarkDeps. Running
		// SQL lineage on Starlark source would always fail — the
		// false-positive validate.lineage_extract_failed would mask
		// actual usage rather than provide signal.
		if m.ScriptType != parser.ScriptTypeNone {
			continue
		}
		deps, err := lineageTablesFor(sess, m.SQL)
		if err != nil {
			// Surface lineage extraction failure as a structured WARN
			// finding so dag.external_ref isn't silently skipped.
			attachDAGFinding(report, models, m.Target,
				validate.NewFinding("", 0, validate.RuleValidateLineageExtractFailed,
					fmt.Sprintf("lineage extraction failed: %v — dag.external_reference checks skipped for this model", err)))
			continue
		}
		for _, dep := range deps {
			if !strings.Contains(dep, ".") {
				continue
			}
			if knownTargets[dep] {
				continue
			}
			attachDAGFinding(report, models, m.Target,
				validate.NewFinding("", 0, validate.RuleDagExternalRef,
					fmt.Sprintf("references %q which is not a model in this project — external table, not validated", dep)))
		}
	}
}

// lineageTablesFor returns the deduped table-reference list for sql.
// Returns (nil, err) when ExtractTables fails — the caller MUST surface
// the error as a structured validate.lineage_extract_failed finding so
// downstream dag.external_ref checks aren't silently skipped.
func lineageTablesFor(sess *duckdb.Session, sql string) ([]string, error) {
	if sess == nil || sql == "" {
		return nil, nil
	}
	tables, err := lineage.ExtractTables(sess, sql)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(tables))
	seen := map[string]bool{}
	for _, t := range tables {
		if seen[t.Table] {
			continue
		}
		seen[t.Table] = true
		out = append(out, t.Table)
	}
	return out, nil
}

// attachDAGFinding adds a Finding to the FileResult for the model named
// `target`. Fixes up the file path and re-tallies the report's summary so
// the new finding is counted.
func attachDAGFinding(report *validate.Report, models map[string]*parser.Model, target string, finding validate.Finding) {
	model, ok := models[target]
	if !ok {
		return
	}
	// model.FilePath is already relative to projectDir (parser stores it that way).
	rel := model.FilePath
	finding.Path = rel
	for i := range report.Files {
		if report.Files[i].Path != rel {
			continue
		}
		// If this file had no findings before, it was counted as Clean
		// in AddFile. Decrement Clean exactly once per transition from
		// "clean" → "has findings" (regardless of how many findings get
		// attached afterwards).
		wasClean := len(report.Files[i].Findings) == 0
		report.Files[i].Findings = append(report.Files[i].Findings, finding)
		report.Files[i].Status = "fail"
		if wasClean && report.Summary.Clean > 0 {
			report.Summary.Clean--
		}
		switch finding.Severity {
		case validate.SeverityBlocker:
			report.Summary.Blockers++
		case validate.SeverityWarn:
			report.Summary.Warns++
		case validate.SeverityInfo:
			report.Summary.Infos++
		}
		return
	}
}

// walkSkip records a file the discovery walk skipped due to an
// unrecoverable per-entry error (broken symlink, permission denied,
// etc.). Collected via the package-level `walkSkips` slice during a
// single discoverFiles call so the caller can surface them as
// structured validate.walk_skipped findings — JSON/NDJSON consumers
// then see degraded coverage instead of a falsely clean report.
type walkSkip struct {
	path string
	err  error
}

// pendingWalkSkips is the per-call accumulator. discoverFiles resets
// it on entry; the pipeline reads it after the call to emit findings.
// Single-threaded use is fine — `pre-pr` invokes validate sequentially.
var pendingWalkSkips []walkSkip

// discoverFiles resolves the user's path arguments into a deduped, sorted
// list of files. Empty args means: walk the whole project scope (models/
// + lib/). Each candidate is symlink-resolved and verified to live within
// projectDir (SEC1).
func discoverFiles(projectDir string, args []string) ([]string, error) {
	pendingWalkSkips = nil
	explicit := len(args) > 0
	var roots []string
	if !explicit {
		roots = []string{
			filepath.Join(projectDir, "models"),
			filepath.Join(projectDir, "lib"),
		}
	} else {
		for _, a := range args {
			roots = append(roots, resolveArgPath(projectDir, a))
		}
	}

	seen := map[string]bool{}
	var out []string
	// Track whether each explicit arg discovered ANY validatable file
	// (including ones already seen via earlier args). Distinct from
	// "added to out" because dedup means the second arg in
	// `validate models models/foo.sql` would otherwise be flagged as
	// "no validatable files" even though it's perfectly valid.
	argDiscovered := make([]bool, len(roots))
	for i, root := range roots {
		discoveredHere := map[string]bool{}
		walker := func(real string) {
			discoveredHere[real] = true
		}
		if err := walkValidatableTrack(projectDir, root, seen, &out, explicit, walker); err != nil {
			if !explicit && errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, err
		}
		argDiscovered[i] = len(discoveredHere) > 0
	}
	if explicit {
		for i, ok := range argDiscovered {
			if !ok {
				return nil, fmt.Errorf("%s: no validatable files found (validate accepts *.sql models and *.star blueprints)", args[i])
			}
		}
	}
	return out, nil
}

func resolveArgPath(projectDir, arg string) string {
	if filepath.IsAbs(arg) {
		return arg
	}
	return filepath.Join(projectDir, arg)
}

// walkValidatable enumerates *.sql under models/ and *.star under lib/ from
// a given root, with symlink resolution to enforce the projectDir
// containment guarantee (SEC1).
//
// When explicit is true (the user passed paths on the command line):
//   - Non-validatable files (anything that isn't .sql or .star) become
//     an invocation error rather than a silent skip — `validate
//     README.md` must not return clean.
//   - Directory args must be `models/`, `lib/`, or under them. Other
//     dirs (e.g. `config/`) contain *.sql files that aren't pipeline
//     models — validating them produces meaningless BLOCKERs.
// walkValidatableTrack walks `root` for validatable files, with an
// `onMatch` callback that fires for every match (including dups via
// `seen`). discoverFiles uses this to distinguish "explicit arg
// actually pointed at a validatable file" (legitimate, even if it's
// a dup) from "explicit arg pointed at nothing" (invocation error).
func walkValidatableTrack(projectDir, root string, seen map[string]bool, out *[]string, explicit bool, onMatch func(real string)) error {
	info, err := os.Stat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		if !isValidatable(root) {
			if explicit {
				return fmt.Errorf("%s: not a validatable file (validate accepts *.sql models and *.star blueprints)", root)
			}
			return nil
		}
		// Resolve symlinks FIRST so the scope check applies to the
		// real path. Without this, `validate models/legit.sql` where
		// `models/legit.sql` symlinks to `config/macros/x.sql` would
		// pass the scope check (looks under models/) but parse the
		// out-of-tree macro file as a pipeline model.
		real, err := safeResolveSymlink(projectDir, root)
		if err != nil {
			if explicit {
				return fmt.Errorf("%s: %w", root, err)
			}
			// Non-explicit walks already filter symlink errors with a
			// stderr warning at the per-file callback below.
			fmt.Fprintf(os.Stderr, "warning: skipping %s: %v\n", root, err)
			return nil
		}
		// File extension must match tree, applied to the resolved path:
		// *.sql under models/, *.star under lib/. Rejects models/foo.star,
		// lib/helper.sql, arbitrary out-of-tree files, and symlinks that
		// resolve to out-of-tree targets.
		if explicit && !fileBelongsToScope(projectDir, real) {
			return fmt.Errorf("%s: file is not part of the validate scope (*.sql under models/, *.star under lib/)", root)
		}
		if onMatch != nil {
			onMatch(real)
		}
		if !seen[real] {
			seen[real] = true
			*out = append(*out, real)
		}
		return nil
	}

	if explicit {
		// Resolve symlinks on the dir BEFORE the scope check, otherwise
		// `validate models/ext` where models/ext → /tmp/foo would pass
		// the scope check on the symlink path and then have WalkDir
		// traverse the external tree.
		realDir, err := filepath.EvalSymlinks(root)
		if err != nil {
			return fmt.Errorf("%s: %w", root, err)
		}
		if !isValidatableDir(projectDir, realDir) {
			return fmt.Errorf("%s: directory is not part of the validate scope (use models/, lib/, or a subdirectory of either)", root)
		}
	}

	return filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			// Walk errors (permission denied on a subdirectory, stat
			// failure, etc.) shouldn't abort the entire validate run.
			// Skip the offending entry, warn on stderr, and record so
			// the caller can emit a validate.walk_skipped finding.
			fmt.Fprintf(os.Stderr, "warning: skipping %s: %v\n", p, err)
			pendingWalkSkips = append(pendingWalkSkips, walkSkip{path: p, err: err})
			if d != nil && d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if !isValidatable(p) {
			return nil
		}
		real, err := safeResolveSymlink(projectDir, p)
		if err != nil {
			// Walk-time symlink resolution failure (broken target,
			// out-of-tree target, etc.) — skip this file but keep
			// walking. Aborting the entire validate run because of one
			// bad symlink means a single noisy fixture suppresses
			// findings on every other valid file.
			fmt.Fprintf(os.Stderr, "warning: skipping %s: %v\n", p, err)
			pendingWalkSkips = append(pendingWalkSkips, walkSkip{path: p, err: err})
			return nil
		}
		// Re-apply scope checks against the RESOLVED path. A symlink
		// like `models/foo.sql -> lib/internal/helper.star` would
		// otherwise pass extension and directory checks on the symlink
		// path while smuggling a non-model file into the validation
		// pipeline.
		if !isValidatable(real) {
			return nil
		}
		if !fileBelongsToScope(projectDir, real) {
			return nil
		}
		// Blueprint convention: Scan/ScanLenient only read top-level
		// `lib/*.star`. Apply the top-level filter to the RESOLVED
		// path so a symlink in lib/ pointing at lib/internal/foo.star
		// is also rejected.
		if strings.HasSuffix(real, ".star") {
			absLib, _ := filepath.Abs(filepath.Join(projectDir, "lib"))
			absParent, _ := filepath.Abs(filepath.Dir(real))
			if absParent != absLib {
				return nil
			}
		}
		if onMatch != nil {
			onMatch(real)
		}
		if seen[real] {
			return nil
		}
		seen[real] = true
		*out = append(*out, real)
		return nil
	})
}

// isValidatable returns true for files validate considers in-scope by
// extension alone — *.sql or *.star. Tree-membership is checked separately
// (a *.sql file under lib/ is rejected even though it passes this check).
func isValidatable(path string) bool {
	return strings.HasSuffix(path, ".sql") || strings.HasSuffix(path, ".star")
}

// fileBelongsToScope reports whether path's extension matches its parent
// tree:
//
//	*.sql  → must be under models/
//	*.star → must be under lib/
//
// This rejects models/foo.star and lib/helper.sql which would otherwise
// pass the extension-only check. Files outside both trees fail too.
//
// projectDir is used as the root for relative tree resolution.
func fileBelongsToScope(projectDir, path string) bool {
	absPath := canonicalisePath(path)
	if absPath == "" {
		return false
	}
	absProject := canonicaliseProjectDir(projectDir)
	if absProject == "" {
		return false
	}
	switch {
	case strings.HasSuffix(absPath, ".sql"):
		return underTree(absPath, filepath.Join(absProject, "models"))
	case strings.HasSuffix(absPath, ".star"):
		return underTree(absPath, filepath.Join(absProject, "lib"))
	}
	return false
}

// isValidatableDir reports whether dir is a sub-tree validate is willing
// to walk: models/ or lib/ or any descendant. Used as a pre-filter on
// directory args so `validate config/` and `validate docs/` reject
// before walking.
func isValidatableDir(projectDir, dir string) bool {
	absDir := canonicalisePath(dir)
	if absDir == "" {
		return false
	}
	absProject := canonicaliseProjectDir(projectDir)
	if absProject == "" {
		return false
	}
	for _, allowed := range []string{"models", "lib"} {
		if underTree(absDir, filepath.Join(absProject, allowed)) {
			return true
		}
	}
	return false
}

// canonicalisePath returns the canonical absolute form of path. For
// paths that exist, this is `EvalSymlinks(path) + Abs`. For paths
// that DON'T exist (which happens when callers reason about hypothetical
// paths — e.g. `isValidatableDir(tmp, tmp+"/models")` on a freshly
// created tmpDir before any subdirs are made), it walks UP to the
// deepest existing parent, canonicalises that, and re-appends the
// missing tail.
//
// This matters on macOS where `t.TempDir()` returns `/var/folders/...`
// whose canonical form is `/private/var/folders/...`. If the helper
// silently passed through the raw `/var/...` for non-existent paths
// while canonicalising the project root to `/private/var/...`, the
// containment check would compare `/var/folders/.../001/models`
// against `/private/var/folders/.../001/models` and report mismatch.
//
// Tests on Linux didn't surface this (no `/var → /private/var`
// symlink) which is why the helper appeared correct in unit testing.
// See Go issue #56259 for upstream context on why t.TempDir() returns
// the un-canonicalised path on macOS.
func canonicalisePath(path string) string {
	if real, err := filepath.EvalSymlinks(path); err == nil {
		if abs, err := filepath.Abs(real); err == nil {
			return abs
		}
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return ""
	}
	// Walk up to the deepest existing parent and canonicalise that
	// — re-append the missing components on the way out.
	parent := abs
	var tail []string
	for {
		if _, err := os.Stat(parent); err == nil {
			break
		}
		next := filepath.Dir(parent)
		if next == parent {
			// Reached root without finding any existing prefix —
			// nothing to canonicalise; return raw absolute path.
			return abs
		}
		tail = append([]string{filepath.Base(parent)}, tail...)
		parent = next
	}
	if real, err := filepath.EvalSymlinks(parent); err == nil {
		if absParent, err := filepath.Abs(real); err == nil {
			return filepath.Join(append([]string{absParent}, tail...)...)
		}
	}
	return abs
}

// canonicaliseProjectDir resolves projectDir to its canonical form so
// downstream containment checks compare like-for-like. Required
// because on macOS `t.TempDir()` returns `/var/folders/...` whose
// canonical form is `/private/var/folders/...` — without
// canonicalisation, paths resolved via EvalSymlinks elsewhere never
// match a raw projectDir prefix. See Go issue #56259.
func canonicaliseProjectDir(projectDir string) string {
	return canonicalisePath(projectDir)
}

// underTree reports whether absPath is the root or a descendant of it.
func underTree(absPath, root string) bool {
	if absPath == root {
		return true
	}
	return strings.HasPrefix(absPath+string(filepath.Separator), root+string(filepath.Separator))
}

// safeResolveSymlink follows symlinks and verifies the resolved path is
// still under projectDir. Returns the real path on success.
//
// projectDir is canonicalised the same way as `path` (via EvalSymlinks)
// so the containment check compares apples-to-apples. See
// canonicaliseProjectDir for the macOS rationale.
func safeResolveSymlink(projectDir, path string) (string, error) {
	real, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("resolve symlink %s: %w", path, err)
	}
	absProject := canonicaliseProjectDir(projectDir)
	if absProject == "" {
		return "", fmt.Errorf("resolve project directory %s", projectDir)
	}
	absReal, err := filepath.Abs(real)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(absReal+string(filepath.Separator), absProject+string(filepath.Separator)) &&
		absReal != absProject {
		return "", fmt.Errorf("path %s resolves outside project directory", path)
	}
	return real, nil
}

// validateFile runs all applicable validators against a single file and
// returns a FileResult tagged with check statuses + findings.
func validateFile(sess *duckdb.Session, reg *libregistry.Registry, builtins map[string]bool, scanErrs map[string]error, builtinsUnreliable bool, projectDir, path string) validate.FileResult {
	rel, _ := filepath.Rel(projectDir, path)
	fr := validate.FileResult{
		Path:   rel,
		Status: "ok",
		Checks: map[string]string{},
	}

	if strings.HasSuffix(path, ".star") {
		validateBlueprintFile(reg, &fr, projectDir, path)
	} else {
		validateModelFile(sess, reg, builtins, scanErrs, builtinsUnreliable, &fr, projectDir, path)
	}

	if len(fr.Findings) > 0 {
		fr.Status = "fail"
	}
	return fr
}

// validateModelFile runs the SQL model validators on a single model.
func validateModelFile(sess *duckdb.Session, reg *libregistry.Registry, builtins map[string]bool, scanErrs map[string]error, builtinsUnreliable bool, fr *validate.FileResult, projectDir, path string) {
	model, err := parser.ParseModel(path, projectDir)
	if err != nil {
		fr.Checks["parser"] = "fail"
		fr.Findings = append(fr.Findings, mapParserError(fr.Path, err))
		return
	}
	fr.Checks["parser"] = "ok"

	// AST serialization — if DuckDB rejects the SQL, surface a parser
	// finding rather than tacit-pass the model. Without AST we cannot
	// run any of the strict-fetch/push/read-only validators.
	astJSON, astErr := lineage.GetAST(sess, model.SQL)
	if astErr != nil {
		fr.Checks["parser"] = "fail"
		fr.Findings = append(fr.Findings, validate.NewFinding(fr.Path, 0,
			validate.RuleParserOther,
			fmt.Sprintf("failed to parse SQL: %v", astErr)))
		return
	}

	// Lib-call detection runs over the same AST. An error here means
	// the AST is structurally unexpected — already covered by astErr
	// above, but defensive: emit a finding rather than a silent pass.
	calls, callsErr := libcall.DetectInSQL(sess, model.SQL, reg)
	if callsErr != nil {
		fr.Checks["parser"] = "fail"
		fr.Findings = append(fr.Findings, validate.NewFinding(fr.Path, 0,
			validate.RuleParserOther,
			fmt.Sprintf("failed to detect lib calls: %v", callsErr)))
		return
	}

	results := execute.ValidateModelStatic(model, astJSON, calls)
	for _, r := range results {
		fr.Checks[normalizeCheckName(r.Category)] = "fail"
		fr.Findings = append(fr.Findings, mapStaticValidatorError(fr.Path, r))
	}

	// Lib-call relationship rule (`@fetch` ⇔ exactly one lib in FROM).
	// When the model references a blueprint that's in scanErrs (broken
	// sibling), we DON'T emit "no lib call" — the lib call exists in
	// the AST, but libcall.Detect dropped it because the registry
	// entry failed to load. Emitting strict_fetch.no_lib_call there
	// would misattribute the cause; the real issue is the broken
	// blueprint, which is already reported on its own file.
	switch {
	case model.Fetch && len(calls) == 0:
		if !modelReferencesBrokenBlueprint(astJSON, scanErrs) {
			fr.Findings = append(fr.Findings, validate.NewFinding(fr.Path, 0,
				validate.RuleStrictFetchNoLibCall,
				"@fetch model must have exactly one lib function call in FROM"))
		}
	case !model.Fetch && len(calls) > 0:
		fr.Findings = append(fr.Findings, validate.NewFinding(fr.Path, 0,
			validate.RuleParserMissingDirective,
			fmt.Sprintf("model contains lib call %q in FROM but lacks @fetch — add `-- @fetch` to declare it", calls[0].FuncName)))
	case model.Fetch && len(calls) > 1:
		fr.Findings = append(fr.Findings, validate.NewFinding(fr.Path, 0,
			validate.RuleStrictFetchMultipleLibCalls,
			"@fetch model must have exactly one lib function call in FROM"))
	}

	// Cross-AST modell × blueprint validation. Catches:
	//   - Model passing MORE args than the blueprint declares (extra
	//     positional args have nowhere to go at runtime)
	//
	// We do NOT flag too-few args: blueprint functions routinely have
	// default values (e.g. `options=""`) so omitting positional args is
	// legitimate. Detecting which args are required would require
	// tracking defaults per Starlark param — out of scope for v0.31.
	for _, c := range calls {
		if got, want := len(c.ArgNodes), len(c.Lib.Args); got > want {
			fr.Findings = append(fr.Findings, validate.NewFinding(fr.Path, 0,
				validate.RuleCrossASTArgsMismatch,
				fmt.Sprintf("model passes %d args to %s but blueprint declares only %d (%s)",
					got, c.FuncName, want, strings.Join(c.Lib.Args, ", "))))
		}
	}

	// Unknown-lib detection: a TABLE_FUNCTION in FROM that doesn't
	// resolve to a registered blueprint AND isn't a DuckDB built-in.
	// libcall.Detect filters non-blueprint TABLE_FUNCTIONs out of the
	// `calls` slice, so we walk the raw AST list here to catch typos
	// like `FROM gam_repor()` that would otherwise fail only at run-time
	// as "table not found".
	//
	// Per-blueprint suppression covers the legitimate degraded cases:
	//   - the TABLE_FUNCTION matches a blueprint that's in scanErrs
	//     (broken sibling, not a typo — already reported on its own file)
	//   - builtins introspection failed AND the name looks like it could
	//     be a built-in (only suppress that specific risk window)
	//
	// We do NOT blanket-suppress on `degraded`: that would hide every
	// real typo across every model whenever extensions.sql or
	// duckdb_functions() had a hiccup. The validate.* WARN findings
	// already tell the user the run was degraded.
	parsedAST, parseErr := duckast.Parse(astJSON)
	if parseErr != nil {
		// Surface the parse failure as a finding rather than silently
		// turning the unknown-lib check into dead code: with a nil AST
		// AllTableFunctionNames returns an empty slice, so typos like
		// `FROM gam_repor()` would silently pass validation.
		fr.Findings = append(fr.Findings, validate.NewFinding(fr.Path, 0,
			validate.RuleValidateLineageExtractFailed,
			fmt.Sprintf("AST parse failed; cross-AST unknown-lib check skipped: %v", parseErr)))
	}
	for _, name := range libcall.AllTableFunctionNames(parsedAST) {
		if reg.Get(name) != nil || builtins[name] {
			continue
		}
		if _, brokenSibling := scanErrs[name+".star"]; brokenSibling {
			continue
		}
		if builtinsUnreliable && looksLikeBuiltinName(name) {
			// builtins introspection failed; don't risk false-positive
			// on names that follow DuckDB's built-in naming convention
			// (read_*, pragma_*, information_schema_*, duckdb_*).
			continue
		}
		fr.Findings = append(fr.Findings, validate.NewFinding(fr.Path, 0,
			validate.RuleCrossASTUnknownLib,
			fmt.Sprintf("FROM %s(...) references an unknown table function — typo or missing lib/%s.star", name, name)))
	}

	// Mark unrun checks as ok for human-mode display. Names match the
	// kebab-cased keys produced by normalizeCheckName so success/failure
	// share a single namespace (no "strict_fetch=fail strict-fetch=ok"
	// double-bookkeeping).
	for _, name := range []string{"strict-fetch", "strict-push", "read-only"} {
		if _, set := fr.Checks[name]; !set {
			fr.Checks[name] = "ok"
		}
	}
}

// normalizeCheckName converts the snake_case category names returned by
// execute.ValidateModelStatic ("strict_fetch", "read_only") to the
// kebab-case names used in the human-mode and JSON output ("strict-fetch",
// "read-only"). Keeps the public output stable across the snake-vs-kebab
// boundary between internal/execute and validate output.
func normalizeCheckName(category string) string {
	return strings.ReplaceAll(category, "_", "-")
}

// validateBlueprintFile runs blueprint-side validators on a single .star
// file by calling ParseLibFile directly. Independent of registry state —
// one broken blueprint never blocks validation of the others.
//
// Outcomes:
//   - parse error                              → BLOCKER blueprint.invalid_api_dict
//   - parse OK, helper (`_`-prefix or no API)  → INFO blueprint.helper_without_api when no `_`
//                                                (no finding when `_`-prefixed — explicit helper)
//   - parse OK, returns LibFunc                → blueprint.* checks pass
//
// The parse step runs FIRST regardless of underscore prefix — helpers
// must still be valid Starlark; the prefix only suppresses the
// "missing API dict" hint, not parse-error detection.
func validateBlueprintFile(_ *libregistry.Registry, fr *validate.FileResult, projectDir, path string) {
	rel, _ := filepath.Rel(projectDir, path)
	name := strings.TrimSuffix(filepath.Base(path), ".star")

	src, err := os.ReadFile(path)
	if err != nil {
		fr.Checks["blueprint"] = "fail"
		fr.Findings = append(fr.Findings, validate.NewFinding(rel, 0,
			validate.RuleBlueprintOther,
			fmt.Sprintf("read file: %v", err)))
		return
	}

	lf, parseErr := libregistry.ParseLibFile(name, rel, string(src))
	if parseErr != nil {
		fr.Checks["blueprint"] = "fail"
		fr.Findings = append(fr.Findings, validate.NewFinding(rel, 0,
			validate.RuleBlueprintInvalidAPIDict,
			parseErr.Error()))
		return
	}

	if lf == nil {
		// No API dict. `_`-prefix is an explicit helper convention;
		// other names get an INFO hint suggesting they add an API dict
		// or rename with the prefix.
		if !strings.HasPrefix(name, "_") {
			fr.Findings = append(fr.Findings, validate.NewFinding(rel, 0,
				validate.RuleBlueprintHelperWithoutAPI,
				"file in lib/ without API dict — intended as helper? Prefix with _ to silence"))
		}
		fr.Checks["blueprint"] = "ok"
		return
	}

	fr.Checks["blueprint"] = "ok"
}

// mapParserError wraps a parser error as a Finding. We rely on string
// matching to pick the right rule-ID — parser errors don't carry IDs
// directly, but their phrasing is stable enough for this mapping.
func mapParserError(path string, err error) validate.Finding {
	msg := err.Error()
	id := validate.RuleParserOther
	switch {
	case strings.Contains(msg, "unterminated `/*`"):
		id = validate.RuleParserUnterminatedBlockComment
	case strings.Contains(msg, "multi-statement"):
		id = validate.RuleParserMultiStatement
	case strings.Contains(msg, "@sink_detect_deletes"):
		id = validate.RuleParserDeprecatedSinkDetectDeletes
	case strings.Contains(msg, "@sink_delete_threshold"):
		id = validate.RuleParserDeprecatedSinkDeleteThresh
	case strings.Contains(msg, "@sink"):
		id = validate.RuleParserDeprecatedSink
	case strings.Contains(msg, "@script"):
		id = validate.RuleParserDeprecatedScript
	case strings.Contains(msg, "@kind"):
		id = validate.RuleParserInvalidKind
	case strings.Contains(msg, "@unique_key"):
		id = validate.RuleParserInvalidUniqueKey
	case strings.Contains(msg, "@incremental"):
		id = validate.RuleParserInvalidIncremental
	case strings.Contains(msg, "@sorted_by"):
		id = validate.RuleParserInvalidSortedBy
	case strings.Contains(msg, "@partitioned_by"):
		id = validate.RuleParserInvalidPartitioned
	case strings.Contains(msg, "events") && strings.Contains(msg, "@column"):
		id = validate.RuleParserEventsMissingCols
	case strings.Contains(msg, "directive"):
		id = validate.RuleParserInvalidDirective
	}
	return validate.NewFinding(path, 0, id, msg)
}

// mapStaticValidatorError wraps an execute.StaticValidationResult as a
// Finding. Category is the discriminant; the error message picks a
// sub-rule via string matching.
func mapStaticValidatorError(path string, r execute.StaticValidationResult) validate.Finding {
	msg := r.Err.Error()
	var id validate.RuleID
	switch r.Category {
	case "strict_fetch":
		id = strictFetchRuleFromMessage(msg)
	case "strict_push":
		id = strictPushRuleFromMessage(msg)
	case "read_only":
		id = readOnlyRuleFromMessage(msg)
	case "parser":
		id = validate.RuleParserOther
	default:
		id = validate.RuleParserOther
	}
	return validate.NewFinding(path, 0, id, msg)
}

func strictFetchRuleFromMessage(msg string) validate.RuleID {
	switch {
	case strings.Contains(msg, "CAST"), strings.Contains(msg, "::"):
		return validate.RuleStrictFetchCastRequired
	case strings.Contains(msg, "alias"), strings.Contains(msg, "AS "):
		return validate.RuleStrictFetchAliasRequired
	case strings.Contains(msg, "WHERE"):
		return validate.RuleStrictFetchWhereDisallowed
	case strings.Contains(msg, "GROUP BY"):
		return validate.RuleStrictFetchGroupByDisallowed
	case strings.Contains(msg, "DISTINCT"):
		return validate.RuleStrictFetchDistinctDisallow
	case strings.Contains(msg, "ORDER BY"):
		return validate.RuleStrictFetchOrderByDisallowed
	case strings.Contains(msg, "UNION"), strings.Contains(msg, "INTERSECT"):
		return validate.RuleStrictFetchUnionDisallowed
	case strings.Contains(msg, "exactly one"):
		return validate.RuleStrictFetchMultipleLibCalls
	case strings.Contains(msg, "lib call"), strings.Contains(msg, "TABLE_FUNCTION"):
		return validate.RuleStrictFetchNoLibCall
	}
	return validate.RuleStrictFetchOther
}

func strictPushRuleFromMessage(msg string) validate.RuleID {
	switch {
	case strings.Contains(msg, "SELECT *"):
		return validate.RuleStrictPushSelectStar
	case strings.Contains(msg, "lib call"):
		return validate.RuleStrictPushLibCallInFrom
	case strings.Contains(msg, "CAST"):
		return validate.RuleStrictPushCastRequired
	case strings.Contains(msg, "alias"):
		return validate.RuleStrictPushAliasRequired
	}
	return validate.RuleStrictPushOther
}

func readOnlyRuleFromMessage(msg string) validate.RuleID {
	if strings.Contains(msg, "OFFSET") {
		return validate.RuleReadOnlyOffsetDisallowed
	}
	return validate.RuleReadOnlyLimitDisallowed
}

// modelReferencesBrokenBlueprint reports whether the model SQL's AST
// contains a TABLE_FUNCTION whose name corresponds to a blueprint that
// failed to parse (and was therefore filtered out of `calls` by
// libcall.Detect). Used to suppress the strict_fetch.no_lib_call
// finding when the real cause is degraded blueprint coverage.
func modelReferencesBrokenBlueprint(astJSON string, scanErrs map[string]error) bool {
	if astJSON == "" || len(scanErrs) == 0 {
		return false
	}
	parsedAST, err := duckast.Parse(astJSON)
	if err != nil {
		return false
	}
	for _, name := range libcall.AllTableFunctionNames(parsedAST) {
		if _, broken := scanErrs[name+".star"]; broken {
			return true
		}
	}
	return false
}

// looksLikeBuiltinName matches DuckDB's built-in table-function naming
// conventions. Used as a defensive heuristic when the dynamic
// duckdb_functions() introspection fails — we suppress unknown-lib
// findings on these prefixes rather than emit mass false positives.
//
// Names that don't match this heuristic still get the unknown-lib
// finding even when introspection fails, so user typos in lib references
// are not hidden.
func looksLikeBuiltinName(name string) bool {
	prefixes := []string{
		"read_",
		"pragma_",
		"information_schema_",
		"duckdb_",
		"sqlite_",
		"postgres_",
		"mysql_",
		"json_",
	}
	lower := strings.ToLower(name)
	for _, p := range prefixes {
		if strings.HasPrefix(lower, p) {
			return true
		}
	}
	return false
}

// loadProjectExtensions executes config/extensions.sql against sess, if
// the file exists.
//
// Returns nil when the file is missing or empty — those are valid
// project states. Returns an error if the file exists but Exec rejects
// its content; the caller should fail loudly because a broken
// extensions.sql breaks validate's correctness (extension-provided
// TABLE_FUNCTIONs would be flagged as cross_ast.unknown_lib typos).
//
// The file is loaded as raw SQL (with $ENV expansion) — same convention
// as Session.InitWithCatalog, but without the rest of catalog init
// which would attach DuckLake state we don't want here.
func loadProjectExtensions(sess *duckdb.Session, configPath string) error {
	if configPath == "" {
		return nil
	}
	path := filepath.Join(configPath, "extensions.sql")
	content, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read %s: %w", path, err)
	}
	if len(content) == 0 {
		return nil
	}
	if err := sess.Exec(os.ExpandEnv(string(content))); err != nil {
		return fmt.Errorf("execute %s: %w", path, err)
	}
	return nil
}
