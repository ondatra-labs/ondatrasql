// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/ondatra-labs/ondatrasql/internal/config"
	"github.com/ondatra-labs/ondatrasql/internal/output"
)

// version is set at build time via -ldflags "-X main.version=x.y.z"
var version = "0.33.0"

// exitCoder is implemented by errors that map to a specific process exit
// code. Used by main() to honour the validate-style 0/1/2 contract.
type exitCoder interface {
	ExitCode() int
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		msg := err.Error()
		// Strip internal wrapper prefixes for cleaner stderr output
		for _, prefix := range []string{"materialize: ", "create temp table: "} {
			msg = strings.TrimPrefix(msg, prefix)
		}
		// Honour custom exit codes (e.g. invocation errors → 2,
		// findings → 1) when the error implements exitCoder.
		code := 1
		var ec exitCoder
		if errors.As(err, &ec) {
			code = ec.ExitCode()
		}
		// Don't print the error message for the silent "findings"
		// sentinel — the report itself was already rendered.
		if !errors.Is(err, errFindings) {
			fmt.Fprintf(os.Stderr, "error: %s\n", msg)
		}
		os.Exit(code)
	}
}

func run(args []string) error {

	// Parse global --json flag
	var jsonMode bool
	var filteredArgs []string
	for _, a := range args {
		if a == "--json" {
			jsonMode = true
		} else {
			filteredArgs = append(filteredArgs, a)
		}
	}
	args = filteredArgs
	output.Init(jsonMode)

	// No args = show help (safe default)
	if len(args) == 0 {
		printHelp()
		return nil
	}

	// Commands that don't require an existing project
	switch args[0] {
	case "init":
		if len(args) > 1 {
			return &invocationErr{fmt.Errorf("usage: ondatrasql init (got %d unexpected args)", len(args)-1)}
		}
		return runInit()
	case "version":
		if len(args) > 1 {
			return &invocationErr{fmt.Errorf("usage: ondatrasql version (got %d unexpected args)", len(args)-1)}
		}
		if output.JSONEnabled {
			output.EmitJSON(map[string]any{
				"schema_version": 1,
				"version":        version,
			})
			return nil
		}
		fmt.Println(version)
		return nil
	case "auth":
		if len(args) < 2 {
			// Try to load .env from current project for local provider detection
			if cwd, err := os.Getwd(); err == nil {
				if root, err := config.FindProjectRoot(cwd); err == nil {
					if err := config.LoadEnvFile(filepath.Join(root, ".env")); err != nil && !os.IsNotExist(err) {
						return fmt.Errorf("load .env: %w", err)
					}
				}
			}
			return runAuthList(context.Background())
		}
	}

	// Find project root
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("get current working directory: %w", err)
	}
	projectDir, err := config.FindProjectRoot(cwd)
	if err != nil {
		return fmt.Errorf("not in an ondatrasql project: %w", err)
	}

	// Load config
	cfg, err := config.Load(projectDir)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// Create root context with cancellation on signals
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown. The signal handler must be cleaned up
	// when run() returns so repeated in-process invocations (e.g.
	// integration tests calling run() in a loop) don't leak a notifier
	// registration AND a goroutine waiting on sigCh forever.
	//
	// Defer ordering matters and is LIFO: signal.Stop must run BEFORE
	// close(stopCh), so the registered defers go (1) close(stopCh)
	// then (2) signal.Stop — meaning at run-time signal.Stop pops
	// first, unregistering the notifier, then close(stopCh) wakes the
	// goroutine which falls through. This sequence guarantees no late
	// signal can be queued onto sigCh after the goroutine has exited.
	// (R8 #14 — pre-fix the order was inverse of the comment, leaving
	// a small window where a signal could land on sigCh after the
	// goroutine had returned. Benign but sloppy.)
	sigCh := make(chan os.Signal, 1)
	stopCh := make(chan struct{})
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer close(stopCh)
	defer signal.Stop(sigCh)
	go func() {
		select {
		case <-sigCh:
			cancel()
		case <-stopCh:
		}
	}()

	// Handle commands
	cmd := args[0]
	switch cmd {
	case "run":
		if len(args) > 2 {
			return &invocationErr{fmt.Errorf("usage: ondatrasql run [<model>] (got %d extra args)", len(args)-2)}
		}
		if len(args) > 1 {
			return runModel(ctx, cfg, args[1], false)
		}
		return runAll(ctx, cfg, false)

	case "sandbox":
		if len(args) > 2 {
			return &invocationErr{fmt.Errorf("usage: ondatrasql sandbox [<model>] (got %d extra args)", len(args)-2)}
		}
		if len(args) > 1 {
			return runModel(ctx, cfg, args[1], true)
		}
		return runAll(ctx, cfg, true)

	case "lineage":
		if len(args) < 2 {
			return &invocationErr{fmt.Errorf("usage: ondatrasql lineage overview | <model> | <model.column>")}
		}
		return runLineage(cfg, args[1:])
	// SQL-based query commands
	case "history":
		return runHistory(cfg, args[1:])
	case "stats":
		if len(args) > 1 {
			return &invocationErr{fmt.Errorf("usage: ondatrasql stats (got %d unexpected args)", len(args)-1)}
		}
		return runStats(cfg)
	case "describe":
		if len(args) < 2 {
			return &invocationErr{fmt.Errorf("usage: ondatrasql describe <model> | describe blueprint [<name>]")}
		}
		// `describe blueprint [<name>] [--fields=...]` is a distinct subcommand.
		// No collision with model names: OndatraSQL targets always have
		// the form `schema.table`, so a bare token "blueprint" can never
		// refer to a model. Schema-qualified names like `blueprint.x` go
		// through runDescribe normally because args[1] != "blueprint".
		if args[1] == "blueprint" {
			return runDescribeBlueprint(cfg, args[2:])
		}
		if len(args) > 2 {
			return &invocationErr{fmt.Errorf("usage: ondatrasql describe <model> (got %d extra args)", len(args)-2)}
		}
		return runDescribe(cfg, args[1])

	case "validate":
		return runValidate(cfg, args[1:])
	case "edit":
		if len(args) < 2 {
			return &invocationErr{fmt.Errorf("usage: ondatrasql edit <target>  (target = <schema.model>, env, catalog, sources, extensions, secrets, settings, macros/<name>, or variables/<name>)")}
		}
		if len(args) > 2 {
			return &invocationErr{fmt.Errorf("usage: ondatrasql edit <target> (got %d extra args)", len(args)-2)}
		}
		return runEdit(cfg, args[1])
	case "new":
		if len(args) < 2 {
			return &invocationErr{fmt.Errorf("usage: ondatrasql new <schema>|<schema.sub>|<schema.model[.sql]>  (bare schema or schema.sub creates a directory; full path creates a model file)")}
		}
		if len(args) > 2 {
			return &invocationErr{fmt.Errorf("usage: ondatrasql new <schema>|<schema.sub>|<schema.model[.sql]> (got %d extra args)", len(args)-2)}
		}
		return runNew(cfg, args[1])

	case "query":
		return runQueryTable(cfg, args[1:])

	case "sql":
		if len(args) < 2 {
			return &invocationErr{fmt.Errorf("usage: ondatrasql sql \"SELECT ...\" [--format csv|json|markdown]")}
		}
		format := "markdown"
		for i := 2; i < len(args); i++ {
			a := args[i]
			switch {
			case a == "--format" || a == "-f":
				if i+1 >= len(args) {
					return &invocationErr{fmt.Errorf("%s requires a value (csv|json|markdown)", a)}
				}
				format = args[i+1]
				i++
			case strings.HasPrefix(a, "--format="):
				format = strings.TrimPrefix(a, "--format=")
			case strings.HasPrefix(a, "-"):
				return &invocationErr{fmt.Errorf("unknown flag: %s", a)}
			default:
				return &invocationErr{fmt.Errorf("unexpected argument: %s", a)}
			}
		}
		switch format {
		case "csv", "json", "markdown", "md":
		default:
			return &invocationErr{fmt.Errorf("invalid --format value %q (want csv|json|markdown)", format)}
		}
		return runSQL(cfg, args[1], format)

	case "schedule":
		return runSchedule(cfg, args[1:])

	case "auth":
		if len(args) < 2 {
			return runAuthList(ctx)
		}
		if len(args) > 2 {
			return &invocationErr{fmt.Errorf("usage: ondatrasql auth [<provider>] (got %d extra args)", len(args)-2)}
		}
		return runAuth(ctx, cfg, args[1])

	default:
		if !isValidCommandName(cmd) {
			return &invocationErr{fmt.Errorf("invalid command: %q", cmd)}
		}
		// Check if there's a sql/<cmd>.sql file to execute
		sqlFile := filepath.Join(cfg.ProjectDir, "sql", cmd+".sql")
		if _, err := os.Stat(sqlFile); err == nil {
			// sql/<cmd>.sql commands run real DuckLake catalog operations.
			// They don't support sandbox mode, but we accept the explicit
			// `<cmd> sandbox` invocation and route it to sqlfile_cmd.go's
			// dedicated rejection path (which produces a friendly
			// "<cmd> cannot run in sandbox mode" message naming the
			// command). Pre-R-strict-args this was the documented UX;
			// without the special-case the rejection collapses into a
			// generic "extra args" error that doesn't tell the user
			// WHY their request is invalid.
			//
			// Any other trailing operand is a typo (`checkpoint typo`
			// must not silently fall through to a prod run).
			if len(args) == 2 && args[1] == "sandbox" {
				return runSQLFile(cfg, sqlFile, true)
			}
			if len(args) > 1 {
				return &invocationErr{fmt.Errorf("usage: ondatrasql %s [sandbox] (got %d extra args)", cmd, len(args)-1)}
			}
			return runSQLFile(cfg, sqlFile, false)
		}
		return &invocationErr{fmt.Errorf("unknown command: %s (run 'ondatrasql' for help)", cmd)}
	}
}

// isValidCommandName checks that a command name contains only safe ASCII
// characters (letters, digits, underscores, hyphens). Prevents path traversal
// and injection via the sql/<cmd>.sql file lookup.
func isValidCommandName(cmd string) bool {
	for _, r := range cmd {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-') {
			return false
		}
	}
	return true
}

func printHelp() {
	help := fmt.Sprintf(`OndatraSQL v%s - A data pipeline runtime for DuckDB and DuckLake

Project:
  init                    Initialize project in current directory
  version                 Print version

Run:
  run [model]             Run all models or specific model
  sandbox [model]         Preview changes without affecting data
  schedule [cron]         Install/show/remove OS scheduler (no args = show)

Introspection:
  stats                          Project overview and all models
  history [model]                Run history [--limit N]
  describe <model>               Model details (schema, deps, SQL)
  describe blueprint [<name>]    Blueprint API contract (no name = list all)
  validate [paths...]            Static validation [--strict] [--output=human|json|ndjson]
  query <schema.table>           Query data [--limit N] [--format csv|json|md]
  sql "SELECT ..."               Run SQL [--format csv|json|md]

Lineage:
  lineage overview        All models with dependencies
  lineage <model>         Model with column-level lineage
  lineage <model.column>  Trace column through lineage

Development:
  new <model.sql>          Create model
  edit <target>           Open in $EDITOR (model, or one of:)
        env               .env
        macros/<name>     config/macros/<name>.sql
        variables/<name>  config/variables/<name>.sql
        sources           config/sources.sql
        secrets           config/secrets.sql
        settings          config/settings.sql
        catalog           config/catalog.sql
        extensions        config/extensions.sql

Auth:
  auth                    List available OAuth2 providers
  auth <provider>         Authenticate with an OAuth2 provider

SQL Commands (from sql/ folder, prod-only — sandbox mode is rejected for these):
  flush                   Flush inlined data to Parquet (ducklake_flush_inlined_data)
  merge                   Merge small files (ducklake_merge_adjacent_files)
  expire                  Expire old snapshots (ducklake_expire_snapshots)
  cleanup                 Delete old files (ducklake_cleanup_old_files)
  orphaned                Delete orphaned files (ducklake_delete_orphaned_files)
  rewrite                 Rewrite files with deletes (ducklake_rewrite_data_files)
  checkpoint              Run all maintenance in order

Global Options:
  --json                  Emit JSON lines to stdout (human output to stderr)`, version)

	output.Println(help)
}
