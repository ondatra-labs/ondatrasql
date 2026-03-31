// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"context"
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
var version = "0.1.0"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
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
		return runInit()
	case "version":
		fmt.Println(version)
		return nil
	}

	// Find project root
	cwd, _ := os.Getwd()
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

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	// Handle commands
	cmd := args[0]
	switch cmd {
	case "run":
		if len(args) > 1 {
			return runModel(ctx, cfg, args[1], false)
		}
		return runAll(ctx, cfg, false)

	case "sandbox":
		if len(args) > 1 {
			return runModel(ctx, cfg, args[1], true)
		}
		return runAll(ctx, cfg, true)

	case "lineage":
		if len(args) < 2 {
			return fmt.Errorf("usage: ondatrasql lineage overview | <model> | <model.column>")
		}
		return runLineage(cfg, args[1:])
	// SQL-based query commands
	case "history":
		return runHistory(cfg, args[1:])
	case "stats":
		return runStats(cfg)
	case "describe":
		if len(args) < 2 {
			return fmt.Errorf("usage: ondatrasql describe <model>")
		}
		return runDescribe(cfg, args[1])
	case "edit":
		if len(args) < 2 {
			return fmt.Errorf("usage: ondatrasql edit <model>")
		}
		return runEdit(cfg, args[1])
	case "new":
		if len(args) < 2 {
			return fmt.Errorf("usage: ondatrasql new <schema.model[.sql|.star]>")
		}
		return runNew(cfg, args[1])

	case "query":
		return runQueryTable(cfg, args[1:])

	case "sql":
		if len(args) < 2 {
			return fmt.Errorf("usage: ondatrasql sql \"SELECT ...\" [--format csv|json|markdown]")
		}
		format := "markdown"
		for i := 2; i < len(args); i++ {
			if (args[i] == "--format" || args[i] == "-f") && i+1 < len(args) {
				format = args[i+1]
				break
			}
		}
		return runSQL(cfg, args[1], format)

	case "daemon":
		return runDaemon(ctx, cfg)

	default:
		// Reject path traversal in command name
		if strings.Contains(cmd, "/") || strings.Contains(cmd, "\\") || strings.Contains(cmd, "..") {
			return fmt.Errorf("invalid command: %q", cmd)
		}
		// Check if there's a sql/<cmd>.sql file to execute
		sqlFile := filepath.Join(cfg.ProjectDir, "sql", cmd+".sql")
		if _, err := os.Stat(sqlFile); err == nil {
			sandboxMode := len(args) > 1 && args[1] == "sandbox"
			return runSQLFile(cfg, sqlFile, sandboxMode)
		}
		return fmt.Errorf("unknown command: %s (run 'ondatrasql' for help)", cmd)
	}
}

func printHelp() {
	help := fmt.Sprintf(`OndatraSQL v%s - Data Pipeline Framework

Project:
  init                    Initialize project in current directory
  version                 Print version

Run:
  run [model]             Run all models or specific model
  sandbox [model]         Preview changes without affecting data
  daemon                  Start event collection daemon

Introspection:
  stats                   Project overview and all models
  history [model]         Run history [--limit N]
  describe <model>        Model details (schema, deps, SQL)
  query <table>           Query data [--limit N] [--format csv|json|md]
  sql "SELECT ..."        Run SQL [--format csv|json|md]

Lineage:
  lineage overview        All models with dependencies
  lineage <model>         Model with column-level lineage
  lineage <model.column>  Trace column through lineage

Development:
  new <model[.ext]>       Create model (.sql or .star)
  edit <target>           Open in $EDITOR (model, or one of:)
        env               .env
        macros            config/macros.sql
        variables         config/variables.sql
        sources           config/sources.sql
        secrets           config/secrets.sql
        settings          config/settings.sql
        catalog           config/catalog.sql
        extensions        config/extensions.sql

SQL Commands (from sql/ folder):
  merge [sandbox]         Merge small files (ducklake_merge_adjacent_files)
  expire [sandbox]        Expire old snapshots (ducklake_expire_snapshots)
  cleanup [sandbox]       Delete old files (ducklake_cleanup_old_files)
  orphaned [sandbox]      Delete orphaned files (ducklake_delete_orphaned_files)

Global Options:
  --json                  Emit JSON lines to stdout (human output to stderr)`, version)

	output.Println(help)
}
