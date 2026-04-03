// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/backfill"
	"github.com/ondatra-labs/ondatrasql/internal/lineage"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"github.com/ondatra-labs/ondatrasql/internal/sql"
)

// runView creates or replaces a DuckLake view.
// Views are not materialized — they resolve live against source tables.
// The view is only recreated if the SQL definition changed.
func (r *Runner) runView(model *parser.Model, result *Result, start time.Time) (*Result, error) {
	// Compute SQL hash
	stepStart := time.Now()
	sqlHash := backfill.ModelHash(model.SQL, backfill.ModelDirectives{Kind: model.Kind})
	r.trace(result, "hash_sql", stepStart, "ok")

	// Check if view needs updating (compare hash with previous commit)
	stepStart = time.Now()
	prevHash := ""
	if commitInfo, err := backfill.GetModelCommitInfo(r.sess, model.Target); err == nil && commitInfo != nil {
		prevHash = commitInfo.SQLHash
	}
	r.trace(result, "check_previous", stepStart, "ok")

	if prevHash == sqlHash {
		result.RunType = "skip"
		result.RunReason = "definition unchanged"
		result.Duration = time.Since(start)
		return result, nil
	}

	if prevHash == "" {
		result.RunType = "create"
		result.RunReason = "first run"
	} else {
		result.RunType = "create"
		result.RunReason = "sql changed"
	}

	// Ensure schema exists
	if err := r.ensureSchemaExists(model.Target); err != nil {
		return nil, fmt.Errorf("ensure schema: %w", err)
	}

	// Sandbox: qualify table refs with prod catalog via AST node manipulation
	execSQL := model.SQL
	if r.sess.ProdAlias() != "" {
		viewAST, astErr := r.getAST(model.SQL)
		if astErr != nil {
			return nil, fmt.Errorf("sandbox AST parse failed for view %s: %w", model.Target, astErr)
		}
		tables, extractErr := lineage.ExtractTablesFromAST(viewAST)
		if extractErr != nil {
			return nil, fmt.Errorf("sandbox table extraction failed for view %s: %w", model.Target, extractErr)
		}
		tablesToQualify := make(map[string]bool)
		for _, t := range tables {
			if !r.tableExistsInCatalog(t.Table, r.sess.CatalogAlias()) {
				tablesToQualify[strings.ToLower(t.Table)] = true
			}
		}
		if len(tablesToQualify) > 0 {
			qualified := false
			if root, parseErr := parseASTJSON(viewAST); parseErr == nil {
				qualifyTablesInAST(root, tablesToQualify, r.sess.ProdAlias())
				if modified, marshalErr := json.Marshal(root); marshalErr == nil {
					if deserialized, deserErr := r.deserializeAST(string(modified)); deserErr == nil {
						execSQL = deserialized
						qualified = true
					}
				}
			}
			if !qualified {
				return nil, fmt.Errorf("sandbox qualification failed for view %s: table refs may resolve to wrong catalog", model.Target)
			}
		}
	}

	// Create or replace the view
	stepStart = time.Now()
	viewSQL := sql.MustFormat("execute/view.sql", model.Target, execSQL)
	if err := r.sess.Exec(viewSQL); err != nil {
		r.trace(result, "create_view", stepStart, "error")
		return nil, fmt.Errorf("create view: %w", err)
	}
	r.trace(result, "create_view", stepStart, "ok")

	// Apply COMMENT ON VIEW (not TABLE) for description
	if model.Description != "" {
		commentSQL := fmt.Sprintf("COMMENT ON VIEW %s IS '%s'",
			model.Target, escapeSQL(model.Description))
		r.sess.Exec(commentSQL)
	}

	// Extract lineage for commit metadata
	stepStart = time.Now()
	colLineage, tableDeps, _ := r.extractLineage(model.SQL)
	r.trace(result, "meta.lineage_extract", stepStart, "ok")

	// Build commit metadata
	endTime := time.Now()
	steps := ConvertTraceToSteps(result.Trace)
	info := backfill.CommitInfo{
		Model:         model.Target,
		SQLHash:       sqlHash,
		ColumnLineage: colLineage,
		RunType:       result.RunType,
		DagRunID:      r.dagRunID,
		Depends:       tableDeps,
		Kind:          model.Kind,
		SourceFile:    model.FilePath,
		StartTime:     start.UTC().Format(time.RFC3339),
		EndTime:       endTime.UTC().Format(time.RFC3339),
		DurationMs:    endTime.Sub(start).Milliseconds(),
		DuckDBVersion: r.sess.GetVersion(),
		Steps:         steps,
		GitCommit:     r.gitInfo.Commit,
		GitBranch:     r.gitInfo.Branch,
		GitRepoURL:    r.gitInfo.RepoURL,
	}
	jsonBytes, err := json.Marshal(info)
	if err != nil {
		return nil, fmt.Errorf("marshal commit metadata: %w", err)
	}
	extraInfo := string(jsonBytes)

	// Commit metadata (registry + snapshot)
	stepStart = time.Now()
	registrySQL := fmt.Sprintf("DELETE FROM _ondatra_registry WHERE target = '%s';\nINSERT INTO _ondatra_registry VALUES ('%s', '%s', current_timestamp)",
		escapeSQL(model.Target), escapeSQL(model.Target), escapeSQL(model.Kind))
	txnSQL := sql.MustFormat("execute/commit.sql", registrySQL, model.Target, escapeSQL(extraInfo))
	if err := r.sess.Exec(txnSQL); err != nil {
		r.trace(result, "commit", stepStart, "error")
		return nil, fmt.Errorf("commit view metadata: %w", err)
	}
	r.trace(result, "commit", stepStart, "ok")

	result.Duration = time.Since(start)
	return result, nil
}

