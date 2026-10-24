// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"fmt"

	"github.com/ondatra-labs/ondatrasql/internal/duckast"
	"github.com/ondatra-labs/ondatrasql/internal/libcall"
	"github.com/ondatra-labs/ondatrasql/internal/lineage"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
)

// StaticValidationResult is one rejection from the static validator suite.
// Category names map to validate.RuleID prefixes so callers can derive the
// public rule-ID without re-classifying errors.
type StaticValidationResult struct {
	Category string // "strict_fetch" | "strict_push" | "read_only"
	Err      error
}

// ValidateModelStatic runs the data-free validators against a model.
//
// This is the entry point used by the `validate` CLI command. It performs
// the same checks that runner.Run() runs at execution time — minus anything
// that requires DuckDB session state (constraints, audits, schema evolution,
// catalog-attach, extension-load, external API connectivity).
//
// astJSON should be the output of lineage.GetAST(sess, model.SQL). Pass ""
// when SQL serialization is known to have failed — the caller is expected
// to have already emitted a parser-error finding in that case.
//
// If astJSON is non-empty but malformed (json_serialize_sql returned
// content that fails duckast.Parse), the function returns a synthetic
// "parser_other" result so callers don't tacit-pass on AST drift.
//
// libCalls is the result of libcall.Detect(ast, reg) — the validate
// orchestrator runs detection once per model and passes the slice in
// rather than re-detecting inside each validator.
//
// Order mirrors runner.Run() so behavior matches at validate vs run time.
// StaticOptions carries project settings that change what the static
// validators accept.
type StaticOptions struct {
	// SingleArrowLambdas is set when config/settings.sql enables DuckDB's
	// deprecated `x -> ...` lambda syntax (InspectProjectLambdas).
	SingleArrowLambdas bool
}

func ValidateModelStatic(model *parser.Model, astJSON string, libCalls []libcall.Call, opts StaticOptions) []StaticValidationResult {
	var out []StaticValidationResult

	if astJSON == "" {
		// Caller already errored on AST serialization; nothing to do.
		return out
	}

	parsedAST, parseErr := duckast.Parse(astJSON)
	if parseErr != nil {
		// AST JSON was produced by DuckDB but our parser couldn't
		// handle it. This usually means an AST schema drift — surface
		// as a parser-level finding so validate doesn't tacit-pass.
		return []StaticValidationResult{
			{Category: "parser", Err: fmt.Errorf("AST parse failed: %w", parseErr)},
		}
	}

	// The run reads the model's source tables from the AST, and for append
	// and merge its column lineage, which CDC needs; failing either fails
	// the run, so it is a BLOCKER here.
	if _, err := lineage.ExtractTablesFromAST(astJSON); err != nil {
		out = append(out, StaticValidationResult{Category: "parser", Err: fmt.Errorf("cannot read the model's source tables: %w", err)})
	}
	if model.Kind == "append" || model.Kind == "merge" {
		if _, err := lineage.ExtractFromAST(astJSON); err != nil {
			out = append(out, StaticValidationResult{Category: "parser", Err: fmt.Errorf("cannot read the model's column lineage, which CDC needs: %w", err)})
		}
	}

	if err := validateNoLimitOffset(parsedAST); err != nil {
		out = append(out, StaticValidationResult{Category: "read_only", Err: err})
	}

	if !opts.SingleArrowLambdas {
		for _, err := range validateNoArrowLambda(parsedAST) {
			out = append(out, StaticValidationResult{Category: "parser", Err: err})
		}
	}

	if model.Fetch {
		if err := validateStrictLibSchema(parsedAST); err != nil {
			out = append(out, StaticValidationResult{Category: "strict_fetch", Err: err})
		}
	}

	if model.Push != "" {
		if err := validateStrictPushSchema(parsedAST, len(libCalls) > 0); err != nil {
			out = append(out, StaticValidationResult{Category: "strict_push", Err: err})
		}
	}

	return out
}
