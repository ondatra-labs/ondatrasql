// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package libcall detects blueprint (lib function) references in SQL ASTs.
//
// This package owns the static-analysis view of a TABLE_FUNCTION call:
// the function name, the matched blueprint, the AST node, and its argument
// expressions. Runtime augmentation (TempTable, ScriptResult, etc.) is
// added by execute.LibCall, which embeds libcall.Call.
//
// Consumers:
//
//   - cmd/ondatrasql `describe` and `validate` (read-only, no runtime state)
//   - internal/execute (wraps Call into LibCall for the run pipeline)
//
// AST is the only detection mechanism. There is no string fallback —
// callers that need to handle macro-expanded SQL during runtime use the
// fallback that lives in execute.detectLibCallsFromSQL, which is bound to
// the runtime path where DuckDB has registered lib functions as macros.
package libcall

import (
	"github.com/ondatra-labs/ondatrasql/internal/duckast"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/libregistry"
	"github.com/ondatra-labs/ondatrasql/internal/lineage"
)

// Call represents a TABLE_FUNCTION node in a SQL AST that matches a
// registered blueprint. Contains static-analysis state only.
type Call struct {
	// FuncName is the lib function name as it appears in SQL
	// (e.g. "gam_fetch"). Always lower-cased by DuckDB.
	FuncName string
	// CallIndex is a unique 0-based index per call within a single SQL
	// statement, used for dedup and ordering.
	CallIndex int
	// Lib is the registry entry matched by FuncName. Never nil for
	// values returned from Detect.
	Lib *libregistry.LibFunc
	// ArgNodes holds the raw AST argument expressions. Order matches
	// declaration order in the SQL FROM clause.
	ArgNodes []*duckast.Node
	// ASTNode is the TABLE_FUNCTION node itself. Used for AST rewriting
	// during runtime materialization.
	ASTNode *duckast.Node
}

// Detect walks ast for TABLE_FUNCTION nodes matching TABLE blueprints in
// reg. Sinks are excluded — push blueprints don't appear in FROM clauses.
//
// Returns nil for nil/empty inputs. The returned slice is owned by the
// caller and may be mutated (callers in runtime paths add temp-table
// pointers and ack metadata to it via the wrapping execute.LibCall).
func Detect(ast *duckast.AST, reg *libregistry.Registry) []Call {
	if ast == nil || reg == nil || reg.Empty() {
		return nil
	}
	var calls []Call
	callIndex := 0
	ast.Walk(func(n *duckast.Node) bool {
		if !n.IsTableFunction() {
			return true
		}
		name := n.TableFunctionName()
		if name == "" {
			return true
		}
		lf := reg.Get(name)
		if lf == nil || lf.IsSink {
			return true
		}
		calls = append(calls, Call{
			FuncName:  name,
			CallIndex: callIndex,
			Lib:       lf,
			ArgNodes:  n.TableFunctionArgs(),
			ASTNode:   n,
		})
		callIndex++
		return true
	})
	return calls
}

// DetectInSQL is a convenience that serializes SQL via DuckDB and runs
// Detect on the resulting AST. The session does NOT need to have the
// project catalog attached — a vanilla session for `json_serialize_sql`
// is enough. The session must NOT have lib functions registered as
// macros (which would expand calls and hide TABLE_FUNCTION nodes from
// the AST); this is the case for `validate` and `describe`-time sessions.
//
// Returns (nil, err) when serialization or AST parsing fails. Returns
// (nil, nil) when any input is zero-valued. Never falls back to string
// matching — the runtime fallback is execute.detectLibCallsFromSQL.
func DetectInSQL(sess *duckdb.Session, sql string, reg *libregistry.Registry) ([]Call, error) {
	if sess == nil || sql == "" || reg == nil || reg.Empty() {
		return nil, nil
	}
	astJSON, err := lineage.GetAST(sess, sql)
	if err != nil {
		return nil, err
	}
	ast, err := duckast.Parse(astJSON)
	if err != nil {
		return nil, err
	}
	return Detect(ast, reg), nil
}

// AllTableFunctionNames walks ast and returns the DEDUPED set of
// TABLE_FUNCTION names referenced in the SQL — including ones that
// DON'T match any registered blueprint. Used by `validate` to detect
// typos: a SQL like `FROM gam_repor()` (typo of `gam_report`) appears
// in the AST but gets filtered out by Detect, so callers need this
// raw view to flag `cross_ast.unknown_lib`.
//
// Names are deduplicated so a model with `FROM x() UNION FROM x()`
// reports `x` once, not twice. Callers that need every call site
// (e.g. for a count) should walk the AST themselves; this function
// is a *set* of names, not a multiset. (R8 #13: doc previously
// claimed "every call" which mismatched the dedup behaviour.)
//
// Callers must filter the result against the set of names that should
// NOT be reported as unknown — both registered blueprints (via
// libregistry.Registry) and DuckDB built-ins (via BuiltinTableFunctions).
func AllTableFunctionNames(ast *duckast.AST) []string {
	if ast == nil {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	ast.Walk(func(n *duckast.Node) bool {
		if !n.IsTableFunction() {
			return true
		}
		name := n.TableFunctionName()
		if name == "" || seen[name] {
			return true
		}
		seen[name] = true
		out = append(out, name)
		return true
	})
	return out
}

// BuiltinTableFunctions queries DuckDB for the set of built-in table
// function names. The result depends on which extensions are loaded; the
// caller's session is the source of truth.
//
// Used by `validate` to filter AllTableFunctionNames so DuckDB built-ins
// (read_csv, read_parquet, unnest, generate_series, etc.) don't get
// flagged as cross_ast.unknown_lib.
//
// Returns (nil, err) if the introspection query fails.
func BuiltinTableFunctions(sess *duckdb.Session) (map[string]bool, error) {
	if sess == nil {
		return nil, nil
	}
	rows, err := sess.QueryRowsMap("SELECT function_name FROM duckdb_functions() WHERE function_type = 'table'")
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(rows))
	for _, r := range rows {
		if name, ok := r["function_name"]; ok && name != "" {
			out[name] = true
		}
	}
	return out, nil
}
