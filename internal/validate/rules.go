// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package validate

// RuleID is a stable string slug identifying a validation rule. The string
// value is part of the public API contract — never change or remove a rule
// ID without bumping SchemaVersion.
type RuleID string

// Rule-IDs grouped by category. Implementers MUST NOT introduce new
// categories — if a check doesn't fit one of these, refactor the name.
//
//	parser.*       — directive syntax, kind consistency, deprecated directives
//	strict_fetch.* — @fetch model projection and FROM strictness
//	strict_push.*  — @push model projection strictness
//	read_only.*    — LIMIT/OFFSET rejection in pipeline models
//	dag.*          — cross-model references, cycle detection
//	blueprint.*    — lib file validation, async/sync mode
//	push.*         — kind/unique-key/group-key/poll-format checks
//	cross_ast.*    — model × blueprint cross-validation
const (
	// parser.* — bound to ParseModel + validateModel.
	RuleParserMultiStatement      RuleID = "parser.multi_statement"
	RuleParserInvalidDirective    RuleID = "parser.invalid_directive"
	RuleParserInvalidKind         RuleID = "parser.invalid_kind"
	RuleParserInvalidUniqueKey    RuleID = "parser.invalid_unique_key"
	RuleParserInvalidIncremental  RuleID = "parser.invalid_incremental"
	RuleParserInvalidSortedBy     RuleID = "parser.invalid_sorted_by"
	RuleParserInvalidPartitioned  RuleID = "parser.invalid_partitioned_by"
	RuleParserExposeOnNonTable    RuleID = "parser.expose_on_non_table"
	RuleParserEventsMissingCols   RuleID = "parser.events_missing_columns"
	RuleParserMissingDirective    RuleID = "parser.missing_directive"
	RuleParserOther               RuleID = "parser.other"
	RuleParserDuplicateTarget     RuleID = "parser.duplicate_target"
	// parser.unterminated_block_comment fires when a `/*` in the
	// header has no closing `*/`. Pre-R11 this surfaced via the
	// generic parser.other category, so clients couldn't filter on
	// a stable rule-ID. (R11 #8.)
	RuleParserUnterminatedBlockComment RuleID = "parser.unterminated_block_comment"
	// parser.deprecated_* are explicit WARN — see severityOverrides.
	RuleParserDeprecatedSink              RuleID = "parser.deprecated_sink"
	RuleParserDeprecatedScript            RuleID = "parser.deprecated_script"
	RuleParserDeprecatedSinkDetectDeletes RuleID = "parser.deprecated_sink_detect_deletes"
	RuleParserDeprecatedSinkDeleteThresh  RuleID = "parser.deprecated_sink_delete_threshold"

	// strict_fetch.* — bound to validateStrictLibSchema, validateFetchTopLevel,
	// validateSelectProjections (in fetch mode).
	RuleStrictFetchCastRequired      RuleID = "strict_fetch.cast_required"
	RuleStrictFetchAliasRequired     RuleID = "strict_fetch.alias_required"
	RuleStrictFetchWhereDisallowed   RuleID = "strict_fetch.where_clause_disallowed"
	RuleStrictFetchGroupByDisallowed RuleID = "strict_fetch.group_by_disallowed"
	RuleStrictFetchDistinctDisallow  RuleID = "strict_fetch.distinct_disallowed"
	RuleStrictFetchOrderByDisallowed RuleID = "strict_fetch.order_by_disallowed"
	RuleStrictFetchUnionDisallowed   RuleID = "strict_fetch.union_disallowed"
	RuleStrictFetchMultipleLibCalls  RuleID = "strict_fetch.multiple_lib_calls"
	RuleStrictFetchNoLibCall         RuleID = "strict_fetch.no_lib_call"
	RuleStrictFetchOther             RuleID = "strict_fetch.other"

	// strict_push.* — bound to validateStrictPushSchema and validateSelectProjections (push mode).
	RuleStrictPushCastRequired     RuleID = "strict_push.cast_required"
	RuleStrictPushAliasRequired    RuleID = "strict_push.alias_required"
	RuleStrictPushSelectStar       RuleID = "strict_push.select_star_disallowed"
	RuleStrictPushLibCallInFrom    RuleID = "strict_push.lib_call_disallowed"
	RuleStrictPushOther            RuleID = "strict_push.other"

	// read_only.* — bound to validateNoLimitOffset.
	RuleReadOnlyLimitDisallowed  RuleID = "read_only.limit_disallowed"
	RuleReadOnlyOffsetDisallowed RuleID = "read_only.offset_disallowed"

	// dag.* — bound to internal/dag.
	RuleDagCycleDetected RuleID = "dag.cycle_detected"
	RuleDagExternalRef   RuleID = "dag.external_reference" // INFO

	// blueprint.* — bound to parseLibFile.
	RuleBlueprintInvalidAPIDict   RuleID = "blueprint.invalid_api_dict"
	RuleBlueprintHelperWithoutAPI RuleID = "blueprint.helper_without_api" // INFO
	RuleBlueprintOther            RuleID = "blueprint.other"

	// push.* — bound to ValidateModelPushCompat.
	RulePushKindNotSupported    RuleID = "push.kind_not_supported"
	RulePushMissingUniqueKey    RuleID = "push.missing_unique_key"
	RulePushMissingGroupKey     RuleID = "push.missing_group_key"
	RulePushInvalidPollInterval RuleID = "push.invalid_poll_interval"
	RulePushInvalidPollTimeout  RuleID = "push.invalid_poll_timeout"
	RulePushOther               RuleID = "push.other"

	// cross_ast.* — bound to model × blueprint cross-validation.
	RuleCrossASTArgsMismatch RuleID = "cross_ast.args_mismatch"
	RuleCrossASTUnknownLib   RuleID = "cross_ast.unknown_lib"

	// validate.* — diagnostics about validate's own environment, not
	// about user code. Emitted when validate's accuracy is degraded by
	// something the user can fix (broken extensions.sql, missing tools).
	// All entries are WARN by default — they don't block code merge but
	// signal that downstream findings may be incomplete.
	RuleValidateExtensionsLoadFailed     RuleID = "validate.extensions_load_failed"
	RuleValidateBlueprintLoadFailed      RuleID = "validate.blueprint_load_failed"
	RuleValidateBuiltinIntrospectionFail RuleID = "validate.builtin_introspection_failed"
	RuleValidateWalkSkipped              RuleID = "validate.walk_skipped"
	RuleValidateLineageExtractFailed     RuleID = "validate.lineage_extract_failed"
)

// severityOverrides lists the explicit non-default severities. Default is
// SeverityBlocker — only entries here are WARN or INFO. The check "would
// this fail at run today?" decides default classification; entries here
// are the cases where the answer is "compiles OK but probably wrong"
// (WARN) or "can't verify without extern state" (INFO).
//
// Adding a new WARN/INFO without listing it here is forbidden — keeps
// severity assignments traceable to a single source.
var severityOverrides = map[RuleID]Severity{
	// Deprecated-directive rule-IDs are kept declared for output
	// stability, but they map to BLOCKER because parser.ParseModel
	// rejects them outright at run-time. validate must not contradict
	// the runtime contract by reporting them as WARN. (Originally WARN
	// was considered to allow staged migrations, but the parser does
	// not allow the model to compile at all, so this is a BLOCKER.)

	// INFO: validator can't verify without extern state.
	RuleDagExternalRef:            SeverityInfo,
	RuleBlueprintHelperWithoutAPI: SeverityInfo,

	// WARN: validate's own environment is degraded — findings downstream
	// may be incomplete, but the report itself is still actionable.
	RuleValidateExtensionsLoadFailed:     SeverityWarn,
	RuleValidateBlueprintLoadFailed:      SeverityWarn,
	RuleValidateBuiltinIntrospectionFail: SeverityWarn,
	RuleValidateWalkSkipped:              SeverityWarn,
	RuleValidateLineageExtractFailed:     SeverityWarn,
}

// SeverityFor returns the severity for a rule ID. Default is BLOCKER —
// only IDs registered in severityOverrides escape that default.
func SeverityFor(id RuleID) Severity {
	if s, ok := severityOverrides[id]; ok {
		return s
	}
	return SeverityBlocker
}

// NewFinding constructs a Finding with the correct severity for the given
// rule ID. Always prefer this over zero-valued Finding{}-literals so the
// severity stays in sync with severityOverrides.
func NewFinding(path string, line int, id RuleID, msg string) Finding {
	return Finding{
		Path:     path,
		Line:     line,
		Severity: SeverityFor(id),
		Rule:     id,
		Message:  msg,
	}
}
