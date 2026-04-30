// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package odata

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/CiscoM31/godata"
)

// qualifiedTableSQL returns the FROM-clause fragment for an entity's
// underlying table, optionally pinned to a DuckLake snapshot.
//
// snapshot == 0 → no pinning; the query reads "current" state. Used by
// callers that don't need cross-statement consistency (e.g. tests, or
// older handlers being migrated). snapshot > 0 → injects
// `AT (VERSION => N)` so every projection in the request reads from the
// same snapshot, eliminating skew between data, $count, $expand, and
// @odata.deltaLink within one request.
//
// `@expose` is only allowed on materialized SQL models (parser/model.go),
// and those models are always materialized into the DuckLake catalog. So
// AT VERSION is always valid against any entity that reaches this layer.
func qualifiedTableSQL(schema, table string, snapshot int64) string {
	q := fmt.Sprintf("%s.%s", quoteIdent(schema), quoteIdent(table))
	if snapshot > 0 {
		q += fmt.Sprintf(" AT (VERSION => %d)", snapshot)
	}
	return q
}

// BuildQuery translates OData query parameters into a DuckDB SQL query.
//
// snapshot pins every base-table reference to a specific DuckLake version.
// Pass 0 to disable pinning (for tests; production callers should always
// pin to ensure intra-request consistency).
func BuildQuery(entity EntitySchema, params url.Values, snapshot int64) (string, error) {
	ctx := context.Background()
	validCols := validColumns(entity)

	// $compute — parse first to extend validCols for $select/$orderby
	computeClause := ""
	computeAliases := map[string]bool{} // track which cols are compute aliases
	if compute := params.Get("$compute"); compute != "" && params.Get("$apply") == "" {
		parts := splitComputeExpressions(compute)
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			asIdx := strings.LastIndex(strings.ToLower(part), " as ")
			if asIdx < 0 {
				return "", fmt.Errorf("invalid $compute: missing 'as alias' in %q", part)
			}
			expr := strings.TrimSpace(part[:asIdx])
			alias := strings.TrimSpace(part[asIdx+4:])
			if err := validateComputeAlias(alias); err != nil {
				return "", fmt.Errorf("invalid $compute alias: %w", err)
			}
			sqlExpr, err := parseComputeExpr(expr, validCols)
			if err != nil {
				return "", fmt.Errorf("invalid $compute expression %q: %w", expr, err)
			}
			// Reject alias that collides with existing column or prior compute alias
			if validCols[alias] {
				return "", fmt.Errorf("$compute alias %q conflicts with existing column", alias)
			}
			computeClause += fmt.Sprintf(", %s AS %s", sqlExpr, quoteIdent(alias))
			validCols[alias] = true
			computeAliases[alias] = true
		}
	}

	// SELECT (skip validation if $apply present — apply has its own output schema)
	selectClause := "*"
	if sel := params.Get("$select"); sel != "" && params.Get("$apply") == "" {
		selectQuery, err := godata.ParseSelectString(ctx, sel)
		if err != nil {
			return "", fmt.Errorf("invalid $select: %w", err)
		}
		var cols []string
		for _, item := range selectQuery.SelectItems {
			name := item.Segments[0].Value
			if !validCols[name] {
				return "", fmt.Errorf("unknown column in $select: %s", name)
			}
			cols = append(cols, quoteIdent(name))
		}
		selectClause = strings.Join(cols, ", ")
	}
	// Compute aliases in $select: since we use subquery, $select just needs the alias name
	// No filtering needed — subquery exposes all aliases, outer SELECT projects what's needed

	// FROM — snapshot-pinned when caller provides one (server-handler does;
	// tests may pass 0 to skip pinning).
	fromClause := qualifiedTableSQL(entity.Schema, entity.Table, snapshot)

	// WHERE
	whereClause := ""
	if filter := params.Get("$filter"); filter != "" {
		filterQuery, err := godata.ParseFilterString(ctx, filter)
		if err != nil {
			return "", fmt.Errorf("invalid $filter: %w", err)
		}
		sql, err := filterNodeToSQL(filterQuery.Tree, validCols)
		if err != nil {
			return "", err
		}
		whereClause = " WHERE " + sql
	}

	// $search — full-text search across all VARCHAR columns
	if search := params.Get("$search"); search != "" {
		searchVal := strings.ReplaceAll(search, "'", "''")
		searchVal = strings.ReplaceAll(searchVal, "%", "\\%")
		searchVal = strings.ReplaceAll(searchVal, "_", "\\_")
		var searchConds []string
		for _, col := range entity.Columns {
			if col.EdmType == "Edm.String" {
				searchConds = append(searchConds, fmt.Sprintf("%s ILIKE '%%%s%%'", quoteIdent(col.Name), searchVal))
			}
		}
		if len(searchConds) > 0 {
			searchSQL := "(" + strings.Join(searchConds, " OR ") + ")"
			if whereClause == "" {
				whereClause = " WHERE " + searchSQL
			} else {
				whereClause += " AND " + searchSQL
			}
		}
	}

	// ORDER BY (skip if $apply is present — handled in $apply block)
	orderByClause := ""
	if orderby := params.Get("$orderby"); orderby != "" && params.Get("$apply") == "" {
		orderbyQuery, err := godata.ParseOrderByString(ctx, orderby)
		if err != nil {
			return "", fmt.Errorf("invalid $orderby: %w", err)
		}
		var parts []string
		for _, item := range orderbyQuery.OrderByItems {
			name := item.Field.Value
			if !validCols[name] {
				return "", fmt.Errorf("unknown column in $orderby: %s", name)
			}
			dir := "ASC"
			if item.Order == godata.DESC {
				dir = "DESC"
			}
			parts = append(parts, quoteIdent(name)+" "+dir)
		}
		orderByClause = " ORDER BY " + strings.Join(parts, ", ")
	}

	// LIMIT
	limitClause := ""
	if top := params.Get("$top"); top != "" {
		topQuery, err := godata.ParseTopString(ctx, top)
		if err != nil {
			return "", fmt.Errorf("invalid $top: %w", err)
		}
		limitClause = fmt.Sprintf(" LIMIT %d", int(*topQuery))
	}

	// OFFSET
	offsetClause := ""
	if skip := params.Get("$skip"); skip != "" {
		skipQuery, err := godata.ParseSkipString(ctx, skip)
		if err != nil {
			return "", fmt.Errorf("invalid $skip: %w", err)
		}
		offsetClause = fmt.Sprintf(" OFFSET %d", int(*skipQuery))
	}

	// $apply — aggregation (groupby, aggregate)
	if apply := params.Get("$apply"); apply != "" {
		// If $compute is present, parse computed columns and extend validCols
		applyValidCols := validCols
		applyWhere := whereClause
		computeExtra := "" // extra SELECT columns for compute subquery wrapping

		if compute := params.Get("$compute"); compute != "" {
			applyValidCols = make(map[string]bool, len(validCols))
			for k, v := range validCols {
				applyValidCols[k] = v
			}
			parts := splitComputeExpressions(compute)
			for _, part := range parts {
				part = strings.TrimSpace(part)
				if part == "" {
					continue
				}
				asIdx := strings.LastIndex(strings.ToLower(part), " as ")
				if asIdx < 0 {
					return "", fmt.Errorf("invalid $compute: missing 'as alias' in %q", part)
				}
				expr := strings.TrimSpace(part[:asIdx])
				alias := strings.TrimSpace(part[asIdx+4:])
				if err := validateComputeAlias(alias); err != nil {
					return "", fmt.Errorf("invalid $compute alias: %w", err)
				}
				sqlExpr, err := parseComputeExpr(expr, validCols)
				if err != nil {
					return "", fmt.Errorf("invalid $compute expression %q: %w", expr, err)
				}
				if applyValidCols[alias] {
					return "", fmt.Errorf("$compute alias %q conflicts with existing column", alias)
				}
				computeExtra += fmt.Sprintf(", %s AS %s", sqlExpr, quoteIdent(alias))
				applyValidCols[alias] = true
			}
		}

		// Build apply query; if $compute exists, wrap FROM as subquery with computed cols
		var applySQL string
		var applyCols []string
		var err error
		if computeExtra != "" {
			// Create a virtual entity backed by a compute subquery. The
			// inner SELECT is snapshot-pinned via qualifiedTableSQL so the
			// $compute aggregation reads from the same DuckLake version as
			// data / $count / $expand. The wrapped entity has Schema=""
			// (signaling pre-built subquery to buildApplyQuery), so the
			// snapshot arg there is irrelevant.
			computeSubquery := fmt.Sprintf("(SELECT *%s FROM %s%s) AS _computed",
				computeExtra,
				qualifiedTableSQL(entity.Schema, entity.Table, snapshot),
				applyWhere)
			computeEntity := entity
			computeEntity.Schema = ""
			computeEntity.Table = computeSubquery
			applySQL, applyCols, err = buildApplyQuery(computeEntity, apply, applyValidCols, "", snapshot)
		} else {
			applySQL, applyCols, err = buildApplyQuery(entity, apply, applyValidCols, applyWhere, snapshot)
		}
		if err != nil {
			return "", fmt.Errorf("invalid $apply: %w", err)
		}
		// $apply replaces the normal SELECT; $orderby validated against apply output.
		applyOrderBy := ""
		applyColSet := make(map[string]bool)
		for _, c := range applyCols {
			applyColSet[c] = true
		}
		if orderby := params.Get("$orderby"); orderby != "" {
			parts := strings.Split(orderby, ",")
			var obParts []string
			for _, p := range parts {
				p = strings.TrimSpace(p)
				tokens := strings.Fields(p)
				if len(tokens) == 0 {
					continue
				}
				if len(tokens) > 2 {
					return "", fmt.Errorf("invalid $orderby: too many tokens in %q", p)
				}
				col := tokens[0]
				// Validate column against apply output (if known)
				if len(applyColSet) > 0 && !applyColSet[col] {
					return "", fmt.Errorf("unknown column in $orderby after $apply: %s (available: %v)", col, applyCols)
				}
				dir := "ASC"
				if len(tokens) == 2 {
					switch strings.ToUpper(tokens[1]) {
					case "ASC":
						dir = "ASC"
					case "DESC":
						dir = "DESC"
					default:
						return "", fmt.Errorf("invalid $orderby direction: %q (use asc or desc)", tokens[1])
					}
				}
				obParts = append(obParts, quoteIdent(col)+" "+dir)
			}
			if len(obParts) > 0 {
				applyOrderBy = " ORDER BY " + strings.Join(obParts, ", ")
			}
		}
		// $select after $apply — validate against apply output columns
		applySelect := "*"
		if sel := params.Get("$select"); sel != "" && len(applyCols) > 0 {
			applyColSet := make(map[string]bool)
			for _, c := range applyCols {
				applyColSet[c] = true
			}
			selectQuery, err := godata.ParseSelectString(ctx, sel)
			if err != nil {
				return "", fmt.Errorf("invalid $select: %w", err)
			}
			var cols []string
			for _, item := range selectQuery.SelectItems {
				name := item.Segments[0].Value
				if !applyColSet[name] {
					return "", fmt.Errorf("unknown column in $select after $apply: %s (available: %v)", name, applyCols)
				}
				cols = append(cols, quoteIdent(name))
			}
			applySelect = strings.Join(cols, ", ")
		}
		return fmt.Sprintf("SELECT %s FROM (%s) AS _apply%s%s%s",
			applySelect, applySQL, applyOrderBy, limitClause, offsetClause), nil
	}

	// If $compute is present, build as subquery so aliases are available to $filter/$orderby
	if computeClause != "" || len(computeAliases) > 0 {
		// Inner query: SELECT * + all compute aliases, with WHERE and ORDER BY
		// computeClause always has all compute expressions (not filtered by $select)
		innerCompute := ""
		for alias := range computeAliases {
			// Re-find expression from original parse
			for _, part := range splitComputeExpressions(params.Get("$compute")) {
				part = strings.TrimSpace(part)
				asIdx := strings.LastIndex(strings.ToLower(part), " as ")
				if asIdx >= 0 && strings.TrimSpace(part[asIdx+4:]) == alias {
					expr := strings.TrimSpace(part[:asIdx])
					sqlExpr, _ := parseComputeExpr(expr, validColumns(entity))
					innerCompute += fmt.Sprintf(", %s AS %s", sqlExpr, quoteIdent(alias))
					break
				}
			}
		}
		innerSQL := fmt.Sprintf("SELECT *%s FROM %s%s", innerCompute, fromClause, whereClause)
		return fmt.Sprintf("SELECT %s FROM (%s) AS _compute%s%s%s",
			selectClause, innerSQL, orderByClause, limitClause, offsetClause), nil
	}

	return fmt.Sprintf("SELECT %s FROM %s%s%s%s%s",
		selectClause, fromClause, whereClause, orderByClause, limitClause, offsetClause), nil
}

// BuildCountQuery builds a COUNT(*) query for the entity.
// Includes both $filter and $search to match the data query.
//
// snapshot pins the count to the same DuckLake version the caller used
// for the data query, so `value` row count and `@odata.count` agree.
// Pass 0 to skip pinning (tests).
func BuildCountQuery(entity EntitySchema, params url.Values, snapshot int64) (string, error) {
	ctx := context.Background()
	validCols := validColumns(entity)

	fromClause := qualifiedTableSQL(entity.Schema, entity.Table, snapshot)

	whereClause := ""
	if filter := params.Get("$filter"); filter != "" {
		filterQuery, err := godata.ParseFilterString(ctx, filter)
		if err != nil {
			return "", fmt.Errorf("invalid $filter: %w", err)
		}
		sql, err := filterNodeToSQL(filterQuery.Tree, validCols)
		if err != nil {
			return "", err
		}
		whereClause = " WHERE " + sql
	}

	// $search must match BuildQuery behavior
	if search := params.Get("$search"); search != "" {
		searchVal := strings.ReplaceAll(search, "'", "''")
		searchVal = strings.ReplaceAll(searchVal, "%", "\\%")
		searchVal = strings.ReplaceAll(searchVal, "_", "\\_")
		var searchConds []string
		for _, col := range entity.Columns {
			if col.EdmType == "Edm.String" {
				searchConds = append(searchConds, fmt.Sprintf("%s ILIKE '%%%s%%'", quoteIdent(col.Name), searchVal))
			}
		}
		if len(searchConds) > 0 {
			searchSQL := "(" + strings.Join(searchConds, " OR ") + ")"
			if whereClause == "" {
				whereClause = " WHERE " + searchSQL
			} else {
				whereClause += " AND " + searchSQL
			}
		}
	}

	return fmt.Sprintf("SELECT COUNT(*) FROM %s%s", fromClause, whereClause), nil
}

// BuildSingleEntityQuery builds a query to fetch a single entity by key.
//
// snapshot pins to a specific DuckLake version. Single-entity reads are
// usually independent enough not to require pinning, but threading it
// through keeps API consistency with the collection handler — a request
// that ends up calling both gets a consistent view.
func BuildSingleEntityQuery(entity EntitySchema, keyValue string, snapshot int64) (string, error) {
	fromClause := qualifiedTableSQL(entity.Schema, entity.Table, snapshot)

	// Determine key column type to decide quoting
	var keyType string
	for _, col := range entity.Columns {
		if col.Name == entity.KeyColumn {
			keyType = col.EdmType
			break
		}
	}

	var whereValue string
	if keyValue == "" {
		return "", fmt.Errorf("key value cannot be empty")
	}

	switch keyType {
	case "Edm.Int16", "Edm.Int32", "Edm.Int64", "Edm.Byte":
		// Integer — parse and normalize
		n, err := strconv.ParseInt(keyValue, 10, 64)
		if err != nil {
			return "", fmt.Errorf("invalid integer key value: %s", keyValue)
		}
		whereValue = strconv.FormatInt(n, 10)
	case "Edm.Double", "Edm.Single", "Edm.Decimal":
		// Float — parse and normalize
		f, err := strconv.ParseFloat(keyValue, 64)
		if err != nil {
			return "", fmt.Errorf("invalid numeric key value: %s", keyValue)
		}
		whereValue = strconv.FormatFloat(f, 'f', -1, 64)
	default:
		// String — strip surrounding quotes, decode OData escape, then SQL escape
		val := keyValue
		if len(val) >= 2 && val[0] == '\'' && val[len(val)-1] == '\'' {
			val = val[1 : len(val)-1]
		}
		// Decode OData escaped quotes: '' → '
		val = strings.ReplaceAll(val, "''", "'")
		// SQL escape: ' → ''
		val = strings.ReplaceAll(val, "'", "''")
		whereValue = "'" + val + "'"
	}

	return fmt.Sprintf("SELECT * FROM %s WHERE %s = %s LIMIT 1",
		fromClause, quoteIdent(entity.KeyColumn), whereValue), nil
}

// CrossJoinSelection captures which entity sets — and which columns
// within each — the client asked for via $select. It's returned by
// BuildCrossJoinQuery so the handler can prune nested objects in the
// response without re-parsing $select. Nil = no $select restriction
// (include every entity / every column / every compute alias).
type CrossJoinSelection struct {
	// Entities is the set of joined entity-set names the client kept.
	// nil → all joined entities are included.
	Entities map[string]bool
	// Cols[entity] is the set of columns kept within that entity.
	// nil for an entity → all columns of that entity (whole-entity
	// select form: `$select=Customers`).
	Cols map[string]map[string]bool
	// ComputeAliases is the set of $compute aliases the client kept.
	// nil → no $select set → include every compute alias.
	// Empty (non-nil) → $select set with no compute aliases listed →
	// drop every compute alias from the response.
	ComputeAliases map[string]bool
}

// IncludeEntity reports whether this entity-set should appear in the
// nested response. Empty Entities map → no restriction → include all.
func (s *CrossJoinSelection) IncludeEntity(name string) bool {
	if s == nil || s.Entities == nil {
		return true
	}
	return s.Entities[name]
}

// IncludeColumn reports whether this column of this entity should
// appear. Empty Cols[entity] → no per-column restriction → include all.
func (s *CrossJoinSelection) IncludeColumn(entity, col string) bool {
	if s == nil || s.Cols == nil {
		return true
	}
	cs := s.Cols[entity]
	if cs == nil {
		return true
	}
	return cs[col]
}

// IncludeComputeAlias reports whether this $compute alias should appear
// at the row top level. Nil ComputeAliases → no $select restriction →
// include every alias.
func (s *CrossJoinSelection) IncludeComputeAlias(name string) bool {
	if s == nil || s.ComputeAliases == nil {
		return true
	}
	return s.ComputeAliases[name]
}

// BuildCrossJoinQuery generates SQL for a $crossjoin path. Aliases each
// entity table to its OData entity-set name so $filter / $orderby can
// reference columns via `<entity>/<col>`. Each output column is aliased
// `<entity>__<col>` so the row decoder can split them back into nested
// objects per entity.
//
// Supports the full OData v4.01 query-option set:
//   - $filter with qualified refs (`A/x eq B/y`) and 3-segment
//     navigation paths (`A/Nav/x`) translated to EXISTS subqueries
//   - $orderby with qualified refs and bare $compute aliases
//   - $select: whole entity (`Customers`) or qualified subset
//     (`Customers/id,Customers/name`). Returns a CrossJoinSelection
//     so the handler can drop pruned entities from the nested response.
//   - $expand: bare form (`Customers,Orders`) is a no-op since we
//     already inline; deep form (`Customers/Orders`) is validated here
//     and the per-row sub-query runs in handleCrossJoin.
//   - $compute: top-level computed scalars referencing qualified refs
//   - $search: ILIKE-OR across every VARCHAR column of every entity
//   - $top / $skip (paging support is added by the handler via skiptoken)
//   - $count (handled separately by the caller)
//
// $apply (groupby/aggregate) is NOT handled here — its response shape
// forks from nested-per-entity to flat aggregated rows, so the handler
// routes those requests to BuildCrossJoinApplyQuery instead. Calling
// this function with $apply set returns an error.
func BuildCrossJoinQuery(entities []EntitySchema, entityMap map[string]EntitySchema, params url.Values, snapshot int64) (string, *CrossJoinSelection, error) {
	if len(entities) < 2 {
		return "", nil, fmt.Errorf("$crossjoin requires at least 2 entity sets, got %d", len(entities))
	}
	ctx := context.Background()

	// validCols: every (entity/col) is a valid reference target for
	// $filter and $orderby — those operate on the cross-product BEFORE
	// $select's projection trims columns. (SQL evaluates WHERE/ORDER BY
	// over the full row, then projects.)
	validCols := make(map[string]bool)
	seenAlias := make(map[string]bool, len(entities))
	for _, e := range entities {
		if seenAlias[e.ODataName] {
			return "", nil, fmt.Errorf("$crossjoin: duplicate entity %q", e.ODataName)
		}
		seenAlias[e.ODataName] = true
		for _, c := range e.Columns {
			validCols[e.ODataName+"/"+c.Name] = true
		}
	}

	// $compute — parsed first so its aliases are available to
	// $select / $filter / $orderby. Computed scalars surface at the
	// top level of each row (not nested under any entity), since they
	// don't belong to a single entity-set.
	var computeExtras []string
	computeAliases := make(map[string]bool)
	computeSQL := make(map[string]string) // alias → "expr AS alias"
	if compute := params.Get("$compute"); compute != "" {
		parts := splitComputeExpressions(compute)
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			asIdx := strings.LastIndex(strings.ToLower(part), " as ")
			if asIdx < 0 {
				return "", nil, fmt.Errorf("invalid $compute: missing 'as alias' in %q", part)
			}
			expr := strings.TrimSpace(part[:asIdx])
			alias := strings.TrimSpace(part[asIdx+4:])
			if err := validateComputeAlias(alias); err != nil {
				return "", nil, fmt.Errorf("invalid $compute alias: %w", err)
			}
			sqlExpr, err := parseComputeExpr(expr, validCols)
			if err != nil {
				return "", nil, fmt.Errorf("invalid $compute expression %q: %w", expr, err)
			}
			if validCols[alias] {
				return "", nil, fmt.Errorf("$compute alias %q conflicts with existing column", alias)
			}
			// Top-level computed col: aliased without any entity prefix
			// so the demuxer in handleCrossJoin sees it as a row-level
			// scalar rather than belonging to an entity sub-object.
			selectFrag := fmt.Sprintf("%s AS %s", sqlExpr, quoteIdent(alias))
			computeExtras = append(computeExtras, selectFrag)
			computeSQL[alias] = selectFrag
			validCols[alias] = true
			computeAliases[alias] = true
		}
	}

	// $select parsing → CrossJoinSelection. Three valid forms per
	// §5.1.3 + §4.15 + §5.1.3.1: whole entity (`Customers`), qualified
	// col (`Customers/id`), or bare $compute alias.
	var selection *CrossJoinSelection
	if selStr := params.Get("$select"); selStr != "" {
		sel, err := parseCrossJoinSelect(ctx, selStr, seenAlias, validCols, computeAliases)
		if err != nil {
			return "", nil, err
		}
		selection = sel
		// Filter the SELECT-list compute fragments to those the client
		// asked for. When $select is unset, ComputeAliases is nil and
		// every alias passes through unchanged.
		filtered := make([]string, 0, len(computeExtras))
		for alias, frag := range computeSQL {
			if selection.IncludeComputeAlias(alias) {
				filtered = append(filtered, frag)
			}
		}
		// Stable order: walk computeExtras (parse order) and keep the
		// fragments still in `filtered` (map iteration above is
		// undefined-order; we want deterministic SQL).
		stable := make([]string, 0, len(filtered))
		keep := make(map[string]bool, len(filtered))
		for _, f := range filtered {
			keep[f] = true
		}
		for _, f := range computeExtras {
			if keep[f] {
				stable = append(stable, f)
			}
		}
		computeExtras = stable
	}

	// $expand validation:
	//   - bare form (`Customers,Orders`): §4.15 explicitly allows; we
	//     already inline so the SQL is a no-op
	//   - deep form (`Customers/Orders`): expand a nav-property of one
	//     joined entity. The SQL is still unchanged — the post-query
	//     handler walks each row, resolves the nav-property's FK, and
	//     inlines the related entity into the source sub-object
	//   - 3+ segments: rejected (no spec support, no clear semantics)
	//
	// We do the validation here (not in the handler) so an invalid
	// $expand fails before the SQL even runs.
	if expand := params.Get("$expand"); expand != "" {
		entityByAlias := make(map[string]*EntitySchema, len(entities))
		for i := range entities {
			entityByAlias[entities[i].ODataName] = &entities[i]
		}
		for _, raw := range strings.Split(expand, ",") {
			name := strings.TrimSpace(raw)
			if name == "" {
				return "", nil, fmt.Errorf("invalid $expand: empty segment")
			}
			segs := strings.SplitN(name, "/", 3)
			if len(segs) > 2 {
				return "", nil, fmt.Errorf("$expand: paths with more than 2 segments (%q) are not supported on $crossjoin", name)
			}
			ent, ok := entityByAlias[segs[0]]
			if !ok {
				return "", nil, fmt.Errorf("$expand: %q is not one of the joined entity sets", segs[0])
			}
			if len(segs) == 2 {
				navName := segs[1]
				found := false
				for _, nav := range ent.NavProperties {
					if nav.Name == navName {
						found = true
						break
					}
				}
				if !found {
					return "", nil, fmt.Errorf("$expand: %q is not a navigation property of %s", navName, segs[0])
				}
			}
		}
	}

	// FROM A AS "A", B AS "B" — comma-separated cross product. Subquery
	// wrap so AT (VERSION => N) stays attached (DuckDB rejects the
	// flat `tbl AT (VERSION => N) AS alias` form).
	fromParts := make([]string, len(entities))
	for i, e := range entities {
		fromParts[i] = fmt.Sprintf("(SELECT * FROM %s) AS %s",
			qualifiedTableSQL(e.Schema, e.Table, snapshot),
			quoteIdent(e.ODataName))
	}

	// Output columns: filtered by $select if present. "A"."col" AS
	// "A__col" — double-underscore separator for the demuxer. If
	// $select excludes an entity entirely, we drop all its columns
	// here so the SQL doesn't carry unused payload.
	selectParts := make([]string, 0, 32)
	for _, e := range entities {
		if !selection.IncludeEntity(e.ODataName) {
			continue
		}
		for _, c := range e.Columns {
			if !selection.IncludeColumn(e.ODataName, c.Name) {
				continue
			}
			selectParts = append(selectParts, fmt.Sprintf("%s.%s AS %s",
				quoteIdent(e.ODataName), quoteIdent(c.Name),
				quoteIdent(e.ODataName+"__"+c.Name)))
		}
	}
	// Append $compute outputs as top-level columns. They sit alongside
	// the entity-prefixed aliases; the demuxer treats anything without
	// an `<entity>__` prefix as row-level (not nested).
	selectParts = append(selectParts, computeExtras...)

	if len(selectParts) == 0 {
		// $select pruned everything. Emit `SELECT 1 FROM …` so the row
		// count is still defined (e.g. for $count). The handler's
		// nesting loop produces empty `{}` per row — which matches the
		// spec model of "client asked for nothing".
		selectParts = append(selectParts, "1 AS _empty")
	}

	whereClause := ""
	if filter := params.Get("$filter"); filter != "" {
		filterQuery, err := godata.ParseFilterString(ctx, filter)
		if err != nil {
			return "", nil, fmt.Errorf("invalid $filter: %w", err)
		}
		// Use the nav-aware walker so 3-segment paths
		// (`Customers/Orders/amount gt 100`) can resolve via the
		// FK nav-property infra and produce EXISTS subqueries.
		entityByAlias := make(map[string]*EntitySchema, len(entities))
		for i := range entities {
			entityByAlias[entities[i].ODataName] = &entities[i]
		}
		nctx := &navFilterCtx{
			validCols:     validCols,
			entityByAlias: entityByAlias,
			entityMap:     entityMap,
			snapshot:      snapshot,
		}
		sqlExpr, err := nctx.toSQL(filterQuery.Tree)
		if err != nil {
			return "", nil, err
		}
		whereClause = " WHERE " + sqlExpr
	}

	// $search — full-text search across every VARCHAR column of every
	// joined entity, OR'd. Cross-join cardinality means each match in
	// either entity multiplies by the unrelated cardinality of the
	// other; that's the spec semantic ($search applies to the
	// collection that the query returns, which here IS the cross
	// product). Clients narrow with $filter to scope the result first.
	if search := params.Get("$search"); search != "" {
		searchVal := strings.ReplaceAll(search, "'", "''")
		searchVal = strings.ReplaceAll(searchVal, "%", "\\%")
		searchVal = strings.ReplaceAll(searchVal, "_", "\\_")
		var conds []string
		for _, e := range entities {
			for _, c := range e.Columns {
				if c.EdmType != "Edm.String" {
					continue
				}
				conds = append(conds, fmt.Sprintf("%s.%s ILIKE '%%%s%%'",
					quoteIdent(e.ODataName), quoteIdent(c.Name), searchVal))
			}
		}
		if len(conds) > 0 {
			searchSQL := "(" + strings.Join(conds, " OR ") + ")"
			if whereClause == "" {
				whereClause = " WHERE " + searchSQL
			} else {
				whereClause += " AND " + searchSQL
			}
		}
	}

	orderByClause := ""
	if orderby := params.Get("$orderby"); orderby != "" {
		obQuery, err := godata.ParseOrderByString(ctx, orderby)
		if err != nil {
			return "", nil, fmt.Errorf("invalid $orderby: %w", err)
		}
		parts := make([]string, 0, len(obQuery.OrderByItems))
		for _, item := range obQuery.OrderByItems {
			// godata stores qualified refs (`Customers/id`) as the raw
			// path string in Field.Value with Tree nil; bare names get
			// a parsed Tree.
			rawName := item.Field.Value
			dir := "ASC"
			if item.Order == godata.DESC {
				dir = "DESC"
			}
			if strings.Contains(rawName, "/") {
				// Qualified ref — must be 2-segment and resolve.
				segs := strings.SplitN(rawName, "/", 3)
				if len(segs) != 2 {
					return "", nil, fmt.Errorf("$orderby: deep navigation paths (%q) are not supported on $crossjoin", rawName)
				}
				if !validCols[rawName] {
					return "", nil, fmt.Errorf("$orderby: unknown qualified column %s", rawName)
				}
				parts = append(parts,
					quoteIdent(segs[0])+"."+quoteIdent(segs[1])+" "+dir)
				continue
			}
			// Bare name — accepted only if it matches a $compute alias
			// (top-level computed scalar). Bare ref to an entity column
			// would be ambiguous when both entity sets define it.
			if !validCols[rawName] {
				return "", nil, fmt.Errorf("$orderby on $crossjoin requires qualified refs (e.g. Customers/id) or a $compute alias, got bare column %q", rawName)
			}
			parts = append(parts, quoteIdent(rawName)+" "+dir)
		}
		orderByClause = " ORDER BY " + strings.Join(parts, ", ")
	}

	// $top / $skip — caller (handler) overrides these with the
	// effective server limit; we just pass them through.
	limitClause := ""
	if top := params.Get("$top"); top != "" {
		topQuery, err := godata.ParseTopString(ctx, top)
		if err != nil {
			return "", nil, fmt.Errorf("invalid $top: %w", err)
		}
		limitClause = fmt.Sprintf(" LIMIT %d", int(*topQuery))
	}
	offsetClause := ""
	if skip := params.Get("$skip"); skip != "" {
		skipQuery, err := godata.ParseSkipString(ctx, skip)
		if err != nil {
			return "", nil, fmt.Errorf("invalid $skip: %w", err)
		}
		offsetClause = fmt.Sprintf(" OFFSET %d", int(*skipQuery))
	}

	// $apply is not handled here — the handler routes to
	// BuildCrossJoinApplyQuery, which wraps the result of this function
	// as a subquery and applies groupby/aggregate on top. It uses a
	// different response shape (flat aggregated rows, not nested-per-
	// entity) so it gets its own code path.
	if params.Get("$apply") != "" {
		return "", nil, fmt.Errorf("$apply on $crossjoin must be routed to BuildCrossJoinApplyQuery, not BuildCrossJoinQuery")
	}

	return fmt.Sprintf("SELECT %s FROM %s%s%s%s%s",
		strings.Join(selectParts, ", "),
		strings.Join(fromParts, ", "),
		whereClause, orderByClause, limitClause, offsetClause), selection, nil
}

// BuildCrossJoinApplyQuery generates SQL for a $crossjoin with $apply
// (groupby / aggregate). The output shape forks from the nested-per-
// entity model that BuildCrossJoinQuery returns: $apply produces flat
// aggregated rows whose properties are the groupby columns and the
// aggregate aliases.
//
// Strategy: build the regular cross-join SQL ($filter/$search applied,
// no $top/$skip/$orderby/$select/$apply), wrap it as a subquery, and
// apply groupby/aggregate on top. Qualified refs in the $apply
// expression (`raw_customers/country`, `raw_orders/amount`) are
// rewritten to the inner SELECT's column aliases (`raw_customers__country`,
// `raw_orders__amount`) before parsing — so the existing single-entity
// $apply parser handles them transparently.
//
// Returns the SQL plus the list of output column names (groupby cols
// keep their aliased form `<entity>__<col>`; aggregate aliases keep
// the user's chosen alias).
func BuildCrossJoinApplyQuery(entities []EntitySchema, entityMap map[string]EntitySchema, params url.Values, snapshot int64) (string, []string, error) {
	apply := params.Get("$apply")
	if apply == "" {
		return "", nil, fmt.Errorf("$apply parameter required")
	}

	// Build the inner cross-join SQL with only options that scope rows.
	// $filter and $search apply BEFORE the aggregation; $top/$skip
	// apply AFTER (caller handles those by wrapping a second time).
	innerParams := url.Values{}
	for _, k := range []string{"$filter", "$search"} {
		if v := params.Get(k); v != "" {
			innerParams.Set(k, v)
		}
	}
	innerSQL, _, err := BuildCrossJoinQuery(entities, entityMap, innerParams, snapshot)
	if err != nil {
		return "", nil, fmt.Errorf("inner cross-join: %w", err)
	}

	// Synthetic entity wrapping the inner SQL. Its "table" IS the
	// subquery; Schema="" tells buildApplyQuery to treat Table as a
	// pre-built FROM expression rather than a `<schema>.<table>` name.
	syntheticCols := make([]ColumnSchema, 0, 32)
	validCols := make(map[string]bool)
	for _, e := range entities {
		for _, c := range e.Columns {
			alias := e.ODataName + "__" + c.Name
			syntheticCols = append(syntheticCols, ColumnSchema{
				Name:    alias,
				Type:    c.Type,
				EdmType: c.EdmType,
			})
			validCols[alias] = true
		}
	}
	syntheticEntity := EntitySchema{
		Schema:  "",
		Table:   "(" + innerSQL + ") AS _src",
		Columns: syntheticCols,
	}

	// Rewrite qualified refs `<entity>/<col>` → `<entity>__<col>` in
	// the $apply string so the existing single-entity parser, which
	// only knows bare names, sees the alias forms.
	rewritten := rewriteQualifiedRefs(apply, entities)

	sql, outputCols, err := buildApplyQuery(syntheticEntity, rewritten, validCols, "", snapshot)
	if err != nil {
		return "", nil, err
	}
	return sql, outputCols, nil
}

// rewriteQualifiedRefs replaces every `<entity>/<col>` outside string
// literals with `<entity>__<col>`, but only when `<entity>` is one of
// the supplied entity-set names. Single-quoted OData literals with `''`
// escaping are skipped so a value like `'see raw_customers/note'` is
// preserved verbatim.
func rewriteQualifiedRefs(s string, entities []EntitySchema) string {
	names := make(map[string]bool, len(entities))
	for _, e := range entities {
		names[e.ODataName] = true
	}
	var out strings.Builder
	out.Grow(len(s))
	i := 0
	for i < len(s) {
		c := s[i]
		// String literal: copy through verbatim, including '' escapes.
		if c == '\'' {
			out.WriteByte(c)
			i++
			for i < len(s) {
				if s[i] == '\'' {
					out.WriteByte('\'')
					i++
					if i < len(s) && s[i] == '\'' {
						out.WriteByte('\'')
						i++
						continue
					}
					break
				}
				out.WriteByte(s[i])
				i++
			}
			continue
		}
		// Identifier start: need word boundary before treating it as an
		// entity name. If the previous emitted char is also an ident
		// char, this is part of a longer identifier — copy as-is.
		if isIdentChar(c) {
			start := i
			for i < len(s) && isIdentChar(s[i]) {
				i++
			}
			word := s[start:i]
			if names[word] && i < len(s) && s[i] == '/' {
				colStart := i + 1
				j := colStart
				for j < len(s) && isIdentChar(s[j]) {
					j++
				}
				if j > colStart {
					out.WriteString(word)
					out.WriteString("__")
					out.WriteString(s[colStart:j])
					i = j
					continue
				}
			}
			out.WriteString(word)
			continue
		}
		out.WriteByte(c)
		i++
	}
	return out.String()
}

// isIdentChar reports whether c is a valid identifier character.
func isIdentChar(c byte) bool {
	return (c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') ||
		c == '_'
}

// parseCrossJoinSelect parses an OData $select string in $crossjoin
// context. Three valid forms:
//   - bare entity-set name (`Customers`) — keeps the whole entity
//   - qualified column (`Customers/id`) — keeps just that column
//   - bare $compute alias (`doubled`) — keeps that top-level scalar
//     (per OData v4.01 §5.1.3.1, computed properties are selectable)
//
// Mixing is allowed: `$select=Customers,Orders/amount,doubled` keeps
// all of Customers, just the amount of Orders, and the doubled compute
// scalar.
//
// If both `Customers` (whole) and `Customers/id` (partial) appear, the
// whole-entity wins — that's the only intent-preserving merge.
//
// Deep paths (3+ segments) and unknown names are rejected with
// 400-class errors so users see the contract violation up front.
//
// computeAliases is the set of $compute alias names (bare, no `/`); a
// 1-segment item that matches one is recorded as a compute selection
// rather than rejected as a non-entity name.
func parseCrossJoinSelect(ctx context.Context, selStr string, joinedEntities map[string]bool, validCols map[string]bool, computeAliases map[string]bool) (*CrossJoinSelection, error) {
	selQuery, err := godata.ParseSelectString(ctx, selStr)
	if err != nil {
		return nil, fmt.Errorf("invalid $select: %w", err)
	}

	// Two-pass: collect whole-entity selects and partial selects
	// separately, then merge with whole-entity winning on conflict.
	whole := make(map[string]bool)
	partial := make(map[string]map[string]bool)
	computes := make(map[string]bool)

	for _, item := range selQuery.SelectItems {
		if len(item.Segments) == 0 {
			return nil, fmt.Errorf("invalid $select: empty segment")
		}
		name := item.Segments[0].Value
		switch len(item.Segments) {
		case 1:
			if joinedEntities[name] {
				whole[name] = true
				continue
			}
			if computeAliases[name] {
				computes[name] = true
				continue
			}
			return nil, fmt.Errorf("$select: %q is not one of the joined entity sets or a $compute alias", name)
		case 2:
			if !joinedEntities[name] {
				return nil, fmt.Errorf("$select: %q is not one of the joined entity sets", name)
			}
			colName := item.Segments[1].Value
			qualified := name + "/" + colName
			if !validCols[qualified] {
				return nil, fmt.Errorf("$select: unknown column %s", qualified)
			}
			if partial[name] == nil {
				partial[name] = make(map[string]bool)
			}
			partial[name][colName] = true
		default:
			return nil, fmt.Errorf("$select: deep paths (%d segments) are not supported on $crossjoin", len(item.Segments))
		}
	}

	sel := &CrossJoinSelection{
		Entities:       make(map[string]bool),
		Cols:           make(map[string]map[string]bool),
		ComputeAliases: computes,
	}
	for e := range whole {
		sel.Entities[e] = true
	}
	for e, cols := range partial {
		sel.Entities[e] = true
		if !whole[e] {
			// Only carry the per-column restriction when there's no
			// whole-entity select for this entity. Otherwise the
			// whole-entity select wins.
			sel.Cols[e] = cols
		}
	}
	return sel, nil
}

// navFilterCtx wraps the standard filter walker with awareness of
// nav-property paths in $crossjoin context. 3-segment paths like
// `Customers/Orders/amount gt 100` translate to EXISTS subqueries via
// FK nav-property infrastructure.
//
// For nodes that don't involve 3-segment paths the walker delegates to
// the plain `filterNodeToSQL` — keeping the recursive logic for
// arithmetic, functions, lambdas, casts, etc. in one place.
type navFilterCtx struct {
	validCols     map[string]bool
	entityByAlias map[string]*EntitySchema   // joined entities only
	entityMap     map[string]EntitySchema    // ALL exposed entities (nav targets may not be joined)
	snapshot      int64
}

// toSQL is the entry point. Recurses through and/or/not nodes; for
// comparison ops, checks for 3-segment Nav children and emits EXISTS
// subqueries when found. Otherwise delegates to filterNodeToSQL.
func (n *navFilterCtx) toSQL(node *godata.ParseNode) (string, error) {
	if node == nil || node.Token == nil {
		return "", fmt.Errorf("nil filter node")
	}
	if node.Token.Type != godata.ExpressionTokenLogical {
		return filterNodeToSQL(node, n.validCols)
	}
	op := strings.ToLower(node.Token.Value)
	switch op {
	case "and", "or":
		if len(node.Children) != 2 {
			return "", fmt.Errorf("%s requires 2 operands", op)
		}
		left, err := n.toSQL(node.Children[0])
		if err != nil {
			return "", err
		}
		right, err := n.toSQL(node.Children[1])
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(%s %s %s)", left, strings.ToUpper(op), right), nil
	case "not":
		if len(node.Children) != 1 {
			return "", fmt.Errorf("not requires 1 operand")
		}
		child, err := n.toSQL(node.Children[0])
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("NOT (%s)", child), nil
	}
	// Comparison op: check for 3-segment Nav children.
	if isComparisonOp(op) && len(node.Children) == 2 {
		for idx := 0; idx < 2; idx++ {
			if isThreeSegmentNav(node.Children[idx]) {
				return n.buildNavExists(node, idx)
			}
		}
	}
	return filterNodeToSQL(node, n.validCols)
}

// isComparisonOp reports whether the op is a binary comparison that
// can take a nav-path as an operand (eq/ne/gt/ge/lt/le).
func isComparisonOp(op string) bool {
	switch op {
	case "eq", "ne", "gt", "ge", "lt", "le":
		return true
	}
	return false
}

// isThreeSegmentNav reports whether `node` is a Nav node with two Nav
// children — i.e. an `A/B/col` path that godata represents as nested
// Nav nodes (`Nav("/", [Nav("/", [A, B]), col])`).
func isThreeSegmentNav(node *godata.ParseNode) bool {
	if node == nil || node.Token == nil {
		return false
	}
	if node.Token.Type != godata.ExpressionTokenNav || node.Token.Value != "/" {
		return false
	}
	if len(node.Children) != 2 {
		return false
	}
	left := node.Children[0]
	if left == nil || left.Token == nil {
		return false
	}
	return left.Token.Type == godata.ExpressionTokenNav && left.Token.Value == "/"
}

// buildNavExists translates a comparison-op-with-3-segment-nav-child
// into an EXISTS subquery. Only handles the common case where one side
// is the 3-segment path and the other is a simple value (literal or
// column). Deep-nav-vs-deep-nav is rejected explicitly.
func (n *navFilterCtx) buildNavExists(opNode *godata.ParseNode, navChildIdx int) (string, error) {
	op := strings.ToLower(opNode.Token.Value)
	sqlOp := odataCmpOpToSQL(op)
	navNode := opNode.Children[navChildIdx]
	otherChild := opNode.Children[1-navChildIdx]

	// Refuse if the other side is also a 3-segment path. The EXISTS
	// pattern handles one path traversal cleanly; two would need a
	// JOIN inside the subquery and is rare enough to defer.
	if isThreeSegmentNav(otherChild) {
		return "", fmt.Errorf("$filter: comparing two nav paths (`A/B/x op C/D/y`) is not supported on $crossjoin")
	}

	// Extract A/B/col segments.
	innerNav := navNode.Children[0]
	entityName := innerNav.Children[0].Token.Value
	navName := innerNav.Children[1].Token.Value
	colName := navNode.Children[1].Token.Value

	// Validate entity is one of the joined ones.
	sourceEntity, ok := n.entityByAlias[entityName]
	if !ok {
		return "", fmt.Errorf("$filter: %q is not one of the joined entity sets", entityName)
	}
	// Find nav-property.
	var nav *NavigationProperty
	for i := range sourceEntity.NavProperties {
		if sourceEntity.NavProperties[i].Name == navName {
			nav = &sourceEntity.NavProperties[i]
			break
		}
	}
	if nav == nil {
		return "", fmt.Errorf("$filter: %q is not a navigation property of %s", navName, entityName)
	}
	// Find target entity (need its schema for the EXISTS subquery
	// SELECT). Looks up in entityMap, not just joined entities, since
	// nav targets often aren't part of the cross-join itself.
	target, ok := n.entityMap[nav.TargetEntity]
	if !ok {
		return "", fmt.Errorf("$filter: nav target %q is not exposed", nav.TargetEntity)
	}
	// Validate col exists on target.
	colOK := false
	for _, c := range target.Columns {
		if c.Name == colName {
			colOK = true
			break
		}
	}
	if !colOK {
		return "", fmt.Errorf("$filter: %q is not a column of %s", colName, target.ODataName)
	}

	// Translate the other operand. For literals it's straightforward;
	// for column refs of the source entity we want them outside the
	// EXISTS to correlate with the cross-join row.
	otherSQL, err := filterNodeToSQL(otherChild, n.validCols)
	if err != nil {
		return "", err
	}

	// Build EXISTS:
	//   EXISTS (SELECT 1 FROM <target> WHERE <target>.<targetCol> =
	//                 "<sourceAlias>"."<sourceCol>"
	//             AND <target>.<colName> <op> <otherSQL>)
	return fmt.Sprintf("EXISTS (SELECT 1 FROM %s WHERE %s = %s.%s AND %s %s %s)",
		qualifiedTableSQL(target.Schema, target.Table, n.snapshot),
		quoteIdent(nav.TargetColumn),
		quoteIdent(entityName), quoteIdent(nav.SourceColumn),
		quoteIdent(colName), sqlOp, otherSQL), nil
}

// odataCmpOpToSQL maps OData comparison operators to SQL.
func odataCmpOpToSQL(op string) string {
	switch op {
	case "eq":
		return "="
	case "ne":
		return "<>"
	case "gt":
		return ">"
	case "ge":
		return ">="
	case "lt":
		return "<"
	case "le":
		return "<="
	}
	return op
}

// filterNodeToSQL recursively translates a godata filter parse tree to SQL.
func filterNodeToSQL(node *godata.ParseNode, validCols map[string]bool) (string, error) {
	if node == nil || node.Token == nil {
		return "", fmt.Errorf("nil filter node")
	}

	t := node.Token.Type

	switch t {
	case godata.ExpressionTokenLogical:
		op := strings.ToLower(node.Token.Value)

		// Unary: not
		if op == "not" {
			if len(node.Children) != 1 {
				return "", fmt.Errorf("not operator requires 1 operand")
			}
			child, err := filterNodeToSQL(node.Children[0], validCols)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("NOT (%s)", child), nil
		}

		// In operator: name in ('Alice','Bob')
		if op == "in" {
			if len(node.Children) != 2 {
				return "", fmt.Errorf("in operator requires 2 operands")
			}
			left, err := filterNodeToSQL(node.Children[0], validCols)
			if err != nil {
				return "", err
			}
			listNode := node.Children[1]
			if listNode.Token != nil && listNode.Token.Type == godata.TokenTypeListExpr {
				var items []string
				for _, child := range listNode.Children {
					item, err := filterNodeToSQL(child, validCols)
					if err != nil {
						return "", err
					}
					items = append(items, item)
				}
				if len(items) == 0 {
					return "FALSE", nil
				}
				return fmt.Sprintf("(%s IN (%s))", left, strings.Join(items, ", ")), nil
			}
			return "", fmt.Errorf("in operator requires a list expression")
		}

		// Has operator: flags has 'value' → bitwise AND check
		if op == "has" {
			if len(node.Children) != 2 {
				return "", fmt.Errorf("has operator requires 2 operands")
			}
			left, err := filterNodeToSQL(node.Children[0], validCols)
			if err != nil {
				return "", err
			}
			right, err := filterNodeToSQL(node.Children[1], validCols)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("(%s & %s = %s)", left, right, right), nil
		}

		if len(node.Children) != 2 {
			return "", fmt.Errorf("operator %s requires 2 operands", op)
		}

		// Handle null comparisons: eq null → IS NULL, ne null → IS NOT NULL
		if (op == "eq" || op == "ne") && node.Children[1].Token != nil && node.Children[1].Token.Type == godata.ExpressionTokenNull {
			left, err := filterNodeToSQL(node.Children[0], validCols)
			if err != nil {
				return "", err
			}
			if op == "eq" {
				return fmt.Sprintf("(%s IS NULL)", left), nil
			}
			return fmt.Sprintf("(%s IS NOT NULL)", left), nil
		}

		left, err := filterNodeToSQL(node.Children[0], validCols)
		if err != nil {
			return "", err
		}
		right, err := filterNodeToSQL(node.Children[1], validCols)
		if err != nil {
			return "", err
		}
		sqlOp := odataOpToSQL(op)
		return fmt.Sprintf("(%s %s %s)", left, sqlOp, right), nil

	// Function calls: contains(), startswith(), year(), round(), etc.
	case godata.ExpressionTokenFunc:
		return funcToSQL(node, validCols)

	// Arithmetic operators: add, sub, mul, div, mod
	case godata.ExpressionTokenOp:
		op := strings.ToLower(node.Token.Value)
		if len(node.Children) != 2 {
			return "", fmt.Errorf("arithmetic operator %s requires 2 operands", op)
		}
		left, err := filterNodeToSQL(node.Children[0], validCols)
		if err != nil {
			return "", err
		}
		right, err := filterNodeToSQL(node.Children[1], validCols)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(%s %s %s)", left, odataArithToSQL(op), right), nil

	case godata.ExpressionTokenString:
		val := node.Token.Value
		if len(val) >= 2 && val[0] == '\'' && val[len(val)-1] == '\'' {
			val = val[1 : len(val)-1]
		}
		val = strings.ReplaceAll(val, "'", "''")
		return "'" + val + "'", nil

	case godata.ExpressionTokenInteger:
		if _, err := strconv.ParseInt(node.Token.Value, 10, 64); err != nil {
			return "", fmt.Errorf("invalid integer in $filter: %s", node.Token.Value)
		}
		return node.Token.Value, nil

	case godata.ExpressionTokenFloat:
		if _, err := strconv.ParseFloat(node.Token.Value, 64); err != nil {
			return "", fmt.Errorf("invalid float in $filter: %s", node.Token.Value)
		}
		return node.Token.Value, nil

	case godata.ExpressionTokenBoolean:
		if strings.ToLower(node.Token.Value) == "true" {
			return "TRUE", nil
		}
		return "FALSE", nil

	case godata.ExpressionTokenNull:
		return "NULL", nil

	case godata.ExpressionTokenLiteral, godata.ExpressionTokenNav:
		// Path expression `<entity>/<column>` — used by $crossjoin to
		// disambiguate which side of the cross product a column refers
		// to. godata represents these as a Nav node with Value="/" and
		// two child tokens. The validCols map carries the qualified key
		// `entity/col` in $crossjoin mode and bare column names in
		// single-entity mode; both modes route through the same code.
		if node.Token.Value == "/" && len(node.Children) == 2 &&
			node.Children[0].Token != nil && node.Children[1].Token != nil {
			// Reject deep navigation (3+ segments, e.g. `A/B/col`) up
			// front so users get a meaningful error instead of seeing
			// "unknown qualified column: /col" — the inner Nav comes
			// through as a child whose Token.Value is itself "/".
			if node.Children[0].Token.Value == "/" || node.Children[1].Token.Value == "/" {
				return "", fmt.Errorf("deep navigation paths (more than 2 segments) are not supported in $filter")
			}
			entityName := node.Children[0].Token.Value
			colName := node.Children[1].Token.Value
			qualified := entityName + "/" + colName
			if !validCols[qualified] {
				return "", fmt.Errorf("unknown qualified column in $filter: %s", qualified)
			}
			return quoteIdent(entityName) + "." + quoteIdent(colName), nil
		}
		// Single-segment column ref.
		name := node.Token.Value
		if !validCols[name] {
			return "", fmt.Errorf("unknown column in $filter: %s", name)
		}
		return quoteIdent(name), nil

	// Case expression: case(condition:value, condition:value, ...)
	case godata.ExpressionTokenCase:
		var parts []string
		for _, pair := range node.Children {
			if pair.Token != nil && pair.Token.Type == godata.ExpressionTokenCasePair && len(pair.Children) == 2 {
				cond, err := filterNodeToSQL(pair.Children[0], validCols)
				if err != nil {
					return "", err
				}
				val, err := filterNodeToSQL(pair.Children[1], validCols)
				if err != nil {
					return "", err
				}
				// Last pair with true condition becomes ELSE
				if cond == "TRUE" {
					parts = append(parts, fmt.Sprintf("ELSE %s", val))
				} else {
					parts = append(parts, fmt.Sprintf("WHEN %s THEN %s", cond, val))
				}
			}
		}
		return fmt.Sprintf("(CASE %s END)", strings.Join(parts, " ")), nil

	// Lambda: any()/all() — translate to EXISTS/NOT EXISTS
	case godata.ExpressionTokenLambda:
		fname := strings.ToLower(node.Token.Value)
		if fname != "any" && fname != "all" {
			return "", fmt.Errorf("unsupported lambda: %s", fname)
		}
		// Lambda nodes: children[0] = range variable, children[1] = predicate
		if len(node.Children) < 2 {
			// any() without predicate = "has any rows"
			return "TRUE", nil
		}
		// The predicate references columns via rangeVar/column — we resolve to just column
		predicate, err := filterNodeToSQL(node.Children[1], validCols)
		if err != nil {
			return "", err
		}
		if fname == "any" {
			return predicate, nil // simplified: filter on current entity
		}
		// all() = NOT (NOT predicate)
		return fmt.Sprintf("NOT (NOT (%s))", predicate), nil

	// Date/time/duration/guid literals — escape quotes to prevent SQL injection
	case godata.ExpressionTokenDate:
		return fmt.Sprintf("DATE '%s'", strings.ReplaceAll(node.Token.Value, "'", "''")), nil
	case godata.ExpressionTokenTime:
		return fmt.Sprintf("TIME '%s'", strings.ReplaceAll(node.Token.Value, "'", "''")), nil
	case godata.ExpressionTokenDateTime:
		return fmt.Sprintf("TIMESTAMP '%s'", strings.ReplaceAll(node.Token.Value, "'", "''")), nil
	case godata.ExpressionTokenDuration:
		return fmt.Sprintf("INTERVAL '%s'", strings.ReplaceAll(node.Token.Value, "'", "''")), nil

	default:
		return "", fmt.Errorf("unsupported filter token: %s (%d)", node.Token.Value, t)
	}
}

// funcToSQL translates an OData function call to DuckDB SQL.
func funcToSQL(node *godata.ParseNode, validCols map[string]bool) (string, error) {
	fname := strings.ToLower(node.Token.Value)

	// Resolve all arguments
	args := make([]string, len(node.Children))
	for i, child := range node.Children {
		s, err := filterNodeToSQL(child, validCols)
		if err != nil {
			return "", err
		}
		args[i] = s
	}

	switch fname {
	// String functions
	case "contains":
		if len(args) != 2 {
			return "", fmt.Errorf("contains() requires 2 arguments")
		}
		return fmt.Sprintf("(%s ILIKE '%%' || %s || '%%' ESCAPE '\\')", args[0], escapeLikeArg(args[1])), nil
	case "startswith":
		if len(args) != 2 {
			return "", fmt.Errorf("startswith() requires 2 arguments")
		}
		return fmt.Sprintf("(%s LIKE %s || '%%' ESCAPE '\\')", args[0], escapeLikeArg(args[1])), nil
	case "endswith":
		if len(args) != 2 {
			return "", fmt.Errorf("endswith() requires 2 arguments")
		}
		return fmt.Sprintf("(%s LIKE '%%' || %s ESCAPE '\\')", args[0], escapeLikeArg(args[1])), nil
	case "tolower":
		if len(args) != 1 {
			return "", fmt.Errorf("tolower() requires 1 argument")
		}
		return fmt.Sprintf("LOWER(%s)", args[0]), nil
	case "toupper":
		if len(args) != 1 {
			return "", fmt.Errorf("toupper() requires 1 argument")
		}
		return fmt.Sprintf("UPPER(%s)", args[0]), nil
	case "length":
		if len(args) != 1 {
			return "", fmt.Errorf("length() requires 1 argument")
		}
		return fmt.Sprintf("LENGTH(%s)", args[0]), nil
	case "trim":
		if len(args) != 1 {
			return "", fmt.Errorf("trim() requires 1 argument")
		}
		return fmt.Sprintf("TRIM(%s)", args[0]), nil
	case "concat":
		if len(args) < 2 {
			return "", fmt.Errorf("concat() requires at least 2 arguments")
		}
		return fmt.Sprintf("CONCAT(%s)", strings.Join(args, ", ")), nil
	case "indexof":
		if len(args) != 2 {
			return "", fmt.Errorf("indexof() requires 2 arguments")
		}
		// OData indexof is 0-based, DuckDB STRPOS is 1-based
		return fmt.Sprintf("(STRPOS(%s, %s) - 1)", args[0], args[1]), nil
	case "substring":
		if len(args) == 2 {
			// OData substring is 0-based, DuckDB SUBSTR is 1-based
			return fmt.Sprintf("SUBSTR(%s, (%s) + 1)", args[0], args[1]), nil
		}
		if len(args) == 3 {
			return fmt.Sprintf("SUBSTR(%s, (%s) + 1, %s)", args[0], args[1], args[2]), nil
		}
		return "", fmt.Errorf("substring() requires 2 or 3 arguments")

	// Date/time functions
	case "year", "month", "day", "hour", "minute", "second":
		if len(args) != 1 {
			return "", fmt.Errorf("%s() requires 1 argument", fname)
		}
		return fmt.Sprintf("EXTRACT(%s FROM %s)", strings.ToUpper(fname), args[0]), nil
	case "now":
		return "NOW()", nil
	case "date":
		if len(args) != 1 {
			return "", fmt.Errorf("date() requires 1 argument")
		}
		return fmt.Sprintf("CAST(%s AS DATE)", args[0]), nil
	case "time":
		if len(args) != 1 {
			return "", fmt.Errorf("time() requires 1 argument")
		}
		return fmt.Sprintf("CAST(%s AS TIME)", args[0]), nil

	// Math functions
	case "round":
		if len(args) != 1 {
			return "", fmt.Errorf("round() requires 1 argument")
		}
		return fmt.Sprintf("ROUND(%s)", args[0]), nil
	case "floor":
		if len(args) != 1 {
			return "", fmt.Errorf("floor() requires 1 argument")
		}
		return fmt.Sprintf("FLOOR(%s)", args[0]), nil
	case "ceiling":
		if len(args) != 1 {
			return "", fmt.Errorf("ceiling() requires 1 argument")
		}
		return fmt.Sprintf("CEIL(%s)", args[0]), nil

	// Legacy OData v3
	case "substringof":
		if len(args) != 2 {
			return "", fmt.Errorf("substringof() requires 2 arguments")
		}
		// substringof('val',col) → col ILIKE '%val%' (args reversed vs contains)
		return fmt.Sprintf("(%s ILIKE '%%' || %s || '%%')", args[1], args[0]), nil

	// Pattern matching — limit pattern length to prevent ReDoS
	case "matchespattern":
		if len(args) != 2 {
			return "", fmt.Errorf("matchesPattern() requires 2 arguments")
		}
		return fmt.Sprintf("CASE WHEN length(%s) > 1000 THEN false ELSE regexp_matches(%s, %s) END", args[1], args[0], args[1]), nil

	// Type functions
	case "cast":
		if len(args) != 2 {
			return "", fmt.Errorf("cast() requires 2 arguments")
		}
		return fmt.Sprintf("CAST(%s AS %s)", args[0], edmToSQL(args[1])), nil
	case "isof":
		if len(args) == 1 {
			return fmt.Sprintf("(typeof(%s) IS NOT NULL)", args[0]), nil
		}
		if len(args) == 2 {
			return fmt.Sprintf("(typeof(%s) = %s)", args[0], args[1]), nil
		}
		return "", fmt.Errorf("isof() requires 1 or 2 arguments")

	// Geo functions
	case "geo.distance":
		if len(args) != 2 {
			return "", fmt.Errorf("geo.distance() requires 2 arguments")
		}
		return fmt.Sprintf("ST_Distance(%s, %s)", args[0], args[1]), nil
	case "geo.intersects":
		if len(args) != 2 {
			return "", fmt.Errorf("geo.intersects() requires 2 arguments")
		}
		return fmt.Sprintf("ST_Intersects(%s, %s)", args[0], args[1]), nil
	case "geo.length":
		if len(args) != 1 {
			return "", fmt.Errorf("geo.length() requires 1 argument")
		}
		return fmt.Sprintf("ST_Length(%s)", args[0]), nil

	// Additional date functions
	case "fractionalseconds":
		if len(args) != 1 {
			return "", fmt.Errorf("fractionalseconds() requires 1 argument")
		}
		return fmt.Sprintf("(EXTRACT(MICROSECOND FROM %s) / 1000000.0)", args[0]), nil
	case "totaloffsetminutes":
		if len(args) != 1 {
			return "", fmt.Errorf("totaloffsetminutes() requires 1 argument")
		}
		return fmt.Sprintf("(EXTRACT(TIMEZONE FROM %s) / 60)", args[0]), nil
	case "totalseconds":
		if len(args) != 1 {
			return "", fmt.Errorf("totalseconds() requires 1 argument")
		}
		return fmt.Sprintf("EXTRACT(EPOCH FROM %s)", args[0]), nil
	case "maxdatetime":
		return "TIMESTAMP '9999-12-31T23:59:59Z'", nil
	case "mindatetime":
		return "TIMESTAMP '0001-01-01T00:00:00Z'", nil

	default:
		return "", fmt.Errorf("unsupported function: %s()", fname)
	}
}

// buildApplyQuery parses OData $apply and generates DuckDB SQL.
// Supports: aggregate(col with func as alias), groupby((col1,col2),aggregate(...))
// buildApplyQuery returns SQL + list of output column names for $orderby validation.
// whereClause includes any $filter/$search conditions from the caller.
//
// snapshot pins the underlying base-table read to a DuckLake version so
// $apply aggregations agree with the rest of the request (data, $count,
// $expand). Pass 0 to skip pinning. The entity.Schema=="" branch is used
// when the caller has already wrapped the table in a $compute subquery
// (whose own FROM is pinned by qualifiedTableSQL at the wrap site), so
// snapshot is irrelevant there.
func buildApplyQuery(entity EntitySchema, apply string, validCols map[string]bool, whereClause string, snapshot int64) (string, []string, error) {
	var fromClause string
	if entity.Schema == "" {
		fromClause = entity.Table // already a subquery like "(SELECT ...) AS _computed"
	} else {
		fromClause = qualifiedTableSQL(entity.Schema, entity.Table, snapshot)
	}
	apply = strings.TrimSpace(apply)

	// groupby((col1,col2),aggregate(col with func as alias))
	if strings.HasPrefix(apply, "groupby(") {
		if !strings.HasSuffix(apply, ")") {
			return "", nil, fmt.Errorf("groupby: missing closing parenthesis")
		}
		inner := apply[len("groupby(") : len(apply)-1] // strip groupby( ... )

		// Find the group columns: ((col1,col2), ...)
		if !strings.HasPrefix(inner, "(") {
			return "", nil, fmt.Errorf("groupby requires column list in parentheses")
		}

		// Find matching closing paren for group columns
		depth := 0
		groupEnd := -1
		for i, c := range inner {
			if c == '(' {
				depth++
			} else if c == ')' {
				depth--
				if depth == 0 {
					groupEnd = i
					break
				}
			}
		}
		if groupEnd < 0 {
			return "", nil, fmt.Errorf("unmatched parentheses in groupby")
		}

		groupCols := strings.TrimSpace(inner[1:groupEnd]) // strip outer parens
		if groupCols == "" {
			return "", nil, fmt.Errorf("groupby: column list cannot be empty")
		}
		var groupByParts []string
		for _, col := range strings.Split(groupCols, ",") {
			col = strings.TrimSpace(col)
			if col == "" {
				continue
			}
			if !validCols[col] {
				return "", nil, fmt.Errorf("unknown column in groupby: %s", col)
			}
			groupByParts = append(groupByParts, quoteIdent(col))
		}

		// Parse aggregate part after the group columns
		rest := strings.TrimSpace(inner[groupEnd+1:])
		rest = strings.TrimPrefix(rest, ",")
		rest = strings.TrimSpace(rest)

		var aggParts []string
		if rest == "" {
			// groupby without aggregate — just GROUP BY
		} else if strings.HasPrefix(rest, "aggregate(") {
			if !strings.HasSuffix(rest, ")") || len(rest) <= len("aggregate()") {
				return "", nil, fmt.Errorf("aggregate: missing closing parenthesis")
			}
			aggInner := rest[len("aggregate(") : len(rest)-1]
			// Pass groupby column names as used to detect alias collisions
			groupByUsed := make(map[string]bool)
			for _, col := range strings.Split(groupCols, ",") {
				groupByUsed[strings.TrimSpace(col)] = true
			}
			aggs, err := parseAggregateExpressions(aggInner, validCols, groupByUsed)
			if err != nil {
				return "", nil, err
			}
			aggParts = aggs
		} else {
			return "", nil, fmt.Errorf("unsupported transformation after groupby: %s", rest)
		}

		selectParts := append(groupByParts, aggParts...)
		outputCols := extractOutputCols(selectParts)
		return fmt.Sprintf("SELECT %s FROM %s%s GROUP BY %s",
			strings.Join(selectParts, ", "), fromClause, whereClause, strings.Join(groupByParts, ", ")), outputCols, nil
	}

	// aggregate(col with func as alias, ...)
	if strings.HasPrefix(apply, "aggregate(") {
		if !strings.HasSuffix(apply, ")") {
			return "", nil, fmt.Errorf("aggregate: missing closing parenthesis")
		}
		aggInner := strings.TrimSpace(apply[len("aggregate(") : len(apply)-1])
		if aggInner == "" {
			return "", nil, fmt.Errorf("aggregate: expression cannot be empty")
		}
		aggs, err := parseAggregateExpressions(aggInner, validCols)
		if err != nil {
			return "", nil, err
		}
		if len(aggs) == 0 {
			return "", nil, fmt.Errorf("aggregate: no valid expressions")
		}
		outputCols := extractOutputCols(aggs)
		return fmt.Sprintf("SELECT %s FROM %s%s", strings.Join(aggs, ", "), fromClause, whereClause), outputCols, nil
	}

	// filter(expr) — apply filter transformation
	if strings.HasPrefix(apply, "filter(") {
		if !strings.HasSuffix(apply, ")") {
			return "", nil, fmt.Errorf("filter: missing closing parenthesis")
		}
		filterExpr := apply[len("filter(") : len(apply)-1]
		ctx := context.Background()
		filterQuery, err := godata.ParseFilterString(ctx, filterExpr)
		if err != nil {
			return "", nil, fmt.Errorf("invalid filter in $apply: %w", err)
		}
		sql, err := filterNodeToSQL(filterQuery.Tree, validCols)
		if err != nil {
			return "", nil, err
		}
		// Combine $filter/search WHERE with $apply filter()
		combinedWhere := "WHERE " + sql
		if whereClause != "" {
			combinedWhere = whereClause + " AND " + sql
		}
		// Return entity columns as output cols (filter preserves schema)
		var entityCols []string
		for _, c := range entity.Columns {
			entityCols = append(entityCols, c.Name)
		}
		return fmt.Sprintf("SELECT * FROM %s %s", fromClause, combinedWhere), entityCols, nil
	}

	return "", nil, fmt.Errorf("unsupported $apply expression: %s", apply)
}

// parseAggregateExpressions parses "col with func as alias, ..."
// usedNames tracks groupby columns + prior aliases to detect collisions.
func parseAggregateExpressions(expr string, validCols map[string]bool, usedNames ...map[string]bool) ([]string, error) {
	used := map[string]bool{}
	if len(usedNames) > 0 && usedNames[0] != nil {
		used = usedNames[0]
	}
	var result []string
	for _, part := range strings.Split(expr, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		// Format: col with func as alias OR $count as alias
		tokens := strings.Fields(part)
		if len(tokens) == 3 && tokens[0] == "$count" && strings.ToLower(tokens[1]) == "as" {
			alias := tokens[2]
			if used[alias] {
				return nil, fmt.Errorf("duplicate aggregate alias: %s", alias)
			}
			used[alias] = true
			result = append(result, fmt.Sprintf("COUNT(*) AS %s", quoteIdent(alias)))
			continue
		}
		if len(tokens) != 5 || strings.ToLower(tokens[1]) != "with" || strings.ToLower(tokens[3]) != "as" {
			return nil, fmt.Errorf("invalid aggregate expression: %s (expected: col with func as alias)", part)
		}
		col, fn, alias := tokens[0], strings.ToLower(tokens[2]), tokens[4]
		if !validCols[col] {
			return nil, fmt.Errorf("unknown column in aggregate: %s", col)
		}
		if used[alias] {
			return nil, fmt.Errorf("duplicate aggregate alias: %s", alias)
		}
		used[alias] = true
		var sqlFunc string
		switch fn {
		case "sum":
			sqlFunc = fmt.Sprintf("SUM(%s)", quoteIdent(col))
		case "avg", "average":
			sqlFunc = fmt.Sprintf("AVG(%s)", quoteIdent(col))
		case "min":
			sqlFunc = fmt.Sprintf("MIN(%s)", quoteIdent(col))
		case "max":
			sqlFunc = fmt.Sprintf("MAX(%s)", quoteIdent(col))
		case "count", "countdistinct":
			if fn == "countdistinct" {
				sqlFunc = fmt.Sprintf("COUNT(DISTINCT %s)", quoteIdent(col))
			} else {
				sqlFunc = fmt.Sprintf("COUNT(%s)", quoteIdent(col))
			}
		default:
			return nil, fmt.Errorf("unsupported aggregate function: %s", fn)
		}
		result = append(result, fmt.Sprintf("%s AS %s", sqlFunc, quoteIdent(alias)))
	}
	return result, nil
}

// edmToSQL converts an OData EDM type name to a DuckDB SQL type for CAST.
func edmToSQL(edmType string) string {
	// Strip quotes if present
	t := strings.Trim(edmType, "'\"")
	t = strings.TrimPrefix(t, "Edm.")
	switch strings.ToLower(t) {
	case "string":
		return "VARCHAR"
	case "int16":
		return "SMALLINT"
	case "int32":
		return "INTEGER"
	case "int64":
		return "BIGINT"
	case "single":
		return "FLOAT"
	case "double":
		return "DOUBLE"
	case "decimal":
		return "DECIMAL"
	case "boolean":
		return "BOOLEAN"
	case "date":
		return "DATE"
	case "datetimeoffset":
		return "TIMESTAMPTZ"
	}
	return "VARCHAR"
}

// parseComputeExpr parses a safe compute expression: "col op literal" or "func(col)".
func parseComputeExpr(expr string, validCols map[string]bool) (string, error) {
	tokens := strings.Fields(expr)

	// Single column
	if len(tokens) == 1 {
		return resolveComputeCol(tokens[0], validCols)
	}

	// col op literal (e.g. "amount mul 2", or "raw_orders/amount mul 2")
	if len(tokens) == 3 {
		col, op, lit := tokens[0], strings.ToLower(tokens[1]), tokens[2]
		sqlCol, err := resolveComputeCol(col, validCols)
		if err != nil {
			return "", err
		}
		sqlOp := odataArithToSQL(op)
		if sqlOp == op {
			return "", fmt.Errorf("unsupported operator: %s", op)
		}
		// Validate literal is a single numeric value (parse and normalize)
		if _, err := strconv.ParseFloat(lit, 64); err != nil {
			return "", fmt.Errorf("invalid numeric literal: %s", lit)
		}
		return fmt.Sprintf("(%s %s %s)", sqlCol, sqlOp, lit), nil
	}

	return "", fmt.Errorf("unsupported expression format (use: col op literal)")
}

// resolveComputeCol turns a column reference into its SQL form, handling
// both bare names (`amount`) and qualified $crossjoin paths
// (`raw_orders/amount`). Qualified detection is `/` in the name; OData
// column names don't contain `/`, so this is unambiguous.
func resolveComputeCol(name string, validCols map[string]bool) (string, error) {
	if !validCols[name] {
		return "", fmt.Errorf("unknown column: %s", name)
	}
	if idx := strings.Index(name, "/"); idx >= 0 {
		segs := strings.SplitN(name, "/", 3)
		if len(segs) != 2 {
			return "", fmt.Errorf("deep navigation paths (%q) are not supported in $compute", name)
		}
		return quoteIdent(segs[0]) + "." + quoteIdent(segs[1]), nil
	}
	return quoteIdent(name), nil
}

// splitComputeExpressions splits $compute value on commas, respecting parentheses.
func splitComputeExpressions(s string) []string {
	var parts []string
	depth := 0
	start := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, s[start:])
	return parts
}

// validateComputeAlias checks that a compute alias is a safe SQL identifier.
// Rejects the `__` substring because the cross-join demuxer uses it as the
// entity-prefix separator (`<entity>__<col>`) — an alias containing `__`
// would be misrouted into an entity sub-object instead of staying at the
// row top level.
func validateComputeAlias(alias string) error {
	if alias == "" {
		return fmt.Errorf("alias cannot be empty")
	}
	for _, c := range alias {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
			return fmt.Errorf("invalid alias %q: only letters, numbers, and underscores", alias)
		}
	}
	if strings.Contains(alias, "__") {
		return fmt.Errorf("invalid alias %q: double underscore is reserved for the $crossjoin demuxer separator", alias)
	}
	return nil
}

// extractOutputCols extracts column names/aliases from SQL select parts.
// Input: ['"customer"', 'SUM("amount") AS "total"'] → ["customer", "total"]
func extractOutputCols(parts []string) []string {
	var cols []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		// Check for AS alias
		if idx := strings.LastIndex(strings.ToUpper(p), " AS "); idx >= 0 {
			alias := strings.TrimSpace(p[idx+4:])
			alias = strings.Trim(alias, `"`)
			cols = append(cols, alias)
		} else {
			// Bare column — strip quotes
			cols = append(cols, strings.Trim(p, `"`))
		}
	}
	return cols
}

func odataArithToSQL(op string) string {
	switch op {
	case "add":
		return "+"
	case "sub":
		return "-"
	case "mul":
		return "*"
	case "div", "divby":
		return "/"
	case "mod":
		return "%"
	}
	return op
}

func odataOpToSQL(op string) string {
	switch op {
	case "eq":
		return "="
	case "ne":
		return "!="
	case "gt":
		return ">"
	case "ge":
		return ">="
	case "lt":
		return "<"
	case "le":
		return "<="
	case "and":
		return "AND"
	case "or":
		return "OR"
	}
	return strings.ToUpper(op)
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// escapeLikeArg escapes LIKE/ILIKE wildcards (% and _) inside a SQL string literal.
// Input is a SQL-quoted string like 'foo%bar'. Returns 'foo\%bar'.
// Column references (quoted identifiers) pass through unchanged.
func escapeLikeArg(sqlArg string) string {
	if len(sqlArg) < 2 || sqlArg[0] != '\'' || sqlArg[len(sqlArg)-1] != '\'' {
		return sqlArg // not a string literal (e.g. column reference) — pass through
	}
	inner := sqlArg[1 : len(sqlArg)-1]
	inner = strings.ReplaceAll(inner, `\`, `\\`)
	inner = strings.ReplaceAll(inner, `%`, `\%`)
	inner = strings.ReplaceAll(inner, `_`, `\_`)
	return "'" + inner + "'"
}

func validColumns(entity EntitySchema) map[string]bool {
	m := make(map[string]bool, len(entity.Columns))
	for _, c := range entity.Columns {
		m[c.Name] = true
	}
	return m
}
