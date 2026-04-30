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
		if !validCols[tokens[0]] {
			return "", fmt.Errorf("unknown column: %s", tokens[0])
		}
		return quoteIdent(tokens[0]), nil
	}

	// col op literal (e.g. "amount mul 2")
	if len(tokens) == 3 {
		col, op, lit := tokens[0], strings.ToLower(tokens[1]), tokens[2]
		if !validCols[col] {
			return "", fmt.Errorf("unknown column: %s", col)
		}
		sqlOp := odataArithToSQL(op)
		if sqlOp == op {
			return "", fmt.Errorf("unsupported operator: %s", op)
		}
		// Validate literal is a single numeric value (parse and normalize)
		if _, err := strconv.ParseFloat(lit, 64); err != nil {
			return "", fmt.Errorf("invalid numeric literal: %s", lit)
		}
		return fmt.Sprintf("(%s %s %s)", quoteIdent(col), sqlOp, lit), nil
	}

	return "", fmt.Errorf("unsupported expression format (use: col op literal)")
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
func validateComputeAlias(alias string) error {
	if alias == "" {
		return fmt.Errorf("alias cannot be empty")
	}
	for _, c := range alias {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
			return fmt.Errorf("invalid alias %q: only letters, numbers, and underscores", alias)
		}
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
