// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package odata

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/CiscoM31/godata"
)

// BuildQuery translates OData query parameters into a DuckDB SQL query.
func BuildQuery(entity EntitySchema, params url.Values) (string, error) {
	ctx := context.Background()
	validCols := validColumns(entity)

	// SELECT
	selectClause := "*"
	if sel := params.Get("$select"); sel != "" {
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

	// FROM
	fromClause := fmt.Sprintf("%s.%s", quoteIdent(entity.Schema), quoteIdent(entity.Table))

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

	// ORDER BY
	orderByClause := ""
	if orderby := params.Get("$orderby"); orderby != "" {
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

	return fmt.Sprintf("SELECT %s FROM %s%s%s%s%s",
		selectClause, fromClause, whereClause, orderByClause, limitClause, offsetClause), nil
}

// BuildCountQuery builds a COUNT(*) query for the entity.
func BuildCountQuery(entity EntitySchema, params url.Values) (string, error) {
	ctx := context.Background()
	validCols := validColumns(entity)

	fromClause := fmt.Sprintf("%s.%s", quoteIdent(entity.Schema), quoteIdent(entity.Table))

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

	return fmt.Sprintf("SELECT COUNT(*) FROM %s%s", fromClause, whereClause), nil
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

	case godata.ExpressionTokenString:
		val := node.Token.Value
		if len(val) >= 2 && val[0] == '\'' && val[len(val)-1] == '\'' {
			val = val[1 : len(val)-1]
		}
		val = strings.ReplaceAll(val, "'", "''")
		return "'" + val + "'", nil

	case godata.ExpressionTokenInteger, godata.ExpressionTokenFloat:
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

	default:
		return "", fmt.Errorf("unsupported filter token: %s (%d)", node.Token.Value, t)
	}
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

func validColumns(entity EntitySchema) map[string]bool {
	m := make(map[string]bool, len(entity.Columns))
	for _, c := range entity.Columns {
		m[c.Name] = true
	}
	return m
}
