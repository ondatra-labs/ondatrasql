// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package dag builds and sorts a directed acyclic graph of model dependencies.
package dag

import (
	"crypto/rand"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/lineage"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
)

// GenerateRunID generates a unique identifier for a DAG run.
// Format: YYYYMMDD-HHMMSS-RANDOM (e.g., 20250610-143052-a1b2c3)
func GenerateRunID() string {
	timestamp := time.Now().UTC().Format("20060102-150405")
	randomBytes := make([]byte, 3)
	rand.Read(randomBytes)
	return fmt.Sprintf("%s-%x", timestamp, randomBytes)
}

// Node represents a model in the dependency graph.
type Node struct {
	Model *parser.Model
	Deps  []string // Target names this model depends on
}

// Graph represents the dependency graph of all models.
type Graph struct {
	nodes      map[string]*Node
	sess       *duckdb.Session // Optional session for AST-based extraction
	projectDir string          // Project root for resolving lib/ scripts
}

// modelInfo holds fields needed for dependency extraction from a parser.Model.
type modelInfo struct {
	sql      string
	source   string
	isScript bool
}

// NewGraph creates a new graph with a DuckDB session for AST-based dependency extraction.
// An optional projectDir enables Starlark dependency extraction for YAML models.
func NewGraph(sess *duckdb.Session, projectDir ...string) *Graph {
	g := &Graph{
		nodes: make(map[string]*Node),
		sess:  sess,
	}
	if len(projectDir) > 0 {
		g.projectDir = projectDir[0]
	}
	return g
}

// Add adds a model to the graph.
func (g *Graph) Add(model *parser.Model) {
	var deps []string

	if model.IsScript {
		// Script model: extract deps from query() calls in Starlark code
		info := &modelInfo{
			sql:      model.SQL,
			source:   model.Source,
			isScript: true,
		}
		deps = g.extractStarlarkDeps(info)
	} else {
		// SQL model: extract deps using DuckDB AST parser
		deps = g.extractDeps(model.SQL)
	}

	// Filter to only include qualified table names (schema.table)
	var validDeps []string
	for _, dep := range deps {
		if strings.Contains(dep, ".") {
			validDeps = append(validDeps, dep)
		}
	}

	g.nodes[model.Target] = &Node{
		Model: model,
		Deps:  validDeps,
	}
}

// extractDeps uses DuckDB's AST parser to extract table dependencies.
// This correctly handles quoted identifiers, CTEs, and complex JOIN syntax.
func (g *Graph) extractDeps(sql string) []string {
	if g.sess == nil {
		return nil // No session, no deps
	}
	tables, err := lineage.ExtractTables(g.sess, sql)
	if err != nil {
		return nil // Parse error, no deps
	}

	var deps []string
	seen := make(map[string]bool)
	for _, t := range tables {
		// TableRef.Table already contains the full name (schema.table)
		name := t.Table
		if !seen[name] && !isSystemTable(name) {
			seen[name] = true
			deps = append(deps, name)
		}
	}
	return deps
}

// Sort returns models in topological order (dependencies first).
// Returns an error if there's a circular dependency.
func (g *Graph) Sort() ([]*parser.Model, error) {
	// Filter deps to only include known models
	for _, node := range g.nodes {
		var validDeps []string
		for _, dep := range node.Deps {
			if _, exists := g.nodes[dep]; exists {
				validDeps = append(validDeps, dep)
			}
		}
		node.Deps = validDeps
	}

	var result []*parser.Model
	visited := make(map[string]bool)
	inStack := make(map[string]bool)

	var visit func(target string) error
	visit = func(target string) error {
		if inStack[target] {
			return fmt.Errorf("circular dependency detected: %s", target)
		}
		if visited[target] {
			return nil
		}

		inStack[target] = true
		node := g.nodes[target]

		for _, dep := range node.Deps {
			if err := visit(dep); err != nil {
				return err
			}
		}

		inStack[target] = false
		visited[target] = true
		result = append(result, node.Model)
		return nil
	}

	// Visit all nodes in deterministic order (sorted by target name)
	// This ensures consistent execution order across runs
	targets := make([]string, 0, len(g.nodes))
	for target := range g.nodes {
		targets = append(targets, target)
	}
	sort.Strings(targets)

	for _, target := range targets {
		if err := visit(target); err != nil {
			return nil, err
		}
	}

	return result, nil
}

// Dependents returns a reverse-dependency map: target → list of downstream targets.
// For each model, lists all models that directly depend on it.
func (g *Graph) Dependents() map[string][]string {
	result := make(map[string][]string)
	for target, node := range g.nodes {
		for _, dep := range node.Deps {
			result[dep] = append(result[dep], target)
		}
	}
	return result
}

// isSystemTable checks if a table name is a system/temp table.
func isSystemTable(name string) bool {
	lower := strings.ToLower(name)
	return strings.HasPrefix(lower, "tmp_") ||
		strings.HasPrefix(lower, "temp_") ||
		strings.HasPrefix(lower, "information_schema") ||
		strings.HasPrefix(lower, "pg_") ||
		lower == "dual"
}
