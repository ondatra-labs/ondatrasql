// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package parser

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// starlarkIdentRe matches valid Starlark/Python identifiers.
var starlarkIdentRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// starlarkKeywords are reserved words in Starlark that cannot be used as identifiers.
var starlarkKeywords = map[string]bool{
	"and": true, "break": true, "continue": true, "def": true, "elif": true,
	"else": true, "for": true, "if": true, "in": true, "lambda": true,
	"load": true, "not": true, "or": true, "pass": true, "return": true,
	// Also block Python builtins used in Starlark
	"None": true, "True": true, "False": true,
}

// validateStarlarkIdent checks that a string is a valid Starlark identifier
// and not a reserved keyword.
func validateStarlarkIdent(s string) error {
	if !starlarkIdentRe.MatchString(s) {
		return fmt.Errorf("must be a valid identifier (letters, digits, underscores; cannot start with digit)")
	}
	if starlarkKeywords[s] {
		return fmt.Errorf("%q is a reserved keyword", s)
	}
	return nil
}

// yamlModel represents the YAML structure for a declarative model file.
type yamlModel struct {
	Kind               string         `yaml:"kind"`
	Source             string         `yaml:"source"`
	Config             map[string]any `yaml:"config"`
	UniqueKey          string         `yaml:"unique_key"`
	PartitionedBy      []string       `yaml:"partitioned_by"`
	SortedBy           []string       `yaml:"sorted_by"`
	Incremental        string         `yaml:"incremental"`
	IncrementalInitial string         `yaml:"incremental_initial"`
	Description        string         `yaml:"description"`
	Extensions         []string       `yaml:"extensions"`
	Constraints        []string       `yaml:"constraints"`
	Audits             []string       `yaml:"audits"`
	Warnings           []string       `yaml:"warnings"`
	Expose             any            `yaml:"expose"`
}

// parseYAMLModel parses a YAML model file into a Model struct.
// Environment variables in the YAML content are expanded via ${VAR} syntax.
func parseYAMLModel(path, projectDir string) (*Model, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read yaml model: %w", err)
	}

	// Expand environment variables in YAML content
	expanded := os.ExpandEnv(string(data))

	var ym yamlModel
	if err := yaml.Unmarshal([]byte(expanded), &ym); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}

	if ym.Source == "" {
		return nil, fmt.Errorf("yaml model requires 'source' field")
	}

	// Validate source is a valid Starlark identifier
	if err := validateStarlarkIdent(ym.Source); err != nil {
		return nil, fmt.Errorf("invalid source %q: %w", ym.Source, err)
	}

	// Validate config keys are valid Starlark identifiers (used as kwargs)
	for key := range ym.Config {
		if err := validateStarlarkIdent(key); err != nil {
			return nil, fmt.Errorf("invalid config key %q: %w", key, err)
		}
	}

	// Reject kinds that are incompatible with YAML source models
	kind := ym.Kind
	if kind == "" {
		kind = "table"
	}
	if kind == "view" {
		return nil, fmt.Errorf("kind 'view' is not supported for YAML models (views are SQL-only)")
	}
	if kind == "events" {
		return nil, fmt.Errorf("kind 'events' is not supported for YAML models (events use column definitions, not source functions)")
	}

	// Build the Model struct
	// SQL is set to the expanded YAML content as a canonical hash source.
	// This ensures that changes to source/config are detected by backfill/run_type logic,
	// which hashes model.SQL. The actual Starlark wrapper is generated at execution time.
	// Parse expose field: bool (true/false) or string (key column name)
	var expose bool
	var exposeKey string
	switch v := ym.Expose.(type) {
	case bool:
		expose = v
	case string:
		expose = true
		exposeKey = v
	case nil:
		// not set — ok
	default:
		return nil, fmt.Errorf("invalid expose value: expected true or column name string, got %T", v)
	}

	model := &Model{
		Kind:               ym.Kind,
		SQL:                expanded,
		Source:             ym.Source,
		SourceConfig:       ym.Config,
		IsScript:           true,
		ScriptType:         ScriptTypeStarlark,
		UniqueKey:          ym.UniqueKey,
		PartitionedBy:      ym.PartitionedBy,
		SortedBy:           ym.SortedBy,
		Incremental:        ym.Incremental,
		IncrementalInitial: ym.IncrementalInitial,
		Description:        ym.Description,
		Extensions:         ym.Extensions,
		Constraints:        ym.Constraints,
		Audits:             ym.Audits,
		Warnings:           ym.Warnings,
		Expose:             expose,
		ExposeKey:          exposeKey,
	}

	if model.Kind == "" {
		model.Kind = "table" // default
	}

	// Calculate target from path
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("get absolute path: %w", err)
	}

	// Calculate relative file path
	relFilePath := absPath
	if projectDir != "" {
		if rel, err := filepath.Rel(projectDir, absPath); err == nil && !strings.HasPrefix(rel, "..") {
			relFilePath = rel
		}
	}
	model.FilePath = relFilePath

	target, err := targetFromPath(absPath, projectDir)
	if err != nil {
		return nil, err
	}
	model.Target = target

	// Validate
	if err := validateModel(model); err != nil {
		return nil, err
	}

	return model, nil
}

// targetFromPath computes the schema.table target name from a file path.
// Validates path components (no __ or dots) — same rules as the SQL parser.
func targetFromPath(absPath, projectDir string) (string, error) {
	ext := filepath.Ext(absPath)
	modelsDir := filepath.Join(projectDir, "models")
	relPath, err := filepath.Rel(modelsDir, absPath)
	if err != nil || strings.HasPrefix(relPath, "..") {
		// File is outside models/ — use tmp schema
		name := strings.TrimSuffix(filepath.Base(absPath), ext)
		name = strings.ReplaceAll(name, ".", "_")
		return "tmp." + name, nil
	}

	relPath = strings.TrimSuffix(relPath, ext)
	parts := strings.Split(relPath, string(filepath.Separator))

	// Validate path components (same rules as SQL parser)
	for _, part := range parts {
		if err := ValidatePathSegment(part); err != nil {
			return "", fmt.Errorf("invalid model path: %w", err)
		}
		if strings.Contains(part, "__") {
			return "", fmt.Errorf("path component %q contains reserved pattern '__' (used as folder separator in table names)", part)
		}
	}

	var schema, table string
	switch len(parts) {
	case 1:
		schema = "main"
		table = parts[0]
	default:
		schema = parts[0]
		table = strings.Join(parts[1:], "__")
	}
	return schema + "." + table, nil
}
