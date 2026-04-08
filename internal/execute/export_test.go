// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import "encoding/json"

// Export unexported methods for testing.

// GetAST exposes getAST for property-based testing.
func (r *Runner) GetAST(sql string) (string, error) {
	return r.getAST(sql)
}

// ExtractLineage exposes extractLineage for property-based testing.
func (r *Runner) ExtractLineage(sql string) ([]string, []string, error) {
	colLineage, tableDeps, err := r.extractLineage(sql)
	// Convert column lineage to string slice for simpler test assertions
	var cols []string
	for _, cl := range colLineage {
		cols = append(cols, cl.Column)
	}
	return cols, tableDeps, err
}

// ApplySmartCDC exposes applySmartCDC for integration testing.
func (r *Runner) ApplySmartCDC(astJSON, kind string, cdcTables []string, snapshotID int64) (string, error) {
	return r.applySmartCDC(astJSON, kind, cdcTables, snapshotID)
}

// ApplyEmptySmartCDC exposes applyEmptySmartCDC for integration testing.
func (r *Runner) ApplyEmptySmartCDC(astJSON string, cdcTables []string) (string, error) {
	return r.applyEmptySmartCDC(astJSON, cdcTables)
}

// DeserializeAST exposes deserializeAST for integration testing.
func (r *Runner) DeserializeAST(astJSON string) (string, error) {
	return r.deserializeAST(astJSON)
}

// TableExistsInCatalog exposes tableExistsInCatalog for regression testing.
// Pinned by tests that verify lookup failures are surfaced as errors rather
// than silently collapsed to "table does not exist".
func (r *Runner) TableExistsInCatalog(table, catalog string) (bool, error) {
	return r.tableExistsInCatalog(table, catalog)
}

// QualifyAndDeserialize qualifies tables in AST and deserializes back to SQL.
func (r *Runner) QualifyAndDeserialize(astJSON string, tablesToQualify map[string]bool, catalog string) (string, error) {
	root, err := parseASTJSON(astJSON)
	if err != nil {
		return "", err
	}
	qualifyTablesInAST(root, tablesToQualify, catalog)
	modified, err := json.Marshal(root)
	if err != nil {
		return "", err
	}
	return r.deserializeAST(string(modified))
}
