// Test fixture: must be named materialize.go to match the analyzer's
// file-suffix scoping (internal/execute/materialize.go).
package execute

import "fmt"

// Runner stands in for the real *execute.Runner.
type Runner struct{}

func (r *Runner) addMissingSyntheticColumnsSQL(target string) (string, error) {
	return "", nil
}

// --- Violations ---

// materializeSCD2Bad emits the synthetic columns inline in the INSERT but never
// ensures the target has them — the kind-conversion gap.
func (r *Runner) materializeSCD2Bad(target, colList string, snap int) string {
	return fmt.Sprintf("TRUNCATE %s;\nINSERT INTO %s BY NAME SELECT %s, %d::BIGINT AS valid_from_snapshot, true AS is_current FROM tmp", target, target, colList, snap) // want `must call addMissingSyntheticColumnsSQL`
}

// materializeTrackedBad references _content_hash in a different statement than
// the BY NAME insert — the diagnostic still fires (function-level), reported at
// the BY NAME literal.
func (r *Runner) materializeTrackedBad(target string) error {
	hashSQL := "SELECT k, sum(hash(row(v)))::VARCHAR AS _content_hash FROM tmp GROUP BY k"
	_ = hashSQL
	mainSQL := "TRUNCATE x;\nINSERT INTO y BY NAME SELECT * FROM tracked_hashed" // want `must call addMissingSyntheticColumnsSQL`
	_ = mainSQL
	return nil
}

// --- Passing cases ---

// materializeSCD2Good calls the ensure helper before the INSERT — no diagnostic.
func (r *Runner) materializeSCD2Good(target, colList string, snap int) (string, error) {
	synth, err := r.addMissingSyntheticColumnsSQL(target)
	if err != nil {
		return "", err
	}
	_ = synth
	return fmt.Sprintf("TRUNCATE %s;\nINSERT INTO %s BY NAME SELECT %s, %d::BIGINT AS valid_from_snapshot FROM tmp", target, target, colList, snap), nil
}

// materializeTrackedGood references _content_hash and calls the ensure helper.
func (r *Runner) materializeTrackedGood(target string) error {
	hashSQL := "SELECT k, sum(hash(row(v)))::VARCHAR AS _content_hash FROM tmp GROUP BY k"
	_ = hashSQL
	if _, err := r.addMissingSyntheticColumnsSQL(target); err != nil {
		return err
	}
	mainSQL := "TRUNCATE x;\nINSERT INTO y BY NAME SELECT * FROM tracked_hashed"
	_ = mainSQL
	return nil
}

// materializeTable is the kind-agnostic dispatcher path: INSERT … BY NAME but
// no synthetic column — table/append/merge need no ensure step.
func (r *Runner) materializeTable(target string) string {
	return fmt.Sprintf("TRUNCATE %s;\nINSERT INTO %s BY NAME SELECT * FROM tmp", target, target)
}

// --- Bypass marker ---

// materializeBypassed legitimately mentions is_current next to an unrelated
// insert; a reason-bearing marker silences the rule.
//
//syntheticcolcheck:allow legacy path kept for migration, target always pre-evolved
func (r *Runner) materializeBypassed(target string) string {
	_ = "WHERE is_current"
	return fmt.Sprintf("INSERT INTO %s BY NAME SELECT * FROM tmp", target)
}

// materializeBareBypass has the marker but NO reason — must still fire.
//
//syntheticcolcheck:allow
func (r *Runner) materializeBareBypass(target string) string {
	_ = "valid_from_snapshot"
	return fmt.Sprintf("INSERT INTO %s BY NAME SELECT * FROM tmp", target) // want `must call addMissingSyntheticColumnsSQL`
}

// materializeMidComment cites the marker mid-sentence — must still fire.
//
// see also //syntheticcolcheck:allow elsewhere
func (r *Runner) materializeMidComment(target string) string {
	_ = "valid_from_snapshot"
	return fmt.Sprintf("INSERT INTO %s BY NAME SELECT * FROM tmp", target) // want `must call addMissingSyntheticColumnsSQL`
}
