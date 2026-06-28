// Test fixture: must be named materialize.go to match the analyzer's
// file-suffix scoping (internal/execute/materialize.go).
package execute

// sqlPkg stands in for internal/sql (the MustFormat provider).
type sqlPkg struct{}

func (sqlPkg) MustFormat(name string, args ...any) string { return name }

var sql sqlPkg

// commitTxnSQL is the centralized wrapper — its direct commit.sql call is the
// one legitimate site and must NOT be flagged.
func commitTxnSQL(writeSQL, auditSQL, target, extraInfo string, extraPreSQL []string) string {
	for _, extra := range extraPreSQL {
		writeSQL = extra + ";\n" + writeSQL
	}
	return sql.MustFormat("execute/commit.sql", writeSQL, auditSQL, target, extraInfo)
}

// materializeGood routes through the helper — no diagnostic.
func materializeGood(mainSQL, auditSQL, target, info string, extra []string) string {
	return commitTxnSQL(mainSQL, auditSQL, target, info, extra)
}

// materializeBad builds the transaction directly — must be flagged.
func materializeBad(mainSQL, auditSQL, target, info string) string {
	return sql.MustFormat("execute/commit.sql", mainSQL, auditSQL, target, info) // want `build the write transaction via commitTxnSQL`
}

// materializeOtherTemplate uses a different template — not the commit txn, so
// it is not the analyzer's concern.
func materializeOtherTemplate(target, tmp string) string {
	return sql.MustFormat("execute/table.sql", target, tmp)
}

// materializeBypassed has a reason-bearing marker — silenced.
func materializeBypassed(mainSQL, auditSQL, target, info string) string {
	//committhreadcheck:allow legacy path, threads extraPreSQL via bespoke preParts
	return sql.MustFormat("execute/commit.sql", mainSQL, auditSQL, target, info)
}

// materializeBareBypass has the marker but NO reason — must still fire.
func materializeBareBypass(mainSQL, auditSQL, target, info string) string {
	//committhreadcheck:allow
	return sql.MustFormat("execute/commit.sql", mainSQL, auditSQL, target, info) // want `build the write transaction via commitTxnSQL`
}

// materializeMidComment cites the marker mid-sentence — must still fire.
func materializeMidComment(mainSQL, auditSQL, target, info string) string {
	// see also //committhreadcheck:allow elsewhere
	return sql.MustFormat("execute/commit.sql", mainSQL, auditSQL, target, info) // want `build the write transaction via commitTxnSQL`
}
