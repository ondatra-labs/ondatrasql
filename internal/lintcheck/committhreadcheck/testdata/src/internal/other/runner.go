// Test fixture: NOT named materialize.go, so the analyzer must ignore it even
// though it contains the otherwise-flagged direct commit.sql call (file-suffix
// scoping).
package other

type sqlPkg struct{}

func (sqlPkg) MustFormat(name string, args ...any) string { return name }

var sql sqlPkg

func buildTxn(mainSQL, auditSQL, target, info string) string {
	return sql.MustFormat("execute/commit.sql", mainSQL, auditSQL, target, info)
}
