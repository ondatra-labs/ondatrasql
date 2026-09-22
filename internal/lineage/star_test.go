// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package lineage

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
)

// A star used to become a single `?` column with no sources, which erased the
// lineage of every model written as `SELECT *` or `* EXCLUDE (...)`. These pin
// the expansion: CTEs and derived tables from their own select list, physical
// tables through the session's schema.

var (
	starTablesOnce sync.Once
	starTablesErr  error
)

// setupStarTables creates the fixtures once; every caller sees the same setup
// error, so a failed setup does not surface as unrelated missing-table noise.
func setupStarTables(t *testing.T) {
	t.Helper()
	starTablesOnce.Do(func() {
		for _, q := range []string{
			"CREATE SCHEMA IF NOT EXISTS star",
			"CREATE TABLE star.bolag (orgnr VARCHAR, namn VARCHAR, ort VARCHAR, secret VARCHAR)",
			"CREATE TABLE star.adress (orgnr VARCHAR, gata VARCHAR, ort VARCHAR)",
			// Same schema and table name in another catalog, different columns.
			"ATTACH ':memory:' AS starcat",
			"CREATE SCHEMA starcat.star",
			"CREATE TABLE starcat.star.bolag (id BIGINT, other VARCHAR)",
			// Same table name in two schemas.
			"CREATE SCHEMA IF NOT EXISTS star2",
			"CREATE TABLE star2.bolag (orgnr VARCHAR, bransch VARCHAR)",
		} {
			if err := shared.Exec(q); err != nil {
				starTablesErr = fmt.Errorf("setup %q: %w", q, err)
				return
			}
		}
	})
	if starTablesErr != nil {
		t.Fatal(starTablesErr)
	}
}

func extractStar(t *testing.T, sql string) []ColumnLineage {
	t.Helper()
	setupStarTables(t)
	got, err := Extract(shared, sql)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	return got
}

func columnNames(cols []ColumnLineage) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.Column
	}
	return out
}

func sourceOf(t *testing.T, cols []ColumnLineage, name string) []SourceColumn {
	t.Helper()
	for _, c := range cols {
		if c.Column == name {
			return c.Sources
		}
	}
	t.Fatalf("no output column %q in %v", name, columnNames(cols))
	return nil
}

func assertColumns(t *testing.T, cols []ColumnLineage, want ...string) {
	t.Helper()
	if got := columnNames(cols); !reflect.DeepEqual(got, want) {
		t.Fatalf("columns = %v, want %v", got, want)
	}
}

func assertSingleSource(t *testing.T, cols []ColumnLineage, name, table, column string, tr TransformationType) {
	t.Helper()
	src := sourceOf(t, cols, name)
	if len(src) != 1 || src[0].Table != table || src[0].Column != column || src[0].Transformation != tr {
		t.Errorf("%s sources = %+v, want %s.%s %s", name, src, table, column, tr)
	}
}

func TestStar_OverCTEExpandsFromItsSelectList(t *testing.T) {
	cols := extractStar(t, `
		WITH utplockat AS (
			SELECT orgnr::VARCHAR AS orgnr, upper(namn) AS namn FROM star.bolag
		)
		SELECT * FROM utplockat`)
	assertColumns(t, cols, "orgnr", "namn")
	assertSingleSource(t, cols, "orgnr", "star.bolag", "orgnr", TransformCast)
	assertSingleSource(t, cols, "namn", "star.bolag", "namn", TransformFunction)
}

func TestStar_OverCTEExpandsWithoutResolver(t *testing.T) {
	setupStarTables(t)
	ast, err := GetAST(shared, `WITH u AS (SELECT orgnr, namn FROM star.bolag) SELECT * FROM u`)
	if err != nil {
		t.Fatal(err)
	}
	cols, err := ExtractFromAST(ast)
	if err != nil {
		t.Fatal(err)
	}
	assertColumns(t, cols, "orgnr", "namn")
	assertSingleSource(t, cols, "orgnr", "star.bolag", "orgnr", TransformIdentity)
}

func TestStar_OverBaseTableUsesSessionSchema(t *testing.T) {
	cols := extractStar(t, `SELECT * FROM star.bolag`)
	assertColumns(t, cols, "orgnr", "namn", "ort", "secret")
	assertSingleSource(t, cols, "ort", "star.bolag", "ort", TransformIdentity)
}

func TestStar_OverBaseTableWithoutResolverStaysUnknown(t *testing.T) {
	setupStarTables(t)
	ast, err := GetAST(shared, `SELECT * FROM star.bolag`)
	if err != nil {
		t.Fatal(err)
	}
	cols, err := ExtractFromAST(ast)
	if err != nil {
		t.Fatal(err)
	}
	// A partial list would look complete, so an unexpandable star keeps the
	// single placeholder.
	assertColumns(t, cols, "?")
}

func TestStar_ExcludeReplaceRename(t *testing.T) {
	cols := extractStar(t, `
		SELECT * EXCLUDE (secret) REPLACE (lower(namn) AS namn) RENAME (ort AS stad)
		FROM star.bolag`)
	assertColumns(t, cols, "orgnr", "namn", "stad")
	assertSingleSource(t, cols, "namn", "star.bolag", "namn", TransformFunction)
	assertSingleSource(t, cols, "stad", "star.bolag", "ort", TransformIdentity)
}

func TestStar_QualifiedExcludeOnlyDropsThatRelation(t *testing.T) {
	cols := extractStar(t, `
		SELECT * EXCLUDE (a.ort)
		FROM star.bolag b JOIN star.adress a ON a.orgnr = b.orgnr`)
	assertColumns(t, cols, "orgnr", "namn", "ort", "secret", "orgnr", "gata")
	assertSingleSource(t, cols, "ort", "star.bolag", "ort", TransformIdentity)
}

func TestStar_RelationStarPicksOneSide(t *testing.T) {
	cols := extractStar(t, `
		SELECT b.orgnr, a.* FROM star.bolag b JOIN star.adress a ON a.orgnr = b.orgnr`)
	assertColumns(t, cols, "orgnr", "orgnr", "gata", "ort")
	assertSingleSource(t, cols, "gata", "star.adress", "gata", TransformIdentity)
}

func TestStar_UsingJoinEmitsSharedColumnOnce(t *testing.T) {
	cols := extractStar(t, `SELECT * FROM star.bolag JOIN star.adress USING (orgnr)`)
	assertColumns(t, cols, "orgnr", "namn", "ort", "secret", "gata", "ort")
}

func TestStar_NaturalJoinEmitsSharedColumnsOnce(t *testing.T) {
	cols := extractStar(t, `SELECT * FROM star.bolag NATURAL JOIN star.adress`)
	assertColumns(t, cols, "orgnr", "namn", "ort", "secret", "gata")
}

func TestStar_ColumnAliasListRenamesLeadingColumns(t *testing.T) {
	cols := extractStar(t, `SELECT * FROM star.bolag AS x(id, bolagsnamn)`)
	assertColumns(t, cols, "id", "bolagsnamn", "ort", "secret")
	assertSingleSource(t, cols, "id", "star.bolag", "orgnr", TransformIdentity)
}

func TestStar_OverDerivedTableAndNestedStar(t *testing.T) {
	cols := extractStar(t, `
		WITH alla AS (SELECT * EXCLUDE (secret) FROM star.bolag)
		SELECT * FROM (SELECT *, 1 AS n FROM alla) d`)
	assertColumns(t, cols, "orgnr", "namn", "ort", "n")
	assertSingleSource(t, cols, "ort", "star.bolag", "ort", TransformIdentity)
}

func TestStar_ColumnRefThroughStarCTEResolves(t *testing.T) {
	cols := extractStar(t, `
		WITH alla AS (SELECT * FROM star.bolag)
		SELECT namn AS bolag FROM alla`)
	assertSingleSource(t, cols, "bolag", "star.bolag", "namn", TransformIdentity)
}

func TestStar_OverTableFunctionStaysUnknown(t *testing.T) {
	cols := extractStar(t, `SELECT * FROM range(3)`)
	assertColumns(t, cols, "?")
}

func TestStar_UnexpandableCTEStarDoesNotLeakIntoJoin(t *testing.T) {
	cols := extractStar(t, `
		WITH x AS (SELECT * FROM range(3))
		SELECT * FROM x JOIN star.bolag ON true`)
	assertColumns(t, cols, "?")
}

func TestStar_UnexpandableCTEStarIsNotRenamedByAliasList(t *testing.T) {
	cols := extractStar(t, `
		WITH x AS (SELECT * FROM range(3))
		SELECT * FROM x AS y(a)`)
	assertColumns(t, cols, "?")
}

func TestStar_CatalogQualifiedTableUsesItsOwnCatalog(t *testing.T) {
	cols := extractStar(t, `SELECT * FROM starcat.star.bolag`)
	assertColumns(t, cols, "id", "other")
}

func TestStar_CTEWithItsOwnWithClause(t *testing.T) {
	cols := extractStar(t, `
		WITH yttre AS (
			WITH inre AS (SELECT orgnr, namn FROM star.bolag)
			SELECT * FROM inre
		)
		SELECT * FROM yttre`)
	assertColumns(t, cols, "orgnr", "namn")
	assertSingleSource(t, cols, "namn", "star.bolag", "namn", TransformIdentity)
}

func TestStar_ReplaceOverJoinKeepsOneColumn(t *testing.T) {
	// DuckDB replaces the first `ort` and drops the second.
	cols := extractStar(t, `
		SELECT * REPLACE (upper(a.ort) AS ort)
		FROM star.bolag b JOIN star.adress a ON a.orgnr = b.orgnr`)
	assertColumns(t, cols, "orgnr", "namn", "ort", "secret", "orgnr", "gata")
	assertSingleSource(t, cols, "ort", "star.adress", "ort", TransformFunction)
}

func TestStar_FullJoinUsingCarriesBothSides(t *testing.T) {
	cols := extractStar(t, `SELECT * FROM star.bolag FULL JOIN star.adress USING (orgnr)`)
	assertColumns(t, cols, "orgnr", "namn", "ort", "secret", "gata", "ort")
	src := sourceOf(t, cols, "orgnr")
	if len(src) != 2 || src[0].Table != "star.bolag" || src[1].Table != "star.adress" {
		t.Errorf("orgnr sources = %+v, want star.bolag and star.adress", src)
	}
}

func TestStar_InnerJoinUsingKeepsLeftSourceOnly(t *testing.T) {
	cols := extractStar(t, `SELECT * FROM star.bolag JOIN star.adress USING (orgnr)`)
	assertSingleSource(t, cols, "orgnr", "star.bolag", "orgnr", TransformIdentity)
}

func TestStar_CTEColumnAliasesRenameBodyColumns(t *testing.T) {
	cols := extractStar(t, `
		WITH c(id, bolagsnamn) AS (SELECT orgnr, namn, ort FROM star.bolag)
		SELECT *, c.bolagsnamn AS via_ref FROM c`)
	assertColumns(t, cols, "id", "bolagsnamn", "ort", "via_ref")
	assertSingleSource(t, cols, "id", "star.bolag", "orgnr", TransformIdentity)
	assertSingleSource(t, cols, "via_ref", "star.bolag", "namn", TransformIdentity)
}

func TestDerivedTableColumnAliasResolvesAggregate(t *testing.T) {
	// `d(total)` renames the subquery's `s`; without applying it, d.total found
	// nothing and the aggregation — which drives CDC — was lost.
	cols := extractStar(t, `
		SELECT d.total FROM (SELECT count(orgnr) AS s FROM star.bolag) d(total)`)
	src := sourceOf(t, cols, "total")
	if len(src) != 1 || src[0].Table != "star.bolag" || src[0].Transformation != TransformAggregation {
		t.Errorf("total sources = %+v, want an aggregation of star.bolag", src)
	}
}

func TestStar_QualifiedTableIsNotMistakenForDottedCTE(t *testing.T) {
	cols := extractStar(t, `
		WITH "star.bolag" AS (SELECT 1 AS fel)
		SELECT * FROM star.bolag`)
	assertColumns(t, cols, "orgnr", "namn", "ort", "secret")
}

func TestStar_RealColumnNamedQuestionMarkIsExpanded(t *testing.T) {
	cols := extractStar(t, `
		WITH q AS (SELECT orgnr AS "?" FROM star.bolag)
		SELECT * FROM q`)
	assertColumns(t, cols, "?")
	assertSingleSource(t, cols, "?", "star.bolag", "orgnr", TransformIdentity)
}

func TestStar_SchemaQualifiedExcludeOnlyDropsThatSchema(t *testing.T) {
	cols := extractStar(t, `
		SELECT * EXCLUDE (star.bolag.orgnr)
		FROM star.bolag JOIN star2.bolag ON true`)
	assertColumns(t, cols, "namn", "ort", "secret", "orgnr", "bransch")
	assertSingleSource(t, cols, "orgnr", "star2.bolag", "orgnr", TransformIdentity)
}

func TestCTE_ResolvedInItsDefiningScope(t *testing.T) {
	// b is defined next to the outer a. Naming b from inside a subquery that
	// declares its own a must not rebind b to the inner a — and that wrong
	// answer must not be cached for the later, unshadowed reference either.
	cols := extractStar(t, `
		WITH a AS (SELECT orgnr AS x FROM star.bolag), b AS (SELECT x FROM a)
		SELECT (WITH a AS (SELECT gata AS x FROM star.adress) SELECT max(x) FROM b) AS v,
		       (SELECT max(x) FROM b) AS w`)
	for _, name := range []string{"v", "w"} {
		src := sourceOf(t, cols, name)
		if len(src) != 1 || src[0].Table != "star.bolag" || src[0].Column != "orgnr" {
			t.Errorf("%s sources = %+v, want star.bolag.orgnr", name, src)
		}
	}
}

func TestCTE_NameIsCaseInsensitive(t *testing.T) {
	cols := extractStar(t, `WITH C AS (SELECT orgnr FROM star.bolag) SELECT *, c.orgnr AS ref FROM c`)
	assertColumns(t, cols, "orgnr", "ref")
	assertSingleSource(t, cols, "ref", "star.bolag", "orgnr", TransformIdentity)
}

func TestStar_OverRecursiveCTEKeepsAnchorLineage(t *testing.T) {
	// DuckDB serialises a RECURSIVE CTE body as RECURSIVE_CTE_NODE, not as a
	// set operation; the anchor arm still carries the real sources.
	cols := extractStar(t, `
		WITH RECURSIVE r(n) AS (
			SELECT length(orgnr) FROM star.bolag
			UNION ALL SELECT n + 1 FROM r WHERE n < 3
		)
		SELECT * FROM r`)
	assertColumns(t, cols, "n")
	src := sourceOf(t, cols, "n")
	if len(src) == 0 || src[0].Table != "star.bolag" || src[0].Column != "orgnr" {
		t.Errorf("n sources = %+v, want star.bolag.orgnr first", src)
	}
	for _, s := range src {
		if s.Table == "r" {
			t.Errorf("the CTE's own name must not appear as a source table: %+v", src)
		}
	}
}

func TestTables_CTENameIsCaseInsensitive(t *testing.T) {
	setupStarTables(t)
	tables, err := GetAllTables(shared, `WITH C AS (SELECT orgnr FROM star.bolag) SELECT * FROM c`)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(tables, []string{"star.bolag"}) {
		t.Errorf("tables = %v, want only star.bolag", tables)
	}
}

func TestSetOp_ArmWithItsOwnWithClause(t *testing.T) {
	cols := extractStar(t, `
		(WITH x AS (SELECT orgnr FROM star.bolag) SELECT * FROM x)
		UNION ALL
		(WITH x AS (SELECT orgnr FROM star.adress) SELECT * FROM x)`)
	assertColumns(t, cols, "orgnr")
	src := sourceOf(t, cols, "orgnr")
	if len(src) != 2 || src[0].Table != "star.bolag" || src[1].Table != "star.adress" {
		t.Errorf("orgnr sources = %+v, want star.bolag then star.adress", src)
	}
}

func TestSetOp_UnexpandableArmMakesResultUnknown(t *testing.T) {
	cols := extractStar(t, `
		SELECT orgnr FROM star.bolag
		UNION ALL
		SELECT * FROM (VALUES ('x')) v(orgnr)`)
	assertColumns(t, cols, "?")
}

func TestStar_OverCTEWithSourcelessExpression(t *testing.T) {
	// An unaliased constant is also named `?` with no sources; it is a real
	// column, not an unexpanded star, and must not collapse the expansion.
	cols := extractStar(t, `
		WITH x AS (SELECT orgnr, 1 FROM star.bolag)
		SELECT * FROM x`)
	assertColumns(t, cols, "orgnr", "?")
	assertSingleSource(t, cols, "orgnr", "star.bolag", "orgnr", TransformIdentity)
}

func TestScalarSubquery_SetOpWithItsOwnWithClause(t *testing.T) {
	cols := extractStar(t, `
		SELECT (WITH x AS (SELECT orgnr FROM star.bolag)
		        SELECT max(orgnr) FROM x UNION ALL SELECT max(orgnr) FROM x) AS v`)
	src := sourceOf(t, cols, "v")
	if len(src) == 0 {
		t.Fatal("v has no sources")
	}
	for _, s := range src {
		if s.Table != "star.bolag" || s.Column != "orgnr" {
			t.Errorf("v sources = %+v, want only star.bolag.orgnr", src)
		}
	}
}

func TestColumnRef_ThroughCTEColumnAliasList(t *testing.T) {
	cols := extractStar(t, `
		WITH c AS (SELECT namn, ort FROM star.bolag)
		SELECT x.id, stad FROM c AS x(id, stad)`)
	assertSingleSource(t, cols, "id", "star.bolag", "namn", TransformIdentity)
	assertSingleSource(t, cols, "stad", "star.bolag", "ort", TransformIdentity)
}

func TestColumnRef_ThroughTableColumnAliasList(t *testing.T) {
	cols := extractStar(t, `SELECT x.id FROM star.bolag AS x(id)`)
	assertSingleSource(t, cols, "id", "star.bolag", "orgnr", TransformIdentity)
}

func TestColumnRef_AliasAndColumnNamesAreCaseInsensitive(t *testing.T) {
	cols := extractStar(t, `
		WITH c AS (SELECT orgnr AS ID FROM star.bolag)
		SELECT X.id AS a, id AS b FROM c AS x`)
	assertSingleSource(t, cols, "a", "star.bolag", "orgnr", TransformIdentity)
	assertSingleSource(t, cols, "b", "star.bolag", "orgnr", TransformIdentity)
}

func TestColumnRef_TableAliasIsCaseInsensitive(t *testing.T) {
	cols := extractStar(t, `SELECT X.namn FROM star.bolag AS x`)
	assertSingleSource(t, cols, "namn", "star.bolag", "namn", TransformIdentity)
}

func TestStar_SemiAndAntiJoinOnlyEmitLeftColumns(t *testing.T) {
	for _, join := range []string{"SEMI", "ANTI"} {
		cols := extractStar(t, `SELECT * FROM star.bolag `+join+` JOIN star.adress USING (orgnr)`)
		assertColumns(t, cols, "orgnr", "namn", "ort", "secret")
	}
}

func TestSetOp_UnionByNameMatchesColumnsByName(t *testing.T) {
	// DuckDB output: namn, orgnr, gata — right-hand columns matched by name,
	// new ones appended.
	cols := extractStar(t, `
		SELECT namn, orgnr FROM star.bolag
		UNION ALL BY NAME
		SELECT orgnr, gata FROM star.adress`)
	assertColumns(t, cols, "namn", "orgnr", "gata")
	if src := sourceOf(t, cols, "namn"); len(src) != 1 || src[0].Table != "star.bolag" {
		t.Errorf("namn sources = %+v, want only star.bolag.namn", src)
	}
	src := sourceOf(t, cols, "orgnr")
	if len(src) != 2 || src[0].Table != "star.bolag" || src[1].Table != "star.adress" {
		t.Errorf("orgnr sources = %+v, want star.bolag then star.adress", src)
	}
	assertSingleSource(t, cols, "gata", "star.adress", "gata", TransformIdentity)
}
