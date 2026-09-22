// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package lineage

import "testing"

// Every expression class DuckDB emits has to carry its operands' columns
// through. The walker used to name a few classes and silently drop the rest:
// casts, CASE else-branches and then every operator — COALESCE, IS NULL, NOT,
// IN, AND/OR, BETWEEN — each produced a column with no sources at all.
func TestOperators_ContributeTheirSources(t *testing.T) {
	setupStarTables(t)
	cases := []struct {
		name, expr string
		transform  TransformationType
		function   string
	}{
		{"coalesce", "coalesce(b.namn, a.gata)", TransformFunction, "COALESCE"},
		{"is null", "b.namn IS NULL", TransformConditional, ""},
		// A wrapper keeps a more specific inner classification, as a cast does.
		{"not over a function", "NOT starts_with(b.namn, a.gata)", TransformFunction, "starts_with"},
		{"is not null", "b.namn IS NOT NULL", TransformConditional, ""},
		{"in", "b.namn IN (a.gata, 'x')", TransformConditional, ""},
		// A condition carries no function name, so it agrees with a CASE
		// arm and the rendered view does not fall back to [MIX].
		{"and", "b.namn = 'x' AND a.gata = 'y'", TransformConditional, ""},
		{"or", "b.namn = 'x' OR a.gata = 'y'", TransformConditional, ""},
		{"comparison", "b.namn = a.gata", TransformConditional, ""},
		{"between", "b.namn BETWEEN a.gata AND 'z'", TransformConditional, ""},
		{"is distinct from", "b.namn IS DISTINCT FROM a.gata", TransformConditional, ""},
		{"coalesce of aggregate", "coalesce(max(b.namn), 'x')", TransformAggregation, "max"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cols, err := Extract(shared, `SELECT `+tc.expr+` AS v
				FROM star.bolag b JOIN star.adress a ON a.orgnr = b.orgnr`)
			if err != nil {
				t.Fatal(err)
			}
			src := sourceOf(t, cols, "v")
			if len(src) == 0 {
				t.Fatalf("%s contributed no sources", tc.expr)
			}
			for _, s := range src {
				real := (s.Table == "star.bolag" && s.Column == "namn") ||
					(s.Table == "star.adress" && s.Column == "gata")
				if !real {
					t.Errorf("%s: unexpected source %+v", tc.expr, s)
				}
			}
			if tc.transform != "" && src[0].Transformation != tc.transform {
				t.Errorf("transformation = %q, want %q (%+v)", src[0].Transformation, tc.transform, src[0])
			}
			if src[0].FunctionName != tc.function {
				t.Errorf("function = %q, want %q", src[0].FunctionName, tc.function)
			}
		})
	}
}

// The lambda parameter is bound by the lambda, not read from a table. Walking
// it as an ordinary column reference invented a source column named after the
// parameter, and persisted it into the commit's column lineage.
func TestLambda_ParameterIsNotASourceColumn(t *testing.T) {
	setupStarTables(t)
	cols, err := Extract(shared, `
		SELECT list_transform([b.orgnr], x -> x || b.namn) AS v FROM star.bolag b`)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"orgnr": true, "namn": true}
	src := sourceOf(t, cols, "v")
	if len(src) == 0 {
		t.Fatal("no sources")
	}
	for _, s := range src {
		if s.Table != "star.bolag" || !want[s.Column] {
			t.Errorf("unexpected source %+v, want only star.bolag.{orgnr,namn}", s)
		}
	}
}

// Element access is an OPERATOR too, but it reads a value out rather than
// testing a condition.
func TestOperators_ElementAccessReadsAsAFunction(t *testing.T) {
	setupStarTables(t)
	for _, expr := range []string{"([namn])[1]", "([namn])[1:2]", "({'n': namn}).n"} {
		cols, err := Extract(shared, `SELECT `+expr+` AS v FROM star.bolag`)
		if err != nil {
			t.Fatalf("%s: %v", expr, err)
		}
		src := sourceOf(t, cols, "v")
		if len(src) != 1 || src[0].Column != "namn" || src[0].Transformation != TransformFunction {
			t.Errorf("%s sources = %+v, want a FUNCTION of namn", expr, src)
		}
	}
}

// GetCDCTables reads AGGREGATION to decide that a delta is unsound for a
// model, so a wrapper must not hide the aggregate underneath it.
func TestOperators_AggregateSurvivesForCDC(t *testing.T) {
	setupStarTables(t)
	cols, err := Extract(shared, `
		SELECT coalesce(max(a.gata), 'x') AS v
		FROM star.bolag b JOIN star.adress a ON a.orgnr = b.orgnr`)
	if err != nil {
		t.Fatal(err)
	}
	if !GetCDCTables(cols)["star.adress"] {
		t.Errorf("star.adress must need CDC through coalesce(max(...)), got %+v", cols)
	}
}

// An expression class the walker does not name explicitly must still reach its
// operands, so a DuckDB upgrade that introduces one cannot silently empty a
// model's lineage.
func TestUnknownExpressionClassStillReachesItsOperands(t *testing.T) {
	setupStarTables(t)
	// COLLATE is its own class; the list and struct constructors are FUNCTION
	// nodes the walker does name, kept here as the shapes most likely to move.
	for _, sql := range []string{
		`SELECT namn COLLATE NOCASE AS v FROM star.bolag`,
		`SELECT [orgnr, namn] AS v FROM star.bolag`,
		`SELECT {'a': orgnr, 'b': namn} AS v FROM star.bolag`,
	} {
		cols, err := Extract(shared, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		src := sourceOf(t, cols, "v")
		if len(src) == 0 {
			t.Errorf("%s produced no sources", sql)
		}
		for _, s := range src {
			if s.Table != "star.bolag" || (s.Column != "orgnr" && s.Column != "namn") {
				t.Errorf("%s: unexpected source %+v", sql, s)
			}
		}
	}
}

// A scalar wrapper must not hide an aggregate: GetCDCTables reads AGGREGATION
// to decide that a delta is unsound, and `upper(max(x))` used to come back a
// plain function, giving the source delta CDC it cannot take.
func TestFunction_DoesNotHideAnAggregate(t *testing.T) {
	setupStarTables(t)
	for _, expr := range []string{
		"upper(max(a.gata))",
		"upper(max(a.gata)) OVER ()",
		"coalesce(upper(max(a.gata)), 'x')",
	} {
		cols, err := Extract(shared, `SELECT `+expr+` AS v
			FROM star.bolag b JOIN star.adress a ON a.orgnr = b.orgnr`)
		if err != nil {
			t.Fatalf("%s: %v", expr, err)
		}
		if !GetCDCTables(cols)["star.adress"] {
			t.Errorf("%s: star.adress must still need CDC, got %+v", expr, cols)
		}
	}
	// The wrapper's own classification still wins when nothing more specific
	// is underneath: SUM(a + b) is an aggregation, not arithmetic.
	cols, err := Extract(shared, `SELECT max(a.gata || b.namn) AS v
		FROM star.bolag b JOIN star.adress a ON a.orgnr = b.orgnr`)
	if err != nil {
		t.Fatal(err)
	}
	for _, src := range sourceOf(t, cols, "v") {
		if src.Transformation != TransformAggregation {
			t.Errorf("max(...) source = %+v, want AGGREGATION", src)
		}
	}
}

// A lambda parameter is only bound inside the lambda. A subquery with its own
// FROM defines its own names, and inheriting the binding there dropped every
// column it reads.
func TestLambda_BindingDoesNotLeakIntoASubqueryWithItsOwnFrom(t *testing.T) {
	setupStarTables(t)
	cols, err := Extract(shared, `
		SELECT list_transform([1], gata -> (SELECT max(gata) FROM star.adress)) AS v`)
	if err != nil {
		t.Fatal(err)
	}
	src := sourceOf(t, cols, "v")
	if len(src) != 1 || src[0].Table != "star.adress" || src[0].Column != "gata" {
		t.Errorf("sources = %+v, want star.adress.gata", src)
	}
}

// An unknown wrapper is not a direct copy. DetectRenames reads IDENTITY as a
// renamed column, so passing one through drives a spurious ALTER TABLE.
func TestUnknownWrapperIsNotIdentity(t *testing.T) {
	setupStarTables(t)
	cols, err := Extract(shared, `SELECT namn COLLATE NOCASE AS v FROM star.bolag`)
	if err != nil {
		t.Fatal(err)
	}
	src := sourceOf(t, cols, "v")
	if len(src) != 1 || src[0].Transformation == TransformIdentity {
		t.Errorf("sources = %+v, want a classified wrapper", src)
	}
}

// `name IN (SELECT ...)` tests the outer column, it does not copy it.
// Reporting IDENTITY there is what DetectRenames reads as a rename.
func TestSubquery_TestedColumnIsNotIdentity(t *testing.T) {
	setupStarTables(t)
	for _, sql := range []string{
		`SELECT namn IN (SELECT gata FROM star.adress) AS v FROM star.bolag`,
		`SELECT namn = ANY (SELECT gata FROM star.adress) AS v FROM star.bolag`,
	} {
		cols, err := Extract(shared, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		for _, src := range sourceOf(t, cols, "v") {
			if src.Transformation == TransformIdentity {
				t.Errorf("%s: %+v is reported as a direct copy", sql, src)
			}
		}
	}
}

// A scalar wrapper keeps whatever is more specific underneath, the same rule
// a cast and a CASE arm follow.
func TestFunction_KeepsTheInnerClassification(t *testing.T) {
	setupStarTables(t)
	cols, err := Extract(shared, `SELECT upper(orgnr::VARCHAR) AS v FROM star.bolag`)
	if err != nil {
		t.Fatal(err)
	}
	if src := sourceOf(t, cols, "v"); len(src) != 1 || src[0].Transformation != TransformCast {
		t.Errorf("upper(orgnr::VARCHAR) = %+v, want CAST", src)
	}
}

// DuckDB writes the JSON arrow `j -> 'a'` as a LAMBDA whose lhs is a real
// column. Binding it as a lambda parameter emptied the column's lineage.
func TestLambda_JSONArrowReadsItsColumn(t *testing.T) {
	setupStarTables(t)
	cols, err := Extract(shared, `SELECT (namn::JSON)->'a' AS v FROM star.bolag`)
	if err != nil {
		t.Fatal(err)
	}
	if src := sourceOf(t, cols, "v"); len(src) != 1 || src[0].Column != "namn" {
		t.Errorf("sources = %+v, want star.bolag.namn", src)
	}
}
