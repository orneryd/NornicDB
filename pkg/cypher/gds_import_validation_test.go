package cypher

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGDSFunctionDetectionIgnoresStringLiterals(t *testing.T) {
	for _, q := range []string{
		"RETURN 'gds.graph.project(DISTINCT' AS text",
		"RETURN 1 AS x /* gds.graph.project(DISTINCT */",
		"RETURN {`gds.graph.project`: 1} AS m",
	} {
		t.Run(q, func(t *testing.T) {
			e, _ := newGDSEngineGraph(t, "reviewscan")
			result, err := e.Execute(context.Background(), q, nil)
			t.Logf("query=%s result=%+v error=%v", q, result, err)
			require.NoError(t, err)
		})
	}
}

func TestGDSFunctionDetectionReadOnlyValues(t *testing.T) {
	for _, q := range []string{"RETURN 'gds.graph.project' AS text", "RETURN {`gds.graph.project`: 1} AS m", "RETURN 1 AS x /* gds.graph.project */"} {
		t.Run(q, func(t *testing.T) {
			e, _ := newGDSEngineGraph(t, "reviewread")
			ctx := WithPermissionChecker(context.Background(), func(p string) bool { return p == "read" })
			result, err := e.Execute(ctx, q, nil)
			t.Logf("requirements=%+v result=%+v error=%v", QueryPermissionRequirements(q), result, err)
			require.NoError(t, err)
		})
	}
}

func TestGDSFunctionDetectionBackquotedCalls(t *testing.T) {
	for _, fn := range []string{"`gds`.`graph`.`project`", "gds.graph.`project`", "gds.`graph`.project", "`gds.graph.project`"} {
		t.Run(fn, func(t *testing.T) {
			gdsFollowUpCatalog(t)
			e, _ := newGDSEngineGraph(t, "reviewescape")
			q := "RETURN " + fn + "('escape',1)"
			ctx := WithPermissionChecker(context.Background(), func(p string) bool { return p == "read" })
			result, err := e.Execute(ctx, q, nil)
			_, lookupErr := lookupGraphProjection("reviewescape", "escape")
			t.Logf("requirements=%+v result=%+v error=%v catalogError=%v", QueryPermissionRequirements(q), result, err, lookupErr)
			require.Error(t, lookupErr, "a read-only caller cannot create a graph")
		})
	}
}

func TestGDSImportLongToDoubleValidation(t *testing.T) {
	for _, value := range []string{
		"CASE i WHEN 0 THEN [1.5] ELSE [9007199254740993] END",
		"CASE i WHEN 0 THEN 1.5 ELSE 9007199254740993 END",
	} {
		t.Run(value, func(t *testing.T) {
			gdsFollowUpCatalog(t)
			_, run := newGDSEngineGraph(t, "reviewlossy")
			q := "UNWIND [0,1] AS i WITH i ORDER BY i RETURN gds.graph.project('lossy',i,null,{sourceNodeProperties:{v:" + value + "},targetNodeProperties:null})"
			_, err := run(q)
			t.Logf("query=%s error=%v", q, err)
			if err == nil {
				p, _ := lookupGraphProjection("reviewlossy", "lossy")
				t.Logf("stored=%+v", p.NodeProperties)
			}
			require.Error(t, err, "GDS 2.13.12 refuses lossy Long-to-Double conversion")
		})
	}
}

func TestGDSAggregationRelationshipLongBoundary(t *testing.T) {
	gdsFollowUpCatalog(t)
	_, run := newGDSEngineGraph(t, "reviewrelboundary")
	result, err := run("RETURN gds.graph.project('relboundary',0,1,{relationshipProperties:{w:9007199254740994}})")
	t.Logf("result=%+v error=%v", result, err)
	if err == nil {
		p, _ := lookupGraphProjection("reviewrelboundary", "relboundary")
		t.Logf("relationshipValues=%+v", p.Relationships[0].Properties)
	}
	require.Error(t, err, "GDS refuses even exactly representable positive Longs above 2^53")
}

func TestGDSFunctionDetectionCallSpellings(t *testing.T) {
	for _, name := range []string{"gds.graph.project", "GdS.GrApH.PrOjEcT", "`gds`.`graph`.`project`", "gds.graph.`project`", "gds.`graph`.project", "`gds.graph.project`", "gds /* a */ . graph . /* b */ project"} {
		for _, gap := range []string{"", " \n\t", "/* comment */", "// comment\n"} {
			for _, prefix := range []string{"RETURN ", "CALL ", "CALL /* comment */ "} {
				q := prefix + name + gap + "('g',1)"
				require.True(t, QueryPermissionRequirements(q).Write, q)
			}
			require.Error(t, gdsProjectDistinctError("RETURN "+name+gap+"( /* comment */ DISTINCT 'g',1)"), name+gap)
			require.NoError(t, gdsProjectDistinctError("CALL "+name+gap+"(DISTINCT 'g',1)"))
		}
	}
	for _, q := range []string{"RETURN 'gds.graph.project(DISTINCT'", "RETURN \"gds.graph.project(DISTINCT\"", "RETURN {`gds.graph.project(DISTINCT`:1}", "RETURN 1 /* gds.graph.project(DISTINCT */", "RETURN 1 // gds.graph.project(DISTINCT", "RETURN gds.graph.projectOther(1)", "USE gds.graph.project RETURN 1"} {
		require.False(t, QueryPermissionRequirements(q).Write, q)
		require.NoError(t, gdsProjectDistinctError(q), q)
	}
}
