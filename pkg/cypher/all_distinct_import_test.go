package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAllDistinctImportRetainsProjectedVariable(t *testing.T) {
	driver := fabricCallRowsReference(t)
	for _, norm := range []bool{true, false} {
		t.Run(fmt.Sprint("normalization_", norm), func(t *testing.T) {
			restore := func() {}
			if !norm {
				restore = config.WithCypherQueryNormalizationDisabled()
			}
			defer restore()
			for _, parser := range []string{"nornic", "antlr"} {
				t.Run(parser, func(t *testing.T) {
					old := config.GetParserType()
					config.SetParserType(parser)
					defer config.SetParserType(old)
					for _, name := range []string{"all", "`all`"} {
						for _, middle := range []string{"WITH DISTINCT " + name + " ", "WITH " + name + " ", ""} {
							t.Run(name+"/"+middle, func(t *testing.T) {
								base := newFabricCallRowsExecutor(t)
								query := "USE comp WITH 7 AS " + name + " " + middle + "CALL { WITH " + name + " USE comp.one RETURN " + name + " AS k } RETURN k"
								reference := "WITH 7 AS " + name + " " + middle + "CALL { WITH " + name + " RETURN " + name + " AS k } RETURN k"
								fabricAssertGrammarCase(t, base, query, []string{"k"}, "[[7]]")
								if driver != nil {
									fabricCompareGrammarCase(t, base, driver, query, reference)
								}
							})
						}
					}
				})
			}
		})
	}
}
func TestProjectionAllRetainsFollowingClause(t *testing.T) {
	for _, source := range []string{"WITH all USE comp.one RETURN all", "WITH all CALL { WITH all RETURN all AS k } RETURN k", "WITH all CREATE (n) RETURN all", "WITH all MATCH (n) RETURN all", "WITH all WITH all RETURN all", "WITH all SET n.v=all RETURN all", "WITH all REMOVE n.v RETURN all"} {
		for _, v25 := range []bool{false, true} {
			edits, err := projectionAllEdits(originalBraceView(source), v25)
			require.NoError(t, err)
			require.Empty(t, edits, source)
		}
	}
	for _, source := range []string{"WITH 7 AS call RETURN ALL call", "WITH 7 AS create RETURN ALL create", "RETURN ALL 7"} {
		edits, err := projectionAllEdits(originalBraceView(source), false)
		require.NoError(t, err)
		require.NotEmpty(t, edits, source)
	}
	for _, v25 := range []bool{false, true} {
		edits, err := projectionAllEdits(originalBraceView("RETURN ALL [1,2]"), v25)
		require.NoError(t, err)
		if v25 {
			require.NotEmpty(t, edits)
		} else {
			require.Empty(t, edits)
		}
	}
	require.Zero(t, testing.AllocsPerRun(100, func() {
		if startsProjectedExpression("USE comp.one RETURN all", 0) {
			panic("lost USE clause")
		}
	}))
}

func TestProjectionAllClauseTriviaAndOperatorPolicy(t *testing.T) {
	for _, tc := range []struct{ word, tail string }{{"USE", "comp.one RETURN all"}, {"MATCH", "(n) RETURN all"}, {"CREATE", "(n) RETURN all"}, {"MERGE", "(n) RETURN all"}, {"RETURN", "all"}, {"WITH", "all RETURN all"}, {"CALL", "{ RETURN 1 AS x } RETURN all"}, {"OPTIONAL", "MATCH (n) RETURN all"}, {"DELETE", "n RETURN all"}, {"REMOVE", "n.x RETURN all"}, {"SET", "n.x=all RETURN all"}, {"FOREACH", "(x IN [1] | CREATE ()) RETURN all"}, {"UNION", "ALL RETURN all"}, {"ORDER", "BY all"}, {"SKIP", "1 RETURN all"}, {"LIMIT", "1 RETURN all"}} {
		for _, gap := range []string{" ", "\t", "/*c*/", " //c\n"} {
			text := tc.word + gap + tc.tail
			require.True(t, startsWithClauseAfterName(text), text)
			require.False(t, startsProjectedExpression(text, 0), text)
			edits, err := projectionAllEdits(originalBraceView("WITH all "+text), false)
			require.NoError(t, err)
			require.Empty(t, edits, text)
		}
	}
	for _, text := range []string{"USE / 2", "USE/*unfinished", "CALL + 1", "MATCH.x", "CREATE AS x", "USE", "UNWIND [1]"} {
		require.False(t, startsWithClauseAfterName(text), text)
	}
	for _, source := range []string{"WITH 7 AS all WITH all USE/*c*/comp.one RETURN all", "WITH 7 AS all WITH all USE //c\ncomp.one RETURN all"} {
		edits, err := projectionAllEdits(originalBraceView(source), false)
		require.NoError(t, err)
		require.Empty(t, edits, source)
	}
	for _, q := range []string{"WITH 7 AS all WITH DISTINCT all CALL { WITH all USE/*c*/comp.one RETURN all AS k } RETURN k", "WITH 7 AS all WITH DISTINCT all CALL { WITH all USE //c\ncomp.one RETURN all AS k } RETURN k"} {
		e := newFabricCallRowsExecutor(t)
		r, err := e.Execute(t.Context(), "USE comp "+q, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(7)}}, r.Rows)
	}
}
