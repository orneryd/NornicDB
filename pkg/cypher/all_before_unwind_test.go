package cypher

import (
	"context"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAllVariableBeforeUnwindPublic(t *testing.T) {
	driver := fabricCallRowsReference(t)
	query := "WITH 7 AS all WITH all UNWIND [1] AS x RETURN all AS k"
	if driver != nil {
		reference, err := neo4j.ExecuteQuery(t.Context(), driver, query, nil, neo4j.EagerResultTransformer)
		require.NoError(t, err)
		require.Equal(t, []interface{}{int64(7)}, reference.Records[0].Values)
	}
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
					for _, explicit := range []bool{false, true} {
						t.Run(fmt.Sprint("explicit_", explicit), func(t *testing.T) {
							e := newFabricCallRowsExecutor(t)
							if explicit {
								_, err := e.Execute(t.Context(), "BEGIN", nil)
								require.NoError(t, err)
								t.Cleanup(func() {
									if e.txContext != nil && e.txContext.active {
										_, err := e.Execute(context.Background(), "ROLLBACK", nil)
										require.NoError(t, err)
									}
								})
							}
							got, err := e.Execute(t.Context(), query, nil)
							require.NoError(t, err)
							require.Equal(t, [][]interface{}{{int64(7)}}, got.Rows)
						})
					}
				})
			}
		})
	}
}
func TestProjectionAllContextPreservesKeywordSubscripts(t *testing.T) {
	for _, tc := range []struct {
		q    string
		want interface{}
	}{{"CYPHER 25 WITH [7] AS unwind WITH ALL unwind[0] AS k RETURN k", int64(7)}, {"CYPHER 25 WITH [7] AS unwind RETURN ALL unwind[0] AS k", int64(7)}, {"CYPHER 25 WITH 7 AS call RETURN ALL call AS k", int64(7)}, {"WITH 7 AS call RETURN ALL call AS k", int64(7)}} {
		t.Run(tc.q, func(t *testing.T) {
			e := newFabricCallRowsExecutor(t)
			r, err := e.Execute(t.Context(), tc.q, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tc.want}}, r.Rows)
		})
	}
}

func TestAllBeforeUnwindDecisionAllocationGuard(t *testing.T) {
	const query = "WITH 7 AS all WITH all UNWIND [1] AS x RETURN all AS k"
	require.Zero(t, testing.AllocsPerRun(100, func() {
		source, ok := splitUnwindBody(originalBraceView("UNWIND [1] AS x RETURN all AS k"))
		if !ok || source.expression != "[1]" || source.variableWritten != "x" {
			panic("UNWIND source product")
		}
	}))
	require.Zero(t, testing.AllocsPerRun(100, func() {
		edits, err := projectionAllEdits(originalBraceView(query), false)
		if err != nil || len(edits) != 0 {
			panic("lost ALL variable")
		}
	}))
}
