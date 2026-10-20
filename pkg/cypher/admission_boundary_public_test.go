package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"testing"
)

// Basic validation uses the same source capability, including comments before
// declarations. Ordinary relationship brackets must not acquire native facts.
func TestBasicSyntaxUsesAdmissionOwner(t *testing.T) {
	executor := newFabricCallRowsExecutor(t)
	for _, query := range []string{
		"MATCH (a)-[:P]->(b) RETURN a,b",
		"RETURN [/* first */ not IN [1,2] | not] AS value",
		"WITH 1 AS x OPTIONAL CALL (x) { RETURN x AS y } RETURN y",
	} {
		input := newSemanticStatementInput(query, nil)
		require.NoError(t, executor.validateSyntaxNornicInput(query, input), query)
		require.True(t, input.examined, query)
		first := input.syntax()
		require.NoError(t, executor.validateSemanticScopesWithInput(context.Background(), query, input), query)
		if first == nil {
			require.Nil(t, input.syntax(), query)
		} else {
			require.Same(t, first, input.syntax(), query)
		}
	}
	require.True(t, containsNotInOperator("RETURN [/* declaration */ not IN [1] | not], 1 NOT /* gap */ IN [2]"))
	require.False(t, containsNotInOperator("RETURN [/* declaration */ not IN [1] | not]"))
}

func TestPhysicalExpressionQueryCaptures(t *testing.T) {
	predicate := "EXISTS { CALL (x) { MATCH (x)<--(y) RETURN y } RETURN y }"
	require.Contains(t, physicalReadReferences(predicate), "x")
	require.NotContains(t, semanticExpressionReferences(predicate), "y")
}

func TestAdmissionCorrelatedPatternFootprintPublic(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	for _, norm := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(norm)
		for _, composite := range []bool{false, true} {
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("norm_%v/composite_%v/tx_%v", norm, composite, explicit), func(t *testing.T) {
					executor := newFabricCallRowsExecutor(t)
					prefix := ""
					ctx := context.Background()
					if composite {
						executor = fabricCallRowsCompositeExecutor(t, executor)
						prefix = "USE comp.one "
					}
					_, err := executor.Execute(ctx, prefix+"CREATE (a:AdmissionCapture {id:1})-[:P]->(b:AdmissionCapture {id:2})", nil)
					require.NoError(t, err)
					if explicit {
						_, err = executor.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer executor.Execute(ctx, "ROLLBACK", nil)
					}
					for _, body := range []string{
						"MATCH (i:AdmissionCapture)-[r]->(x) WHERE EXISTS { CALL (x) { MATCH (x)<--(y) RETURN y } RETURN y } RETURN x.id AS id",
						"MATCH (i:AdmissionCapture)-[r]->(x) WHERE COUNT { CALL (x) { MATCH (x)<--(y) RETURN y } RETURN y } = 1 RETURN x.id AS id",
						"MATCH (i:AdmissionCapture)-[r]->(x) WHERE size(COLLECT { CALL (x) { MATCH (x)<--(y) RETURN y } RETURN y }) = 1 RETURN x.id AS id",
					} {
						require.NoError(t, executor.validateSemanticScopes(ctx, prefix+body), "admission: "+body)
						for repeat := 0; repeat < 2; repeat++ {
							result, err := executor.Execute(ctx, prefix+body, nil)
							require.NoError(t, err, body)
							require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows, body)
						}
					}
				})
			}
		}
	}
}
