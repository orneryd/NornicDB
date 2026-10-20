package cypher

import (
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/stretchr/testify/require"
)

func TestPatternAdmissionInputBoundary(t *testing.T) {
	for _, source := range []string{
		"MATCH (n WHERE all(x IN [1] WHERE x=1)) RETURN n",
		"USE comp.one MATCH (n)-[r WHERE r.w=1]->(m) RETURN r",
		"WITH 7 AS x CALL(x) { USE comp.one MATCH(n WHERE x=7) RETURN n } RETURN n",
		"CYPHER 25 USE comp.one OPTIONAL MATCH(n WHERE true) RETURN {k:v IN {a:1} | k:v} AS value",
		"USE comp.one MATCH(n WHERE true) RETURN vector([1,2],2,FLOAT) AS value",
	} {
		var syntax *fabric.QuerySyntaxError
		require.ErrorAs(t, fabric.ReadQueryStructure(source).Err, &syntax, source)
		require.True(t, syntax.PatternLowering, source)
		lowered, rewrite, err := lowerPatternAdmission(source, true)
		require.NoError(t, err, source)
		require.NotNil(t, rewrite, source)
		require.NotEqual(t, source, lowered)
		require.NoError(t, fabric.ReadQueryStructure(lowered).Err, lowered)
		require.NoError(t, internalFunctionCallError(lowered), lowered)
	}
	for _, source := range []string{
		"USE comp.one MATCH(n) WHERE n.x=1 RETURN n",
		"USE comp.one RETURN 'MATCH (n WHERE false)' AS value",
		"USE comp.one MATCH(n) WHERE ] RETURN n",
		"USE comp.one CALL { RETURN 1 AS value } RETURN value",
	} {
		original := fabric.ReadQueryStructure(source)
		var syntax *fabric.QuerySyntaxError
		if errors.As(original.Err, &syntax) {
			require.False(t, syntax.PatternLowering, source)
		}
		lowered, rewrite, err := lowerPatternAdmission(source, false)
		require.NoError(t, err)
		require.Equal(t, source, lowered)
		require.Nil(t, rewrite)
		require.Same(t, original, fabric.ReadQueryStructure(lowered))
	}
}
