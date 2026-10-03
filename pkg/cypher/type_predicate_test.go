package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Expected values are Neo4j 5.26.30's (#838).
func TestTypePredicatesMatchNeo4j(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	for expression, want := range map[string]bool{
		"1 IS :: INTEGER": true, "'a' IS :: STRING NOT NULL": true, "null IS :: INTEGER": true,
		"null IS :: INTEGER NOT NULL": false, "1 IS NOT :: STRING": true, "[1, 2] IS :: LIST<INTEGER>": true,
		"1.5 IS :: INTEGER | FLOAT": true, "1 IS :: INT": true, "1 IS :: SIGNED INTEGER": true,
		"1 IS :: FLOAT": false, "1.0 IS :: INTEGER": false, "'a' IS :: VARCHAR": true, "true IS :: BOOL": true,
		"null IS :: NULL": true, "1 IS :: NULL": false, "null IS :: NOTHING": false, "1 IS :: NOTHING": false,
		"1 IS :: ANY": true, "null IS :: ANY NOT NULL": false, "1 IS :: ANY VALUE": true,
		"[1, null] IS :: LIST<INTEGER>": true, "[1, null] IS :: LIST<INTEGER NOT NULL>": false,
		"[] IS :: LIST<NOTHING>": true, "[1, 'a'] IS :: LIST<INTEGER | STRING>": true,
		"[1, 'a'] IS :: LIST<INTEGER> | LIST<STRING>": false, "[1, 2] IS :: ARRAY<INT>": true,
		"1 IS :: ANY<INTEGER | FLOAT>": true, "{a: 1} IS :: MAP": true, "{a: 1} IS :: ANY MAP": true,
		"date() IS :: DATE": true, "localtime() IS :: TIME WITHOUT TIME ZONE": true,
		"time() IS :: TIME WITH TIME ZONE": true, "localdatetime() IS :: TIMESTAMP WITHOUT TIME ZONE": true,
		"datetime() IS :: TIMESTAMP WITH TIME ZONE": true, "datetime() IS :: ZONED DATETIME": true,
		"duration('P1D') IS :: DURATION": true, "1 IS :: PROPERTY VALUE": true, "[1, 2] IS :: PROPERTY VALUE": true,
		"{a: 1} IS :: PROPERTY VALUE": false, "[1, 'a'] IS :: PROPERTY VALUE": false,
		"null IS NOT :: STRING": false, "null IS NOT :: STRING NOT NULL": true,
		"1 :: INTEGER": true, "1 IS TYPED INTEGER": true, "1 IS NOT TYPED INTEGER": false,
	} {
		result, err := exec.Execute(ctx, "RETURN "+expression+" AS v", nil)
		if assert.NoError(t, err, expression) {
			assert.Equal(t, [][]interface{}{{want}}, result.Rows, expression)
		}
	}
	_, err := exec.Execute(ctx, "RETURN 1 IS :: INTEGER NOT NULL | FLOAT AS v", nil)
	require.Error(t, err)

	_, err = exec.Execute(ctx, "UNWIND [1, 'a', 2.5, null] AS x CREATE (:T {v: x, i: 1})", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH (n:T) WHERE n.v IS :: INTEGER | FLOAT RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows, "the null property matches the nullable union")
	result, err = exec.Execute(ctx, "MATCH (n:T) WHERE n.v IS NOT :: STRING RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
	result, err = exec.Execute(ctx, "MATCH (n:T) RETURN CASE WHEN n.v IS :: STRING THEN 'text' ELSE 'other' END AS k ORDER BY k", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"other"}, {"other"}, {"text"}, {"text"}}, result.Rows, "null IS :: STRING is true")
}
