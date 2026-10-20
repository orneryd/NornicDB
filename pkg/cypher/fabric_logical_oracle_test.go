package cypher

import (
	"context"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

func TestFabricLogicalNeo4jOracle(t *testing.T) {
	driver := fabricCallRowsReference(t)
	if driver == nil {
		t.Skip("NORNICDB_NEO4J_REFERENCE_URI is required for the Neo4j oracle")
	}
	ctx := context.Background()
	for _, query := range []string{
		"WITH 1 AS x OPTIONAL CALL { MATCH (n:ReviewXhighNoNode) RETURN n } RETURN x,n",
		"WITH 11 AS x CALL (x) { WITH 1 AS y RETURN x AS v } RETURN v",
		"WITH 11 AS x CALL (*) { WITH 1 AS y RETURN x AS v } RETURN v",
		"WITH 11 AS x CALL (x) { UNWIND [] AS y RETURN count(*) AS c ORDER BY x } RETURN c",
	} {
		t.Run(query, func(t *testing.T) {
			result, err := neo4j.ExecuteQuery(ctx, driver, query, nil, neo4j.EagerResultTransformer)
			require.NoError(t, err)
			require.Len(t, result.Records, 1)
			t.Logf("rows=%v", result.Records[0].Values)
		})
	}
	for _, query := range []string{
		"UNWIND [] AS x CALL { RETURN toUpper(CASE WHEN true THEN [1] ELSE ['a'] END) AS v } RETURN v",
		"WITH [1] AS x CALL (x) { UNWIND [] AS ignored RETURN toUpper(x) AS v } RETURN v",
		"UNWIND [] AS unused RETURN (CASE WHEN true THEN 1 ELSE 2.0 END) + {a:1} AS v",
		"RETURN (CASE WHEN true THEN 1 ELSE 2.0 END) + {a:1} AS v",
		"UNWIND [] AS unused RETURN CASE WHEN (CASE WHEN true THEN 1 ELSE 2.0 END) THEN 1 ELSE 0 END AS v",
		"WITH 11 AS x CALL () { RETURN x AS v } RETURN v",
	} {
		t.Run(query, func(t *testing.T) {
			_, err := neo4j.ExecuteQuery(ctx, driver, query, nil, neo4j.EagerResultTransformer)
			require.Error(t, err)
			var ne *neo4j.Neo4jError
			require.ErrorAs(t, err, &ne)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", ne.Code)
			t.Logf("%s: %s", ne.Code, ne.Msg)
		})
	}
}
