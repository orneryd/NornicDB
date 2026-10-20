package cypher

import (
	"context"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReviewer5276QuantifiedParameters(t *testing.T) {
	ref := fabricCallRowsReference(t)
	require.NotNil(t, ref, "real 5.26.30 reference required")
	old := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(old) })
	for _, predicate := range []string{"$bad - 1 > 0", "$bad.a = 1", "abs($bad - 1) = 1"} {
		q := "MATCH (s:ReviewerEmpty) MATCH path = (s)(()-[r:R WHERE " + predicate + "]->())*(t) RETURN count(path) AS v"
		params := map[string]interface{}{"bad": "bad"}
		_, referr := neo4j.ExecuteQuery(context.Background(), ref, q, params, neo4j.EagerResultTransformer)
		require.Error(t, referr, q)
		neoerr, ok := referr.(*neo4j.Neo4jError)
		require.True(t, ok, "%T %v", referr, referr)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", neoerr.Code)
		t.Logf("REFERENCE query=%s code=%s message=%s", q, neoerr.Code, neoerr.Msg)
		for _, norm := range []bool{false, true} {
			config.SetCypherQueryNormalizationEnabled(norm)
			for _, route := range []string{"native", "use"} {
				for _, tx := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/norm_%v/tx_%v", predicate, route, norm, tx), func(t *testing.T) {
						e := newFabricCallRowsExecutor(t)
						query := q
						if route == "use" {
							e = fabricCallRowsCompositeExecutor(t, e)
							query = "USE comp.one " + query
						}
						ctx := context.Background()
						if tx {
							_, err := e.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							defer e.Execute(ctx, "ROLLBACK", nil)
						}
						result, err := e.Execute(ctx, query, params)
						require.Error(t, err, "query=%s result=%v", query, result)
						code, _ := nornicerrors.Neo4jStatus(err)
						t.Logf("PUBLIC code=%s err=%v rows=%v", code, err, result)
						require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, "query=%s", query)
					})
				}
			}
		}
	}
}
