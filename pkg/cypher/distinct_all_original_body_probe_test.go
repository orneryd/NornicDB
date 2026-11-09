package cypher

import (
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDistinctAllOriginalBodyProbe(t *testing.T) {
	driver := fabricCallRowsReference(t)
	for _, tc := range []struct {
		query string
		want  int64
	}{{"WITH 7 AS all RETURN sum(DISTINCT all) AS k", 7}, {"WITH 7 AS all RETURN count(DISTINCT all) AS k", 1}} {
		t.Run(tc.query, func(t *testing.T) {
			e := newFabricCallRowsExecutor(t)
			r, err := e.Execute(t.Context(), tc.query, nil)
			require.NoError(t, err)
			require.Equal(t, []string{"k"}, r.Columns)
			require.Equal(t, [][]interface{}{{tc.want}}, r.Rows)
			if driver != nil {
				reference, err := neo4j.ExecuteQuery(t.Context(), driver, tc.query, nil, neo4j.EagerResultTransformer)
				require.NoError(t, err)
				require.Len(t, reference.Records, 1)
				require.Equal(t, reference.Records[0].Values, r.Rows[0])
			}
		})
	}
}
func TestLoneDistinctCommentAndUnicodeArity(t *testing.T) {
	driver := fabricCallRowsReference(t)
	for _, q := range []string{"RETURN count(/*h*/DISTINCT/*t*/) AS k", "RETURN sum(/*h*/DISTINCT) AS k", "RETURN count(DISTINCT/*t*/) AS k"} {
		t.Run(q, func(t *testing.T) {
			e := newFabricCallRowsExecutor(t)
			_, err := e.Execute(t.Context(), q, nil)
			require.ErrorContains(t, err, "Insufficient parameters")
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
			if driver != nil {
				_, err := neo4j.ExecuteQuery(t.Context(), driver, q, nil, neo4j.EagerResultTransformer)
				require.ErrorContains(t, err, "Insufficient parameters")
			}
		})
	}
	q := "WITH 7 AS diſtinct RETURN sum(diſtinct) AS k"
	e := newFabricCallRowsExecutor(t)
	r, err := e.Execute(t.Context(), q, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"k"}, r.Columns)
	require.Equal(t, [][]interface{}{{int64(7)}}, r.Rows)
	if driver != nil {
		reference, err := neo4j.ExecuteQuery(t.Context(), driver, q, nil, neo4j.EagerResultTransformer)
		require.NoError(t, err)
		require.Len(t, reference.Records, 1)
		require.Equal(t, reference.Records[0].Values, r.Rows[0])
	}
}
