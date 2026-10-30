package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestClausesIntegrationCoverageMatrix(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "clauses_cov_matrix")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
CREATE (a:Person {name:'Alice', team:'red'}),
       (b:Person {name:'Bob', team:'blue'}),
       (c:Person {name:'Cara', team:'red'}),
       (r:Robot {name:'R2'})
`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'})
CREATE (a)-[:KNOWS]->(b)
`, nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `
MATCH (n:Person)
WITH n.team AS team, count(*) AS c
RETURN team, c
ORDER BY team
`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"team", "c"}, res.Columns)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "blue", res.Rows[0][0])
	require.EqualValues(t, 1, res.Rows[0][1])
	require.Equal(t, "red", res.Rows[1][0])
	require.EqualValues(t, 2, res.Rows[1][1])

	res, err = exec.Execute(ctx, `UNWIND [1,2] AS x UNWIND [3,4] AS y RETURN x, y ORDER BY x, y`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"x", "y"}, res.Columns)
	require.Len(t, res.Rows, 4)
	require.EqualValues(t, 1, res.Rows[0][0])
	require.EqualValues(t, 3, res.Rows[0][1])
	require.EqualValues(t, 2, res.Rows[3][0])
	require.EqualValues(t, 4, res.Rows[3][1])

	res, err = exec.Execute(ctx, `
MATCH (n:Person)
OPTIONAL MATCH (n)-[:KNOWS]->(m:Person)
RETURN n.name AS nname, m.name AS mname
ORDER BY nname
`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"nname", "mname"}, res.Columns)
	require.Len(t, res.Rows, 3)
	require.Equal(t, "Alice", res.Rows[0][0])
	require.Equal(t, "Bob", res.Rows[0][1])
	require.Equal(t, "Bob", res.Rows[1][0])
	require.Nil(t, res.Rows[1][1])

	res, err = exec.Execute(ctx, `MATCH (n:Person) WITH n ORDER BY n.name RETURN collect(n.name) AS names`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"names"}, res.Columns)
	require.Len(t, res.Rows, 1)
	names, ok := res.Rows[0][0].([]interface{})
	require.True(t, ok)
	require.Equal(t, []interface{}{"Alice", "Bob", "Cara"}, names)
}
