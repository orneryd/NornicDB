package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRelationshipCountRemainsExactAfterDetachDeleteAndRecreate(t *testing.T) {
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer func() { require.NoError(t, base.Close()) }()

	store := storage.NewNamespacedEngine(base, "count_lifecycle")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err = exec.Execute(ctx, `
CREATE (a {name: 'David'}),
       (b {name: 'Other'}),
       (c {name: 'NotOther'}),
       (d {name: 'NotOther2'}),
       (a)-[:REL]->(b),
       (a)-[:REL]->(c),
       (a)-[:REL]->(d),
       (b)-[:REL]->(),
       (b)-[:REL]->(),
       (c)-[:REL]->(),
       (c)-[:REL]->(),
       (d)-[:REL]->()`, nil)
	require.NoError(t, err)

	count, err := store.EdgeCount()
	require.NoError(t, err)
	require.Equal(t, int64(8), count)

	_, err = exec.Execute(ctx, "MATCH (n) DETACH DELETE n", nil)
	require.NoError(t, err)
	count, err = store.EdgeCount()
	require.NoError(t, err)
	require.Zero(t, count)

	_, err = exec.Execute(ctx, "CREATE (a), (a)-[:R]->(a)", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH ()-[r]-() RETURN count(r)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
}
