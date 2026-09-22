package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMatchMergeOptionalMatchPreservesEveryInputRow(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "match-merge-optional")
	executor := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := executor.Execute(ctx, `
		CREATE (a:A), (b:B)
		CREATE (a)-[:T1]->(b),
		       (b)-[:T2]->(a)
	`, nil)
	require.NoError(t, err)

	result, err := executor.Execute(ctx, `
		MATCH (a)
		MERGE (b)
		WITH *
		OPTIONAL MATCH (a)--(b)
		RETURN count(*)
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(6)}}, result.Rows)
}

func TestIndependentMatchAfterWithProducesCartesianRows(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "independent-match")
	executor := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := executor.Execute(ctx, `
		CREATE (andres {name: 'Andres'}),
		       (michael {name: 'Michael'}),
		       (peter {name: 'Peter'}),
		       (bread {type: 'Bread'}),
		       (veggies {type: 'Veggies'}),
		       (meat {type: 'Meat'})
		CREATE (andres)-[:ATE {times: 10}]->(bread),
		       (andres)-[:ATE {times: 8}]->(veggies),
		       (michael)-[:ATE {times: 4}]->(veggies),
		       (michael)-[:ATE {times: 6}]->(bread),
		       (michael)-[:ATE {times: 9}]->(meat),
		       (peter)-[:ATE {times: 7}]->(veggies),
		       (peter)-[:ATE {times: 7}]->(bread),
		       (peter)-[:ATE {times: 4}]->(meat)
	`, nil)
	require.NoError(t, err)

	relationships, err := executor.Execute(ctx, `
		MATCH ()-[r1]->()<--()
		RETURN r1
	`, nil)
	require.NoError(t, err)
	require.Len(t, relationships.Rows, 14)
	for _, row := range relationships.Rows {
		require.IsType(t, &storage.Edge{}, row[0])
	}

	direct, err := executor.Execute(ctx, `
		MATCH ()-[r1]->()<--()
		RETURN sum(r1.times)
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(97)}}, direct.Rows)

	result, err := executor.Execute(ctx, `
		MATCH ()-->()
		WITH 1 AS x
		MATCH ()-[r1]->()<--()
		RETURN sum(r1.times)
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(776)}}, result.Rows)
}
