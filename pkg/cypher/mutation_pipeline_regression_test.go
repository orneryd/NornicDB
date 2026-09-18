package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestUnwindCreateSetWithMatchCreatePreservesBindings(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "mutation-pipeline-bindings")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `
		UNWIND [{k: 0, name: 'zero'}, {k: 1, name: 'one'}] AS r
		CREATE (t:PipelineTarget)
		SET t = r
		WITH t
		MATCH (x:PipelineTarget {k: 0})
		CREATE (x)-[:LINKS_TO]->(t)
		RETURN t.k AS k, t.name AS name
	`, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{{int64(0), "zero"}, {int64(1), "one"}}, result.Rows)

	stored, err := exec.Execute(ctx, `MATCH (n:PipelineTarget) RETURN n.k AS k, n.name AS name ORDER BY k`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0), "zero"}, {int64(1), "one"}}, stored.Rows)

	links, err := exec.Execute(ctx, `MATCH (:PipelineTarget {k: 0})-[r:LINKS_TO]->(:PipelineTarget) RETURN count(r) AS count`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, links.Rows)
}

func TestUnwindCreateSetMergeWithAliasPreservesBindingsAndStats(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "mutation-pipeline-merge")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `
		UNWIND $rows AS row
		CREATE (created:PipelineMerge {seed: true})
		SET created += row
		WITH created AS target
		MATCH (source:PipelineMerge {k: 0})
		CREATE (source)-[:LINKS_TO]->(target)
		RETURN target.k AS key, target.name AS name
	`, map[string]interface{}{
		"rows": []interface{}{
			map[string]interface{}{"k": int64(0), "name": "zero"},
			map[string]interface{}{"k": int64(1), "name": "one"},
		},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{{int64(0), "zero"}, {int64(1), "one"}}, result.Rows)
	require.Equal(t, 2, result.Stats.NodesCreated)
	require.Equal(t, 2, result.Stats.RelationshipsCreated)
	require.Equal(t, 4, result.Stats.PropertiesSet)
}
