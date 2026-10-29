package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDecodedNameNamespaceIndexCleanup(t *testing.T) {
	engine, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	ctx := context.Background()
	victim := NewStorageExecutor(storage.NewNamespacedEngine(engine, "victim"))
	survivor := NewStorageExecutor(storage.NewNamespacedEngine(engine, "survivor"))
	_, err = victim.Execute(ctx, "CREATE (:Removed)-[:REMOVED]->(:Removed)", nil)
	require.NoError(t, err)
	names := []string{"OrdinaryControl", "kept\x00victim:looks-like-id", "kept\x00other:looks-like-id"}
	for index, name := range names {
		query := fmt.Sprintf("CREATE (n:%s {value:%d})-[:%s {value:%d}]->()", cyphertext.QuoteName(name), index+7, cyphertext.QuoteName(name), index+7)
		_, err = survivor.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
	require.NoError(t, engine.FlushWriteBehind())
	nodes, edges, err := engine.DeleteByPrefix("victim:")
	require.NoError(t, err)
	require.EqualValues(t, 2, nodes)
	require.EqualValues(t, 1, edges)
	for index, name := range names {
		for _, carrier := range []struct{ name, pattern, variable string }{
			{"label", "(n:" + cyphertext.QuoteName(name) + ")", "n"},
			{"type", "()-[r:" + cyphertext.QuoteName(name) + "]->()", "r"},
		} {
			t.Run(fmt.Sprintf("%d/%s", index, carrier.name), func(t *testing.T) {
				query := "MATCH " + carrier.pattern + " RETURN " + carrier.variable + ".value AS value"
				result, err := survivor.Execute(ctx, query, nil)
				require.NoError(t, err, query)
				require.Equal(t, [][]interface{}{{int64(index + 7)}}, result.Rows, "deleting another namespace must preserve its exact decoded index name")
			})
		}
	}
}
