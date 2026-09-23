package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRelationshipsCreatedWithTheirEndpointsAreVisibleInEveryTransactionMode(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		name := "auto_commit"
		if explicit {
			name = "explicit_transaction"
		}
		t.Run(name, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "relationship_visibility")
			executor := NewStorageExecutor(store)
			ctx := context.Background()

			if !explicit {
				_, err := executor.Execute(ctx, "CREATE (:Owner {id: 1})-[:HAS {weight: 5}]->(:Item {id: 2})", nil)
				require.NoError(t, err)
			}

			if explicit {
				_, err := executor.Execute(ctx, "CREATE (:Owner {id: 1})-[:HAS {weight: 5}]->(:Item {id: 2})", nil)
				require.NoError(t, err)
				_, err = executor.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() {
					if executor.txContext != nil && executor.txContext.active {
						_, _ = executor.Execute(ctx, "ROLLBACK", nil)
					}
				})
			}

			result, err := executor.Execute(ctx, "MATCH (:Owner)-[relationship:HAS]->(:Item) RETURN relationship.weight AS weight", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(5)}}, result.Rows)

			count, err := executor.Execute(ctx, "MATCH (:Owner)-[relationship:HAS]->(:Item) RETURN count(relationship) AS count", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, count.Rows)

			if explicit {
				_, err = executor.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		})
	}
}

func TestAcknowledgedRelationshipCreateIsVisibleToFollowingTransactions(t *testing.T) {
	base := newTestMemoryEngine(t)
	async := storage.NewAsyncEngine(base, &storage.AsyncEngineConfig{
		FlushInterval:    time.Hour,
		MaxNodeCacheSize: 1000,
		MaxEdgeCacheSize: 1000,
	})
	t.Cleanup(func() { require.NoError(t, async.Close()) })
	executor := NewStorageExecutor(storage.NewNamespacedEngine(async, "relationship_snapshot_visibility"))
	ctx := context.Background()

	created, err := executor.Execute(ctx,
		"CREATE (owner:Owner {id: 2})-[relationship:HAS {weight: 5}]->(item:Item {sku: 'a1'})", nil)
	require.NoError(t, err)
	require.Equal(t, 2, created.Stats.NodesCreated)
	require.Equal(t, 1, created.Stats.RelationshipsCreated)

	_, err = executor.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if executor.txContext != nil && executor.txContext.active {
			_, _ = executor.Execute(ctx, "ROLLBACK", nil)
		}
	})

	queries := []struct {
		name  string
		query string
		rows  [][]interface{}
	}{
		{
			name:  "typed endpoints and relationship",
			query: "MATCH (owner:Owner)-[relationship:HAS]->(item:Item) RETURN relationship.weight AS weight",
			rows:  [][]interface{}{{int64(5)}},
		},
		{
			name:  "untyped start endpoint",
			query: "MATCH (owner)-[relationship:HAS]->(item:Item) RETURN relationship.weight AS weight",
			rows:  [][]interface{}{{int64(5)}},
		},
		{
			name:  "untyped relationship",
			query: "MATCH (owner:Owner)-[relationship]->(item:Item) RETURN relationship.weight AS weight",
			rows:  [][]interface{}{{int64(5)}},
		},
		{
			name:  "untyped endpoints and relationship",
			query: "MATCH (owner)-[relationship]->(item) RETURN relationship.weight AS weight",
			rows:  [][]interface{}{{int64(5)}},
		},
		{
			name:  "relationship count with typed endpoints",
			query: "MATCH (:Owner)-->(:Item) RETURN count(*) AS count",
			rows:  [][]interface{}{{int64(1)}},
		},
		{
			name:  "typed relationship count",
			query: "MATCH ()-[relationship:HAS]->() RETURN count(relationship) AS count",
			rows:  [][]interface{}{{int64(1)}},
		},
	}
	for _, test := range queries {
		t.Run(test.name, func(t *testing.T) {
			result, queryErr := executor.Execute(ctx, test.query, nil)
			require.NoError(t, queryErr)
			require.Equal(t, test.rows, result.Rows)
		})
	}

	_, err = executor.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)

	updated, err := executor.Execute(ctx,
		"MATCH (:Owner)-[relationship:HAS]->(:Item) SET relationship.weight = relationship.weight + 1 RETURN relationship.weight AS weight", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(6)}}, updated.Rows)
}
