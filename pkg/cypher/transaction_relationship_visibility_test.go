package cypher

import (
	"context"
	"testing"

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
