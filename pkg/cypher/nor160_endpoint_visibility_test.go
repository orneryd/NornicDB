package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR160 #1688: a Cypher traversal reaches an endpoint through the canonical
// query read, not through storage.RelationshipEndpointChecker.
//
// pkg/cypher/relationship_endpoint_projection.go (relationshipEndpointReader.read)
// reads every endpoint with getQueryNode, and the only remaining
// RelationshipEndpointVisible call site in this package is the forwarding
// wrapper pkg/cypher/transaction_storage_wrapper.go, which no production read
// calls: degree counts and labelled endpoint counts read the endpoint
// canonically. So a hidden endpoint is excluded here because the point read
// hides it, and this test records that coverage limit for the storage-level
// check rather than claiming a Cypher row leak.
func TestNOR160CypherTraversalMasksHiddenPendingEndpoint(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		ctx := t.Context()
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor160") // badger-backed: the production read path
		exec := NewStorageExecutor(store)

		_, err := exec.Execute(ctx, "CREATE (a:S {id: 1}), (b:E {id: 2}) CREATE (a)-[:R]->(b)", nil)
		require.NoError(t, err)
		endpoint, err := exec.Execute(ctx, "MATCH (b:E) RETURN b", nil)
		require.NoError(t, err)
		require.Len(t, endpoint.Rows, 1)
		endpointID := endpoint.Rows[0][0].(*storage.Node).ID

		count := func(query string) int64 {
			t.Helper()
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			require.Len(t, result.Rows, 1, query)
			value, ok := result.Rows[0][0].(int64)
			require.True(t, ok, "%s: expected a count, got %T", query, result.Rows[0][0])
			return value
		}
		const edgeOnly = "MATCH (a:S)-[r:R]->() RETURN count(r) AS c"
		const namedEndpoint = "MATCH (a:S)-[r:R]->(b:E) RETURN count(r) AS c"

		// The endpoint becomes a pending image of an explicit transaction.
		_, err = exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		t.Cleanup(func() {
			if exec.txContext != nil && exec.txContext.active {
				_, _ = exec.handleRollback(context.Background(), nil)
			}
		})
		_, err = exec.Execute(ctx, "MATCH (b:E) SET b.touched = true", nil)
		require.NoError(t, err)
		require.Equal(t, int64(1), count(edgeOnly), "the pending endpoint is visible before suppression")
		require.Equal(t, int64(1), count(namedEndpoint))

		require.NoError(t, store.SetManualSuppression(ctx, storage.StoredEntityNodeKind, string(endpointID), true))

		// Both forms exclude the hidden endpoint. The edge-only form still checks
		// the endpoint through the canonical read, so it cannot bypass the point
		// read either.
		require.Equal(t, int64(0), count(edgeOnly), "a hidden endpoint must not be counted")
		require.Equal(t, int64(0), count(namedEndpoint), "a hidden endpoint must not be counted")

		// The transaction capability Cypher exposes forwards to the transaction's
		// check under its namespace prefix; it must agree with the rows MATCH
		// returned, and once a read-time filter can hide the image it leaves the
		// answer to GetNode.
		require.NotNil(t, exec.txContext)
		require.NotNil(t, exec.txContext.storageWrapper)
		visible, answered := exec.txContext.storageWrapper.RelationshipEndpointVisible(endpointID)
		if answered {
			require.Equal(t, count(edgeOnly) > 0, visible, "the endpoint answer must agree with the rows MATCH returned")
		}

		// Shown again, the traversal counts the relationship.
		require.NoError(t, store.SetManualSuppression(ctx, storage.StoredEntityNodeKind, string(endpointID), false))
		require.Equal(t, int64(1), count(edgeOnly))
		require.Equal(t, int64(1), count(namedEndpoint))

		_, err = exec.Execute(ctx, "ROLLBACK", nil)
		require.NoError(t, err)
	})
}
