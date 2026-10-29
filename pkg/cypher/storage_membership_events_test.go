package cypher

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTransactionOwnershipViewPreservesEventPublisher(t *testing.T) {
	inner := storage.NewNamespacedEngine(newTestMemoryEngine(t), "events")
	view := &transactionOwnedStorage{Engine: inner, owner: &TransactionContext{active: true}}
	var created []storage.NodeID
	require.NoError(t, storage.SubscribeEvents(view, storage.EventHandlers{NodeCreated: func(node *storage.Node) { created = append(created, node.ID) }}))
	_, err := inner.CreateNode(&storage.Node{ID: "one", Labels: []string{"Event"}})
	require.NoError(t, err)
	require.Equal(t, []storage.NodeID{"one"}, created)
	require.Same(t, inner, view.GetInnerEngine())
	require.Same(t, view.owner, membershipOfStorage(view).transaction)
	hidden := &transactionOwnedStorage{Engine: struct{ storage.Engine }{Engine: inner}}
	require.ErrorIs(t, storage.SubscribeEvents(hidden, storage.EventHandlers{}), storage.ErrEventSubscriptionUnsupported)
}
