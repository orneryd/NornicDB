package auth_test

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Author: Lane #1644 (Codex). The public auth transaction callback must
// expose the same pending node view to both point and batch consumers.
func TestFoundationAuthIssuedBatch(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	for _, namespace := range []string{"", "system"} {
		t.Run("namespace="+namespace, func(t *testing.T) {
			var engine storage.Engine = base
			id := storage.NodeID("system:pending")
			if namespace != "" {
				engine = storage.NewNamespacedEngine(base, namespace)
				id = "pending"
			}
			abort := errors.New("rollback auth batch schedule")
			err := auth.WithStoreTransaction(context.Background(), engine, func(issued storage.Engine) error {
				_, err := issued.CreateNode(&storage.Node{ID: id, Labels: []string{"AuthIssuedBatch"}})
				require.NoError(t, err)
				_, err = issued.GetNode(id)
				require.NoError(t, err, "pending point-read control")
				for _, contextual := range []bool{false, true} {
					var got map[storage.NodeID]*storage.Node
					if contextual {
						got, err = storage.BatchGetNodesWithContext(context.Background(), issued, []storage.NodeID{id})
					} else {
						got, err = issued.BatchGetNodes([]storage.NodeID{id})
					}
					if assert.NoError(t, err) {
						assert.Contains(t, got, id, "batch must read the pending auth view; contextual=%v", contextual)
					}
				}
				return abort
			})
			require.ErrorIs(t, err, abort)
			_, err = engine.GetNode(id)
			require.ErrorIs(t, err, storage.ErrNotFound, "the auth owner rolled back")
		})
	}
}
